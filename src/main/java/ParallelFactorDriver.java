import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import java.io.*;
import java.util.*;

public class ParallelFactorDriver {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: ParallelFactorDriver <hdfs-input> <local-linux-output> [num-days]");
            System.exit(1);
        }

        String inputPathStr = args[0];
        String localLinuxOutputDir = args[1];
        long start_time = System.currentTimeMillis();
        int numDays = args.length > 2 ? Integer.parseInt(args[2]) : 999;

        Configuration conf = new Configuration();

        // [优化1] 开启 JVM 重用
        conf.set("mapreduce.job.jvm.numtasks", "-1");

        // [优化2] 开启 Uber 模式 (极速模式，针对小文件和小集群)
        conf.setBoolean("mapreduce.job.ubertask.enable", true);
        conf.setFloat("mapreduce.job.ubertask.maxmaps", 2000);
        conf.setFloat("mapreduce.job.ubertask.maxreduces", 1);

        configureForPseudoDistributed(conf);

        FileSystem hdfs = FileSystem.get(conf);

        // 1. 检查 HDFS 输入目录
        String[] allDateDirs = detectDateDirectories(hdfs, inputPathStr);
        if (allDateDirs.length == 0) {
            System.err.println("Error: No date directories found in " + inputPathStr);
            System.exit(1);
        }

        int daysToProcess = Math.min(numDays, allDateDirs.length);
        String[] dateDirs = new String[daysToProcess];
        System.arraycopy(allDateDirs, 0, dateDirs, 0, daysToProcess);

        // 2. 设置 HDFS 临时目录
        String hdfsTempPath = "/tmp/stock_all_" + System.currentTimeMillis();
        System.out.println("Processing " + daysToProcess + " days in ONE Job. Temp Output: " + hdfsTempPath);

        // 3. 运行 单个 MapReduce Job 处理所有日期
        if (!runAllDatesJob(conf, inputPathStr, dateDirs, hdfsTempPath)) {
            System.err.println("Job Failed");
            System.exit(1);
        }

        // 4. 读取大结果文件 -> 拆分 -> 写入 Linux 本地
        splitAndSaveToLocal(conf, hdfsTempPath, localLinuxOutputDir);

        long time_spent = System.currentTimeMillis() - start_time;
        System.out.printf("Total Time: %d ms (approx %.2f sec)\n", time_spent, time_spent / 1000.0);

        // 5. 清理 HDFS 临时文件
        hdfs.delete(new Path(hdfsTempPath), true);

        System.out.println("\n=== Mission Complete! Files saved to: " + localLinuxOutputDir + " ===");
    }

    private static void configureForPseudoDistributed(Configuration conf) {
        conf.set("mapreduce.map.memory.mb", "1024");
        conf.set("mapreduce.reduce.memory.mb", "2048");
        conf.setBoolean("mapreduce.map.output.compress", true);
        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
    }

    private static String[] detectDateDirectories(FileSystem fs, String inputBase) throws IOException {
        Path basePath = new Path(inputBase);
        if (!fs.exists(basePath)) {
            throw new IOException("Input path not found on HDFS: " + inputBase);
        }
        FileStatus[] items = fs.listStatus(basePath);
        List<String> dates = new ArrayList<>();
        for (FileStatus item : items) {
            if (item.isDirectory()) {
                String name = item.getPath().getName();
                if (name.matches("\\d{4}") || name.matches("\\d{8}")) {
                    dates.add(name);
                }
            }
        }
        Collections.sort(dates);
        return dates.toArray(new String[0]);
    }

    private static boolean runAllDatesJob(Configuration conf, String inputBase, String[] dates, String outputPath) throws Exception {
        Job job = Job.getInstance(conf, "All-Dates-Factor-Calc");
        job.setJarByClass(ParallelFactorDriver.class);

        // 循环添加所有日期文件夹作为输入
        for (String date : dates) {
            Path p = new Path(inputBase + "/" + date);
            FileInputFormat.addInputPath(job, p);
        }

        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapperClass(SimplifiedFactorMapper.class);
        job.setCombinerClass(LocalAggregator.class);
        job.setReducerClass(SingleDateReducer.class);

        job.setInputFormatClass(CombineTextInputFormat.class);
        // 稍微调大 Split Size 以减少 Map 数量
        CombineTextInputFormat.setMaxInputSplitSize(job, 256 * 1024 * 1024);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true);
    }

    // === [核心修改] 结果拆分逻辑 ===
    private static void splitAndSaveToLocal(Configuration conf, String hdfsOutput, String localOutDir) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        FileSystem localFs = FileSystem.getLocal(conf).getRaw(); // 无CRC

        Path localOutPath = new Path(localOutDir);
        if (!localFs.exists(localOutPath)) {
            localFs.mkdirs(localOutPath);
        }

        StringBuilder headerBuilder = new StringBuilder("tradeTime");
        for (int i = 1; i <= 20; i++) headerBuilder.append(String.format(",alpha_%d", i));
        String header = headerBuilder.toString();

        Path sourceFile = new Path(hdfsOutput + "/part-r-00000");
        if (!hdfs.exists(sourceFile)) {
            System.err.println("Error: Output file not generated");
            return;
        }

        System.out.println("Splitting results and writing to local disk...");

        Map<String, BufferedWriter> writers = new HashMap<>();

        try (FSDataInputStream in = hdfs.open(sourceFile);
             BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {

            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                // Mapper输出的Key是 "20240102_093000"
                // Hadoop默认输出分隔符是 Tab (\t) 而不是逗号
                // 格式大概是: 20240102_093000 \t 1|0.1,0.2... (如果Reducer直接输出value)
                // 或者: 20240102_093000 \t 0.1,0.2... (如果Reducer处理过)

                // 1. 找下划线 (区分日期)
                int underscoreIndex = line.indexOf('_');
                if (underscoreIndex == -1) continue;

                // 2. 提取日期
                String dateStr = line.substring(0, underscoreIndex);

                // 3. 提取 "093000 \t 0.1,0.2..."
                String remaining = line.substring(underscoreIndex + 1);

                // 4. [关键] 将 Hadoop 的 Tab 分隔符替换为逗号，变成 CSV 格式
                // 结果变成 "093000,0.1,0.2..."
                String csvLine = remaining.replace('\t', ',');

                // 生成文件名 (0102.csv)
                String shortName = dateStr.length() >= 4 ? dateStr.substring(dateStr.length() - 4) : dateStr;

                BufferedWriter writer = writers.get(shortName);
                if (writer == null) {
                    Path targetFile = new Path(localOutDir + "/" + shortName + ".csv");
                    OutputStream os = localFs.create(targetFile, true);
                    writer = new BufferedWriter(new OutputStreamWriter(os));
                    writer.write(header);
                    writer.newLine();
                    writers.put(shortName, writer);
                    System.out.println("Writing file: " + shortName + ".csv");
                }

                writer.write(csvLine);
                writer.newLine();
            }
        }

        for (BufferedWriter w : writers.values()) {
            w.flush();
            w.close();
        }
    }
}