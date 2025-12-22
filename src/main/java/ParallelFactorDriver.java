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
        System.out.println("=== 🏆 Running V8 Pro: The Champion Tune ===");

        if (args.length < 2) {
            System.err.println("Usage: ParallelFactorDriver <hdfs-input> <local-linux-output> [num-days]");
            System.exit(1);
        }

        String inputPathStr = args[0];
        String localLinuxOutputDir = args[1];
        long start_time = System.currentTimeMillis();

        Configuration conf = new Configuration();

        // 1. [基础] 递归读取 + JVM 重用
        conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
        conf.set("mapreduce.job.jvm.numtasks", "-1");

        // 2. [微调] 关闭推测执行 (单机环境不需要猜测，节省 Overhead)
        conf.setBoolean("mapreduce.map.speculative", false);
        conf.setBoolean("mapreduce.reduce.speculative", false);

        // 3. [保留] V8 的核心内存配置 (这是最快的组合)
        conf.set("mapreduce.task.io.sort.mb", "256");
        conf.setFloat("mapreduce.map.sort.spill.percent", 0.95f); // 稍微激进一点，95%再写盘

        // 4. [微调] 给 Reducer 更大的 Buffer
        // 既然是单 Reducer，让它尽量在内存里 Merge 数据
        conf.setFloat("mapreduce.reduce.input.buffer.percent", 0.90f);

        // 5. 内存分配
        conf.set("mapreduce.map.memory.mb", "1024");
        conf.set("mapreduce.reduce.memory.mb", "2048"); // Reducer 内存给足

        FileSystem hdfs = FileSystem.get(conf);
        Path inputRoot = new Path(inputPathStr);
        if (!hdfs.exists(inputRoot)) {
            System.exit(1);
        }

        String hdfsTempPath = "/tmp/stock_v8_pro_" + System.currentTimeMillis();

        Job job = Job.getInstance(conf, "Stock-V8-Pro");
        job.setJarByClass(ParallelFactorDriver.class);

        FileInputFormat.addInputPath(job, inputRoot);
        FileOutputFormat.setOutputPath(job, new Path(hdfsTempPath));

        job.setMapperClass(SimplifiedFactorMapper.class);
        // job.setCombinerClass(...) // 确认移除 Combiner

        job.setReducerClass(SingleDateReducer.class);
        job.setNumReduceTasks(1); // 保持单 Reducer

        job.setInputFormatClass(CombineTextInputFormat.class);

        // 6. [保留] 64MB 切片 (经测试这是最佳并行度)
        CombineTextInputFormat.setMaxInputSplitSize(job, 64 * 1024 * 1024);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        if (!job.waitForCompletion(true)) {
            System.exit(1);
        }

        long hadoop_end = System.currentTimeMillis();
        System.out.printf(">>> Hadoop Phase: %.2f sec\n", (hadoop_end - start_time) / 1000.0);

        // 7. [微调] 优化的本地写入逻辑
        splitAndSaveToLocal(conf, hdfsTempPath, localLinuxOutputDir);

        long total_time = System.currentTimeMillis() - start_time;
        System.out.printf(">>> Total Time: %d ms (%.2f sec)\n", total_time, total_time / 1000.0);

        hdfs.delete(new Path(hdfsTempPath), true);
        System.out.println("=== Mission Complete! ===");
    }

    // 优化的结果保存方法：增大缓冲区
    private static void splitAndSaveToLocal(Configuration conf, String hdfsOutput, String localOutDir) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        FileSystem localFs = FileSystem.getLocal(conf).getRaw();
        Path localOutPath = new Path(localOutDir);
        if (!localFs.exists(localOutPath)) localFs.mkdirs(localOutPath);

        StringBuilder headerBuilder = new StringBuilder("tradeTime");
        for (int i = 1; i <= 20; i++) headerBuilder.append(String.format(",alpha_%d", i));
        String header = headerBuilder.toString();

        FileStatus[] resultFiles = hdfs.listStatus(new Path(hdfsOutput), path -> path.getName().startsWith("part-r-"));
        if (resultFiles.length == 0) return;

        System.out.println("⬇️  Merging results with High-Speed Buffer...");
        Map<String, BufferedWriter> writers = new HashMap<>();

        // 使用 64KB 的读写缓冲区 (默认是 8KB)
        int bufferSize = 64 * 1024;

        for (FileStatus file : resultFiles) {
            try (FSDataInputStream in = hdfs.open(file.getPath());
                 BufferedReader reader = new BufferedReader(new InputStreamReader(in), bufferSize)) {

                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;

                    int underscoreIndex = line.indexOf('_');
                    if (underscoreIndex == -1) continue;

                    String dateStr = line.substring(0, underscoreIndex);
                    // 快速替换
                    String csvLine = line.substring(underscoreIndex + 1).replace('\t', ',');
                    String shortName = dateStr.length() >= 4 ? dateStr.substring(dateStr.length() - 4) : dateStr;

                    BufferedWriter writer = writers.get(shortName);
                    if (writer == null) {
                        Path targetFile = new Path(localOutDir + "/" + shortName + ".csv");
                        OutputStream os = localFs.create(targetFile, true);
                        writer = new BufferedWriter(new OutputStreamWriter(os), bufferSize);
                        writer.write(header);
                        writer.newLine();
                        writers.put(shortName, writer);
                    }
                    writer.write(csvLine);
                    writer.newLine();
                }
            }
        }
        for (BufferedWriter w : writers.values()) {
            w.flush(); w.close();
        }
    }
}