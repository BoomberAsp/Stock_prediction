// [file name]: OptimizedDateParallelDriver.java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import java.io.*;

/**
 * 优化的按日期并行Driver
 */
public class ParallelFactorDriver {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: OptimizedDateParallelDriver <input-base> <output-dir> <num-days-to-process>");
            System.exit(1);
        }

        String inputBase = args[0];
        String outputDir = args[1];
        int numDays = Integer.parseInt(args[2]);

        Configuration conf = new Configuration();
        configureForPseudoDistributed(conf);

        // 检测日期目录
        System.out.println("Detecting date directories...");
        String[] allDateDirs = detectDateDirectories(inputBase, conf);

        if (allDateDirs.length == 0) {
            System.err.println("No date directories found!");
            System.exit(1);
        }

        // 只处理指定数量的日期
        int daysToProcess = Math.min(numDays, allDateDirs.length);
        String[] dateDirs = new String[daysToProcess];
        System.arraycopy(allDateDirs, 0, dateDirs, 0, daysToProcess);

        System.out.println("Found " + allDateDirs.length + " date directories");
        System.out.println("Will process " + daysToProcess + " days: " +
                String.join(", ", dateDirs));

        // 按日期处理
        for (String date : dateDirs) {
            System.out.println("\n=== Processing date: " + date + " ===");

            boolean success = processDate(conf, inputBase, date, outputDir + "/" + date);
            if (!success) {
                System.err.println("Failed to process date: " + date);
                System.exit(1);
            }
        }

        // 合并结果
        System.out.println("\n=== Merging results ===");
        mergeResults(conf, outputDir, dateDirs);

        System.out.println("\n=== All dates processed successfully! ===");
    }

    private static void configureForPseudoDistributed(Configuration conf) {
        // 伪分布式优化配置
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.address", "localhost:8032");

        // 单节点优化：合理配置并行度
        conf.setInt("mapreduce.job.maps", 4);           // 4个Map任务
        conf.setInt("mapreduce.job.reduces", 2);        // 2个Reduce任务

        // 内存配置
        conf.setInt("mapreduce.map.memory.mb", 1024);
        conf.setInt("mapreduce.reduce.memory.mb", 2048);
        conf.set("mapreduce.map.java.opts", "-Xmx768m");
        conf.set("mapreduce.reduce.java.opts", "-Xmx1536m");

        // 性能优化
        conf.setInt("mapreduce.task.io.sort.mb", 200);
        conf.setInt("mapreduce.task.io.sort.factor", 50);
        conf.setBoolean("mapreduce.map.output.compress", true);
        conf.set("mapreduce.map.output.compress.codec",
                "org.apache.hadoop.io.compress.SnappyCodec");

        // 递归读取
        conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
    }

    private static String[] detectDateDirectories(String inputBase, Configuration conf)
            throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path basePath = new Path(inputBase);

        if (!fs.exists(basePath)) {
            throw new IOException("Input path not found: " + inputBase);
        }

        FileStatus[] items = fs.listStatus(basePath);
        java.util.ArrayList<String> dates = new java.util.ArrayList<>();

        for (FileStatus item : items) {
            if (item.isDirectory()) {
                String name = item.getPath().getName();
                // 匹配8位日期格式：20240102
                if (name.matches("\\d{8}") || name.matches("\\d{4}")) {
                    dates.add(name);
                }
            }
        }

        dates.sort(String::compareTo);
        return dates.toArray(new String[0]);
    }

    private static boolean processDate(Configuration conf, String inputBase,
                                       String date, String outputPath)
            throws Exception {

        Job job = Job.getInstance(conf, "FactorCalculation-" + date);
        job.setJarByClass(ParallelFactorDriver.class);

        // 设置输入路径：支持多种目录结构
        String[] inputPatterns = {
                inputBase + "/" + date + "/*/*.csv",          // 日期/股票/文件
                inputBase + "/" + date + "/*.csv",            // 日期/文件
                inputBase + "/*/" + date + "/*.csv"           // 股票/日期/文件
        };

        boolean foundInput = false;
        for (String pattern : inputPatterns) {
            try {
                FileStatus[] files = FileSystem.get(conf).globStatus(new Path(pattern));
                if (files != null && files.length > 0) {
                    FileInputFormat.addInputPath(job, new Path(pattern));
                    System.out.println("Using input pattern: " + pattern);
                    foundInput = true;
                    break;
                }
            } catch (Exception e) {
                // 继续尝试下一个模式
            }
        }

        if (!foundInput) {
            // 最后尝试：递归查找所有CSV文件
            String recursivePattern = inputBase + "/" + date + "/**/*.csv";
            FileInputFormat.addInputPath(job, new Path(recursivePattern));
            System.out.println("Using recursive pattern: " + recursivePattern);
        }

        // 设置输出路径
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // 设置Mapper和Reducer
        job.setMapperClass(SimplifiedFactorMapper.class);
        job.setReducerClass(SingleDateReducer.class);

        // 使用Combiner优化
        job.setCombinerClass(LocalAggregator.class);

        // 设置输入输出格式
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // 设置输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        long startTime = System.currentTimeMillis();
        boolean success = job.waitForCompletion(true);
        long endTime = System.currentTimeMillis();

        System.out.printf("Date %s processed in %.2f minutes\n",
                date, (endTime - startTime) / 60000.0);

        return success;
    }

    private static void mergeResults(Configuration conf, String outputDir,
                                     String[] dateDirs) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path finalOutput = new Path(outputDir + "/final_result.csv");

        if (fs.exists(finalOutput)) {
            fs.delete(finalOutput, false);
        }

        // 创建输出流
        FSDataOutputStream out = fs.create(finalOutput);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));

        // 写入表头
        writer.write("tradeTime");
        for (int i = 1; i <= 20; i++) {
            writer.write(String.format(",alpha_%d", i));
        }
        writer.newLine();

        // 合并所有日期的结果，按时间排序
        java.util.Map<String, String> allResults = new java.util.TreeMap<>();

        for (String date : dateDirs) {
            Path dateResultFile = new Path(outputDir + "/" + date + "/part-r-00000");

            if (fs.exists(dateResultFile)) {
                try (FSDataInputStream in = fs.open(dateResultFile)) {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                    String line;

                    while ((line = reader.readLine()) != null) {
                        String[] parts = line.split("\t");
                        if (parts.length == 2) {
                            // key: tradeTime, value: factor values
                            allResults.put(parts[0], parts[1]);
                        }
                    }
                }
            }
        }

        // 写入合并后的结果
        for (java.util.Map.Entry<String, String> entry : allResults.entrySet()) {
            writer.write(entry.getKey() + entry.getValue());
            writer.newLine();
        }

        writer.close();
        System.out.println("Final result saved to: " + finalOutput);
        System.out.println("Total records merged: " + allResults.size());
    }
}