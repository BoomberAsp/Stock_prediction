import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 主驱动类
 */
public class FactorCalculationDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: FactorCalculationDriver <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        // 设置缓存相关参数
        conf.setInt("cache.max.size", 500);
        conf.setBoolean("mapreduce.map.speculative", false);  // 关闭推测执行，避免重复计算
        conf.setBoolean("mapreduce.reduce.speculative", false);

        // 设置压缩（可选，提高Shuffle效率）
        conf.setBoolean("mapreduce.map.output.compress", true);
        conf.set("mapreduce.map.output.compress.codec",
                "org.apache.hadoop.io.compress.SnappyCodec");

        Job job = Job.getInstance(conf, "High-Frequency Factor Calculation");
        job.setJarByClass(FactorCalculationDriver.class);

        // 设置Mapper
        job.setMapperClass(FactorCalculatorMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 设置Partitioner
        job.setPartitionerClass(TimeFactorPartitioner.class);

        // 设置Reducer
        job.setReducerClass(FactorAverageReducer.class); // 需要实现
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // 设置Combiner（可选，可以显著减少Shuffle数据量）
        job.setCombinerClass(FactorCombiner.class); // 需要实现

        // 设置Reducer数量（根据时刻数量优化）
        // 5天 × 6.5小时 × 1200个3秒时刻 = 39000个时刻
        // 每个Reducer处理约400个时刻的数据
        int numReducers = Math.max(1, Math.min(100, Integer.parseInt(args[2])));
        job.setNumReduceTasks(numReducers);

        // 设置输入输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交作业
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}