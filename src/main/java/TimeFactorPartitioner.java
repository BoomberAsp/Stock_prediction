import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 按交易时间分区的Partitioner
 */
public class TimeFactorPartitioner extends Partitioner<Text, Text> {
    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
        // Key格式: "tradeTime_factorId"
        String[] parts = key.toString().split("_");
        long time = Long.parseLong(parts[0]);

        // 确保同一时刻的所有因子数据去往同一个Reducer
        // 使用时间的哈希值确保均匀分布
        int partition = Math.abs(String.valueOf(time).hashCode()) % numPartitions;

        // 确保分区号在有效范围内
        return Math.max(0, Math.min(partition, numPartitions - 1));
    }
}
