// [file name]: LocalAggregator.java
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class LocalAggregator extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // 本地聚合：合并相同tradeTime的数据
        Map<String, StringBuilder> stockFactors = new HashMap<>();

        for (Text value : values) {
            String valStr = value.toString();
            String[] parts = valStr.split("\\|");

            if (parts.length == 2) {
                stockFactors.put(parts[0], new StringBuilder(parts[1]));
            }
        }

        // 重新输出（这里简化处理，实际应该做本地聚合）
        for (Map.Entry<String, StringBuilder> entry : stockFactors.entrySet()) {
            String output = entry.getKey() + "|" + entry.getValue();
            context.write(key, new Text(output));
        }
    }
}