// [file name]: SingleDateReducer.java
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import java.util.HashMap;
import java.util.Map;

public class SingleDateReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        //String tradeTime = key.toString();

        // 累加各因子值
        double[] finalSums = new double[21];
        long finalCount = 0;

        for (Text value : values) {
            String valStr = value.toString();
            String[] parts = valStr.split("\\|");

            if (parts.length == 2) {
                // 1. 解析样本数量
                long count = Long.parseLong(parts[0]);
                finalCount += count;
                // 2. 解析因子总和
                String[] factorStrList = parts[1].split(",");
                for (int i = 0; i < factorStrList.length && i < 20; i++) {
                    try {
                        // 直接累加传过来的“和”
                        double sumVal = Double.parseDouble(factorStrList[i]);
                        finalSums[i+1] += sumVal;
                    } catch (NumberFormatException e) {
                        // 忽略格式错误
                    }
                }
            }
        }

        // 如果没有有效数据，直接跳过
        if (finalCount == 0) return;

        // 3. 计算平均值并构建输出字符串
        StringBuilder result = new StringBuilder();
        result.append(key.toString());
        // 输出格式: ,avg1,avg2... (注意最前面有个逗号)
        // 这样 Driver 读取后，Key(Time) + Value(,avg...) 拼起来就是完整的 CSV 行
        for (int i = 1; i <= 20; i++) {
            double avg = finalSums[i] / finalCount;
            result.append(String.format(",%.6f", avg));
        }

        context.write(new Text(result.toString()), new Text(""));
    }
}
