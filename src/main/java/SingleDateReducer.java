// [file name]: SingleDateReducer.java
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SingleDateReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String tradeTime = key.toString();

        // 累加各因子值
        double[] factorSums = new double[21];
        int[] factorCounts = new int[21];
        int stockCount = 0;

        for (Text value : values) {
            String valStr = value.toString();
            String[] parts = valStr.split("\\|");

            if (parts.length == 2) {
                String[] factors = parts[1].split(",");
                stockCount++;

                for (int i = 0; i < Math.min(factors.length, 20); i++) {
                    try {
                        double factorValue = Double.parseDouble(factors[i]);
                        factorSums[i+1] += factorValue;
                        factorCounts[i+1]++;
                    } catch (NumberFormatException e) {
                        // 忽略格式错误
                    }
                }
            }
        }

        // 计算每个因子的平均值
        StringBuilder result = new StringBuilder();
        result.append(tradeTime);

        for (int i = 1; i <= 20; i++) {
            double avg = (factorCounts[i] > 0) ?
                    factorSums[i] / factorCounts[i] : 0.0;
            result.append(String.format(",%.6f", avg));
        }

        context.write(new Text(result.toString()), new Text(""));
    }
}
