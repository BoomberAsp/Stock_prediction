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

        // 用来存20个因子的累加值 (索引1-20)
        double[] sumFactors = new double[21];
        long totalCount = 0;

        for (Text value : values) {
            String valStr = value.toString();
            // 输入格式可能是: "1|f1,f2..." (来自Mapper)
            // 也可能是: "N|sum1,sum2..." (来自之前的Combiner合并)
            String[] parts = valStr.split("\\|");

            if (parts.length == 2) {
                long count = Long.parseLong(parts[0]); // 取出样本数
                String[] factors = parts[1].split(",");

                totalCount += count; // 累加样本数

                // 累加因子值
                for (int i = 0; i < factors.length && i < 20; i++) {
                    // factors[0] 是第一个因子，存入 sumFactors[1]
                    sumFactors[i+1] += Double.parseDouble(factors[i]);
                }
            }
        }

        // 输出格式: TotalCount|Sum1,Sum2...
        // 这样 Reducer 拿到后只要做除法就可以了
        StringBuilder sb = new StringBuilder();
        sb.append(totalCount).append("|");

        for (int i = 1; i <= 20; i++) {
            sb.append(String.format("%.6f", sumFactors[i]));
            if (i < 20) sb.append(",");
        }

        context.write(key, new Text(sb.toString()));
    }
}