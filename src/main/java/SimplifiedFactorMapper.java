import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 极速版 Mapper：移除了 split，使用手动解析 CSV
 */
public class SimplifiedFactorMapper extends Mapper<LongWritable, Text, Text, Text> {

    // 缓存：股票代码 -> 前一tick数据
    private Map<String, PreviousTickData> prevDataCache;

    // 复用 Hadoop 的 Text 对象，减少 GC
    private Text outKey = new Text();
    private Text outValue = new Text();

    // 统计
    private long processedCount = 0;
    private long validCount = 0;
    private long filteredByTimeCount = 0;

    // 预分配数组用于存储逗号位置，避免每行都 new
    private int[] commaIndices = new int[100];

    @Override
    protected void setup(Context context) {
        prevDataCache = new HashMap<>(500); // 沪深300股票
        System.out.println("SimplifiedFactorMapper (Fast-Parse Version) initialized");
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        processedCount++;
        String line = value.toString(); // 这里不 trim，为了保持索引准确，且 split 也是不 trim 的

        // === 1. 快速解析：扫描逗号位置 ===
        int commaCount = 0;
        int len = line.length();

        // 记录每一个逗号的索引
        for (int i = 0; i < len; i++) {
            if (line.charAt(i) == ',') {
                commaIndices[commaCount++] = i;
                // 我们只需要解析到前 40 列左右即可，不需要扫描整行
                if (commaCount >= 50) break;
            }
        }

        // 验证字段数量 (至少要有37个字段，即36个逗号)
        if (commaCount < 36) {
            return; // 格式错误或空行
        }

        // === 2. 提取关键字段 (Date, Time, Code) ===
        // 第0列: Date (0 到 第1个逗号)
        String tradingDay = line.substring(0, commaIndices[0]);

        // 第1列: Time (第1个逗号+1 到 第2个逗号)
        String timeStr = line.substring(commaIndices[0] + 1, commaIndices[1]);

        // 跳过表头
        if (Character.isLetter(tradingDay.charAt(0))) return;

        // === 3. 极速时间过滤 (使用字符串比较，不转 Long) ===
        // 过滤非交易时间（9:30:00 - 14:57:00）
        // 注意：这里根据你的逻辑，如果是 15:00:00 也要包含的话，可以调整
        if (timeStr.compareTo("093000") < 0 || timeStr.compareTo("145700") > 0) {
            filteredByTimeCount++;
            return;
        }

        // 第4列: StockCode (第4个逗号+1 到 第5个逗号)
        String stockCode = line.substring(commaIndices[3] + 1, commaIndices[4]);

        long tradeTime;
        try {
            tradeTime = Long.parseLong(timeStr);
        } catch (NumberFormatException e) {
            return;
        }

        // === 4. 提取数据 (使用优化的提取方法) ===
        TickData current = extractTickDataFast(line, commaIndices, tradeTime);
        if (current == null) return; // 解析失败

        // === 5. 缓存与计算 (逻辑保持不变) ===
        String cacheKey = stockCode + "_" + tradingDay;
        PreviousTickData prevData = prevDataCache.get(cacheKey);

        // 计算因子
        double[] factors = calculateFactorsWithHistory(current, prevData);

        // === 6. 构造输出 ===
        // Key: 日期_时间 (让 Hadoop 自动排序)
        outKey.set(tradingDay + "_" + timeStr);

        // Value: 1|因子1,因子2...
        StringBuilder factorStr = new StringBuilder();
        factorStr.append("1|");

        for (int i = 1; i <= 20; i++) {
            factorStr.append(String.format("%.6f", factors[i]));
            if (i < 20) factorStr.append(",");
        }

        outValue.set(factorStr.toString());
        context.write(outKey, outValue);
        validCount++;

        // 更新缓存
        updateCache(cacheKey, current, tradeTime);
    }

    /**
     * 极速提取 TickData (直接从 line 截取，不使用 split 数组)
     */
    private TickData extractTickDataFast(String line, int[] cIdx, long tradeTimeVal) {
        TickData data = new TickData();
        data.tradeTime = tradeTimeVal;

        try {
            // Helper function to get substring between commas
            // Col N is between cIdx[N-1] and cIdx[N]

            // last: Col 8
            data.last = parseLongFast(line, cIdx[7] + 1, cIdx[8]);

            // tBidVol: Col 12
            data.tBidVol = parseLongFast(line, cIdx[11] + 1, cIdx[12]);
            // tAskVol: Col 13
            data.tAskVol = parseLongFast(line, cIdx[12] + 1, cIdx[13]);

            // 买卖一档 (Col 17-20)
            data.bp1 = parseLongFast(line, cIdx[16] + 1, cIdx[17]);
            data.bv1 = parseLongFast(line, cIdx[17] + 1, cIdx[18]);
            data.ap1 = parseLongFast(line, cIdx[18] + 1, cIdx[19]);
            data.av1 = parseLongFast(line, cIdx[19] + 1, cIdx[20]);

            // 前5档买卖价量
            for (int i = 1; i <= 5; i++) {
                // Base Index for Level i (Level 1 starts at 17)
                // Col Index logic: 17 + (i-1)*4
                // Array logic: cIdx[base-1] to cIdx[base]

                int baseCol = 17 + (i - 1) * 4;

                // 确保没有越界 (虽然之前检查过 commaCount)
                // bp[i]
                data.bp[i] = parseLongFast(line, cIdx[baseCol-1] + 1, cIdx[baseCol]);
                // bv[i]
                data.bv[i] = parseLongFast(line, cIdx[baseCol] + 1, cIdx[baseCol+1]);
                // ap[i]
                data.ap[i] = parseLongFast(line, cIdx[baseCol+1] + 1, cIdx[baseCol+2]);
                // av[i]
                data.av[i] = parseLongFast(line, cIdx[baseCol+2] + 1, cIdx[baseCol+3]);
            }
        } catch (Exception e) {
            return null; // 遇到解析错误直接丢弃该行
        }
        return data;
    }

    /**
     * 快速解析 Long，替代 Long.parseLong
     * 处理空串和包含小数点的整数 (如 "100.0")
     */
    private long parseLongFast(String line, int start, int end) {
        if (start >= end) return 0L;

        // 检查是否为空字符串
        boolean isEmpty = true;
        for(int i=start; i<end; i++) {
            if(line.charAt(i) != ' ') {
                isEmpty = false;
                break;
            }
        }
        if(isEmpty) return 0L;

        try {
            String sub = line.substring(start, end).trim();
            if (sub.isEmpty()) return 0L;
            if (sub.contains(".")) {
                return (long) Double.parseDouble(sub);
            }
            return Long.parseLong(sub);
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    // === 下面所有的计算逻辑保持完全不变 ===

    private double[] calculateFactorsWithHistory(TickData current, PreviousTickData prev) {
        double[] factors = new double[21];

        // 因子1-10：不需要历史数据
        factors[1] = calculateFactor1(current);   // 最优价差
        factors[2] = calculateFactor2(current);   // 相对价差
        factors[3] = calculateFactor3(current);   // 中间价
        factors[4] = calculateFactor4(current);   // 买一不平衡
        factors[5] = calculateFactor5(current);   // 多档不平衡
        factors[6] = calculateFactor6(current);   // 买方深度
        factors[7] = calculateFactor7(current);   // 卖方深度
        factors[8] = calculateFactor8(factors[6], factors[7]); // 深度差
        factors[9] = calculateFactor9(factors[6], factors[7]); // 深度比
        factors[10] = calculateFactor10(current); // 买卖量平衡指数

        // 因子11-14：加权价格
        double vwapBid = calculateFactor11(current);
        double vwapAsk = calculateFactor12(current);
        factors[11] = vwapBid;
        factors[12] = vwapAsk;
        factors[13] = calculateFactor13(current); // 加权中间价
        factors[14] = vwapAsk - vwapBid;          // 加权价差

        // 因子15-16：密度和不对称度
        factors[15] = calculateFactor15(current); // 买卖密度差
        factors[16] = calculateFactor16(current); // 买卖不对称度

        // 因子17-19：需要历史数据
        factors[17] = calculateFactor17(current, prev); // 最优价变动
        factors[18] = calculateFactor18(current, prev); // 中间价变动
        factors[19] = calculateFactor19(current, prev); // 深度比变动

        // 因子20：价压指标
        factors[20] = calculateFactor20(current);

        return factors;
    }

    // 因子计算辅助方法
    private double calculateFactor1(TickData data) {
        return data.ap1 - data.bp1;
    }

    private double calculateFactor2(TickData data) {
        double spread = data.ap1 - data.bp1;
        double mid = (data.ap1 + data.bp1) / 2.0;
        return spread / (mid + 1e-7);
    }

    private double calculateFactor3(TickData data) {
        return (data.ap1 + data.bp1) / 2.0;
    }

    private double calculateFactor4(TickData data) {
        return (data.bv1 - data.av1) / (double)(data.bv1 + data.av1 + 1e-7);
    }

    private double calculateFactor5(TickData data) {
        double bidSum = 0, askSum = 0;
        for (int i = 1; i <= 5; i++) {
            bidSum += data.bv[i];
            askSum += data.av[i];
        }
        return (bidSum - askSum) / (bidSum + askSum + 1e-7);
    }

    private double calculateFactor6(TickData data) {
        double sum = 0;
        for (int i = 1; i <= 5; i++) {
            sum += data.bv[i];
        }
        return sum;
    }

    private double calculateFactor7(TickData data) {
        double sum = 0;
        for (int i = 1; i <= 5; i++) {
            sum += data.av[i];
        }
        return sum;
    }

    private double calculateFactor8(double bidDepth, double askDepth) {
        return bidDepth - askDepth;
    }

    private double calculateFactor9(double bidDepth, double askDepth) {
        return bidDepth / (askDepth + 1e-7);
    }

    private double calculateFactor10(TickData data) {
        return (data.tBidVol - data.tAskVol) / (double)(data.tBidVol + data.tAskVol + 1e-7);
    }

    private double calculateFactor11(TickData data) {
        double weightedSum = 0;
        double totalVol = 0;
        for (int i = 1; i <= 5; i++) {
            weightedSum += data.bp[i] * data.bv[i];
            totalVol += data.bv[i];
        }
        return weightedSum / (totalVol + 1e-7);
    }

    private double calculateFactor12(TickData data) {
        double weightedSum = 0;
        double totalVol = 0;
        for (int i = 1; i <= 5; i++) {
            weightedSum += data.ap[i] * data.av[i];
            totalVol += data.av[i];
        }
        return weightedSum / (totalVol + 1e-7);
    }

    private double calculateFactor13(TickData data) {
        double bidWeighted = 0, askWeighted = 0;
        double bidVol = 0, askVol = 0;
        for (int i = 1; i <= 5; i++) {
            bidWeighted += data.bp[i] * data.bv[i];
            askWeighted += data.ap[i] * data.av[i];
            bidVol += data.bv[i];
            askVol += data.av[i];
        }
        return (bidWeighted + askWeighted) / (bidVol + askVol + 1e-7);
    }

    private double calculateFactor15(TickData data) {
        double bidSum = 0, askSum = 0;
        for (int i = 1; i <= 5; i++) {
            bidSum += data.bv[i];
            askSum += data.av[i];
        }
        return (bidSum - askSum) / 5.0;
    }

    private double calculateFactor16(TickData data) {
        double weightedBid = 0, weightedAsk = 0;
        for (int i = 1; i <= 5; i++) {
            double weight = 1.0 / i; // 按档位衰减
            weightedBid += data.bv[i] * weight;
            weightedAsk += data.av[i] * weight;
        }
        return (weightedBid - weightedAsk) / (weightedBid + weightedAsk + 1e-7);
    }

    private double calculateFactor17(TickData current, PreviousTickData prev) {
        if (prev == null) return 0.0;
        return current.ap1 - prev.getAp1();
    }

    private double calculateFactor18(TickData current, PreviousTickData prev) {
        if (prev == null) return 0.0;
        double currentMid = (current.ap1 + current.bp1) / 2.0;
        double prevMid = (prev.getAp1() + prev.getBp1()) / 2.0;
        return currentMid - prevMid;
    }

    private double calculateFactor19(TickData current, PreviousTickData prev) {
        if (prev == null) return 0.0;

        // 当前深度比
        double currentBidDepth = calculateFactor6(current);
        double currentAskDepth = calculateFactor7(current);
        double currentRatio = currentBidDepth / (currentAskDepth + 1e-7);

        // 历史深度比
        double prevBidDepth = 0, prevAskDepth = 0;
        for (int i = 0; i < 5; i++) {
            prevBidDepth += prev.getBv(i);
            prevAskDepth += prev.getAv(i);
        }
        double prevRatio = prevBidDepth / (prevAskDepth + 1e-7);

        return currentRatio - prevRatio;
    }

    private double calculateFactor20(TickData data) {
        double spread = data.ap1 - data.bp1;
        double totalDepth = 0;
        for (int i = 1; i <= 5; i++) {
            totalDepth += data.bv[i] + data.av[i];
        }
        return spread / (totalDepth + 1e-7);
    }

    /**
     * 更新缓存
     */
    private void updateCache(String cacheKey, TickData current, long tradeTime) {
        long[] bvArray = new long[5];
        long[] avArray = new long[5];

        // 复制前5档数据
        for (int i = 0; i < 5; i++) {
            bvArray[i] = current.bv[i + 1];
            avArray[i] = current.av[i + 1];
        }

        PreviousTickData prevData = new PreviousTickData(
                tradeTime, current.ap1, current.bp1, bvArray, avArray);

        prevDataCache.put(cacheKey, prevData);

        // 简单缓存清理（如果缓存过大）
        if (prevDataCache.size() > 1000) {
            // 保留最近使用的500个
            if (prevDataCache.size() > 500) {
                prevDataCache.keySet().removeIf(key -> Math.random() < 0.5);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // 关闭不必要的日志，减少IO
    }
}