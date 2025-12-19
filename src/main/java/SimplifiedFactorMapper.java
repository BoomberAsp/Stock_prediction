// [file name]: SixDigitTimeFactorMapper.java
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 专门处理6位时间格式的因子计算Mapper
 */
public class SimplifiedFactorMapper extends Mapper<LongWritable, Text, Text, Text> {

    // 缓存：股票代码 -> 前一tick数据
    private Map<String, PreviousTickData> prevDataCache;

    // 统计
    private long processedCount = 0;
    private long validCount = 0;
    private long filteredByTimeCount = 0;

    @Override
    protected void setup(Context context) {
        prevDataCache = new HashMap<>(500); // 沪深300股票
        System.out.println("SixDigitTimeFactorMapper initialized");
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        processedCount++;

        // 进度报告
        if (processedCount % 50000 == 0) {
            System.out.printf("Processed: %,d, Valid: %,d, Filtered by time: %,d\n",
                    processedCount, validCount, filteredByTimeCount);
        }

        String line = value.toString().trim();

        // 跳过表头
        if (line.startsWith("tradingDay") || line.startsWith("tradeTime") || line.isEmpty()) {
            return;
        }

        String[] fields = line.split(",");

        // 验证字段数量
        if (fields.length < 37) {
            if (processedCount <= 10) {
                System.out.println("Warning: Skipping line with " + fields.length + " fields");
            }
            return;
        }

        // 解析字段
        String tradingDay = fields[0];
        long tradeTime;
        String stockCode;

        try {
            tradeTime = Long.parseLong(fields[1]);
            stockCode = fields[4];
        } catch (NumberFormatException e) {
            return;
        }

        // 过滤非交易时间（9:30:00 - 15:00:00）
        if (!FixedTimeParser.isTradingTime(tradeTime)) {
            filteredByTimeCount++;
            return;
        }

        // 提取数据
        TickData current = extractTickData(fields);

        // 构建缓存键：股票代码 + 交易日
        String cacheKey = stockCode + "_" + tradingDay;

        // 获取历史数据
        PreviousTickData prevData = prevDataCache.get(cacheKey);

        // 计算因子（包含需要历史数据的因子）
        double[] factors = calculateFactorsWithHistory(current, prevData);

        // 输出：tradeTime -> stockCode|factor1,...,factor20
        String outputKey = String.valueOf(tradeTime);
        StringBuilder factorStr = new StringBuilder();
        factorStr.append(stockCode).append("|");

        for (int i = 1; i <= 20; i++) {
            factorStr.append(String.format("%.6f", factors[i]));
            if (i < 20) factorStr.append(",");
        }

        context.write(new Text(outputKey), new Text(factorStr.toString()));
        validCount++;

        // 更新缓存：保存当前数据作为下一时刻的历史数据
        updateCache(cacheKey, current, tradeTime);
    }

    /**
     * 提取tick数据
     */
    private TickData extractTickData(String[] fields) {
        TickData data = new TickData();

        try {
            // 基础字段
            data.tradeTime = Long.parseLong(fields[1]);
            data.last = Long.parseLong(fields[8]);
            data.tBidVol = Long.parseLong(fields[12]);
            data.tAskVol = Long.parseLong(fields[13]);

            // 买卖一档
            data.bp1 = Long.parseLong(fields[17]);
            data.bv1 = Long.parseLong(fields[18]);
            data.ap1 = Long.parseLong(fields[19]);
            data.av1 = Long.parseLong(fields[20]);

            // 前5档买卖价量
            for (int i = 1; i <= 5; i++) {
                int baseIdx = 17 + (i-1) * 4;
                if (baseIdx + 3 < fields.length) {
                    data.bp[i] = parseLongSafe(fields[baseIdx]);
                    data.bv[i] = parseLongSafe(fields[baseIdx + 1]);
                    data.ap[i] = parseLongSafe(fields[baseIdx + 2]);
                    data.av[i] = parseLongSafe(fields[baseIdx + 3]);
                }
            }

        } catch (NumberFormatException e) {
            // 记录错误但不中断处理
            if (processedCount <= 100) {
                System.err.println("Parse error at record " + processedCount + ": " + e.getMessage());
            }
        }

        return data;
    }

    private long parseLongSafe(String str) {
        try {
            return Long.parseLong(str);
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    /**
     * 计算所有因子（包含需要历史数据的因子）
     */
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
        System.out.println("\n=== Mapper Summary ===");
        System.out.println("Total processed: " + processedCount);
        System.out.println("Valid records: " + validCount);
        System.out.println("Filtered by time: " + filteredByTimeCount);
        System.out.println("Cache size: " + prevDataCache.size());
        System.out.println("===================\n");
    }
}