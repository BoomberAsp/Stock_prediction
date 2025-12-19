import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * 完整因子计算Mapper
 */
public class ParallelFactorMapper extends Mapper<LongWritable, Text, Text, Text> {

    // 按股票缓存前一时刻数据
    private Map<String, PreviousTickData> tickCache;
    private MultipleOutputs<Text, Text> mos;

    // 性能统计
    private long recordsProcessed = 0;
    private long startTime;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        tickCache = new HashMap<>(3000); // 缓存3000只股票
        mos = new MultipleOutputs<>(context);
        startTime = System.currentTimeMillis();
    }

    private boolean isTradingTime(long tradeTime) {
        // 将时间格式化为6位字符串（补零）
        String timeStr = String.format("%06d", tradeTime);

        if (timeStr.length() < 6) {
            // 如果时间不是6位数字，尝试其他格式
            System.err.println("警告：非标准时间格式: " + tradeTime + " -> " + timeStr);
            return false;
        }

        // 提取小时和分钟
        int hour = Integer.parseInt(timeStr.substring(0, 2));
        int minute = Integer.parseInt(timeStr.substring(2, 4));
        int second = Integer.parseInt(timeStr.substring(4, 6));

        // 转换为分钟数（从00:00开始）
        int totalMinutes = hour * 60 + minute;

        // 上午交易时间：9:30-11:30 (包含11:30:00)
        boolean morningSession = (totalMinutes >= 9*60 + 30) && (totalMinutes <= 11*60 + 30);

        // 下午交易时间：13:00-15:00 (包含15:00:00)
        boolean afternoonSession = (totalMinutes >= 13*60) && (totalMinutes <= 15*60);

        // 如果是11:30:00或15:00:00，需要考虑秒数
        if (totalMinutes == 11*60 + 30) {
            return second == 0;  // 只有11:30:00是有效的
        }
        if (totalMinutes == 15*60) {
            return second == 0;  // 只有15:00:00是有效的
        }

        return morningSession || afternoonSession;
    }


    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        recordsProcessed++;
        // 输出当前进度
        if (recordsProcessed % 100000 == 0) {
            double rate = recordsProcessed / ((System.currentTimeMillis() - startTime) / 1000.0);
            System.out.printf("Mapper processed: %d records (%.1f records/sec)\n",
                    recordsProcessed, rate);
        }

        String line = value.toString();

        // 跳过头部
        if (line.startsWith("tradingDay") || line.trim().isEmpty()) {
            return;
        }

        String[] fields = line.split(",");
        if (fields.length < 37) {
            if (recordsProcessed <= 10) {
                System.out.println("字段不足37个，跳过: " + fields.length);
            }
            return;
        }

        // 解析关键字段
        String tradingDay = fields[0];
        long tradeTime = Long.parseLong(fields[1]);
        String stockCode = fields[4];

        // 快速过滤（交易时间判断）
        if (!isTradingTime(tradeTime)) {
            return;
        }

        // 获取前一时刻数据
        String cacheKey = stockCode + "_" + tradingDay;
        PreviousTickData prevData = tickCache.get(cacheKey);

        // 提取当前数据
        TickData currentData = extractTickData(fields);

        // 计算因子
        double[] factors = calculateAllFactors(currentData, prevData);

        // 输出格式为：tradeTime_factorId -> stockCode,factorValue
        for (int factorId = 1; factorId <= 20; factorId++) {
            String outputKey = String.format("%d_%d", tradeTime, factorId);
            String outputValue = stockCode + "," + String.format("%.6f", factors[factorId]);
            context.write(new Text(outputKey), new Text(outputValue));
        }

        // 更新缓存
        updateTickCache(cacheKey, currentData, tradeTime);
    }

    private PreviousTickData createCacheData(TickData current, long tradeTime) {
        PreviousTickData cacheData = new PreviousTickData();
        cacheData.setTradeTime(tradeTime);
        cacheData.setAp1(current.ap1);
        cacheData.setBp1(current.bp1);

        // 复制前5档买卖量
        for (int i = 0; i < 5; i++) {
            cacheData.setBv(i, current.bv[i + 1]);
            cacheData.setAv(i, current.av[i + 1]);
        }

        return cacheData;
    }


    private double[] calculateAllFactors(TickData current, PreviousTickData prev) {
        double[] factors = new double[21]; // 索引1-20

        // 使用之前写的calculateCompleteFactor方法
        for (int i = 1; i <= 20; i++) {
            factors[i] = calculateCompleteFactor(i, current, prev);
        }

        return factors;
    }

    private TickData extractTickData(String[] fields) {
        TickData data = new TickData();

        try {
            // 根据你的CSV列顺序解析字段
            // 字段索引（从0开始）：
            // 0:tradingDay, 1:tradeTime, 2:recvTime, 3:MIC, 4:code,
            // 5:cumCnt, 6:cumVol, 7:turnover, 8:last, 9:open, 10:high, 11:low,
            // 12:tBidVol, 13:tAskVol, 14:wBidPrc, 15:wAskPrc, 16:openInterest,
            // 17:bp1, 18:bv1, 19:ap1, 20:av1, 21:bp2, 22:bv2, 23:ap2, 24:av2,
            // 25:bp3, 26:bv3, 27:ap3, 28:av3, 29:bp4, 30:bv4, 31:ap4, 32:av4,
            // 33:bp5, 34:bv5, 35:ap5, 36:av5, ...

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
                int baseIdx = 17 + (i-1) * 4;  // bp1在索引17，每档4个字段

                if (baseIdx + 3 < fields.length) {
                    data.bp[i] = Long.parseLong(fields[baseIdx]);      // bp1, bp2, ...
                    data.bv[i] = Long.parseLong(fields[baseIdx + 1]);  // bv1, bv2, ...
                    data.ap[i] = Long.parseLong(fields[baseIdx + 2]);  // ap1, ap2, ...
                    data.av[i] = Long.parseLong(fields[baseIdx + 3]);  // av1, av2, ...
                }
            }

        } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
            System.err.println("Error parsing fields: " + e.getMessage());
            // 返回空数据或部分数据
        }

        return data;
    }

    private double calculateCompleteFactor(int factorId, TickData current, PreviousTickData prev) {
        switch (factorId) {
            // 因子1：最优价差
            case 1:
                return current.ap1 - current.bp1;

            // 因子2：相对价差
            case 2:
                double spread = current.ap1 - current.bp1;
                double midPrice = (current.ap1 + current.bp1) / 2.0;
                return spread / (midPrice + 1e-7);

            // 因子3：中间价
            case 3:
                return (current.ap1 + current.bp1) / 2.0;

            // 因子4：买一不平衡
            case 4:
                return (current.bv1 - current.av1) / (double)(current.bv1 + current.av1 + 1e-7);

            // 因子5：多档不平衡（n=5）
            case 5:
                double bidSum5 = 0, askSum5 = 0;
                for (int i = 1; i <= 5; i++) {
                    bidSum5 += current.bv[i];
                    askSum5 += current.av[i];
                }
                return (bidSum5 - askSum5) / (bidSum5 + askSum5 + 1e-7);

            // 因子6：买方深度
            case 6:
                double bidDepth = 0;
                for (int i = 1; i <= 5; i++) {
                    bidDepth += current.bv[i];
                }
                return bidDepth;

            // 因子7：卖方深度
            case 7:
                double askDepth = 0;
                for (int i = 1; i <= 5; i++) {
                    askDepth += current.av[i];
                }
                return askDepth;

            // 因子8：深度差
            case 8:
                return factor(6, current, prev) - factor(7, current, prev);

            // 因子9：深度比
            case 9:
                double bidDepth9 = factor(6, current, prev);
                double askDepth9 = factor(7, current, prev);
                return bidDepth9 / (askDepth9 + 1e-7);

            // 因子10：买卖量平衡指数
            case 10:
                return (current.tBidVol - current.tAskVol) / (double)(current.tBidVol + current.tAskVol + 1e-7);

            // 因子11：买方加权价格（VWAPBid）
            case 11:
                double bidWeightedSum = 0, bidTotalVol = 0;
                for (int i = 1; i <= 5; i++) {
                    bidWeightedSum += current.bp[i] * current.bv[i];
                    bidTotalVol += current.bv[i];
                }
                return bidWeightedSum / (bidTotalVol + 1e-7);

            // 因子12：卖方加权价格（VWAPAsk）
            case 12:
                double askWeightedSum = 0, askTotalVol = 0;
                for (int i = 1; i <= 5; i++) {
                    askWeightedSum += current.ap[i] * current.av[i];
                    askTotalVol += current.av[i];
                }
                return askWeightedSum / (askTotalVol + 1e-7);

            // 因子13：加权中间价
            case 13:
                double totalWeightedSum = 0, totalVol = 0;
                for (int i = 1; i <= 5; i++) {
                    totalWeightedSum += current.bp[i] * current.bv[i] + current.ap[i] * current.av[i];
                    totalVol += current.bv[i] + current.av[i];
                }
                return totalWeightedSum / (totalVol + 1e-7);

            // 因子14：加权价差
            case 14:
                return factor(12, current, prev) - factor(11, current, prev);

            // 因子15：买卖密度差
            case 15:
                double avgBid = 0, avgAsk = 0;
                for (int i = 1; i <= 5; i++) {
                    avgBid += current.bv[i];
                    avgAsk += current.av[i];
                }
                return (avgBid - avgAsk) / 5.0;

            // 因子16：买卖不对称度
            case 16:
                double weightedBid = 0, weightedAsk = 0;
                for (int i = 1; i <= 5; i++) {
                    double weight = 1.0 / i;  // 按档位衰减
                    weightedBid += current.bv[i] * weight;
                    weightedAsk += current.av[i] * weight;
                }
                return (weightedBid - weightedAsk) / (weightedBid + weightedAsk + 1e-7);

            // 因子17：最优价变动（需要前一时间数据）
            case 17:
                if (prev != null) {
                    return current.ap1 - prev.getAp1();
                }
                return 0.0;

            // 因子18：中间价变动（需要前一时间数据）
            case 18:
                if (prev != null) {
                    double currentMid = (current.ap1 + current.bp1) / 2.0;
                    double prevMid = (prev.getAp1() + prev.getBp1()) / 2.0;
                    return currentMid - prevMid;
                }
                return 0.0;

            // 因子19：深度比变动（需要前一时间数据）
            case 19:
                if (prev != null) {
                    double currentBidDepth = factor(6, current, prev);
                    double currentAskDepth = factor(7, current, prev);
                    double prevBidDepth = 0, prevAskDepth = 0;
                    for (int i = 0; i < 5; i++) {
                        prevBidDepth += prev.bv[i];
                        prevAskDepth += prev.av[i];
                    }
                    double currentRatio = currentBidDepth / (currentAskDepth + 1e-7);
                    double prevRatio = prevBidDepth / (prevAskDepth + 1e-7);
                    return currentRatio - prevRatio;
                }
                return 0.0;

            // 因子20：价压指标
            case 20:
                double spread20 = current.ap1 - current.bp1;
                double totalDepth = 0;
                for (int i = 1; i <= 5; i++) {
                    totalDepth += current.bv[i] + current.av[i];
                }
                return spread20 / (totalDepth + 1e-7);

            default:
                return 0.0;
        }
    }

    private double factor(int factorId, TickData current, PreviousTickData prev) {
        // 辅助方法：计算因子值
        return calculateCompleteFactor(factorId, current, prev);
    }

    private void updateTickCache(String cacheKey, TickData current, long tradeTime) {
        PreviousTickData cacheData = new PreviousTickData();
        cacheData.setTradeTime(tradeTime);
        cacheData.setAp1(current.ap1);
        cacheData.setBp1(current.bp1);

        // 只缓存前5档数据（大部分因子只需要这些）
        for (int i = 0; i < 5; i++) {
            cacheData.setBv(i, current.bv[i + 1]);
            cacheData.setAv(i, current.av[i + 1]);
        }

        tickCache.put(cacheKey, cacheData);

        // 简单的缓存清理：超过10000条时清理一半
        if (tickCache.size() > 10000) {
            Iterator<Map.Entry<String, PreviousTickData>> it = tickCache.entrySet().iterator();
            int count = 0;
            while (it.hasNext() && count < 5000) {
                it.next();
                it.remove();
                count++;
            }
        }
    }

    private String buildFactorString(String stockCode, double[] factors) {
        // 紧凑格式：股票代码|因子1,因子2,...,因子20
        StringBuilder sb = new StringBuilder();
        sb.append(stockCode).append("|");

        for (int i = 1; i <= 20; i++) {
            sb.append(String.format("%.6f", factors[i]));
            if (i < 20) {
                sb.append(",");
            }
        }

        return sb.toString();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
        System.out.printf("Mapper finished: %d total records processed\n", recordsProcessed);
        System.out.printf("Cache size: %d entries\n", tickCache.size());
    }
}
