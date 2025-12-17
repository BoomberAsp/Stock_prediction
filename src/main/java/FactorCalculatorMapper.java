import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 因子计算Mapper，包含完整的内存缓存机制
 */
public class FactorCalculatorMapper extends Mapper<LongWritable, Text, Text, Text> {

    // LRU缓存，最多缓存500只股票的数据
    private LRUTickCache tickCache;

    // 批量输出缓冲区，减少IO操作
    private List<Map.Entry<String, String>> outputBuffer;
    private static final int BUFFER_SIZE = 1000;

    // 时间格式化器，用于判断是否跨交易日
    private SimpleDateFormat timeFormat;

    // 记录当前处理的交易日
    private int currentTradingDay = 0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        tickCache = new LRUTickCache(500); // 沪深300共300只股票，500足够
        outputBuffer = new ArrayList<>(BUFFER_SIZE);
        timeFormat = new SimpleDateFormat("HHmmss");

        // 初始化缓存（可以在这里加载checkpoint数据，如果需要）
        context.getConfiguration().setInt("cache.max.size", 500);
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // 1. 解析CSV行
        String line = value.toString();
        String[] fields = line.split(",");

        // 字段索引映射（根据实际数据格式调整）
        int idx = 0;
        int tradingDay = Integer.parseInt(fields[idx++]);      // 交易日期
        long tradeTime = Long.parseLong(fields[idx++]);        // 交易时间
        String stockCode = fields[4];                          // 股票代码（假设是第5个字段）

        // 2. 检查是否跨交易日，如果是则清理缓存
        checkAndClearCache(tradingDay, stockCode);

        // 3. 提取计算所需字段
        Map<String, Long> fieldMap = extractFields(fields);

        // 4. 获取前一时刻数据
        PreviousTickData prevData = tickCache.safeGet(stockCode);

        // 5. 计算20个因子
        for (int factorId = 1; factorId <= 20; factorId++) {
            double factorValue = calculateFactor(factorId, fieldMap, prevData);

            // 构建输出键值
            String outputKey = tradeTime + "_" + factorId;
            String outputValue = stockCode + "," + factorValue;

            // 添加到缓冲区
            outputBuffer.add(new AbstractMap.SimpleEntry<>(outputKey, outputValue));

            // 缓冲区满时批量输出
            if (outputBuffer.size() >= BUFFER_SIZE) {
                flushBuffer(context);
            }
        }

        // 6. 更新缓存：将当前时刻数据存入缓存
        updateCache(stockCode, tradeTime, fieldMap);
    }

    /**
     * 检查并清理缓存（跨交易日时使用）
     */
    private void checkAndClearCache(int tradingDay, String stockCode) {
        if (tradingDay != currentTradingDay) {
            // 新交易日，清理该股票的缓存数据
            tickCache.clearStockData(stockCode);
            currentTradingDay = tradingDay;
        }
    }

    /**
     * 提取计算因子所需的字段
     */
    private Map<String, Long> extractFields(String[] fields) {
        Map<String, Long> fieldMap = new HashMap<>();

        // 根据实际字段顺序提取
        // 这里假设字段顺序与文档中表格一致
        fieldMap.put("ap1", Long.parseLong(fields[19]));  // 卖一价
        fieldMap.put("av1", Long.parseLong(fields[20]));  // 卖一量
        fieldMap.put("bp1", Long.parseLong(fields[17]));  // 买一价
        fieldMap.put("bv1", Long.parseLong(fields[18]));  // 买一量

        // 提取前5档买卖价量
        for (int i = 1; i <= 5; i++) {
            int bidPriceIdx = 16 + (i-1)*4 + 1;  // 计算字段索引
            int bidVolIdx = bidPriceIdx + 1;
            int askPriceIdx = bidPriceIdx + 2;
            int askVolIdx = bidPriceIdx + 3;

            fieldMap.put("bp" + i, Long.parseLong(fields[bidPriceIdx]));
            fieldMap.put("bv" + i, Long.parseLong(fields[bidVolIdx]));
            fieldMap.put("ap" + i, Long.parseLong(fields[askPriceIdx]));
            fieldMap.put("av" + i, Long.parseLong(fields[askVolIdx]));
        }

        return fieldMap;
    }

    /**
     * 计算单个因子
     */
    private double calculateFactor(int factorId, Map<String, Long> currentFields,
                                   PreviousTickData prevData) {
        double result = 0.0;

        switch (factorId) {
            // 因子1：最优价差
            case 1:
                result = currentFields.get("ap1") - currentFields.get("bp1");
                break;

            // 因子2：相对价差
            case 2:
                double spread = currentFields.get("ap1") - currentFields.get("bp1");
                double midPrice = (currentFields.get("ap1") + currentFields.get("bp1")) / 2.0;
                result = spread / (midPrice + 1e-7);
                break;

            // ... 其他非时间序列因子 (3-16, 20)

            // 因子17：最优价变动（需要前一时刻数据）
            case 17:
                if (prevData != null) {
                    result = currentFields.get("ap1") - prevData.getAp1();
                } else {
                    result = 0.0;  // 首条数据，变动为0
                }
                break;

            // 因子18：中间价变动
            case 18:
                if (prevData != null) {
                    double currentMid = (currentFields.get("ap1") + currentFields.get("bp1")) / 2.0;
                    double prevMid = (prevData.getAp1() + prevData.getBp1()) / 2.0;
                    result = currentMid - prevMid;
                } else {
                    result = 0.0;
                }
                break;

            // 因子19：深度比变动
            case 19:
                if (prevData != null) {
                    // 计算当前时刻前5档买卖总量
                    double currentBidSum = 0, currentAskSum = 0;
                    double prevBidSum = 0, prevAskSum = 0;

                    for (int i = 1; i <= 5; i++) {
                        currentBidSum += currentFields.get("bv" + i);
                        currentAskSum += currentFields.get("av" + i);
                        prevBidSum += prevData.getBv(i-1);
                        prevAskSum += prevData.getAv(i-1);
                    }

                    double currentRatio = currentBidSum / (currentAskSum + 1e-7);
                    double prevRatio = prevBidSum / (prevAskSum + 1e-7);
                    result = currentRatio - prevRatio;
                } else {
                    result = 0.0;
                }
                break;

            // 因子20：价压指标
            case 20:
                double spread20 = currentFields.get("ap1") - currentFields.get("bp1");
                double depthSum = 0;
                for (int i = 1; i <= 5; i++) {
                    depthSum += currentFields.get("bv" + i) + currentFields.get("av" + i);
                }
                result = spread20 / (depthSum + 1e-7);
                break;

            default:
                // 其他因子计算逻辑...
                result = calculateOtherFactor(factorId, currentFields);
        }

        return result;
    }

    /**
     * 计算其他因子（3-16）
     */
    private double calculateOtherFactor(int factorId, Map<String, Long> fields) {
        // 这里实现其他因子的计算逻辑
        // 根据项目文档中的公式实现
        switch (factorId) {
            case 3: // 中间价
                return (fields.get("ap1") + fields.get("bp1")) / 2.0;

            case 4: // 买一不平衡
                double bidAskDiff = fields.get("bv1") - fields.get("av1");
                double bidAskSum = fields.get("bv1") + fields.get("av1");
                return bidAskDiff / (bidAskSum + 1e-7);

            case 5: // 多档不平衡
                double bidSum5 = 0, askSum5 = 0;
                for (int i = 1; i <= 5; i++) {
                    bidSum5 += fields.get("bv" + i);
                    askSum5 += fields.get("av" + i);
                }
                return (bidSum5 - askSum5) / (bidSum5 + askSum5 + 1e-7);

            // ... 实现其他因子
            default:
                return 0.0;
        }
    }

    /**
     * 更新缓存
     */
    private void updateCache(String stockCode, long tradeTime, Map<String, Long> currentFields) {
        PreviousTickData currentData = new PreviousTickData();
        currentData.setTradeTime(tradeTime);
        currentData.setAp1(currentFields.get("ap1"));
        currentData.setBp1(currentFields.get("bp1"));

        // 存储前5档买卖量
        for (int i = 1; i <= 5; i++) {
            currentData.setBv(i-1, currentFields.get("bv" + i));
            currentData.setAv(i-1, currentFields.get("av" + i));
        }

        // 存入缓存
        tickCache.safePut(stockCode, currentData);
    }

    /**
     * 批量输出缓冲区中的数据
     */
    private void flushBuffer(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, String> entry : outputBuffer) {
            context.write(new Text(entry.getKey()), new Text(entry.getValue()));
        }
        outputBuffer.clear();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // 清空缓冲区中剩余的数据
        if (!outputBuffer.isEmpty()) {
            flushBuffer(context);
        }

        // 可选：输出缓存统计信息用于调试
        context.getCounter("CACHE_STATS", "CACHE_SIZE").setValue(tickCache.size());
    }
}
