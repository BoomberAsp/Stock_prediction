// [file name]: FactorCalculator.java
/**
 * 完整的因子计算引擎
 */
public class FactorCalculator {

    private static final int N = 5;      // 前n档，根据题目n=5
    private static final int DELTA_T = 1; // Δt=1（3秒）
    private static final double EPSILON = 1e-7; // 防止除零

    /**
     * 计算所有20个因子
     */
    public static double[] calculateAllFactors(EnhancedTickData current) {
        double[] factors = new double[21]; // 索引1-20

        // 基础因子（不需要历史数据）
        factors[1] = calculateSpread(current);          // 最优价差
        factors[2] = calculateRelativeSpread(current);  // 相对价差
        factors[3] = calculateMidPrice(current);        // 中间价
        factors[4] = calculateBidAskImbalance1(current); // 买一不平衡
        factors[5] = calculateMultiLevelImbalance(current, N); // 多档不平衡
        factors[6] = calculateBidDepth(current, N);     // 买方深度
        factors[7] = calculateAskDepth(current, N);     // 卖方深度
        factors[8] = calculateDepthDifference(current, N); // 深度差
        factors[9] = calculateDepthRatio(current, N);   // 深度比
        factors[10] = calculateMarketImbalance(current); // 买卖量平衡指数

        // 加权价格因子
        factors[11] = calculateVWAPBid(current, N);     // 买方加权价格
        factors[12] = calculateVWAPAsk(current, N);     // 卖方加权价格
        factors[13] = calculateWeightedMidPrice(current, N); // 加权中间价
        factors[14] = calculateWeightedSpread(current, N); // 加权价差

        // 密度和不对称度
        factors[15] = calculateDensityDifference(current, N); // 买卖密度差
        factors[16] = calculateAsymmetryDegree(current, N); // 买卖不对称度

        // 需要历史数据的因子
        PreviousTickData prev = current.getPreviousData();
        factors[17] = calculateBestPriceChange(current, prev); // 最优价变动
        factors[18] = calculateMidPriceChange(current, prev);  // 中间价变动
        factors[19] = calculateDepthRatioChange(current, prev, N); // 深度比变动
        factors[20] = calculatePressureIndicator(current, N); // 价压指标

        return factors;
    }

    // ========== 基础因子（1-10） ==========

    private static double calculateSpread(EnhancedTickData data) {
        return data.ap1 - data.bp1;
    }

    private static double calculateRelativeSpread(EnhancedTickData data) {
        double spread = data.ap1 - data.bp1;
        double midPrice = (data.ap1 + data.bp1) / 2.0;
        return spread / (midPrice + EPSILON);
    }

    private static double calculateMidPrice(EnhancedTickData data) {
        return (data.ap1 + data.bp1) / 2.0;
    }

    private static double calculateBidAskImbalance1(EnhancedTickData data) {
        long bv1 = data.bv1;
        long av1 = data.av1;
        return (double)(bv1 - av1) / (bv1 + av1 + EPSILON);
    }

    private static double calculateMultiLevelImbalance(EnhancedTickData data, int n) {
        long bidSum = 0, askSum = 0;
        for (int i = 1; i <= n && i < data.bv.length; i++) {
            bidSum += data.bv[i];
            askSum += data.av[i];
        }
        return (double)(bidSum - askSum) / (bidSum + askSum + EPSILON);
    }

    private static double calculateBidDepth(EnhancedTickData data, int n) {
        long depth = 0;
        for (int i = 1; i <= n && i < data.bv.length; i++) {
            depth += data.bv[i];
        }
        return depth;
    }

    private static double calculateAskDepth(EnhancedTickData data, int n) {
        long depth = 0;
        for (int i = 1; i <= n && i < data.av.length; i++) {
            depth += data.av[i];
        }
        return depth;
    }

    private static double calculateDepthDifference(EnhancedTickData data, int n) {
        return calculateBidDepth(data, n) - calculateAskDepth(data, n);
    }

    private static double calculateDepthRatio(EnhancedTickData data, int n) {
        double bidDepth = calculateBidDepth(data, n);
        double askDepth = calculateAskDepth(data, n);
        return bidDepth / (askDepth + EPSILON);
    }

    private static double calculateMarketImbalance(EnhancedTickData data) {
        long tBidVol = data.tBidVol;
        long tAskVol = data.tAskVol;
        return (double)(tBidVol - tAskVol) / (tBidVol + tAskVol + EPSILON);
    }

    // ========== 加权价格因子（11-14） ==========

    private static double calculateVWAPBid(EnhancedTickData data, int n) {
        double weightedSum = 0.0;
        long totalVolume = 0;

        for (int i = 1; i <= n && i < data.bp.length; i++) {
            weightedSum += data.bp[i] * data.bv[i];
            totalVolume += data.bv[i];
        }

        return weightedSum / (totalVolume + EPSILON);
    }

    private static double calculateVWAPAsk(EnhancedTickData data, int n) {
        double weightedSum = 0.0;
        long totalVolume = 0;

        for (int i = 1; i <= n && i < data.ap.length; i++) {
            weightedSum += data.ap[i] * data.av[i];
            totalVolume += data.av[i];
        }

        return weightedSum / (totalVolume + EPSILON);
    }

    private static double calculateWeightedMidPrice(EnhancedTickData data, int n) {
        double bidWeightedSum = 0.0;
        double askWeightedSum = 0.0;
        long bidTotalVol = 0;
        long askTotalVol = 0;

        for (int i = 1; i <= n && i < data.bp.length; i++) {
            bidWeightedSum += data.bp[i] * data.bv[i];
            bidTotalVol += data.bv[i];
        }

        for (int i = 1; i <= n && i < data.ap.length; i++) {
            askWeightedSum += data.ap[i] * data.av[i];
            askTotalVol += data.av[i];
        }

        return (bidWeightedSum + askWeightedSum) /
                (bidTotalVol + askTotalVol + EPSILON);
    }

    private static double calculateWeightedSpread(EnhancedTickData data, int n) {
        double vwapAsk = calculateVWAPAsk(data, n);
        double vwapBid = calculateVWAPBid(data, n);
        return vwapAsk - vwapBid;
    }

    // ========== 密度和不对称度（15-16） ==========

    private static double calculateDensityDifference(EnhancedTickData data, int n) {
        long bidSum = 0, askSum = 0;
        for (int i = 1; i <= n && i < data.bv.length; i++) {
            bidSum += data.bv[i];
            askSum += data.av[i];
        }
        return (bidSum - askSum) / (double)n;
    }

    private static double calculateAsymmetryDegree(EnhancedTickData data, int n) {
        double weightedBid = 0.0;
        double weightedAsk = 0.0;

        for (int i = 1; i <= n && i < data.bv.length; i++) {
            double weight = 1.0 / i;  // 按档位衰减
            weightedBid += data.bv[i] * weight;
            weightedAsk += data.av[i] * weight;
        }

        return (weightedBid - weightedAsk) / (weightedBid + weightedAsk + EPSILON);
    }

    // ========== 需要历史数据的因子（17-20） ==========

    private static double calculateBestPriceChange(EnhancedTickData current, PreviousTickData prev) {
        if (prev == null) {
            return 0.0; // 没有历史数据
        }
        return current.ap1 - prev.getAp1();
    }

    private static double calculateMidPriceChange(EnhancedTickData current, PreviousTickData prev) {
        if (prev == null) {
            return 0.0;
        }
        double currentMid = (current.ap1 + current.bp1) / 2.0;
        double prevMid = (prev.getAp1() + prev.getBp1()) / 2.0;
        return currentMid - prevMid;
    }

    private static double calculateDepthRatioChange(EnhancedTickData current, PreviousTickData prev, int n) {
        if (prev == null) {
            return 0.0;
        }

        // 当前深度比
        double currentBidDepth = calculateBidDepth(current, n);
        double currentAskDepth = calculateAskDepth(current, n);
        double currentRatio = currentBidDepth / (currentAskDepth + EPSILON);

        // 历史深度比
        double prevBidDepth = 0.0;
        double prevAskDepth = 0.0;
        for (int i = 0; i < n; i++) {
            prevBidDepth += prev.getBv(i);
            prevAskDepth += prev.getAv(i);
        }
        double prevRatio = prevBidDepth / (prevAskDepth + EPSILON);

        return currentRatio - prevRatio;
    }

    private static double calculatePressureIndicator(EnhancedTickData data, int n) {
        double spread = data.ap1 - data.bp1;
        double totalDepth = 0.0;

        for (int i = 1; i <= n && i < data.bv.length; i++) {
            totalDepth += data.bv[i] + data.av[i];
        }

        return spread / (totalDepth + EPSILON);
    }

    /**
     * 计算累积值（用于减少重复计算）
     */
    public static class CumulativeValues {
        public double bidDepth = 0.0;
        public double askDepth = 0.0;
        public double bidWeightedSum = 0.0;
        public double askWeightedSum = 0.0;
        public double bidTotalVol = 0.0;
        public double askTotalVol = 0.0;

        public void calculate(EnhancedTickData data, int n) {
            bidDepth = 0.0;
            askDepth = 0.0;
            bidWeightedSum = 0.0;
            askWeightedSum = 0.0;
            bidTotalVol = 0.0;
            askTotalVol = 0.0;

            for (int i = 1; i <= n && i < data.bv.length; i++) {
                bidDepth += data.bv[i];
                askDepth += data.av[i];
                bidWeightedSum += data.bp[i] * data.bv[i];
                askWeightedSum += data.ap[i] * data.av[i];
                bidTotalVol += data.bv[i];
                askTotalVol += data.av[i];
            }
        }
    }
}
