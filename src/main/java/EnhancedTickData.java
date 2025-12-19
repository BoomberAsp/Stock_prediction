// [file name]: EnhancedTickData.java
/**
 * 增强的TickData类，包含历史数据支持
 */
public class EnhancedTickData extends TickData {
    // 前一时间点的数据
    private PreviousTickData previousData;

    // 辅助计算字段
    public double[] factorValues = new double[21]; // 索引1-20存储因子值

    public EnhancedTickData() {
        super();
    }

    public void setPreviousData(PreviousTickData previous) {
        this.previousData = previous;
    }

    public PreviousTickData getPreviousData() {
        return previousData;
    }

    public double getFactorValue(int factorId) {
        if (factorId >= 1 && factorId <= 20) {
            return factorValues[factorId];
        }
        return 0.0;
    }

    public void setFactorValue(int factorId, double value) {
        if (factorId >= 1 && factorId <= 20) {
            factorValues[factorId] = value;
        }
    }
}