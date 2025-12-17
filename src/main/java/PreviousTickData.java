import java.io.Serializable;

/**
 * 前一时刻的数据缓存结构
 */
public class PreviousTickData implements Serializable {
    private long tradeTime;      // 交易时间
    private long ap1;            // 卖一价
    private long bp1;            // 买一价
    private long[] bv;           // 前5档买单量
    private long[] av;           // 前5档卖单量

    public PreviousTickData() {
        this.bv = new long[5];
        this.av = new long[5];
    }

    // Getters and Setters
    public long getTradeTime() { return tradeTime; }
    public void setTradeTime(long tradeTime) { this.tradeTime = tradeTime; }

    public long getAp1() { return ap1; }
    public void setAp1(long ap1) { this.ap1 = ap1; }

    public long getBp1() { return bp1; }
    public void setBp1(long bp1) { this.bp1 = bp1; }

    public long getBv(int index) { return bv[index]; }
    public void setBv(int index, long value) { bv[index] = value; }

    public long getAv(int index) { return av[index]; }
    public void setAv(int index, long value) { av[index] = value; }

    /**
     * 深拷贝方法
     */
    public PreviousTickData deepCopy() {
        PreviousTickData copy = new PreviousTickData();
        copy.tradeTime = this.tradeTime;
        copy.ap1 = this.ap1;
        copy.bp1 = this.bp1;
        System.arraycopy(this.bv, 0, copy.bv, 0, 5);
        System.arraycopy(this.av, 0, copy.av, 0, 5);
        return copy;
    }
}