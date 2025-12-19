import java.io.Serializable;

/**
 * 前一时刻的数据缓存结构
 */
public class PreviousTickData {
    private long tradeTime;
    private long ap1;
    private long bp1;
    long[] bv = new long[5];  // 前5档买单量（索引0-4）
    long[] av = new long[5];  // 前5档卖单量（索引0-4）

    public PreviousTickData() {}

    public PreviousTickData(long tradeTime, long ap1, long bp1, long[] bv, long[] av) {
        this.tradeTime = tradeTime;
        this.ap1 = ap1;
        this.bp1 = bp1;
        if (bv != null && bv.length >= 5) {
            System.arraycopy(bv, 0, this.bv, 0, 5);
        }
        if (av != null && av.length >= 5) {
            System.arraycopy(av, 0, this.av, 0, 5);
        }
    }

    // Getters and Setters
    public long getTradeTime() { return tradeTime; }
    public void setTradeTime(long tradeTime) { this.tradeTime = tradeTime; }

    public long getAp1() { return ap1; }
    public void setAp1(long ap1) { this.ap1 = ap1; }

    public long getBp1() { return bp1; }
    public void setBp1(long bp1) { this.bp1 = bp1; }

    public long getBv(int index) {
        if (index >= 0 && index < 5) return bv[index];
        return 0;
    }

    public void setBv(int index, long value) {
        if (index >= 0 && index < 5) bv[index] = value;
    }

    public long getAv(int index) {
        if (index >= 0 && index < 5) return av[index];
        return 0;
    }

    public void setAv(int index, long value) {
        if (index >= 0 && index < 5) av[index] = value;
    }


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