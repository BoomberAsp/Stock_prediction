/**
 * 当前时刻的数据结构
 */
public class TickData {
    // 基础字段
    public long ap1, av1;  // 卖一价量
    public long bp1, bv1;  // 买一价量

    // 前5档数据（索引1-5）
    public long[] bp = new long[6];  // 买1-5价（索引1-5使用）
    public long[] bv = new long[6];  // 买1-5量
    public long[] ap = new long[6];  // 卖1-5价
    public long[] av = new long[6];  // 卖1-5量

    // 全市场总量
    public long tBidVol;  // 全市场买单总量
    public long tAskVol;  // 全市场卖单总量

    // 最新成交价（用于因子计算）
    public long last;

    // 时间
    public long tradeTime;
}
