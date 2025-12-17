import java.util.LinkedHashMap;
import java.util.Map;

/**
 * LRU缓存管理器
 * 限制最大缓存大小，自动淘汰最久未使用的条目
 */
public class LRUTickCache extends LinkedHashMap<String, PreviousTickData> {
    private static final long serialVersionUID = 1L;
    private final int maxCacheSize;

    public LRUTickCache(int maxCacheSize) {
        // accessOrder=true 表示按访问顺序排序，最近访问的放在最后
        super(maxCacheSize * 4 / 3, 0.75f, true);
        this.maxCacheSize = maxCacheSize;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<String, PreviousTickData> eldest) {
        // 当缓存大小超过限制时，移除最久未使用的条目
        return size() > maxCacheSize;
    }

    /**
     * 安全获取数据（避免空指针）
     */
    public PreviousTickData safeGet(String stockCode) {
        PreviousTickData data = get(stockCode);
        return data != null ? data.deepCopy() : null;
    }

    /**
     * 安全存入数据
     */
    public void safePut(String stockCode, PreviousTickData data) {
        put(stockCode, data.deepCopy());
    }

    /**
     * 清理特定股票的历史数据（用于跨交易日重置）
     */
    public void clearStockData(String stockCode) {
        remove(stockCode);
    }
}