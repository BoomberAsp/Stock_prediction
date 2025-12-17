/**
 * 缓存命中率监控器（可选，用于性能调优）
 */
public class CacheMonitor {
    private long hitCount = 0;
    private long missCount = 0;
    private long totalGetCount = 0;

    public void recordHit() {
        hitCount++;
        totalGetCount++;
    }

    public void recordMiss() {
        missCount++;
        totalGetCount++;
    }

    public double getHitRate() {
        return totalGetCount > 0 ? (double) hitCount / totalGetCount : 0.0;
    }

    public void reset() {
        hitCount = 0;
        missCount = 0;
        totalGetCount = 0;
    }
}