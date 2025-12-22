import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.text.DecimalFormat;

public class SimplifiedFactorMapper extends Mapper<LongWritable, Text, Text, Text> {

    // === 核心对象复用池 ===
    private Map<String, PreviousTickData> prevDataCache;

    private final Text outKey = new Text();
    private final Text outValue = new Text();

    // 复用 TickData 对象 (注意：这里会使用外部的 TickData 类)
    private final TickData currentTick = new TickData();

    private final double[] factors = new double[21];
    private final StringBuilder sb = new StringBuilder(1024);
    private final DecimalFormat df = new DecimalFormat("0.000000");
    private final int[] commaIndices = new int[100];

    @Override
    protected void setup(Context context) {
        prevDataCache = new HashMap<>(1024);
        System.out.println("=== 🚀 Zero-GC Mapper Initialized (Clean Split) ===");
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        int commaCount = 0;
        int len = line.length();
        for (int i = 0; i < len; i++) {
            if (line.charAt(i) == ',') {
                commaIndices[commaCount++] = i;
                if (commaCount >= 50) break;
            }
        }

        if (commaCount < 36) return;

        String tradingDay = line.substring(0, commaIndices[0]);
        String timeStr = line.substring(commaIndices[0] + 1, commaIndices[1]);

        if (Character.isLetter(tradingDay.charAt(0))) return;

        if (timeStr.compareTo("093000") < 0 || timeStr.compareTo("145700") > 0) return;

        String stockCode = line.substring(commaIndices[3] + 1, commaIndices[4]);

        long tradeTime;
        try {
            tradeTime = Long.parseLong(timeStr);
        } catch (NumberFormatException e) {
            return;
        }

        // 重置并填充 (调用外部 TickData 的 reset 方法)
        currentTick.reset();
        if (!fillTickDataFast(currentTick, line, commaIndices, tradeTime)) {
            return;
        }

        String cacheKey = stockCode + "_" + tradingDay;
        PreviousTickData prevData = prevDataCache.get(cacheKey);

        calculateFactorsInPlace(factors, currentTick, prevData);

        sb.setLength(0);
        sb.append("1|");

        for (int i = 1; i <= 20; i++) {
            sb.append(df.format(factors[i]));
            if (i < 20) sb.append(',');
        }

        outKey.set(tradingDay + "_" + timeStr);
        outValue.set(sb.toString());
        context.write(outKey, outValue);

        updateCache(cacheKey, currentTick, tradeTime);
    }

    private boolean fillTickDataFast(TickData data, String line, int[] cIdx, long tradeTimeVal) {
        data.tradeTime = tradeTimeVal;
        try {
            data.last = parseLongFast(line, cIdx[7] + 1, cIdx[8]);
            data.tBidVol = parseLongFast(line, cIdx[11] + 1, cIdx[12]);
            data.tAskVol = parseLongFast(line, cIdx[12] + 1, cIdx[13]);

            data.bp1 = parseLongFast(line, cIdx[16] + 1, cIdx[17]);
            data.bv1 = parseLongFast(line, cIdx[17] + 1, cIdx[18]);
            data.ap1 = parseLongFast(line, cIdx[18] + 1, cIdx[19]);
            data.av1 = parseLongFast(line, cIdx[19] + 1, cIdx[20]);

            for (int i = 1; i <= 5; i++) {
                int baseCol = 17 + (i - 1) * 4;
                data.bp[i] = parseLongFast(line, cIdx[baseCol-1] + 1, cIdx[baseCol]);
                data.bv[i] = parseLongFast(line, cIdx[baseCol] + 1, cIdx[baseCol+1]);
                data.ap[i] = parseLongFast(line, cIdx[baseCol+1] + 1, cIdx[baseCol+2]);
                data.av[i] = parseLongFast(line, cIdx[baseCol+2] + 1, cIdx[baseCol+3]);
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private long parseLongFast(String line, int start, int end) {
        if (start >= end) return 0L;
        while (start < end && line.charAt(start) == ' ') start++;
        if (start >= end) return 0L;
        long result = 0;
        for (int i = start; i < end; i++) {
            char c = line.charAt(i);
            if (c == '.') break;
            if (c < '0' || c > '9') break;
            result = result * 10 + (c - '0');
        }
        return result;
    }

    private void calculateFactorsInPlace(double[] f, TickData current, PreviousTickData prev) {
        f[1] = calculateFactor1(current);
        f[2] = calculateFactor2(current);
        f[3] = calculateFactor3(current);
        f[4] = calculateFactor4(current);
        f[5] = calculateFactor5(current);
        f[6] = calculateFactor6(current);
        f[7] = calculateFactor7(current);
        f[8] = calculateFactor8(f[6], f[7]);
        f[9] = calculateFactor9(f[6], f[7]);
        f[10] = calculateFactor10(current);

        double vwapBid = calculateFactor11(current);
        double vwapAsk = calculateFactor12(current);
        f[11] = vwapBid;
        f[12] = vwapAsk;
        f[13] = calculateFactor13(current);
        f[14] = vwapAsk - vwapBid;

        f[15] = calculateFactor15(current);
        f[16] = calculateFactor16(current);

        f[17] = calculateFactor17(current, prev);
        f[18] = calculateFactor18(current, prev);
        f[19] = calculateFactor19(current, prev);
        f[20] = calculateFactor20(current);
    }

    // === 辅助计算逻辑 (保持不变) ===
    private double calculateFactor1(TickData data) { return data.ap1 - data.bp1; }
    private double calculateFactor2(TickData data) { double mid = (data.ap1 + data.bp1) / 2.0; return (data.ap1 - data.bp1) / (mid + 1e-7); }
    private double calculateFactor3(TickData data) { return (data.ap1 + data.bp1) / 2.0; }
    private double calculateFactor4(TickData data) { return (data.bv1 - data.av1) / (double)(data.bv1 + data.av1 + 1e-7); }
    private double calculateFactor5(TickData data) { double b=0, a=0; for(int i=1;i<=5;i++){b+=data.bv[i]; a+=data.av[i];} return (b-a)/(b+a+1e-7); }
    private double calculateFactor6(TickData data) { double s=0; for(int i=1;i<=5;i++) s+=data.bv[i]; return s; }
    private double calculateFactor7(TickData data) { double s=0; for(int i=1;i<=5;i++) s+=data.av[i]; return s; }
    private double calculateFactor8(double b, double a) { return b - a; }
    private double calculateFactor9(double b, double a) { return b / (a + 1e-7); }
    private double calculateFactor10(TickData data) { return (data.tBidVol - data.tAskVol)/(double)(data.tBidVol + data.tAskVol + 1e-7); }
    private double calculateFactor11(TickData data) { double w=0, t=0; for(int i=1;i<=5;i++){w+=data.bp[i]*data.bv[i]; t+=data.bv[i];} return w/(t+1e-7); }
    private double calculateFactor12(TickData data) { double w=0, t=0; for(int i=1;i<=5;i++){w+=data.ap[i]*data.av[i]; t+=data.av[i];} return w/(t+1e-7); }
    private double calculateFactor13(TickData data) { double wb=0, wa=0, vb=0, va=0; for(int i=1;i<=5;i++){wb+=data.bp[i]*data.bv[i]; wa+=data.ap[i]*data.av[i]; vb+=data.bv[i]; va+=data.av[i];} return (wb+wa)/(vb+va+1e-7); }
    private double calculateFactor15(TickData data) { double b=0, a=0; for(int i=1;i<=5;i++){b+=data.bv[i]; a+=data.av[i];} return (b-a)/5.0; }
    private double calculateFactor16(TickData data) { double wb=0, wa=0; for(int i=1;i<=5;i++){double w=1.0/i; wb+=data.bv[i]*w; wa+=data.av[i]*w;} return (wb-wa)/(wb+wa+1e-7); }
    private double calculateFactor17(TickData c, PreviousTickData p) { return (p==null)?0.0 : c.ap1 - p.getAp1(); }
    private double calculateFactor18(TickData c, PreviousTickData p) { if(p==null)return 0.0; double cm=(c.ap1+c.bp1)/2.0; double pm=(p.getAp1()+p.getBp1())/2.0; return cm-pm; }
    private double calculateFactor19(TickData c, PreviousTickData p) {
        if(p==null)return 0.0;
        double cr=calculateFactor9(calculateFactor6(c), calculateFactor7(c));
        double pb=0, pa=0;
        for(int i=0;i<5;i++){ pb+=p.getBv(i); pa+=p.getAv(i); }
        double pr=pb/(pa+1e-7);
        return cr-pr;
    }
    private double calculateFactor20(TickData d) { double s=d.ap1-d.bp1; double t=0; for(int i=1;i<=5;i++) t+=d.bv[i]+d.av[i]; return s/(t+1e-7); }

    private void updateCache(String cacheKey, TickData current, long tradeTime) {
        long[] bvArray = new long[5];
        long[] avArray = new long[5];
        for (int i = 0; i < 5; i++) {
            bvArray[i] = current.bv[i + 1];
            avArray[i] = current.av[i + 1];
        }
        PreviousTickData prevData = new PreviousTickData(tradeTime, current.ap1, current.bp1, bvArray, avArray);
        prevDataCache.put(cacheKey, prevData);
        if (prevDataCache.size() > 2000) prevDataCache.clear();
    }
}