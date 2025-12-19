// [file name]: FixedTimeParser.java
/**
 * 修正的时间解析器：处理6位HHMMSS格式
 */
public class FixedTimeParser {

    /**
     * 解析6位HHMMSS格式的时间
     * @param tradeTime 6位整数，如092954
     * @return TimeInfo对象
     */
    public static TimeInfo parseTradeTime(long tradeTime) {
        TimeInfo info = new TimeInfo();

        // 将时间转换为6位字符串（前面补零）
        String timeStr = String.format("%06d", tradeTime);

        if (timeStr.length() != 6) {
            throw new IllegalArgumentException("Invalid time format, expected 6 digits: " + tradeTime);
        }

        // 解析HHMMSS
        info.hour = Integer.parseInt(timeStr.substring(0, 2));
        info.minute = Integer.parseInt(timeStr.substring(2, 4));
        info.second = Integer.parseInt(timeStr.substring(4, 6));
        info.totalSeconds = info.hour * 3600 + info.minute * 60 + info.second;

        return info;
    }

    /**
     * 判断是否为交易时间（9:30:00 - 15:00:00）
     */
    public static boolean isTradingTime(long tradeTime) {
        try {
            TimeInfo info = parseTradeTime(tradeTime);

            // 上午交易时段：9:30:00 - 11:30:00
            int morningStart = 9 * 3600 + 30 * 60;   // 9:30:00
            int morningEnd = 11 * 3600 + 30 * 60;    // 11:30:00

            // 下午交易时段：13:00:00 - 15:00:00
            int afternoonStart = 13 * 3600;          // 13:00:00
            int afternoonEnd = 15 * 3600;            // 15:00:00

            boolean inMorning = info.totalSeconds >= morningStart && info.totalSeconds <= morningEnd;
            boolean inAfternoon = info.totalSeconds >= afternoonStart && info.totalSeconds <= afternoonEnd;

            return inMorning || inAfternoon;
        } catch (Exception e) {
            System.err.println("Error parsing time: " + tradeTime + " - " + e.getMessage());
            return false;
        }
    }

    /**
     * 将时间格式化为HH:MM:SS
     */
    public static String formatTime(long tradeTime) {
        TimeInfo info = parseTradeTime(tradeTime);
        return String.format("%02d:%02d:%02d", info.hour, info.minute, info.second);
    }

    /**
     * 获取时间戳的排序键（用于分区和排序）
     */
    public static long getTimeSortKey(long tradeTime) {
        TimeInfo info = parseTradeTime(tradeTime);
        // 转换为从9:30开始的分钟数
        int minutesFromStart;

        if (info.hour < 12) {
            // 上午时段：从9:30开始
            minutesFromStart = (info.hour - 9) * 60 + (info.minute - 30);
        } else {
            // 下午时段：从13:00开始，但要考虑午间休市
            minutesFromStart = (info.hour - 13) * 60 + info.minute + 120; // 120 = 11:30-13:00的120分钟
        }

        return minutesFromStart * 100 + info.second; // 保留秒数信息
    }

    /**
     * 时间信息结构体
     */
    public static class TimeInfo {
        public int hour;
        public int minute;
        public int second;
        public int totalSeconds;

        @Override
        public String toString() {
            return String.format("%02d:%02d:%02d", hour, minute, second);
        }
    }
}
