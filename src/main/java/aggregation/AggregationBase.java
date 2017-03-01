package aggregation;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.*;

/*
漏斗及留存的基类
 */
public class AggregationBase {

    // 日志记录
    public static final Logger logger = Logger.get(AggregationBase.class);

    // 静态数值, 一个byte的长度
    public static final int MAX_COUNT_BYTE = 7;

    // 静态数值, 一个short的长度
    public static final int MAX_COUNT_SHORT = 15;

    // 静态数值, 一个int的长度
    public static final int MAX_COUNT_INT = 31;

    // 静态数值, 一个long的长度
    public static final int MAX_COUNT_LONG = 63;

    // 用于判断某一位为0或1的变量(byte类型): [1, 2, 4, ..., 64]
    public static List<Byte> bit_array_byte = new ArrayList<>();

    // 用于判断某一位为0或1的变量(short类型): [1, 2, 4, ..., 64, ...]
    public static List<Short> bit_array_short = new ArrayList<>();

    // 用于判断某一位为0或1的变量(int类型): [1, 2, 4, ..., 64, ...]
    public static List<Integer> bit_array_int = new ArrayList<>();

    // 用于判断某一位为0或1的变量(long类型): [1, 2, 4, ..., 64, ...]
    public static List<Long> bit_array_long = new ArrayList<>();

    // 查询的最大值(byte类型), 用于提前退出 [1, 3, 7, 15, ..., 127]
    public static List<Byte> max_value_array_byte = new ArrayList<>();

    // 查询的最大值(short类型), 用于提前退出 [1, 3, 7, 15, ..., 127, ...]
    public static List<Short> max_value_array_short = new ArrayList<>();

    // 查询的最大值(int类型), 用于提前退出 [1, 3, 7, 15, ..., 127, ...]
    public static List<Integer> max_value_array_int = new ArrayList<>();

    // 查询的最大值(long类型), 用于提前退出 [1, 3, 7, 15, ..., 127, ...]
    public static List<Long> max_value_array_long = new ArrayList<>();

    // 初始化变量
    static {
        // ---- bit_array ----
        for (int i = 0; i < MAX_COUNT_BYTE; ++i) {
            bit_array_byte.add((byte) (1 << i));
        }

        for (int i = 0; i < MAX_COUNT_SHORT; ++i) {
            bit_array_short.add((short) (1 << i));
        }

        for (int i = 0; i < MAX_COUNT_INT; ++i) {
            bit_array_int.add((1 << i));
        }

        for (int i = 0; i < MAX_COUNT_LONG; ++i) {
            bit_array_long.add((1L << i));
        }

        // ---- max_value_array ----
        byte max_value_byte = 0;
        for (int i = 0; i < MAX_COUNT_BYTE; ++i) {
            max_value_byte += (byte) (1 << i);
            max_value_array_byte.add(max_value_byte);
        }

        short max_value_short = 0;
        for (int i = 0; i < MAX_COUNT_SHORT; ++i) {
            max_value_short += (short) (1 << i);
            max_value_array_short.add(max_value_short);
        }

        int max_value_int = 0;
        for (int i = 0; i < MAX_COUNT_INT; ++i) {
            max_value_int += (1 << i);
            max_value_array_int.add(max_value_int);
        }

        long max_value_long = 0;
        for (int i = 0; i < MAX_COUNT_LONG; ++i) {
            max_value_long += (1L << i);
            max_value_array_long.add(max_value_long);
        }
    }

    // 查询漏斗中, 事件和下标的对应关系: {events: {event: index, ...}, ....}
    public static Map<Slice, Map<Slice, Byte>> event_pos_dict = new HashMap<>();

    // 查询漏斗中, 初始化事件(最多7个事件)
    public static void init_events(Slice events) {
        List<String> fs = Arrays.asList(new String(events.getBytes()).split(","));

        Map<Slice, Byte> pos_dict = new HashMap<>();
        for (byte i = 0; i < fs.size(); ++i) {
            pos_dict.put(Slices.utf8Slice(fs.get(i)), i);
        }
        event_pos_dict.put(events, pos_dict);
    }

    public static void main(String argus[]){
    }
}
