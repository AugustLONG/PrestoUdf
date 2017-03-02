package aggregation;

import com.facebook.presto.operator.aggregation.state.SliceState;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.function.*;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.*;

/*
计算漏斗的聚合函数, 步骤一

查询12月1号到20号20天, 时间窗口为3天的漏斗:
select ld_sum(temp, 3) from(
select ld_count(xwhen, 3*86400000, xwhat, 'A,B,C') as temp from tablename
where ds >= '2016-12-01' and ds <= '2016-12-20' and xwhen >= m and xwhen < n and xwhat in ('A', 'B', 'C')
group by xwho);
 */
@AggregationFunction("ld_count")
public class AggregationLDCount extends AggregationBase {

    private static final int COUNT_FLAG_LENGTH = 8;     // 状态slice最前边的2位存放临时变量, 每个临时变量都为int类型
    private static final int COUNT_ONE_LENGTH = 5;      // input中每个事件所占位数, 包含一个int(时间戳)和一个byte(事件下标)

    @InputFunction
    public static void input(SliceState state,                                  // 每个用户的状态
                             @SqlType(StandardTypes.BIGINT) long xwhen,         // 当前事件的时间戳, 精确到毫秒
                             @SqlType(StandardTypes.BIGINT) long win_length,    // 当前查询的时间窗口大小, 精确到毫秒
                             @SqlType(StandardTypes.VARCHAR) Slice xwhat,       // 当前事件的名称, A还是B或者C
                             @SqlType(StandardTypes.VARCHAR) Slice events) {    // 当前查询的全部事件, 逗号分隔
        // 获取状态
        Slice slice = state.getSlice();

        // 判断是否需要初始化events
        if (!event_pos_dict.containsKey(events)) {
            init_events(events);
        }

        // 计算
        if (null == slice) {
            // 初始化slice
            slice = Slices.allocate(COUNT_FLAG_LENGTH + COUNT_ONE_LENGTH);

            // 初始化前2位int类型临时变量: {窗口大小, 事件个数}
            slice.setInt(0, (int) win_length);
            slice.setInt(4, event_pos_dict.get(events).size());

            // 更改状态变量
            slice.setInt(COUNT_FLAG_LENGTH, (int) xwhen);
            slice.setByte(COUNT_FLAG_LENGTH + 4, event_pos_dict.get(events).get(xwhat));

            // 返回结果
            state.setSlice(slice);
        } else {
            int slice_length = slice.length();

            // 新建slice, 并初始化
            Slice new_slice = Slices.allocate(slice_length + COUNT_ONE_LENGTH);
            new_slice.setBytes(0, slice.getBytes());

            // 更改状态变量
            new_slice.setInt(slice_length, (int) xwhen);
            new_slice.setByte(slice_length + 4, event_pos_dict.get(events).get(xwhat));

            // 返回结果
            state.setSlice(new_slice);
        }
    }

    @CombineFunction
    public static void combine(SliceState state, SliceState otherState) {
        // 获取状态
        Slice slice = state.getSlice();
        Slice otherslice = otherState.getSlice();

        // 更新状态, 并返回结果
        if (null == slice) {
            state.setSlice(otherslice);
        } else {
            // 初始化
            Slice slice_new = Slices.allocate(slice.length() + otherslice.length() - COUNT_FLAG_LENGTH);

            // 赋值
            slice_new.setBytes(0, slice.getBytes());
            slice_new.setBytes(slice.length(), otherslice.getBytes(), COUNT_FLAG_LENGTH, otherslice.length() - COUNT_FLAG_LENGTH);

            // 返回结果
            state.setSlice(slice_new);
        }
    }

    @OutputFunction(StandardTypes.INTEGER)
    public static void output(SliceState state, BlockBuilder out) {
        // 获取状态
        Slice slice = state.getSlice();

        // 数据为空，返回0
        if (null == slice) {
            out.writeInt(0);
            out.closeEntry();
            return;
        }

        // 添加临时变量
        boolean is_a = false;

        // 构造列表和字典, 为排序做准备
        List<Integer> time_array = new ArrayList<>();
        Map<Integer, Byte> time_xwhat_map = new HashMap<>();
        for (int i = COUNT_FLAG_LENGTH; i < slice.length(); i += COUNT_ONE_LENGTH) {
            // 获取事件的时间戳和对应的事件
            int timestamp = slice.getInt(i);
            byte xwhat = slice.getByte(i + 4);

            // 更新临时变量
            if ((!is_a) && xwhat == 0) {
                is_a = true;
            }

            // 赋值time_array和time_xwhat_map
            time_array.add(timestamp);
            time_xwhat_map.put(timestamp, xwhat);
        }

        // 判断是否符合要求
        if (!is_a) {
            out.writeInt(0);
            out.closeEntry();
            return;
        }

        // 排序时间戳数组, 这里可能比较耗时
        Collections.sort(time_array);

        // 获取中间变量
        int win_length = slice.getInt(0);
        int events_count = slice.getInt(4);

        // 遍历时间戳数据, 也就是遍历有序事件, 并构造结果
        int max_xwhat_index = 0;
        List<int[]> temp = new ArrayList<>();
        for (int timestamp: time_array) {
            // 事件有序进入
            byte xwhat = time_xwhat_map.get(timestamp);
            if (xwhat == 0) {
                // 新建临时对象, 存放 (A事件的时间戳, 当前最后一个事件的下标)
                int[] flag = {timestamp, xwhat};
                temp.add(flag);
            } else {
                // 更新临时对象: 从后往前, 并根据条件适当跳出
                for (int i = temp.size() - 1; i >= 0; --i) {
                    int[] flag = temp.get(i);
                    if ((timestamp - flag[0]) >= win_length) {
                        // 当前事件的时间戳减去flag[0]超过时间窗口不合法, 跳出
                        break;
                    } else if (xwhat == (flag[1] + 1)) {
                        // 当前事件为下一个事件, 更新数据并跳出
                        flag[1] = xwhat;
                        if (max_xwhat_index < xwhat) {
                            max_xwhat_index = xwhat;
                        }
                        break;
                    }
                }

                // 漏斗流程结束, 提前退出
                if ((max_xwhat_index + 1) == events_count) {
                    break;
                }
            }
        }

        // 返回结果
        out.writeInt(max_xwhat_index + 1);
        out.closeEntry();
    }
}
