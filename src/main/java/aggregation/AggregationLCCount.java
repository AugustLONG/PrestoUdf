package aggregation;

import com.facebook.presto.operator.aggregation.state.SliceState;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.function.*;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.StandardTypes;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

/*
计算留存(日留存、周留存、月留存)的聚合函数, 步骤一
限制条件:
1. 日留存最大 15 * 30
2. 周留存最大 12 * 8
3. 月留存最大 6 * 3

查询12月1号开始7天, 往后推30天的日留存
select lc_sum(temp, 7, 30) from(
select lc_count(
date_diff('day', from_iso8601_timestamp('2007-01-01'), from_unixtime(xwhen)),
date_diff('day', from_iso8601_timestamp('2007-01-01'), from_iso8601_timestamp('2016-12-01')),
7, 30, xwhat, 'Action,payment') as temp from tablename
where (ds >= '2016-12-01' and ds < '2016-12-08' and xwhat = 'Action') or
    (ds >= '2016-12-02' and ds < '2016-12-29' and xwhat = 'payment')
group by xwho);

查询12月5号开始3周, 往后推2周的周留存
select lc_sum(temp, 3, 2) from(
select lc_count(
date_diff('week', from_iso8601_timestamp('2007-01-01'), from_unixtime(xwhen)),
date_diff('week', from_iso8601_timestamp('2007-01-01'), from_iso8601_timestamp('2016-12-05')),
3, 2, xwhat, 'Action,payment') as temp from tablename
where (ds >= '2016-12-05' and ds < '2016-12-26' and xwhat = 'Action') or
    (ds >= '2016-12-12' and ds < '2016-12-29' and xwhat = 'payment')
group by xwho);
 */
@AggregationFunction("lc_count")
public class AggregationLCCount extends AggregationBase {

    private static final int FIRST = 2;
    private static final int SECOND = 8;
    private static final int INDEX = FIRST;

    @InputFunction
    public static void input(SliceState state,                                      // 每个用户的状态
                             @SqlType(StandardTypes.BIGINT) long xwhen,             // 当前事件的时间戳距离某固定日期的差值
                             @SqlType(StandardTypes.BIGINT) long xwhen_start,       // 当前查询的起始日期距离某固定日期的差值
                             @SqlType(StandardTypes.INTEGER) long base_length,      // 当前查询的base长度(15天, 12周, 6月)
                             @SqlType(StandardTypes.INTEGER) long sub_length,       // 当前查询的sub长度(30天, 8周, 3月)
                             @SqlType(StandardTypes.VARCHAR) Slice xwhat,           // 当前事件的名称, A还是B
                             @SqlType(StandardTypes.VARCHAR) Slice events) {        // 当前查询的全部事件, 逗号分隔
        // 获取状态
        Slice slice = state.getSlice();

        // 判断是否需要初始化events
        if (!event_pos_dict.containsKey(events)) {
            init_events(events);
        }

        // 初始化某一个用户的state, 分别存放不同事件在每个时间段的标示
        if (null == slice) {
            slice = Slices.allocate(FIRST + SECOND);
        }

        int xwhat_index = event_pos_dict.get(events).get(xwhat);
        if (xwhat_index == 0) {
            int xindex_max = (int) base_length - 1;

            // 获取用户在当前index的状态
            short current_value = slice.getShort(0);
            if (current_value >= max_value_array_short.get(xindex_max)) {
                return;
            }

            // 获取下标
            int xindex = (int) (xwhen - xwhen_start);
            if (xindex < 0 || xindex > xindex_max) {
                return;
            }

            // 更新状态
            slice.setShort(0, current_value | bit_array_short.get(xindex));
        } else {
            int xindex_max = (int) (base_length + sub_length - 1) - 1;

            // 获取用户在当前index的状态
            long current_value = slice.getLong(INDEX);
            if (current_value >= max_value_array_long.get(xindex_max)) {
                return;
            }

            // 获取下标
            int xindex = (int) (xwhen - (xwhen_start + 1));
            if (xindex < 0 || xindex > xindex_max) {
                return;
            }

            // 更新状态
            slice.setLong(INDEX, current_value | bit_array_long.get(xindex));
        }

        // 返回结果
        state.setSlice(slice);
    }

    @CombineFunction
    public static void combine(SliceState state, SliceState otherState) {
        // 获取状态
        Slice slice = state.getSlice();
        Slice otherslice = otherState.getSlice();

        // 更新状态并返回结果
        if (null == slice) {
            state.setSlice(otherslice);
        } else {
            slice.setShort(0, slice.getShort(0) | otherslice.getShort(0));
            slice.setLong(INDEX, slice.getLong(INDEX) | otherslice.getLong(INDEX));
            state.setSlice(slice);
        }
    }

    @OutputFunction("array(" + StandardTypes.BIGINT + ")")
    public static void output(SliceState state, BlockBuilder out) {
        // 获取状态
        Slice slice = state.getSlice();

        // 构造结果: 当前用户在第一个事件中每一天(周/月)的状态, 和在第二个事件中每一天(周/月)的状态
        BlockBuilder blockBuilder = BigintType.BIGINT.createBlockBuilder(new BlockBuilderStatus(), 2);
        if (null == slice) {
            BigintType.BIGINT.writeLong(blockBuilder, 0);
            BigintType.BIGINT.writeLong(blockBuilder, 0);
        } else {
            BigintType.BIGINT.writeLong(blockBuilder, slice.getShort(0));
            BigintType.BIGINT.writeLong(blockBuilder, slice.getLong(INDEX));
        }

        // 返回结果
        out.writeObject(blockBuilder.build());
        out.closeEntry();
    }
}
