package com.reyun.presto.aggregation;

import com.facebook.presto.operator.aggregation.state.SliceState;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.function.*;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.StandardTypes;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.spi.type.TypeUtils.readNativeValue;

/*
计算漏斗的聚合函数, 步骤二

查询12月1号到20号20天(最大60天), 时间窗口为3天(最大7天)的漏斗:
select ld_sum(temp, 3) from (...);

效率: 5.0秒左右
 */
@AggregationFunction("ld_sum")
public class RYAggregationLDSum extends RYAggregationBase {

    @InputFunction
    public static void input(SliceState state,
                             @SqlType("array(" + StandardTypes.BIGINT + ")") Block xwho_count,  // 每个用户的状态
                             @SqlType(StandardTypes.INTEGER) long events_count) {               // 查询事件的个数
        // 获取state状态
        Slice slice = state.getSlice();

        // 获取int类型的事件个数
        int events_length = (int) events_count;

        // 获取用户状态: [1, 2, 0, 0, 2], 这里的day_length包含总数据
        int day_length = xwho_count.getPositionCount();

        // 初始化state, 长度为(day_length * events_length)个int
        if (null == slice) {
            slice = Slices.allocate(day_length * events_length * 4);
        }

        for(int day = 0; day < day_length; ++day) {
            // 读取每一天的状态, 包含总状态
            long day_status = (long) readNativeValue(BigintType.BIGINT, xwho_count, day);
            for (int status = 0; status < day_status; ++status) {
                // 更新计数
                int index = (day * events_length + status) * 4;
                slice.setInt(index, slice.getInt(index) + 1);
            }
        }

        // 返回状态
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
            for (int index = 0; index < slice.length(); index += 4) {
                slice.setInt(index, slice.getInt(index) + otherslice.getInt(index));
            }
            state.setSlice(slice);
        }
    }

    @OutputFunction("array(" + StandardTypes.BIGINT + ")")
    public static void output(SliceState state, BlockBuilder out) {
        // 获取状态
        Slice slice = state.getSlice();
        if (null == slice) {
            BlockBuilder blockBuilder = BigintType.BIGINT.createBlockBuilder(new BlockBuilderStatus(), 0);
            out.writeObject(blockBuilder.build());
            out.closeEntry();
            return;
        }

        // 构造结果: 每一天的事件漏斗[A:100, B:50, C:10, A:120, ......], 最后为总的事件漏斗
        BlockBuilder blockBuilder = BigintType.BIGINT.createBlockBuilder(new BlockBuilderStatus(), slice.length() / 4);
        for (int index = 0; index < slice.length(); index += 4) {
            BigintType.BIGINT.writeLong(blockBuilder, slice.getInt(index));
        }

        // 返回结果
        out.writeObject(blockBuilder.build());
        out.closeEntry();
    }
}
