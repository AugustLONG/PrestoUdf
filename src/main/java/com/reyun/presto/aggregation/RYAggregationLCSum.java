package com.reyun.presto.aggregation;

import com.facebook.presto.operator.aggregation.state.SliceState;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.function.*;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.spi.type.TypeUtils.readNativeValue;

/*
计算留存(日留存、周留存、月留存)的聚合函数, 步骤二

实例: select lc_sum(temp, *, *) from (...);
 */
@AggregationFunction("lc_sum")
public class RYAggregationLCSum extends RYAggregationBase {

    @InputFunction
    public static void input(SliceState state,
                             @SqlType("array(" + StandardTypes.BIGINT + ")") Block xwho_count,  // 每个用户的状态
                             @SqlType(StandardTypes.INTEGER) long base_length,      // 当前查询的base长度(15, 12, 6)
                             @SqlType(StandardTypes.INTEGER) long sub_length) {     // 当前查询的sub长度(30, 8, 3)
        // 获取状态
        Slice slice = state.getSlice();

        // 初始化state, 长度为(base_length * sub_length + base_length)个int
        if (null == slice) {
            slice = Slices.allocate((int) (base_length * sub_length + base_length) * 4);
        }

        // 获取值
        long base_value = (long) readNativeValue(BigintType.BIGINT, xwho_count, 0);
        long sub_value = (long) readNativeValue(BigintType.BIGINT, xwho_count, 1);

        // 计算状态
        for (int i = 0; i < base_length; ++i) {
            // 判断是否更改base计数
            if ((base_value & bit_array_long.get(i)) != 0) {
                // 第一个事件在某一天存在, 更改base计数
                int base_index = (int) (base_length * sub_length + i) * 4;
                slice.setInt(base_index, slice.getInt(base_index) + 1);

                // 判断是否更改sub计数
                for (int j = i; j < i + sub_length; ++j) {
                    if ((sub_value & bit_array_long.get(j)) != 0) {
                        // 第二个事件在某一天存在, 更改sub计数
                        int sub_index = (int) (i * sub_length + (j - i)) * 4;
                        slice.setInt(sub_index, slice.getInt(sub_index) + 1);
                    }
                }
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

        // 构造结果: base_length日/周/月中每日/周/月的sub_length留存数, 最后为base_length日/周/月的总用户数
        BlockBuilder blockBuilder = BigintType.BIGINT.createBlockBuilder(new BlockBuilderStatus(), slice.length() / 4);
        for (int index = 0; index < slice.length(); index += 4) {
            BigintType.BIGINT.writeLong(blockBuilder, slice.getInt(index));
        }

        // 返回结果
        out.writeObject(blockBuilder.build());
        out.closeEntry();
    }
}
