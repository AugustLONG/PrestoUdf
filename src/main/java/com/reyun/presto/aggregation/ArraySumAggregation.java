package com.reyun.presto.aggregation;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.*;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.StandardTypes;
import com.reyun.presto.aggregation.state.LongListState;
import com.reyun.presto.utils.ArrayUtils;
import io.airlift.log.Logger;
import javax.annotation.Nullable;

import java.util.List;

/**
 * Created by jake on 16/10/27.
 */
@AggregationFunction("array_sum")
public class ArraySumAggregation {

    private static Logger log = Logger.get(ArraySumAggregation.class);

    @InputFunction
    public static void input(LongListState state, @Nullable @SqlType("array(" + StandardTypes.BIGINT + ")") Block block) {
        if(block != null) {
            List<Long> list = ArrayUtils.blockListLongOf(block);
            int listSize = list.size();
            if(state.getList().isEmpty()) {
                state.getList().addAll(list);
            } else {
                int statListSize = state.getList().size();
                if(listSize == statListSize) {
                    for(int i = 0; i < listSize; i ++) {
                        state.getList().set(i, state.getList().get(i) + list.get(i));
                    }
                } else {
                    throw new PrestoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "array input num must be same.");
                }
            }
        }
    }

    @CombineFunction
    public static void combine(LongListState state, LongListState otherState) {

        int listSize = state.getList().size();
        int otherListSize = otherState.getList().size();

        if(listSize > 0 || otherListSize > 0) {
            if(listSize == 0 && otherListSize > 0) {
                state.getList().addAll(otherState.getList());
            } else if(listSize > 0 && otherListSize > 0 && otherListSize == listSize) {
                for(int i = 0; i < listSize; i ++) {
                    Long l1 = state.getList().get(i);
                    Long l2 = otherState.getList().get(i);
                    state.getList().set(i, l1 + l2);
                }
            }
        }

    }

    /**
     * export Bigint
     */
    @OutputFunction("array(" + StandardTypes.BIGINT + ")")
    public static void output(LongListState state, BlockBuilder out) {
        if(state.getList().isEmpty()) {
            out.appendNull();
        } else {
            log.debug("state now " + ArrayUtils.ListLongStringOf(state.getList()));
            Block block = ArrayUtils.arrayLongBlockOf(state.getList(), BigintType.BIGINT);
            out.writeObject(block);
            out.closeEntry();
        }
    }

}
