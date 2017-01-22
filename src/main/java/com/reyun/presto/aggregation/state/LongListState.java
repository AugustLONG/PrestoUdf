package com.reyun.presto.aggregation.state;

import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateMetadata;

import java.util.List;

/**
 * Created by jake on 16/10/27.
 */
@AccumulatorStateMetadata(stateSerializerClass = LongListSerializer.class, stateFactoryClass = LongListFactory.class)
public interface LongListState  extends AccumulatorState {

    /**
     * memory
     * @param memory
     */
    void addMemoryUsage(int memory);

    /**
     * 获取 list
     * @return
     */
    List<Long> getList();

}
