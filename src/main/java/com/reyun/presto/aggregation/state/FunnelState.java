package com.reyun.presto.aggregation.state;

import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateMetadata;

import java.util.*;

/**
 * Created by jake on 16/10/25.
 */
@AccumulatorStateMetadata(stateSerializerClass = FunnelStateSerializer.class, stateFactoryClass = FunnelStateFactory.class)
public interface FunnelState extends AccumulatorState {

    /**
     * memory
     * @param memory
     */
    void addMemoryUsage(int memory);


    /**
     * get funnel action list, action is key, timestamp is array list.
     * @return
     */
    Map<String, HashSet<Integer>> getActionLists();

    /**
     * Funnel steps
     * @return
     */
    List<ArrayList<String>> getFunnelSteps();

    /**
     * funnel set
     * @return
     */
    Set<String> getFunnelSets();


}
