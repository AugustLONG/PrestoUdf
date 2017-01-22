package com.reyun.presto;

import com.facebook.presto.spi.Plugin;
import com.google.common.collect.ImmutableSet;
import com.reyun.presto.aggregation.*;

import java.util.Set;

public class UdfPlugin implements Plugin {

    @Override
    public Set<Class<?>> getFunctions() {
        return ImmutableSet.<Class<?>>builder()
                .add(FunnelAggregation.class)
                .add(ArraySumAggregation.class)
                .add(RYAggregationLDCount.class)
                .add(RYAggregationLDSum.class)
                .add(RYAggregationLCCount.class)
                .add(RYAggregationLCSum.class)
                .build();
    }
}
