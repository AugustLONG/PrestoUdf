package com.reyun.presto.aggregation;


import com.reyun.presto.utils.ArrayUtils;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.*;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.StandardTypes;
import com.reyun.presto.aggregation.state.FunnelState;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;

import javax.annotation.Nullable;
import java.util.*;

import static com.facebook.presto.spi.type.TypeUtils.readNativeValue;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;


/**
 * Created by jake on 16/10/18.
 */

@AggregationFunction("funnel")
public final class FunnelAggregation  {

    private FunnelAggregation() {}

    private static Logger log = Logger.get(FunnelAggregation.class);

    public static void inputBase(FunnelState state, Slice action, long timestamp, Block... steps) {
        if(state.getFunnelSteps().isEmpty()) {
            addFunnelSteps(state, steps);
        }
        String actionString = action.toStringUtf8();
        if(action != null && timestamp > 0 && state.getFunnelSets().contains(actionString)) {
            Map<String, HashSet<Integer>> actionList = state.getActionLists();
            if(!actionList.containsKey(actionString)) {
                actionList.put(actionString, new HashSet<>());
            }
            actionList.get(actionString).add((int) timestamp);
        }
    }

    /**
     * add funnel steps
     * @param state
     * @param steps
     */
    private static void addFunnelSteps(FunnelState state, Block... steps) {
        List<ArrayList<String>> stepAll = new ArrayList<>();
        Set<String> stepSet = new HashSet<>();
        for(int i = 0; i < steps.length; i ++) {
            ArrayList<String> stepItems = new ArrayList<>();
            Block itemBlock = steps[i];
            int positionCount = itemBlock.getPositionCount();
            for(int x = 0; x < positionCount; x ++) {
                Slice stepItem = (Slice) readNativeValue(VARCHAR, itemBlock, x);
                String stepItemString = stepItem.toStringUtf8();
                stepItems.add(stepItemString);
                stepSet.add(stepItemString);
            }
            stepAll.add(stepItems);
        }
        state.getFunnelSteps().addAll(stepAll);
        state.getFunnelSets().addAll(stepSet);
    }


    @CombineFunction
    public static void combine(FunnelState state, FunnelState otherState) {
        log.debug("combine state funnel action list size " + state.getActionLists().size());
        log.debug("combine otherState funnel action list size " + otherState.getActionLists().size());
        // merge data
        Map<String, HashSet<Integer>> source = state.getActionLists();
        Map<String, HashSet<Integer>> target = otherState.getActionLists();

        for(String itemStep : target.keySet()) {
            if(source.containsKey(itemStep)) {
                source.get(itemStep).addAll(target.get(itemStep));
            } else {
                HashSet<Integer> timeList = new HashSet<>();
                timeList.addAll(target.get(itemStep));
                source.put(itemStep, timeList);
            }
        }

        if(state.getFunnelSteps().isEmpty()) {
            state.getFunnelSteps().addAll(otherState.getFunnelSteps());
        }
        if(state.getFunnelSets().isEmpty()) {
            state.getFunnelSets().addAll(otherState.getFunnelSets());
        }
    }

    @OutputFunction("array(" + StandardTypes.BIGINT + ")")
    public static void output(FunnelState state, BlockBuilder out) {
        if(state.getFunnelSteps().isEmpty()) {
            out.appendNull();
        } else {
            FunnelUtils funnelUtil = new FunnelUtils(state.getActionLists(), state.getFunnelSteps());
            Block block = ArrayUtils.arrayLongBlockOf(funnelUtil.computer(), BigintType.BIGINT);
            out.writeObject(block);
            out.closeEntry();
        }
    }


    @InputFunction
    public static void input(FunnelState state, @Nullable @SqlType(StandardTypes.VARCHAR) Slice action,
                             @Nullable @SqlType(StandardTypes.BIGINT) long timestamp,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s1,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s2) {
        inputBase(state, action, timestamp, s1, s2);
    }

    @InputFunction
    public static void input(FunnelState state, @Nullable @SqlType(StandardTypes.VARCHAR) Slice action,
                             @Nullable @SqlType(StandardTypes.BIGINT) long timestamp,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s1,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s2,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s3) {
        inputBase(state, action, timestamp, s1, s2, s3);
    }

    @InputFunction
    public static void input(FunnelState state, @Nullable @SqlType(StandardTypes.VARCHAR) Slice action,
                             @Nullable @SqlType(StandardTypes.BIGINT) long timestamp,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s1,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s2,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s3,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s4) {
        inputBase(state, action, timestamp, s1, s2, s3, s4);
    }

    @InputFunction
    public static void input(FunnelState state, @Nullable @SqlType(StandardTypes.VARCHAR) Slice action,
                             @Nullable @SqlType(StandardTypes.BIGINT) long timestamp,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s1,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s2,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s3,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s4,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s5) {
        inputBase(state, action, timestamp, s1, s2, s3, s4, s5);
    }

    @InputFunction
    public static void input(FunnelState state, @Nullable @SqlType(StandardTypes.VARCHAR) Slice action,
                             @Nullable @SqlType(StandardTypes.BIGINT) long timestamp,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s1,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s2,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s3,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s4,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s5,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s6) {
        inputBase(state, action, timestamp, s1, s2, s3, s4, s5, s6);
    }

    @InputFunction
    public static void input(FunnelState state, @Nullable @SqlType(StandardTypes.VARCHAR) Slice action,
                             @Nullable @SqlType(StandardTypes.BIGINT) long timestamp,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s1,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s2,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s3,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s4,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s5,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s6,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s7) {
        inputBase(state, action, timestamp, s1, s2, s3, s4, s5, s6, s7);
    }

    @InputFunction
    public static void input(FunnelState state, @Nullable @SqlType(StandardTypes.VARCHAR) Slice action,
                             @Nullable @SqlType(StandardTypes.BIGINT) long timestamp,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s1,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s2,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s3,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s4,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s5,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s6,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s7,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s8) {
        inputBase(state, action, timestamp, s1, s2, s3, s4, s5, s6, s7, s8);
    }

    @InputFunction
    public static void input(FunnelState state, @Nullable @SqlType(StandardTypes.VARCHAR) Slice action,
                             @Nullable @SqlType(StandardTypes.BIGINT) long timestamp,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s1,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s2,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s3,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s4,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s5,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s6,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s7,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s8,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s9) {
        inputBase(state, action, timestamp, s1, s2, s3, s4, s5, s6, s7, s9);
    }

    @InputFunction
    public static void input(FunnelState state, @Nullable @SqlType(StandardTypes.VARCHAR) Slice action,
                             @Nullable @SqlType(StandardTypes.BIGINT) long timestamp,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s1,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s2,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s3,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s4,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s5,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s6,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s7,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s8,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s9,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s10) {
        inputBase(state, action, timestamp, s1, s2, s3, s4, s5, s6, s7, s9, s10);
    }














































}
