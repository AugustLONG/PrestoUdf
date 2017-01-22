package com.reyun.presto.aggregation;

import com.reyun.presto.utils.ArrayUtils;
import io.airlift.log.Logger;

import java.util.*;
import java.util.stream.IntStream;

/**
 * Created by jake on 16/10/25.
 */
public final class FunnelUtils {

    private static final Logger log = Logger.get(FunnelUtils.class);

    private List<String> actions = new ArrayList<>();
    private List<Integer> timestamps = new ArrayList<>();
    private List<ArrayList<String>> funnelSteps;

    public FunnelUtils(Map<String, HashSet<Integer>> actionLists, List<ArrayList<String>> funnelSteps) {
        // change action list to actions list and timestamps list
        for(String step : actionLists.keySet()) {
            HashSet<Integer> stepTimes = actionLists.get(step);
            log.debug("now step : " + step + ", now step size is : " + stepTimes.size());
            for(Integer stepTime : stepTimes) {
                actions.add(step);
                timestamps.add(stepTime);
            }
        }
        this.funnelSteps = funnelSteps;
        log.debug("actions size : " + actions.size());
        log.debug("timestamps size : " + timestamps.size());
        log.debug("funnelsteps size :" + funnelSteps.size());
    }

    public List<Long> computer() {
        int inputSize = actions.size();
        int currentFunnelStep = 0;
        int funnelStepSize = funnelSteps.size();
        List<Long> ret = new ArrayList<>(Collections.nCopies(funnelStepSize, 0L));

        Integer[] sortedIndex = IntStream.rangeClosed(0, actions.size() - 1).boxed().sorted(this::funnelAggregateComparator).toArray(Integer[]::new);

        for(int i = 0; i < inputSize && currentFunnelStep < funnelStepSize; i ++) {
            if(funnelSteps.get(currentFunnelStep).contains(actions.get(sortedIndex[i]))) {
                ret.set(currentFunnelStep, 1L);
                currentFunnelStep ++;
            }
        }

        // debug mode
        log.debug("debug mode :::: actions : " + ArrayUtils.ListStringOf(actions));
        log.debug("debug mode :::: timestamps : " + ArrayUtils.ListIntegerStringOf(timestamps));
        StringBuffer sb = new StringBuffer();
        for(int i = 0; i < sortedIndex.length; i ++) {
            sb.append("|").append(sortedIndex[i]);
        }
        log.debug("debug mode :::: sortedIndex size : " + sortedIndex.length + ", info : " + sb.toString());
        log.debug("debug mode :::: ret : " + ArrayUtils.ListLongStringOf(ret));
        return ret;
    }


    private int funnelAggregateComparator(Integer i1, Integer i2) {
        int ret = ((Comparable) timestamps.get(i1)).compareTo(timestamps.get(i2));
        if(ret == 0) {
            return ((Comparable) actions.get(i1)).compareTo(actions.get(i2));
        }
        return ret;
    }

}
