package com.reyun.presto.aggregation.state;

import com.facebook.presto.array.ObjectBigArray;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.GroupedAccumulatorState;
import io.airlift.log.Logger;
import org.openjdk.jol.info.ClassLayout;

import java.util.*;

/**
 * Created by jake on 16/10/25.
 */
public class FunnelStateFactory implements AccumulatorStateFactory {

    private final static Logger log = Logger.get(FunnelStateFactory.class);

    private static final long ARRAY_LIST_SIZE = ClassLayout.parseClass(ArrayList.class).instanceSize();
    private static final long HASH_SET_SIZE = ClassLayout.parseClass(HashSet.class).instanceSize();

    @Override
    public Object createSingleState() {
        return new SingleFunnelState();
    }

    @Override
    public Class<? extends FunnelState> getSingleStateClass() {
        return SingleFunnelState.class;
    }

    @Override
    public Object createGroupedState() {
        return new GroupedFunnelState();
    }

    @Override
    public Class<? extends FunnelState> getGroupedStateClass() {
        return GroupedFunnelState.class;
    }


    public static class GroupedFunnelState implements GroupedAccumulatorState, FunnelState {

        private final static Logger log = Logger.get(GroupedFunnelState.class);

        private final ObjectBigArray<Map<String, HashSet<Integer>>> actionLists = new ObjectBigArray<>();
        private final ObjectBigArray<List<ArrayList<String>>> funnelSteps = new ObjectBigArray<>();
        private final ObjectBigArray<Set<String>> funnelSets = new ObjectBigArray<>();

        private long groupId;
        private long memoryUsage;

        @Override
        public void setGroupId(long groupId) {
            this.groupId = groupId;
        }

        @Override
        public void ensureCapacity(long size) {
            actionLists.ensureCapacity(size);
            funnelSteps.ensureCapacity(size);
            funnelSets.ensureCapacity(size);
        }

        @Override
        public void addMemoryUsage(int memory) {
            memoryUsage += memory;
        }

        @Override
        public Map<String, HashSet<Integer>> getActionLists() {
            if(actionLists.get(groupId) == null) {
                actionLists.set(groupId, new HashMap<>());
                memoryUsage += ARRAY_LIST_SIZE;
            }
            return actionLists.get(groupId);
        }

        @Override
        public List<ArrayList<String>> getFunnelSteps() {
            if(funnelSteps.get(groupId) == null) {
                funnelSteps.set(groupId, new ArrayList<>());
                memoryUsage += ARRAY_LIST_SIZE;
            }
            return funnelSteps.get(groupId);
        }

        @Override
        public Set<String> getFunnelSets() {
            if(funnelSets.get(groupId) == null) {
                funnelSets.set(groupId, new HashSet<>());
                memoryUsage += HASH_SET_SIZE;
            }
            return funnelSets.get(groupId);
        }


        @Override
        public long getEstimatedSize() {
            return memoryUsage + actionLists.sizeOf() + funnelSteps.sizeOf() + funnelSets.sizeOf();
        }
    }


    public static class SingleFunnelState implements FunnelState {

        private final static Logger log = Logger.get(SingleFunnelState.class);

        private final Map<String, HashSet<Integer>> actionLists = new HashMap<>();
        private final List<ArrayList<String>> funnelSteps = new ArrayList<>();
        private final Set<String> funnelSets = new HashSet<>();

        private int memoryUsage;


        @Override
        public long getEstimatedSize() {
            return memoryUsage + 3 * ARRAY_LIST_SIZE + HASH_SET_SIZE;
        }

        @Override
        public void addMemoryUsage(int memory) {
            memoryUsage += memory;
        }

        @Override
        public Map<String, HashSet<Integer>> getActionLists() {
            return actionLists;
        }


        @Override
        public List<ArrayList<String>> getFunnelSteps() {
            return funnelSteps;
        }

        @Override
        public Set<String> getFunnelSets() {
            return funnelSets;
        }

    }

}
