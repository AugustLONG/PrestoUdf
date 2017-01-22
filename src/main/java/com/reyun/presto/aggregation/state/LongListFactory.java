package com.reyun.presto.aggregation.state;

import com.facebook.presto.array.ObjectBigArray;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.GroupedAccumulatorState;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jake on 16/10/27.
 */
public class LongListFactory implements AccumulatorStateFactory {

    private static final long ARRAY_LIST_SIZE = ClassLayout.parseClass(ArrayList.class).instanceSize();

    @Override
    public Object createSingleState() {
        return new SingleLongListFactory();
    }

    @Override
    public Class getSingleStateClass() {
        return SingleLongListFactory.class;
    }

    @Override
    public Object createGroupedState() {
        return new GroupedLongListFactory();
    }

    @Override
    public Class getGroupedStateClass() {
        return GroupedLongListFactory.class;
    }


    public static class GroupedLongListFactory implements GroupedAccumulatorState, LongListState {

        private final ObjectBigArray<List<Long>> lists = new ObjectBigArray<>();

        private long memoryUsage;
        private long groupId;

        @Override
        public void setGroupId(long groupId) {
            this.groupId = groupId;
        }

        @Override
        public void ensureCapacity(long size) {
            lists.ensureCapacity(size);
        }

        @Override
        public void addMemoryUsage(int memory) {
            memoryUsage += memory;
        }

        @Override
        public List<Long> getList() {
            if(lists.get(groupId) == null) {
                lists.set(groupId, new ArrayList<Long>());
                memoryUsage += ARRAY_LIST_SIZE;
            }
            return lists.get(groupId);
        }

        @Override
        public long getEstimatedSize() {
            return memoryUsage;
        }
    }

    public static class SingleLongListFactory implements LongListState {

        private final List<Long> list = new ArrayList<>();

        private int memoryUsage;

        @Override
        public void addMemoryUsage(int memory) {
            memoryUsage += memory;
        }

        @Override
        public List<Long> getList() {
            return list;
        }

        @Override
        public long getEstimatedSize() {
            return memoryUsage + ARRAY_LIST_SIZE;
        }
    }

}
