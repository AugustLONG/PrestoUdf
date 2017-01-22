package com.reyun.presto.utils;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.spi.type.TypeUtils.readNativeValue;


/**
 * Created by jake on 16/10/27.
 */
public class ArrayUtils {

    public static Block arrayLongBlockOf(List<Long> values, Type elementType) {
        BlockBuilder blockBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), values.size());
        for(Long value : values) {
            elementType.writeLong(blockBuilder, value);
        }
        return blockBuilder.build();
    }

    public static List<Long> blockListLongOf(Block block) {
        List<Long> ret = new ArrayList<Long>();
        int positionCount = block.getPositionCount();
        for(int i = 0; i < positionCount; i ++) {
            ret.add((Long) readNativeValue(BigintType.BIGINT, block, i));
        }
        return ret;
    }

    public static String ListLongStringOf(List<Long> list) {
        StringBuilder sb = new StringBuilder();
        list.forEach(item -> sb.append(item).append("|"));
        return sb.toString();
    }

    public static String ListStringOf(List<String> list) {
        StringBuilder sb = new StringBuilder();
        list.forEach(item -> sb.append(item).append("|"));
        return sb.toString();
    }

    public static String ListIntegerStringOf(List<Integer> list) {
        StringBuilder sb = new StringBuilder();
        list.forEach(item -> sb.append(item).append("|"));
        return sb.toString();
    }

}
