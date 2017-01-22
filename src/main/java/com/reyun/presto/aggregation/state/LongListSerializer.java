package com.reyun.presto.aggregation.state;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;

/**
 * Created by jake on 16/10/27.
 */
public class LongListSerializer implements AccumulatorStateSerializer<LongListState> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();

    @Override
    public Type getSerializedType() {
        return VARBINARY;
    }

    @Override
    public void serialize(LongListState state, BlockBuilder out) {
        if(state.getList() == null) {
            out.appendNull();
        } else {
            try {
                VARBINARY.writeSlice(out, Slices.utf8Slice(OBJECT_MAPPER.writeValueAsString(state.getList())));
            } catch (JsonProcessingException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    @Override
    public void deserialize(Block block, int index, LongListState state) {
        if(!block.isNull(index)) {
            SliceInput slice = VARBINARY.getSlice(block, index).getInput();
            List<Long> listState;
            try {
                listState = OBJECT_MAPPER.readValue(slice.readSlice(slice.available()).getBytes(), new TypeReference<List<Long>>() {});
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
            state.getList().clear();
            state.getList().addAll((List<Long>) listState);
        }
    }
}
