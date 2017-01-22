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
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.util.*;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

/**
 * Created by jake on 16/10/25.
 */
public class FunnelStateSerializer implements AccumulatorStateSerializer<FunnelState> {

    private final static Logger log = Logger.get(FunnelStateSerializer.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();

    private static final String ACTIONLISTS = "actionLists";
    private static final String FUNNELSTEPS = "funnelsteps";
    private static final String FUNNELSET = "funnelset";

    @Override
    public Type getSerializedType() {
        return VARCHAR;
    }

    @Override
    public void serialize(FunnelState state, BlockBuilder out) {
        Map<String, Object> jsonState = new HashMap<>();
        jsonState.put(ACTIONLISTS, state.getActionLists());
        jsonState.put(FUNNELSTEPS, state.getFunnelSteps());
        jsonState.put(FUNNELSET, state.getFunnelSets());
        log.debug("actions_List_size_serialize : " + state.getActionLists().size());
        try {
            VARCHAR.writeSlice(out, Slices.utf8Slice(OBJECT_MAPPER.writeValueAsString(jsonState)));
        } catch (JsonProcessingException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void deserialize(Block block, int index, FunnelState state) {
        Slice slice = VARCHAR.getSlice(block, index);
        Map<String, Object> jsonState;
        try {
            jsonState = OBJECT_MAPPER.readValue(slice.getBytes(), new TypeReference<Map<String, Object>>() {});
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        state.addMemoryUsage(slice.length());
        // debug mode
        log.debug("action_list_size_deserialize is : " + state.getActionLists().size());
        // add to exists funnel state
        Map<String, HashSet<Integer>> source = state.getActionLists();
        //source.clear();
        Map<String, HashSet<Integer>> target =  (Map<String, HashSet<Integer>>) jsonState.getOrDefault(ACTIONLISTS, new HashMap<String, HashSet<Integer>>());
        source.clear();
        source.putAll(target);

        if(state.getFunnelSteps().isEmpty()) {
            state.getFunnelSteps().addAll((List<ArrayList<String>>) jsonState.getOrDefault(FUNNELSTEPS, new ArrayList<ArrayList<String>>()));
        }
        if(state.getFunnelSets().isEmpty()) {
            state.getFunnelSets().addAll((List<String>) jsonState.getOrDefault(FUNNELSET, new HashSet<String>()));
        }
    }
}
