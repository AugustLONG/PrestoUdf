import com.facebook.presto.spi.Plugin;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class UdfPlugin implements Plugin {

    @Override
    public Set<Class<?>> getFunctions() {
        return ImmutableSet.<Class<?>>builder()
                .add(aggregation.AggregationLCCount.class)
                .add(aggregation.AggregationLCSum.class)
                .add(aggregation.AggregationLDCount.class)
                .add(aggregation.AggregationLDSum.class)
                .build();
    }
}
