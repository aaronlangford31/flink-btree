package flink.state.BTreeState.util;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.*;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.metrics.MetricGroup;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockRuntimeContext implements RuntimeContext {
    private HashMap<String, ValueState<?>> existingValueStates;
    private HashMap<String, MapState<?, ?>> existingMapStates;

    public MockRuntimeContext() {
        this.existingValueStates = new HashMap<>();
        this.existingMapStates = new HashMap<>();
    }

    @Override
    public String getTaskName() {
        return null;
    }

    @Override
    public MetricGroup getMetricGroup() {
        return null;
    }

    @Override
    public int getNumberOfParallelSubtasks() {
        return 0;
    }

    @Override
    public int getMaxNumberOfParallelSubtasks() {
        return 0;
    }

    @Override
    public int getIndexOfThisSubtask() {
        return 0;
    }

    @Override
    public int getAttemptNumber() {
        return 0;
    }

    @Override
    public String getTaskNameWithSubtasks() {
        return null;
    }

    @Override
    public ExecutionConfig getExecutionConfig() {
        return null;
    }

    @Override
    public ClassLoader getUserCodeClassLoader() {
        return null;
    }

    @Override
    public <V, A extends Serializable> void addAccumulator(String name, Accumulator<V, A> accumulator) {

    }

    @Override
    public <V, A extends Serializable> Accumulator<V, A> getAccumulator(String name) {
        return null;
    }

    @Override
    public Map<String, Accumulator<?, ?>> getAllAccumulators() {
        return null;
    }

    @Override
    public IntCounter getIntCounter(String name) {
        return null;
    }

    @Override
    public LongCounter getLongCounter(String name) {
        return null;
    }

    @Override
    public DoubleCounter getDoubleCounter(String name) {
        return null;
    }

    @Override
    public Histogram getHistogram(String name) {
        return null;
    }

    @Override
    public boolean hasBroadcastVariable(String name) {
        return false;
    }

    @Override
    public <RT> List<RT> getBroadcastVariable(String name) {
        return null;
    }

    @Override
    public <T, C> C getBroadcastVariableWithInitializer(String name, BroadcastVariableInitializer<T, C> initializer) {
        return null;
    }

    @Override
    public DistributedCache getDistributedCache() {
        return null;
    }

    @Override
    public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
        if (!this.existingValueStates.containsKey(stateProperties.getName())) {
            this.existingValueStates.put(stateProperties.getName(), new InMemoryValueState<T>());
        }
        return (ValueState<T>)this.existingValueStates.get(stateProperties.getName());
    }

    @Override
    public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
        return null;
    }

    @Override
    public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) {
        return null;
    }

    @Override
    public <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(AggregatingStateDescriptor<IN, ACC, OUT> stateProperties) {
        return null;
    }

    @Override
    public <T, ACC> FoldingState<T, ACC> getFoldingState(FoldingStateDescriptor<T, ACC> stateProperties) {
        return null;
    }

    @Override
    public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> stateProperties) {
        if (!this.existingMapStates.containsKey(stateProperties.getName())) {
            this.existingMapStates.put(stateProperties.getName(), new HeapMapState<UK, UV>());
        }

        return (MapState<UK, UV>)this.existingMapStates.get(stateProperties.getName());
    }
}
