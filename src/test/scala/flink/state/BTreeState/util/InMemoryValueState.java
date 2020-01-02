package flink.state.BTreeState.util;

import org.apache.flink.api.common.state.ValueState;

import java.io.IOException;

public class InMemoryValueState<T> implements ValueState<T> {
    private T value = null;

    @Override
    public T value() throws IOException {
        return this.value;
    }

    @Override
    public void update(T value) throws IOException {
        this.value = value;
    }

    @Override
    public void clear() {
        this.value = null;
    }
}
