package flink.state.BTreeState.testutil;

import org.apache.flink.api.common.state.MapState;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

class HeapMapState<UK, UV> implements MapState<UK, UV> {

    private final Map<UK, UV> internalMap;

    HeapMapState() {
        internalMap = new HashMap<>();
    }

    @Override
    public UV get(UK key) throws Exception {
        return internalMap.get(key);
    }

    @Override
    public void put(UK key, UV value) throws Exception {
        internalMap.put(key, value);
    }

    @Override
    public void putAll(Map<UK, UV> map) throws Exception {
        internalMap.putAll(map);
    }

    @Override
    public void remove(UK key) throws Exception {
        internalMap.remove(key);
    }

    @Override
    public boolean contains(UK key) throws Exception {
        return internalMap.containsKey(key);
    }

    @Override
    public Iterable<Map.Entry<UK, UV>> entries() throws Exception {
        return internalMap.entrySet();
    }

    @Override
    public Iterable<UK> keys() throws Exception {
        return internalMap.keySet();
    }

    @Override
    public Iterable<UV> values() throws Exception {
        return internalMap.values();
    }

    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() throws Exception {
        return internalMap.entrySet().iterator();
    }

    @Override
    public void clear() {
        internalMap.clear();
    }
}
