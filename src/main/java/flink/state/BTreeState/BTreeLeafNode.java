package flink.state.BTreeState;

public class BTreeLeafNode<K, V> {
    private K key;
    private V value;

    public BTreeLeafNode(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    public void setValue(V value) {
        this.value = value;
    }
}
