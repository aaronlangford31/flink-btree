package flink.state.BTreeState;

public class BTreeInternalNode<T> {
    private T key;
    private PageId childPage;

    public BTreeInternalNode(T key, PageId childPage) {
        this.key = key;
        this.childPage = childPage;
    }

    public T getKey() {
        return key;
    }

    public PageId getChildPage() {
        return childPage;
    }

    public void setKey(T key) {
        this.key = key;
    }
}
