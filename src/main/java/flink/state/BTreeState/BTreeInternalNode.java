package flink.state.BTreeState;

import flink.state.BTreeState.serializers.DeepCloneable;

public class BTreeInternalNode<T extends DeepCloneable> implements DeepCloneable {
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

    @Override
    public Object clone() {
        return new BTreeInternalNode<T>((T)this.key.clone(), (PageId)this.childPage.clone());
    }
}
