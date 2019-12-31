package flink.state.BTreeState;

import flink.state.BTreeState.serializers.DeepCloneable;

import java.util.Objects;

public class PageId implements DeepCloneable {
    private long id;

    public static PageId getRootPageId() {
        return new PageId(0);
    }

    public PageId(long id) {
        this.id = id;
    }

    public PageId() { }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public boolean isRootPage() {
        return this.id == PageId.getRootPageId().id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PageId pageId = (PageId) o;
        return id == pageId.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public Object clone() {
        return new PageId(this.id);
    }
}
