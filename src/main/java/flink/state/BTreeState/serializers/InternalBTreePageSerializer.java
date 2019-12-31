package flink.state.BTreeState.serializers;

import flink.state.BTreeState.InternalBTreePage;
import flink.state.BTreeState.PageType;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;

public class InternalBTreePageSerializer<K extends Comparable & DeepCloneable> extends TypeSerializer<InternalBTreePage<K>> {
    private int pageCapacity;

    public InternalBTreePageSerializer(int pageCapacity) {
        this.pageCapacity = pageCapacity;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<InternalBTreePage<K>> duplicate() {
        return this;
    }

    @Override
    public InternalBTreePage<K> createInstance() {
        // It is super important that the only thing that should be created here
        // is a root page.
        // The type system shouldn't create any other pages.
        return new InternalBTreePage<K>(PageType.LEAF, this.pageCapacity);
    }

    @Override
    public InternalBTreePage<K> copy(InternalBTreePage<K> from) {
        return (InternalBTreePage<K>) from.clone();
    }

    @Override
    public InternalBTreePage<K> copy(InternalBTreePage<K> from, InternalBTreePage<K> reuse) {
        return this.copy(from);
    }

    @Override
    public int getLength() {
        // This is a variable length structure (for now)
        return -1;
    }

    @Override
    public void serialize(InternalBTreePage<K> record, DataOutputView target) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public InternalBTreePage<K> deserialize(DataInputView source) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public InternalBTreePage<K> deserialize(InternalBTreePage<K> reuse, DataInputView source) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public boolean equals(Object obj) {
        throw new NotImplementedException();
    }

    @Override
    public int hashCode() {
        throw new NotImplementedException();
    }

    @Override
    public TypeSerializerSnapshot<InternalBTreePage<K>> snapshotConfiguration() {
        throw new NotImplementedException();
    }
}
