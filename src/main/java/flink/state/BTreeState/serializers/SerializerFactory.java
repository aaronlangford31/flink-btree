package flink.state.BTreeState.serializers;

import flink.state.BTreeState.InternalBTreePage;
import flink.state.BTreeState.LeafBTreePage;
import flink.state.BTreeState.PageFactory;
import flink.state.BTreeState.PageId;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class SerializerFactory<K extends Comparable & DeepCloneable, V> {

    public TypeSerializer<InternalBTreePage<K>> getInternalBTreePageSerializer() {
        return new InternalBTreePageSerializer<>(4096);
    }

    public TypeSerializer<LeafBTreePage<K, V>> getLeafBTreePageSerializer() {
        throw new NotImplementedException();
    }

    public TypeSerializer<PageId> getPageIdSerializer() {
        throw new NotImplementedException();
    }

    public TypeSerializer<PageFactory<K, V>> getPageFactorySerializer() {
        throw new NotImplementedException();
    }
}
