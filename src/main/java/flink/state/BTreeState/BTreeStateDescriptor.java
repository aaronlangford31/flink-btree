package flink.state.BTreeState;

import flink.state.BTreeState.serializers.DeepCloneable;
import flink.state.BTreeState.serializers.SerializerFactory;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class BTreeStateDescriptor<K extends Comparable & DeepCloneable, V> {
    private final String stateName;
    private final TypeInformation<K> keyTypeInformation;
    private final TypeInformation<V> valueTypeInformation;
    private final SerializerFactory<K, V> serializerFactory;

    public BTreeStateDescriptor(String stateName,
                                TypeInformation<K> keyTypeInformation,
                                TypeInformation<V> valueTypeInformation
    ) {
        this.stateName = stateName;
        this.keyTypeInformation = keyTypeInformation;
        this.valueTypeInformation = valueTypeInformation;

        this.serializerFactory = new SerializerFactory<>();
    }

    public ValueStateDescriptor<InternalBTreePage<K>> getRootPageDescriptor() {
        return new ValueStateDescriptor<>(
                this.stateName + "_rootPage",
                TypeInformation.of(new TypeHint<InternalBTreePage<K>>() {
                })
        );
    }

    public MapStateDescriptor<PageId, InternalBTreePage<K>> getInternalPageMapDescriptor() {
        return new MapStateDescriptor<>(
                this.stateName + "_internalPages",
                TypeInformation.of(PageId.class),
                TypeInformation.of(new TypeHint<InternalBTreePage<K>>() {
                })
        );
    }

    public MapStateDescriptor<PageId, LeafBTreePage<K, V>> getLeafPageMapDescriptor() {
        return new MapStateDescriptor<>(
                this.stateName + "_leafPages",
                TypeInformation.of(PageId.class),
                TypeInformation.of(new TypeHint<LeafBTreePage<K, V>>() {
                })
        );
    }

    public ValueStateDescriptor<PageFactory<K, V>> getPageFactoryDescriptor() {
        return new ValueStateDescriptor<>(
                this.stateName + "_pageFactory",
                TypeInformation.of(new TypeHint<PageFactory<K, V>>() {
                })
        );
    }
}
