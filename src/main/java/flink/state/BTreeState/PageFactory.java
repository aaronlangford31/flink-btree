package flink.state.BTreeState;

import flink.state.BTreeState.serializers.DeepCloneable;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

public class PageFactory<K extends Comparable & DeepCloneable, V> {
    private long mostRecentPageId;
    private final int internalPageCapacity;
    private final int leafPageCapacity;

    public PageFactory() {
        this.mostRecentPageId = 0;
        this.internalPageCapacity = 4096;
        this.leafPageCapacity = 4096;
    }

    public InternalBTreePage<K> getEmptyRootPage() {
        return new InternalBTreePage<>(PageId.getRootPageId(),
                PageId.getRootPageId(),
                new ArrayList<>(this.internalPageCapacity),
                PageType.INTERNAL,
                this.internalPageCapacity);
    }

    public InternalBTreePage<K> getNewRootPage(Iterable<InternalBTreePage<K>> childPages) {
        ArrayList<BTreeInternalNode<K>> childKeys = new ArrayList<>();
        for (InternalBTreePage<K> childPage : childPages) {
            childKeys.add(new BTreeInternalNode<>(childPage.getFirstKey(), childPage.getPageId()));
        }

        return new InternalBTreePage<>(PageId.getRootPageId(),
                PageId.getRootPageId(),
                childKeys,
                PageType.INTERNAL,
                this.internalPageCapacity);
    }

    public LeafBTreePage<K, V> getNewLeafPage(PageId parentId, K key, V value) {
        PageId newPageId = this.makeNewPageId();

        ArrayList<BTreeLeafNode<K, V>> nodes = new ArrayList<>(this.leafPageCapacity);
        nodes.add(new BTreeLeafNode<>(key, value));

        return new LeafBTreePage<K, V>(newPageId, parentId, null, null, nodes, this.leafPageCapacity);
    }

    public Tuple2<LeafBTreePage<K, V>, LeafBTreePage<K, V>> split(LeafBTreePage<K, V> page) {
        ArrayList<BTreeLeafNode<K, V>> newNodes = page.split();

        LeafBTreePage<K, V> newPage = this.getNewLeafPage(page.getParentPageId(), newNodes);

        newPage.setLeftSiblingPageId(page.getPageId());
        newPage.setRightSiblingPageId(page.getRightSiblingPageId());
        page.setRightSiblingPageId(newPage.getPageId());

        return Tuple2.of(page, newPage);
    }

    public Tuple2<InternalBTreePage<K>, InternalBTreePage<K>> split(InternalBTreePage<K> page) {
        return this.split(page, false);
    }

    public Tuple2<InternalBTreePage<K>, InternalBTreePage<K>> split(InternalBTreePage<K> page, boolean assignNewIdToLeft) {
        ArrayList<BTreeInternalNode<K>> newNodes = page.split();

        InternalBTreePage<K> newPage = this.getNewInternalPage(page.getParentPageId(), newNodes, page.getChildrenType());

        if (assignNewIdToLeft) {
            page.setParentPageId(this.makeNewPageId());
        }

        return Tuple2.of(page, newPage);
    }

    private LeafBTreePage<K,V> getNewLeafPage(PageId parentId, ArrayList<BTreeLeafNode<K, V>> initialNodes) {
        PageId newPageId = this.makeNewPageId();

        return new LeafBTreePage<>(newPageId, parentId, null, null, initialNodes, this.leafPageCapacity);
    }

    private InternalBTreePage<K> getNewInternalPage(PageId parentPageId, ArrayList<BTreeInternalNode<K>> newNodes, PageType childrenType) {
        PageId newPageId = this.makeNewPageId();

        return new InternalBTreePage<>(newPageId, parentPageId, newNodes, childrenType, this.internalPageCapacity);
    }

    private PageId makeNewPageId() {
        PageId newPageId = new PageId(this.mostRecentPageId + 1);
        this.mostRecentPageId = newPageId.getId();

        return newPageId;
    }
}