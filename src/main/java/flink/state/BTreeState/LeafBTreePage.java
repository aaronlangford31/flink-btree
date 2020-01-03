package flink.state.BTreeState;

import flink.state.BTreeState.util.ArrayUtil;
import flink.state.BTreeState.util.Search;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Optional;

public class LeafBTreePage<K extends Comparable, V> {
    private PageId pageId, parentPageId, leftSiblingPageId, rightSiblingPageId;
    private ArrayList<BTreeLeafNode<K, V>> nodes;
    private int capacity;

    public LeafBTreePage(PageId id,
                         PageId parentPageId,
                         PageId leftSiblingPageId,
                         PageId rightSiblingPageId,
                         ArrayList<BTreeLeafNode<K, V>> nodes,
                         int capacity) {
        if (nodes == null || nodes.size() == 0) {
            throw new IllegalArgumentException("Tried to initialize internal BTree page without any children");
        }

        this.pageId = id;
        this.parentPageId = parentPageId;
        this.leftSiblingPageId = leftSiblingPageId;
        this.rightSiblingPageId = rightSiblingPageId;
        this.nodes = nodes;
        this.capacity = capacity;
    }

    public boolean hasCapacity() {
        return nodes.size() < capacity;
    }

    public boolean isEmpty() {
        return nodes.size() == 0;
    }

    public V get(K key) {
        Tuple2<Integer, Boolean> searchResult = this.search(key);

        if (!searchResult.f1) {
            return null;
        } else {
            return this.nodes.get(searchResult.f0).getValue();
        }
    }

    /**
     * @param key   the key of the record to insert
     * @param value the value of the record to insert
     * @return value of previous first key if the inserted key became the new first key
     */
    public Optional<K> put(K key, V value) {
        if (!this.hasCapacity()) {
            throw new IllegalStateException("attempted to insert leaf into full page");
        }
        // TODO: handle what happens if this page is empty
        Tuple2<Integer, Boolean> searchResult = this.search(key);

        // if key already exists, overwrite in place
        if (searchResult.f1) {
            BTreeLeafNode<K, V> node = this.nodes.get(searchResult.f0);
            node.setValue(value);
        } else {
            // else the key does not exist, so insert it into the page
            ArrayUtil.insertAt(this.nodes, new BTreeLeafNode<>(key, value), searchResult.f0);
        }

        // if we inserted at the head of this array
        if (searchResult.f0 == 0) {
            // return the key that used to be at the head of this array
            return Optional.of(this.nodes.get(1).getKey());
        } else {
            return Optional.empty();
        }
    }

    public ArrayList<BTreeLeafNode<K, V>> split() {
        int indexOfSplit = this.nodes.size() / 2;

        ArrayList<BTreeLeafNode<K, V>> newList = new ArrayList<>(this.nodes.subList(indexOfSplit, this.nodes.size()));
        this.nodes = new ArrayList<>(this.nodes.subList(0, indexOfSplit));

        return newList;
    }

    public PageId getLeftSiblingPageId() {
        return this.leftSiblingPageId;
    }

    public PageId getRightSiblingPageId() {
        return this.rightSiblingPageId;
    }

    public PageId getPageId() {
        return this.pageId;
    }

    public PageId getParentPageId() {
        return this.parentPageId;
    }

    public K getFirstKey() {
        if (this.isEmpty()) {
            return null;
        } else {
            return this.nodes.get(0).getKey();
        }

    }

    public void setLeftSiblingPageId(PageId pageId) {
        this.leftSiblingPageId = pageId;
    }

    public void setParentPageId(PageId pageId) {
        this.parentPageId = pageId;
    }

    public void setRightSiblingPageId(PageId pageId) {
        this.rightSiblingPageId = pageId;
    }

    private Tuple2<Integer, Boolean> search(K key) {
        Search.SearchResult result = new Search<>(this.nodes)
                .doSearch(new BTreeLeafNode<>(key, null),
                        (it, that) -> {
                            int compared = it.getKey().compareTo(that.getKey());
                            if (compared == 0) return Search.Comparison.EQUAL;
                            if (compared < 0) return Search.Comparison.LESS_THAN;
                            return Search.Comparison.GREATER_THAN;
                        });

        if (result.comparisonAtIndex == Search.Comparison.EQUAL)
            return Tuple2.of(result.index, true);
        else if (result.comparisonAtIndex == Search.Comparison.LESS_THAN)
            return Tuple2.of(result.index, false);
        else
            return Tuple2.of(result.index + 1, false);
    }

    public RecordIterator getIterator(K startingKey) {
        return new RecordIterator(this, startingKey);
    }

    private class RecordIterator implements Iterator<Tuple2<K, V>> {
        private int currentIndex;
        private final LeafBTreePage<K, V> page;

        public RecordIterator(LeafBTreePage<K, V> page, K startingKey) {
            Tuple2<Integer, Boolean> searchResult = page.search(startingKey);

            this.page = page;
            this.currentIndex = searchResult.f0;
        }

        @Override
        public boolean hasNext() {
            return this.currentIndex < this.page.nodes.size();
        }

        @Override
        public Tuple2<K, V> next() {
            this.currentIndex += 1;
            return Tuple2.of(
                    this.page.nodes.get(this.currentIndex - 1).getKey(),
                    this.page.nodes.get(this.currentIndex - 1).getValue()
            );
        }
    }
}
