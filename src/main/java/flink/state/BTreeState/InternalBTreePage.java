package flink.state.BTreeState;

import flink.state.BTreeState.util.ArrayUtil;
import flink.state.BTreeState.util.Search;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Optional;

public class InternalBTreePage<T extends Comparable> {
    private PageId pageId;
    private PageId parentPageId;
    private ArrayList<BTreeInternalNode<T>> nodes;
    private PageType childrenType;
    private int capacity;

    public InternalBTreePage(PageId id,
                             PageId parentPageId,
                             ArrayList<BTreeInternalNode<T>> nodes,
                             PageType childrenType,
                             int capacity) {
        this.pageId = id;
        this.parentPageId = parentPageId;
        this.nodes = nodes;
        this.childrenType = childrenType;
        this.capacity = capacity;
    }

    public boolean hasCapacity() {
        return this.nodes.size() < this.capacity;
    }

    public boolean isEmpty() {
        return this.nodes.isEmpty();
    }

    public Iterator<PageId> getChildrenPageIds() {
        return this.nodes.stream().map(BTreeInternalNode::getChildPage).iterator();
    }

    public PageId getParentPageId() {
        return parentPageId;
    }

    public T getFirstKey() {
        return this.nodes.get(0).getKey();
    }

    public void setParentPageId(PageId pageId) {
        this.parentPageId = pageId;
    }

    public void setPageId(PageId pageId) {
        this.pageId = pageId;
    }

    public Optional<T> update(T oldKey, T newKey) {

        Search.SearchResult searchResult = search(oldKey);

        if (searchResult.comparisonAtIndex != Search.Comparison.EQUAL) {
            throw new IllegalArgumentException("Tried to update a key, but the key was not found");
        }

        this.nodes.get(searchResult.index).setKey(newKey);

        if (searchResult.index == 0) {
            return Optional.of(newKey);
        } else {
            return Optional.empty();
        }
    }

    public PageId findPageForInsertion(T key) {
        // find first node which this key comes before in ordering
        // the child page will be "between" the first key it is "less than"
        // and the previous key.
        Search.SearchResult searchResult = search(key);

        if (searchResult.comparisonAtIndex == Search.Comparison.LESS_THAN) {
            return this.nodes.get(Math.max(0, searchResult.index - 1)).getChildPage();
        } else {
            return this.nodes.get(searchResult.index).getChildPage();
        }
    }

    public Optional<PageId> findPage(T key) {
        Search.SearchResult searchResult = search(key);

        // if the result pointed us to the first page, then
        // make sure the key is actually in range.
        if (searchResult.index == 0 && searchResult.comparisonAtIndex == Search.Comparison.LESS_THAN) {
            return Optional.empty();
        } else if (searchResult.comparisonAtIndex == Search.Comparison.LESS_THAN) {
            return Optional.of(this.nodes.get(searchResult.index - 1).getChildPage());
        } else {
            return Optional.of(this.nodes.get(searchResult.index).getChildPage());
        }
    }

    public void insert(T key, PageId childPageId) {
        if (!this.hasCapacity()) {
            throw new IllegalStateException("attempted to insert another node into full page");
        }

        if (this.isEmpty()) {
            ArrayUtil.insertAt(this.nodes, new BTreeInternalNode<T>(key, childPageId), 0);
            return;
        }

        Search.SearchResult searchResult = search(key);

        if (searchResult.comparisonAtIndex == Search.Comparison.EQUAL) {
            throw new IllegalArgumentException("attempted to insert node for key that already exists");
        } else if (searchResult.comparisonAtIndex == Search.Comparison.GREATER_THAN) {
            ArrayUtil.insertAt(this.nodes, new BTreeInternalNode<>(key, childPageId), searchResult.index + 1);
        } else {
            ArrayUtil.insertAt(this.nodes, new BTreeInternalNode<>(key, childPageId), searchResult.index);
        }
    }

    public ArrayList<BTreeInternalNode<T>> split() {
        int indexOfSplit = this.nodes.size() / 2;

        ArrayList<BTreeInternalNode<T>> newList = new ArrayList<>(this.nodes.subList(indexOfSplit, this.nodes.size()));
        this.nodes = new ArrayList<>(this.nodes.subList(0, indexOfSplit));

        return newList;
    }

    public PageId getPageId() {
        return this.pageId;
    }

    public PageType getChildrenType() {
        return childrenType;
    }

    public PageId getFirstChildPageId() {
        return this.nodes.get(0).getChildPage();
    }

//    @Override
//    public Object clone() {
//        ArrayList<BTreeInternalNode<T>> copiedNodes = new ArrayList<>(this.nodes.size());
//
//        for (BTreeInternalNode<T> node : this.nodes) {
//            copiedNodes.add((BTreeInternalNode<T>)node.clone());
//        }
//
//        return new InternalBTreePage<T>(
//                (PageId)this.pageId.clone(),
//                (PageId)this.parentPageId.clone(),
//                copiedNodes,
//                this.childrenType,
//                this.capacity
//        );
//    }

    private boolean isBefore(T it, T that) {
        return it.compareTo(that) < 0;
    }

    private Search.SearchResult search(T key) {
        return new Search<>(this.nodes)
                .doSearch(new BTreeInternalNode<>(key, null),
                        (it, that) -> {
                            int compared = it.getKey().compareTo(that.getKey());
                            if (compared == 0) return Search.Comparison.EQUAL;
                            if (compared < 0) return Search.Comparison.LESS_THAN;
                            return Search.Comparison.GREATER_THAN;
                        });
    }
}
