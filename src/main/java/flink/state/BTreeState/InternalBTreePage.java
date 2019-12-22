package flink.state.BTreeState;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Optional;

public class InternalBTreePage<T extends Comparable> {
    private PageId pageId;
    private PageId parentPageId;
    private ArrayList<BTreeInternalNode<T>> nodes;
    private PageType childrenType;
    private int capacity;

    public InternalBTreePage(PageId id, PageId parentPageId, ArrayList<BTreeInternalNode<T>> nodes, PageType childrenType, int capacity) {
        if (nodes == null || nodes.size() == 0) {
            throw new IllegalArgumentException("Tried to initialize internal BTree page without any children");
        }

        this.pageId = id;
        this.parentPageId = parentPageId;
        this.nodes = nodes;
        this.childrenType = childrenType;
        this.capacity = capacity;
    }

    public boolean hasCapacity() {
        return this.capacity < this.nodes.size();
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

    public Optional<T> update(T oldKey, T newKey) {

        Tuple2<Integer, Boolean> searchResult = search(oldKey);

        if (!searchResult.f1) {
            throw new IllegalArgumentException("Tried to update a key, but the key was not found");
        }

        this.nodes.get(searchResult.f0).setKey(newKey);

        if (searchResult.f0 == 0) {
            return Optional.of(newKey);
        } else {
            return Optional.empty();
        }
    }

    public PageId findPageForInsertion(T key) {
        // find first node which this key comes before in ordering
        // the child page will be "between" the first key it is "less than"
        // and the previous key.
        Tuple2<Integer, Boolean> searchResult = search(key);

        return nodes.get(searchResult.f0).getChildPage();
    }

    public Optional<PageId> findPage(T key) {
        Tuple2<Integer, Boolean> searchResult = search(key);

        // if the result pointed us to the first page, then
        // make sure the key is actually in range.
        if (searchResult.f0 == 0 && isBefore(key, this.nodes.get(0).getKey())) {
            return Optional.empty();
        }

        return Optional.of(this.nodes.get(searchResult.f0).getChildPage());
    }

    public void insert(T key, PageId childPageId) {
        if (!this.hasCapacity()) {
            throw new IllegalStateException("attempted to insert another node into full page");
        }

        Tuple2<Integer, Boolean> searchResult = search(key);

        if (searchResult.f1) {
            throw new IllegalArgumentException("attempted to insert node for key that already exists");
        }

        insertAt(this.nodes, new BTreeInternalNode<T>(key, childPageId), searchResult.f0);
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

    private boolean isBefore(T it, T that) {
        return it.compareTo(that) < 0;
    }

    private void insertAt(ArrayList<BTreeInternalNode<T>> arr, BTreeInternalNode<T> val, int index) {
        BTreeInternalNode<T> toInsert = val;

        for (int i = index; i < arr.size(); i += 1) {
            BTreeInternalNode<T> toBump = arr.get(index);
            arr.set(i, toInsert);
            toInsert = toBump;
        }

        arr.add(toInsert);
    }

    private Tuple2<Integer, Boolean> search(T key) {
        int pivotBegin = 0;
        int pivotEnd = this.nodes.size() - 1;

        while (pivotEnd - pivotBegin > 0) {
            int pivotIx = pivotBegin + ((pivotEnd - pivotBegin) / 2);

            T pivotKey = this.nodes.get(pivotIx).getKey();

            if (key.equals(pivotKey)) {
                return Tuple2.of(pivotIx, true);
            } else if (isBefore(key, pivotKey)) {
                pivotEnd = pivotIx;
            } else {
                pivotBegin = pivotIx;
            }
        }

        int pivotIx = pivotBegin + ((pivotEnd - pivotBegin) / 2);
        T pivotKey = this.nodes.get(pivotIx).getKey();

        if (key.equals(pivotKey)) {
            return Tuple2.of(pivotIx, true);
        } else {
            return Tuple2.of(pivotIx, false);
        }
    }

    public PageId getFirstChildPageId() {
        return this.nodes.get(0).getChildPage();
    }
}
