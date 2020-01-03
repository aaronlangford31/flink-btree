package flink.state.BTreeState;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;

public class BTreeState<K extends Comparable, V> {
    private ValueState<PageFactory<K, V>> pageFactory;

    private ValueState<InternalBTreePage<K>> rootPage;
    private MapState<PageId, InternalBTreePage<K>> internalPages;
    private MapState<PageId, LeafBTreePage<K, V>> leafPages;

    public BTreeState(RuntimeContext context, BTreeStateDescriptor<K, V> stateDescriptor) throws IOException {
        this(context, stateDescriptor, 4096, 4096);
    }

    public BTreeState(RuntimeContext context, BTreeStateDescriptor<K, V> stateDescriptor, int internalPageCapacity, int leafPageCapacity) throws IOException {
        this.pageFactory = context.getState(stateDescriptor.getPageFactoryDescriptor());
        if (this.pageFactory.value() == null) {
            this.pageFactory.update(new PageFactory<>(internalPageCapacity, leafPageCapacity));
        }

        this.rootPage = context.getState(stateDescriptor.getRootPageDescriptor());
        if (this.rootPage.value() == null) {
            this.rootPage.update(this.pageFactory.value().getEmptyRootPage());
        }

        this.internalPages = context.getMapState(stateDescriptor.getInternalPageMapDescriptor());
        this.leafPages = context.getMapState(stateDescriptor.getLeafPageMapDescriptor());
    }

    public void insert(K key, V value) {
        // find where the key should go
        try {
            InternalBTreePage<K> currPage = this.rootPage.value();

            // if the root page is empty
            if (currPage.isEmpty()) {
                // create a new leaf page
                LeafBTreePage<K, V> newLeafPage = this.pageFactory.value().getNewLeafPage(currPage.getPageId(), key, value);

                // add leaf page to parent
                currPage.insert(key, newLeafPage.getPageId());

                // update the root page
                this.rootPage.update(currPage);
                // insert the new leaf page
                this.leafPages.put(newLeafPage.getPageId(), newLeafPage);
                return;
            }

            while (currPage.getChildrenType() != PageType.LEAF) {
                Optional<PageId> childId = currPage.findPage(key);

                // if child is not present, then go find the left most leaf in the tree
                if (!childId.isPresent()) {
                    while (currPage.getChildrenType() != PageType.LEAF) {
                        PageId firstChildId = currPage.getFirstChildPageId();
                        currPage = internalPages.get(firstChildId);
                    }
                    break;
                }
                currPage = internalPages.get(childId.get());
            }

            Optional<PageId> leafId = currPage.findPage(key);
            if (!leafId.isPresent()) {
                leafId = Optional.of(currPage.getFirstChildPageId());
            }
            LeafBTreePage<K, V> leafPage = this.getLeafPage(leafId.get());

            // if leaf page has capacity, insert into the page
            if (leafPage.hasCapacity()) {
                Optional<K> maybeOldKey = leafPage.put(key, value);

                // update this leaf page in state
                this.leafPages.put(leafId.get(), leafPage);

                // if we changed the first key in the leaf
                if (maybeOldKey.isPresent()) {
                    // we need to percolate the change up the tree
                    this.percolateKey(maybeOldKey.get(), leafPage.getFirstKey(), leafPage);
                }
            } else {
                // split the page
                Tuple2<LeafBTreePage<K, V>, LeafBTreePage<K, V>> splitPages = this.splitLeafPage(leafPage);

                // determine which split to use:
                // if the key is before the second of the split pages,
                // then it belongs in the first page
                if (isBefore(key, splitPages.f1.getFirstKey())) {
                    Optional<K> maybeOldKey = splitPages.f0.put(key, value);

                    // update this leaf page in state
                    this.leafPages.put(splitPages.f0.getPageId(), splitPages.f0);

                    if (maybeOldKey.isPresent()) {
                        this.percolateKey(maybeOldKey.get(), splitPages.f0.getFirstKey(), splitPages.f0);
                    }
                } else {
                    // else it belongs in the second page
                    Optional<K> maybeOldKey = splitPages.f1.put(key, value);

                    // update this leaf page in state
                    this.leafPages.put(splitPages.f1.getPageId(), splitPages.f1);

                    if (maybeOldKey.isPresent()) {
                        this.percolateKey(maybeOldKey.get(), splitPages.f1.getFirstKey(), splitPages.f1);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public V get(K key) throws Exception {
        InternalBTreePage<K> currPage = this.rootPage.value();

        if (currPage.isEmpty()) {
            return null;
        }

        while (currPage.getChildrenType() != PageType.LEAF) {
            Optional<PageId> childId = currPage.findPage(key);

            if (!childId.isPresent()) {
                return null;
            }

            currPage = internalPages.get(childId.get());
        }

        PageId leafId = currPage.findPageForInsertion(key);
        LeafBTreePage<K, V> leafPage = this.getLeafPage(leafId);

        return leafPage.get(key);
    }

    public Iterator<V> getValuesInRange(K lower, K upper) throws Exception {
        InternalBTreePage<K> currPage = this.rootPage.value();
        Optional<PageId> childId = Optional.empty();

        while (currPage.getChildrenType() != PageType.LEAF) {
            childId = currPage.findPage(lower);

            // if child is not present, break while loop with empty childId
            if (!childId.isPresent()) {
                break;
            }
            currPage = internalPages.get(childId.get());
        }

        LeafBTreePage<K, V> leafPage;
        if (!childId.isPresent()) {
            leafPage = this.getLeftmostOf(currPage);
        } else {
            Optional<PageId> leafId = currPage.findPage(lower);
            leafPage = this.getLeafPage(leafId.get());
        }

        return new RecordIterator(this, leafPage, lower, upper);
    }

    public Iterator<V> getAllValues() throws Exception {
        LeafBTreePage<K, V> leftLeaf = this.getLeftmostOf(this.rootPage.value());
        return new RecordIterator(this, leftLeaf, leftLeaf.getFirstKey(), null);
    }

    private LeafBTreePage<K, V> getLeafPage(PageId pageId) throws Exception {
        return this.leafPages.get(pageId);
    }

    private Tuple2<LeafBTreePage<K, V>, LeafBTreePage<K, V>> splitLeafPage(LeafBTreePage<K, V> page) throws Exception {
        PageId parentPageId = page.getParentPageId();

        InternalBTreePage<K> parentPage;
        if (parentPageId.isRootPage()) {
            parentPage = this.rootPage.value();
        } else {
            // TODO: wrap this in a PageNotFound exception
            parentPage = this.internalPages.get(parentPageId);
        }

        Tuple2<LeafBTreePage<K, V>, LeafBTreePage<K, V>> splitPages = this.pageFactory.value().split(page);
        LeafBTreePage<K, V> newLeafPage = splitPages.f1;

        // if the parent page has capacity, just insert the new page key into it
        if (parentPage.hasCapacity()) {
            // the left page already is in the parent, only need to insert the right
            K newKey = newLeafPage.getFirstKey();
            PageId newPageId = newLeafPage.getPageId();

            parentPage.insert(newKey, newPageId);

            // update page in state
            if (parentPageId.isRootPage()) {
                this.rootPage.update(parentPage);
            } else {
                this.internalPages.put(parentPageId, parentPage);
            }
        } else if (parentPageId.isRootPage()) {
            Tuple2<InternalBTreePage<K>, InternalBTreePage<K>> splitRootPages = this.splitRootPage();
            InternalBTreePage<K> leftRootSplit = splitRootPages.f0;
            InternalBTreePage<K> rightRootSplit = splitRootPages.f1;

            if (isBefore(newLeafPage.getFirstKey(), rightRootSplit.getFirstKey())) {
                leftRootSplit.insert(newLeafPage.getFirstKey(), newLeafPage.getPageId());
                newLeafPage.setParentPageId(leftRootSplit.getPageId());

                this.internalPages.put(leftRootSplit.getPageId(), leftRootSplit);
            } else {
                rightRootSplit.insert(newLeafPage.getFirstKey(), newLeafPage.getPageId());
                newLeafPage.setParentPageId(rightRootSplit.getPageId());

                this.internalPages.put(rightRootSplit.getPageId(), rightRootSplit);
            }
        } else {
            // no capacity in parent, we need to split an internal page
            Tuple2<InternalBTreePage<K>, InternalBTreePage<K>> splitInternalPages = this.splitInternalPage(parentPage);
            InternalBTreePage<K> newInternalPage = splitInternalPages.f1;

            // determine which of the split internal pages to use:
            // the original leaf page will already be in one of the pages,
            // so we just need to figure out where to put the key for the
            // new internal page.
            // if key of new leaf page is before the new internal page
            // then insert into the old internal page
            if (isBefore(newLeafPage.getFirstKey(), newInternalPage.getFirstKey())) {
                splitInternalPages.f0.insert(newLeafPage.getFirstKey(), newLeafPage.getPageId());
                // need to update the page parent id
                newLeafPage.setParentPageId(splitInternalPages.f0.getPageId());
                // update page in state
                this.internalPages.put(splitInternalPages.f0.getPageId(), splitInternalPages.f0);
            } else {
                // else it belongs in the second page
                newInternalPage.insert(newLeafPage.getFirstKey(), newLeafPage.getPageId());
                // need to update the page parent id
                newLeafPage.setParentPageId(newInternalPage.getPageId());

                // update new page in state
                this.internalPages.put(newInternalPage.getPageId(), newInternalPage);
            }
        }

        // insert new page into map
        this.leafPages.put(newLeafPage.getPageId(), newLeafPage);

        return splitPages;
    }

    private Tuple2<InternalBTreePage<K>, InternalBTreePage<K>> splitInternalPage(InternalBTreePage<K> page) throws Exception {
        PageId parentPageId = page.getParentPageId();

        InternalBTreePage<K> parentPage;
        if (parentPageId.isRootPage()) {
            // TODO: wrap this in a PageNotFound exception
            parentPage = this.rootPage.value();
        } else {
            // TODO: wrap this in a PageNotFound exception
            parentPage = this.internalPages.get(parentPageId);
        }

        Tuple2<InternalBTreePage<K>, InternalBTreePage<K>> splitInternalPages = pageFactory.value().split(page);
        InternalBTreePage<K> newInternalPage = splitInternalPages.f1;

        // since a new internal page was created, the children need their parent ids updated
        if (newInternalPage.getChildrenType() == PageType.INTERNAL) {
            for (Iterator<PageId> it = newInternalPage.getChildrenPageIds(); it.hasNext(); ) {
                PageId childPageId = it.next();

                InternalBTreePage<K> childPage = this.internalPages.get(childPageId);
                childPage.setParentPageId(newInternalPage.getPageId());
                this.internalPages.put(childPageId, childPage);
            }
        } else {
            for (Iterator<PageId> it = newInternalPage.getChildrenPageIds(); it.hasNext(); ) {
                PageId childPageId = it.next();

                LeafBTreePage<K, V> childPage = this.leafPages.get(childPageId);
                childPage.setParentPageId(newInternalPage.getPageId());
                this.leafPages.put(childPageId, childPage);
            }
        }

        // if parent page has capacity, just insert the new key
        if (parentPage.hasCapacity()) {
            parentPage.insert(newInternalPage.getFirstKey(), newInternalPage.getPageId());

            // update page in state
            if (parentPageId.isRootPage()) {
                this.rootPage.update(parentPage);
            } else {
                this.internalPages.put(parentPageId, parentPage);
            }
        } else if (parentPageId.isRootPage()) {
            Tuple2<InternalBTreePage<K>, InternalBTreePage<K>> splitRootPages = this.splitRootPage();
            InternalBTreePage<K> leftRootSplit = splitRootPages.f0;
            InternalBTreePage<K> rightRootSplit = splitRootPages.f1;

            if (isBefore(newInternalPage.getFirstKey(), rightRootSplit.getFirstKey())) {
                leftRootSplit.insert(newInternalPage.getFirstKey(), newInternalPage.getPageId());
                // update pageId of the new internal page
                newInternalPage.setParentPageId(leftRootSplit.getPageId());
                // update the parent in state
                this.internalPages.put(leftRootSplit.getPageId(), leftRootSplit);
            } else {
                rightRootSplit.insert(newInternalPage.getFirstKey(), newInternalPage.getPageId());
                // update pageId of the new internal page
                newInternalPage.setParentPageId(rightRootSplit.getPageId());
                // update the parent in state
                this.internalPages.put(rightRootSplit.getPageId(), rightRootSplit);
            }
        } else {
            // no capacity in parent, we need to split an internal page
            Tuple2<InternalBTreePage<K>, InternalBTreePage<K>> splitParentInternalPages = this.splitInternalPage(parentPage);
            InternalBTreePage<K> oldParentInternalPage = splitParentInternalPages.f0;
            InternalBTreePage<K> newParentInternalPage = splitParentInternalPages.f1;

            // determine which of the split internal pages to use:
            // the original leaf page will already be in one of the pages,
            // so we just need to figure out where to put the key for the
            // new internal page.
            // if key of new internal page is before the new parent internal page
            // then insert into the old parent internal page
            if (isBefore(newInternalPage.getFirstKey(), newParentInternalPage.getFirstKey())) {
                oldParentInternalPage.insert(newInternalPage.getFirstKey(), newInternalPage.getPageId());
                // need to update the page parent id
                newInternalPage.setParentPageId(oldParentInternalPage.getPageId());
                // update parent page in state
                this.internalPages.put(oldParentInternalPage.getPageId(), oldParentInternalPage);
            } else {
                // else it belongs in the second page
                newParentInternalPage.insert(newInternalPage.getFirstKey(), newInternalPage.getPageId());
                // need to update the page parent id
                newInternalPage.setParentPageId(newParentInternalPage.getPageId());
                // update new page in state
                this.internalPages.put(newParentInternalPage.getPageId(), newParentInternalPage);
            }
        }

        // insert new page into map
        this.internalPages.put(newInternalPage.getPageId(), newInternalPage);

        return splitInternalPages;
    }

    private Tuple2<InternalBTreePage<K>, InternalBTreePage<K>> splitRootPage() throws Exception {
        // split the root into two internal pages, assigning new ids to the split pages
        Tuple2<InternalBTreePage<K>, InternalBTreePage<K>> splitInternalPages = this.pageFactory.value().split(this.rootPage.value(), true);

        // get a new root page, inserting the split root pages into that page
        InternalBTreePage<K> newRoot = this.pageFactory.value().getNewRootPage(Arrays.asList(splitInternalPages.f0, splitInternalPages.f1));

        // since these pages got new parent IDs, child pages must be updated
        if (splitInternalPages.f0.getChildrenType() == PageType.INTERNAL) {
            for (Iterator<PageId> it = splitInternalPages.f0.getChildrenPageIds(); it.hasNext(); ) {
                PageId childPageId = it.next();

                InternalBTreePage<K> childPage = this.internalPages.get(childPageId);
                childPage.setParentPageId(splitInternalPages.f0.getPageId());
                this.internalPages.put(childPageId, childPage);
            }

            for (Iterator<PageId> it = splitInternalPages.f1.getChildrenPageIds(); it.hasNext(); ) {
                PageId childPageId = it.next();

                InternalBTreePage<K> childPage = this.internalPages.get(childPageId);
                childPage.setParentPageId(splitInternalPages.f1.getPageId());
                this.internalPages.put(childPageId, childPage);
            }
        } else {
            for (Iterator<PageId> it = splitInternalPages.f0.getChildrenPageIds(); it.hasNext(); ) {
                PageId childPageId = it.next();

                LeafBTreePage<K, V> childPage = this.leafPages.get(childPageId);
                childPage.setParentPageId(splitInternalPages.f0.getPageId());
                this.leafPages.put(childPageId, childPage);
            }

            for (Iterator<PageId> it = splitInternalPages.f1.getChildrenPageIds(); it.hasNext(); ) {
                PageId childPageId = it.next();

                LeafBTreePage<K, V> childPage = this.leafPages.get(childPageId);
                childPage.setParentPageId(splitInternalPages.f1.getPageId());
                this.leafPages.put(childPageId, childPage);
            }
        }


        // insert new pages into internalPages map
        this.internalPages.put(splitInternalPages.f0.getPageId(), splitInternalPages.f0);
        this.internalPages.put(splitInternalPages.f1.getPageId(), splitInternalPages.f1);

        // update root page state
        this.rootPage.update(newRoot);

        // return the split pages
        return splitInternalPages;
    }

    private void percolateKey(K oldKey, K newKey, LeafBTreePage<K, V> page) throws Exception {
        PageId parentPageId = page.getParentPageId();
        InternalBTreePage<K> parentPage;

        if (parentPageId.isRootPage()) {
            parentPage = this.rootPage.value();
            parentPage.update(oldKey, newKey);

            this.rootPage.update(parentPage);
        } else {
            parentPage = this.internalPages.get(page.getPageId());
            Optional<K> maybeNewKey = parentPage.update(oldKey, newKey);

            this.internalPages.put(parentPageId, parentPage);

            // if the update of the old key caused the first key of the parent page
            // to change, then we need to percolate one more level up.
            if (maybeNewKey.isPresent()) {
                this.percolateKey(oldKey, newKey, parentPage);
            }
        }
    }

    private void percolateKey(K oldKey, K newKey, InternalBTreePage<K> page) throws Exception {
        PageId parentPageId = page.getParentPageId();
        InternalBTreePage<K> parentPage;

        if (parentPageId.isRootPage()) {
            parentPage = this.rootPage.value();
            parentPage.update(oldKey, newKey);

            this.rootPage.update(parentPage);
        } else {
            parentPage = this.internalPages.get(page.getPageId());
            Optional<K> maybeNewKey = parentPage.update(oldKey, newKey);

            this.internalPages.put(parentPageId, parentPage);

            // if the update of the old key caused the first key of the parent page
            // to change, then we need to percolate one more level up.
            if (maybeNewKey.isPresent()) {
                this.percolateKey(oldKey, newKey, parentPage);
            }
        }
    }

    private LeafBTreePage<K, V> getLeftmostOf(InternalBTreePage<K> page) throws Exception {
        InternalBTreePage<K> currPage = page;

        while (currPage.getChildrenType() != PageType.LEAF) {
            PageId firstChildId = currPage.getFirstChildPageId();
            currPage = internalPages.get(firstChildId);
        }

        return this.getLeafPage(currPage.getFirstChildPageId());
    }

    private boolean isBefore(K it, K that) {
        return it.compareTo(that) < 0;
    }

    private class RecordIterator implements Iterator<V> {

        private final BTreeState<K, V> treeState;
        private final K endingKey;

        private LeafBTreePage<K, V> currentPage;
        private K currentKey;
        private Iterator<Tuple2<K, V>> currentIterator;

        public RecordIterator(BTreeState<K, V> treeState, LeafBTreePage<K, V> startingPage, K startingKey, K endingKey) {
            this.treeState = treeState;
            this.currentPage = startingPage;
            this.currentIterator = startingPage.getIterator(startingKey);
            this.currentKey = startingKey;
            this.endingKey = endingKey;
        }

        @Override
        public boolean hasNext() {
            // (current iterator has next
            // OR current page has a right sibling) AND
            // current key is not equal to ending key
            return (currentIterator.hasNext() || currentPage.getRightSiblingPageId() != null)
                    && !this.currentKey.equals(endingKey);
        }

        @Override
        public V next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }

            if (!this.currentIterator.hasNext()) {
                PageId nextPageId = this.currentPage.getRightSiblingPageId();
                try {
                    this.currentPage = this.treeState.getLeafPage(nextPageId);
                } catch (Exception e) {
                    // TODO: better handle this exception
                    e.printStackTrace();
                }

                this.currentIterator = this.currentPage.getIterator(this.currentKey);
            }

            Tuple2<K, V> nextPair = this.currentIterator.next();
            this.currentKey = nextPair.f0;

            return nextPair.f1;
        }
    }
}
