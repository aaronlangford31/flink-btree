package flink.state.BTreeState.util;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;
import java.util.function.BiFunction;

public class Search<T> {
    List<T> elements;
    public Search(List<T> elements) {
        this.elements = elements;
    }

    public SearchResult doSearch(T elementToFind, BiFunction<T, T, Comparison> compareFn) {
        if (this.elements.size() == 0) {
            return new SearchResult(0, Comparison.LESS_THAN);
        }

        int pivotBegin = 0;
        int pivotEnd = this.elements.size() - 1;

        T beginKey = this.elements.get(pivotBegin);
        T endKey = this.elements.get(pivotEnd);

        Comparison beginComparison = compareFn.apply(elementToFind, beginKey);
        if (beginComparison == Comparison.EQUAL) {
            return new SearchResult(pivotBegin, Comparison.EQUAL);
        } else if (beginComparison == Comparison.LESS_THAN){
            return new SearchResult(pivotBegin, Comparison.LESS_THAN);
        }

        Comparison endComparison = compareFn.apply(elementToFind, endKey);
        if (endComparison == Comparison.EQUAL) {
            return new SearchResult(pivotEnd, Comparison.EQUAL);
        } else if (endComparison == Comparison.GREATER_THAN) {
            return new SearchResult(pivotEnd, Comparison.GREATER_THAN);
        }

        while (pivotEnd - pivotBegin > 1) {
            int pivotIx = pivotBegin + ((pivotEnd - pivotBegin) / 2);

            T pivotKey = this.elements.get(pivotIx);
            Comparison pivotComparison = compareFn.apply(elementToFind, pivotKey);

            if (pivotComparison == Comparison.EQUAL) {
                return new SearchResult(pivotIx, Comparison.EQUAL);
            } else if (pivotComparison == Comparison.LESS_THAN) {
                pivotEnd = pivotIx;
            } else {
                pivotBegin = pivotIx;
            }
        }

        // search is down to two elements
        // one of them is equal, or the key should be at the right index if it
        // were to exist in this sequence
        beginKey = this.elements.get(pivotBegin);
        endKey = this.elements.get(pivotEnd);

        beginComparison = compareFn.apply(elementToFind, beginKey);
        if (beginComparison == Comparison.EQUAL) {
            return new SearchResult(pivotBegin, Comparison.EQUAL);
        }

        endComparison = compareFn.apply(elementToFind, endKey);
        if (endComparison == Comparison.EQUAL) {
            return new SearchResult(pivotEnd, Comparison.EQUAL);
        }

        // else endComparison == Comparison.LESS_THAN)
        return new SearchResult(pivotEnd, Comparison.LESS_THAN);
    }

    public enum Comparison {
        LESS_THAN,
        EQUAL,
        GREATER_THAN
    }
    public static class SearchResult {
        public int index;
        public Comparison comparisonAtIndex;

        public SearchResult(int index, Comparison comparison) {
            this.index = index;
            this.comparisonAtIndex = comparison;
        }
    }
}


