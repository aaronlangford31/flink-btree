package flink.state.BTreeState.util;

import java.util.List;

public class ArrayUtil {
    public static <T> void insertAt(List<T> target, T el, int ix) {
        T toInsert = el;

        for (int i = ix; i < target.size(); i += 1) {
            T toBump = target.get(i);
            target.set(i, toInsert);
            toInsert = toBump;
        }

        target.add(toInsert);
    }
}
