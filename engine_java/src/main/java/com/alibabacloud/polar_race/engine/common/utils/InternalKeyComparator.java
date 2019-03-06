package com.alibabacloud.polar_race.engine.common.utils;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;

public class InternalKeyComparator
        implements Comparator<PolarSlice> {


    @Override
    public int compare(PolarSlice left, PolarSlice right) {
        return left.compareTo(right);
    }

    /**
     * Returns {@code true} if each element in {@code iterable} after the first is
     * greater than or equal to the element that preceded it, according to this
     * ordering. Note that this is always true when the iterable has fewer than
     * two elements.
     */
    public boolean isOrdered(PolarSlice... keys) {
        return isOrdered(Arrays.asList(keys));
    }

    /**
     * Returns {@code true} if each element in {@code iterable} after the first is
     * greater than or equal to the element that preceded it, according to this
     * ordering. Note that this is always true when the iterable has fewer than
     * two elements.
     */
    public boolean isOrdered(Iterable<PolarSlice> keys) {
        Iterator<PolarSlice> iterator = keys.iterator();
        if (!iterator.hasNext()) {
            return true;
        }

        PolarSlice previous = iterator.next();
        while (iterator.hasNext()) {
            PolarSlice next = iterator.next();
            if (compare(previous, next) > 0) {
                return false;
            }
            previous = next;
        }
        return true;
    }
}
