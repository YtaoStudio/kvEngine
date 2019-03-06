package com.alibabacloud.polar_race.engine.common.impl;

import gnu.trove.map.hash.TLongIntHashMap;

import java.util.concurrent.atomic.AtomicInteger;

public class TSortedLongIntHashMap extends TLongIntHashMap {

    public int[] keyIndexArray = null;

    public TSortedLongIntHashMap(int initialCapacity, float loadFactor, long noEntryKey, int noEntryValue){
        super(initialCapacity, loadFactor, noEntryKey, noEntryValue);
    }

    public void sort() {
        keyIndexArray = new int[this.size()];
        long[] k = this._set;
        byte[] states = this._states;
        int i = k.length;
        int var5 = 0;

        while(i-- > 0) {
            if (states[i] == 1) {
                keyIndexArray[var5++] = i;
            }
        }
        quickSort(keyIndexArray, k, 0, keyIndexArray.length-1);
    }

//    public long getKey(int idx) {
//        return this._set[idx];
//    }
    public long getKey(int idx) {
        return this._set[this.keyIndexArray[idx]];
    }

    private static int partition(int[] idx, long[] value, int left, int right) {
        // 用子表的第一个记录作枢轴记录
        long pivot_val = value[idx[left]];
        int pivot_key = idx[left];
        while (left < right) {
            // 先从后面开始找小于枢轴的元素
            while (left < right && value[idx[right]] >= pivot_val) right --;
            if (left < right) idx[left++] = idx[right];
            // 然后再从前面往后找大于枢轴的元素
            while (left < right && value[idx[left]] <= pivot_val) left ++;
            if (left < right) idx[right--] = idx[left];
        }
        idx[left] = pivot_key;
        return left;
    }

    private static void quickSort(int[] idx, long[] value, int left, int right) {
        if (idx.length == 0) return;
        if (left > right) return;
        int pivot = partition(idx, value, left, right);
        quickSort(idx, value, left, pivot -1);
        quickSort(idx, value,pivot + 1, right);
    }

    public void sort(AtomicInteger[] whichPosCount) {
        keyIndexArray = new int[this.size()];
        long[] k = this._set;
        byte[] states = this._states;
        int i = k.length;
        int var5 = 0;

        while(i-- > 0) {
            if (states[i] == 1) {
                whichPosCount[var5] = new AtomicInteger(0);
                keyIndexArray[var5++] = i;
            }
        }
        quickSort(keyIndexArray, k, 0, keyIndexArray.length-1);
    }
}
