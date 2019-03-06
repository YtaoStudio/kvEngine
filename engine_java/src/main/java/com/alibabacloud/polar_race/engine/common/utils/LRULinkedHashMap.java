package com.alibabacloud.polar_race.engine.common.utils;

import java.util.LinkedHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LRULinkedHashMap extends LinkedHashMap<Long, byte[]> {


    private final int maxCapacity;
    private static final float DEFAULT_LOAD_FACTOR = 0.75f;
    /**
     * Main lock guarding all access
     */
    final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public LRULinkedHashMap(int maxCapacity) {
        super(maxCapacity, DEFAULT_LOAD_FACTOR, false); // 根据插入顺序
        this.maxCapacity = maxCapacity;
    }

    @Override
    protected boolean removeEldestEntry(java.util.Map.Entry<Long, byte[]> eldest) {
        return size() > maxCapacity;
    }

    @Override
    public byte[] get(Object key) {
        final ReentrantReadWriteLock lock = this.lock;
        lock.readLock().lock();
        try {
            return super.get(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public byte[] put(Long key, byte[] value) {
        final ReentrantReadWriteLock lock = this.lock;
        lock.writeLock().lock();
        try {
            return super.put(key, value);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public byte[] remove(Object key) {
        final ReentrantReadWriteLock lock = this.lock;
        lock.writeLock().lock();
        try {
            return super.remove(key);
        } finally {
            lock.writeLock().unlock();
        }
    }
}
