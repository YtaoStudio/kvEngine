package com.alibabacloud.polar_race.engine.common.utils;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static java.nio.charset.StandardCharsets.UTF_8;

public class CacheLinkedHashMap extends LinkedHashMap<Long, byte[]> {

    private static final long serialVersionUID = -2287156302885299704L;
    private final int maxCapacity;
    private static final float DEFAULT_LOAD_FACTOR = 0.75f;
    /**
     * Main lock guarding all access
     */
    final ReentrantLock lock = new ReentrantLock();

    /**
     * Condition for waiting takes
     */
    private final Condition notEmpty = lock.newCondition();

    /**
     * Condition for waiting puts
     */
    private final Condition notFull = lock.newCondition();

    public CacheLinkedHashMap(int maxCapacity) {
        super(maxCapacity, DEFAULT_LOAD_FACTOR, false); // 根据插入顺序
        this.maxCapacity = maxCapacity;
    }

    @Override
    protected boolean removeEldestEntry(java.util.Map.Entry<Long, byte[]> eldest) {
        return size() > maxCapacity;
    }

    @Override
    public byte[] get(Object key) {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            byte[] x;
            while ((x = super.get(key)) == null)
                try {
                    notEmpty.await();
                } catch (InterruptedException e) {
                    System.out.println("get, " + e.getMessage());
                    System.exit(-1);
                }
            System.out.println("get:" + key + ",\n" + Arrays.toString(x));
            return x;
        } finally {
            lock.unlock();
        }
    }

    //    @Override
    public byte[] put(Long key, byte[] value, int idx) {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            // 外部逻辑控制是否put
            if (idx == 0 || idx == maxCapacity)
                System.out.println("size:" + size() + "," + key + ",\n" + Arrays.toString(value));
            while (size() >= maxCapacity) {
                try {
                    notFull.await();
                } catch (InterruptedException e) {
                    System.out.println("put, " + e.getMessage());
                    System.exit(-1);
                }
            }
            byte[] x = super.put(key, value);
            if (x != null)
                System.out.println(key + "," + new String(x, UTF_8) + "\n" + new String(value, UTF_8));
            try {
                notEmpty.signal();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return x;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] remove(Object key) {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            System.out.println("Remove:" + key);
            byte[] x = super.remove(key);
            notFull.signal();
            return x;
        } finally {
            lock.unlock();
        }
    }
}
