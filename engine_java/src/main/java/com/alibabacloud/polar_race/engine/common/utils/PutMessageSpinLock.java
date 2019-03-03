package com.alibabacloud.polar_race.engine.common.utils;

import java.util.concurrent.atomic.AtomicBoolean;

public class PutMessageSpinLock implements PutMessageLock {
    private AtomicBoolean putMessageSpinLock = new AtomicBoolean(true);

    public void lock() {
        boolean flag;
        do {
            flag = this.putMessageSpinLock.compareAndSet(true, false);
        }
        while (!flag);
    }


    public void unlock() {
        this.putMessageSpinLock.compareAndSet(false, true);
    }
}

