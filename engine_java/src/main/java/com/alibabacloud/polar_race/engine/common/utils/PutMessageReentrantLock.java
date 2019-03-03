package com.alibabacloud.polar_race.engine.common.utils;

import java.util.concurrent.locks.ReentrantLock;

public class PutMessageReentrantLock implements PutMessageLock {
    private ReentrantLock putMessageNormalLock = new ReentrantLock(); // NonfairSync

    public void lock() {
        putMessageNormalLock.lock();
    }

    public void unlock() {
        putMessageNormalLock.unlock();
    }
}
