package com.alibabacloud.polar_race.engine.common.impl;

import com.alibabacloud.polar_race.engine.common.utils.MemoryMappedFile;

import java.nio.ByteBuffer;

public class MemConsumers implements Runnable {

    private TSortedLongIntHashMap tmap;
    private MemoryLog keyLog;
    private ThreadLocal<byte[]> threadLocalReadBytes = ThreadLocal.withInitial(() -> new byte[8]);

    public MemConsumers(MemoryLog keyLog, TSortedLongIntHashMap tmap) {
        this.keyLog = keyLog;
        this.tmap = tmap;
    }

    @Override
    public void run() {
        MemoryMappedFile reader = keyLog.getMappedBusReader();
        int pos = 0;
        byte[] key = threadLocalReadBytes.get();
        long keyLong;
        int value;
//        long indexCheckStart = System.currentTimeMillis();
        while (true) {
            try {
                reader.getBytes(pos, key, 0, 8);
                keyLong = ByteBuffer.wrap(key).getLong();
                value = reader.getInt(pos + 8);
                if (keyLong == 0 && value == 0) {
                    break;
                } else {
                    tmap.put(keyLong, value);
                }
                pos += 12;
            } catch (Throwable t) {
                t.printStackTrace();
                System.exit(-1);
            }
        }
//        long indexCheckEnd = System.currentTimeMillis();
//        System.out.printf("Consuemr Check: %d ms\n", indexCheckEnd - indexCheckStart);
        threadLocalReadBytes = null;
    }
}