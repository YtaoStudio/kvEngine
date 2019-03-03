package com.alibabacloud.polar_race.engine.common.impl;

import gnu.trove.map.hash.TLongIntHashMap;
import com.alibabacloud.polar_race.engine.common.utils.MemoryMappedFile;

import static com.alibabacloud.polar_race.engine.common.utils.ByteToInt.byteArrayToLong;

public class MemConsumer extends Thread {

    private TLongIntHashMap tmap;
    private MemoryLog keyLog;

    public MemConsumer(MemoryLog keyLog, TLongIntHashMap tmap) {
        this.keyLog = keyLog;
        this.tmap = tmap;
    }

    @Override
    public void run() {
        MemoryMappedFile reader = keyLog.getMappedBusReader();
        int pos = 0;
        byte[] key = new byte[8];
        long keyLong;
        int value;
        while (true) {
            try {
                reader.getBytes(pos, key, 0, 8);
//                keyLong = bytesToLong(key);
//                keyLong = ByteBuffer.wrap(key).getLong();
                keyLong = byteArrayToLong(key);
                value = reader.getInt(pos + 8);
                if (value == 0) {
                    if (keyLong == 0) {
                        break;
                    }
                }
                tmap.put(keyLong, value);
                pos += 12;
            } catch (Throwable t) {
                t.printStackTrace();
                System.exit(-1);
            }
        }
        System.out.println(Thread.currentThread().getName() + " exit," + "," + tmap.size());
    }
}
