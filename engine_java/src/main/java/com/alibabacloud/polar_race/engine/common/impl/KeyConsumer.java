package com.alibabacloud.polar_race.engine.common.impl;

import gnu.trove.map.hash.TLongIntHashMap;

import java.nio.ByteBuffer;

import static com.alibabacloud.polar_race.engine.util.Constants.bytesGetTwo;

public class KeyConsumer implements Runnable {

    private TLongIntHashMap tmap;
    private int index;
    private int sum;
    private KeyLog keyLog;

    public KeyConsumer(KeyLog keyLog, TLongIntHashMap tmap, int index, int sum) {
        this.keyLog = keyLog;
        this.tmap = tmap;
        this.index = index;
        this.sum = sum;
    }

    @Override
    public void run() {
        ByteBuffer byteBuffer = keyLog.getKeyBuffer();
        byteBuffer.position(0);
        // key是否在当前数组
        int keyIndex = 0;
        long keyLong = 0L;
        byte[] key = new byte[8];
        int count = 0;
        int size = 0;
        int value = 0;
        while (sum > 0) {
            try {
                byteBuffer.get(key);
                keyLong = ByteBuffer.wrap(key).getLong();
                value = byteBuffer.getInt();
//                tmap.put(ByteBuffer.wrap(key).getLong(), byteBuffer.getInt());
//                tmap.put(keyLong, byteBuffer.getInt());
                keyIndex = bytesGetTwo(key) % 64;
//                    keyIndex = Integer.parseInt(keyStr.substring(0, 2));
//                keyIndex = keyIndex % 64;
                if (keyIndex == index) {
                    tmap.put(keyLong, value);
                    count++;
                } else {
                    size++;
                }
                sum--;
            } catch (Throwable t) {
                t.printStackTrace();
                System.exit(-1);
            }
        }
        System.out.println(Thread.currentThread().getName() + " exit," + count + "," + size + "," + sum);
    }
}
