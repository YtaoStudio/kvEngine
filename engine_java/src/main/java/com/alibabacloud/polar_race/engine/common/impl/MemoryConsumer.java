package com.alibabacloud.polar_race.engine.common.impl;

import gnu.trove.map.hash.TLongIntHashMap;
import io.mappedbus.MemoryMappedFile;

import java.nio.ByteBuffer;

import static com.alibabacloud.polar_race.engine.common.utils.ByteToInt.bytesGetTwo;

public class MemoryConsumer implements Runnable {

    private TLongIntHashMap tmap;
    private int index;
    private int sum;
    private MemoryMappedFile reader;

    public MemoryConsumer(MemoryMappedFile reader, TLongIntHashMap tmap, int index, int sum) {
        this.reader = reader;
        this.tmap = tmap;
        this.index = index;
        this.sum = sum;
    }

    @Override
    public void run() {
        // key是否在当前数组
        int keyIndex = 0;
        byte[] key = new byte[8];
        int count = 0;
        int size = 0;
        int pos = 0;
        while (this.sum > 0) {
            try {
                reader.getBytes(pos, key, 0, 8);
                keyIndex = bytesGetTwo(key) % 64;
                if (keyIndex == index) {
                    tmap.put(ByteBuffer.wrap(key).getLong(), reader.getInt(pos + 8));
                    count++;
                } else {
                    reader.getInt(pos + 8);
                    size++;
                }
                pos += 12;
                this.sum--;
            } catch (Throwable t) {
                t.printStackTrace();
                System.exit(-1);
            }
        }
        System.out.println(Thread.currentThread().getName() + " exit," + count + "," + size + "," + sum);
    }
}
