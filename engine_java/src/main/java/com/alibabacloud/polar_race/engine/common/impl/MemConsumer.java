package com.alibabacloud.polar_race.engine.common.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class MemConsumer implements Runnable {

    private TSortedLongIntHashMap tmap;
    private FileChannel fileChannel;
    private ThreadLocal<byte[]> threadLocalKeyBytes = ThreadLocal.withInitial(() -> new byte[8]);

    public MemConsumer(String storePath, TSortedLongIntHashMap tmap, int index) {
        File file = new File(storePath, "k" + index);
        try {
            fileChannel = new RandomAccessFile(file, "rw").getChannel();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        this.tmap = tmap;
    }

    @Override
    public void run() {
//        if (keyLog.getSize() > 0) {
        int pos = 0;
        byte[] key = threadLocalKeyBytes.get();
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(8);
        ByteBuffer offfsetBuffer = ByteBuffer.allocateDirect(4);
        int res = 0;
        long keyLong;
        int value;
        while (true) {
            try {
                byteBuffer.clear();
                try {
                    res = fileChannel.read(byteBuffer);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                if (res == -1) {
                    break;
                }
                byteBuffer.flip();
                byteBuffer.get(key);

                offfsetBuffer.clear();
                try {
                    res = fileChannel.read(offfsetBuffer);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                if (res == -1) {
                    break;
                }
                offfsetBuffer.flip();
                value = offfsetBuffer.getInt();

                keyLong = ByteBuffer.wrap(key).getLong();
                if (keyLong == 0 && value == 0) {
                    break;
                }else{
                    tmap.put(keyLong, Integer.reverseBytes(value));
                }

                pos += 12;
            } catch (Throwable t) {
                t.printStackTrace();
                System.exit(-1);
            }
        }
        if (tmap.size() > 0) {
            tmap.sort();
        }
        byteBuffer = null;
        offfsetBuffer = null;
        threadLocalKeyBytes = null;
        close();
    }

    private void close() {
        try {
            fileChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
//    }
}
