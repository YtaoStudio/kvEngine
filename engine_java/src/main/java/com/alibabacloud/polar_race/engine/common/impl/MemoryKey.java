package com.alibabacloud.polar_race.engine.common.impl;


import io.mappedbus.MappedBusMessage;
import io.mappedbus.MemoryMappedFile;

import java.util.Arrays;

import static java.nio.charset.StandardCharsets.UTF_8;

public class MemoryKey implements MappedBusMessage {
    private byte[] key;
    private int offset;

    public byte[] getKey() {
        return key;
    }

    public int getOffset() {
        return offset;
    }

    public MemoryKey() {
    }


    public MemoryKey(byte[] key) {
        this.key = key;
    }

    public MemoryKey(byte[] key, int offset) {
        this.key = key;
        this.offset = offset;
    }

    @Override
    public void write(MemoryMappedFile mem, long pos) {
        mem.putBytes(pos, key, 0, 8);
        mem.putInt(pos + 8, offset);
    }

    @Override
    public void read(MemoryMappedFile mem, long pos) {
        mem.getBytes(pos, key, 0, 8);
//        for (int i = 0; i < 8; i++)
//            key[i] = mem.getByte(pos + i);
        offset = mem.getInt(pos + 8);
    }

    @Override
    public int type() {
        return 0;
    }

    @Override
    public String toString() {
        return "MemoryKey{" +
                "key=" + new String(key, UTF_8) +
                ", offset=" + offset +
                '}';
    }
}
