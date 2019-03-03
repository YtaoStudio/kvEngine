package com.alibabacloud.polar_race.engine.common.impl;

import io.mappedbus.MemoryMappedFile;

import java.io.File;
import java.io.IOException;

public class KeyMemoryLog {
    /*映射的内存对象*/
    private MemoryMappedFile memoryMappedFile;
    public static final long PAGE_SIZE = 12 * 1024 * 1024; // 12 * 64 * 1024 * 1024

    public KeyMemoryLog(String storePath) {
        /*打开文件，并将文件映射到内存*/
        try {
            File file = new File(storePath, "key");
            if (!file.exists()) {
                try {
                    file.createNewFile();
                } catch (IOException e) {
                    System.out.println("Create file" + "key" + "failed");
                    e.printStackTrace();
                }
            }
            this.memoryMappedFile = new MemoryMappedFile(storePath + "/key", PAGE_SIZE);
        } catch (Exception e) {
            System.out.println("map file " + "key" + " Failed. ");
        }
    }

    public void putKey(byte[] key, int offset, int wrotePosition) {
        this.memoryMappedFile.putBytes(wrotePosition, key, 0, 8);
        this.memoryMappedFile.putInt(wrotePosition + 8, offset);
    }

    //mappedbytebuffer读取数据,用于恢复hash
    public MemoryMappedFile getMappedBusReader() {
        return this.memoryMappedFile;
    }

    //清理memoryMappedFile
    public void close() {
        System.out.println("===============unmap================");

        if (this.memoryMappedFile != null)
            try {
                this.memoryMappedFile.unmap();
            } catch (Exception e) {
                e.printStackTrace();
            }
    }

}
