package com.alibabacloud.polar_race.engine.common.impl;

import io.mappedbus.MemoryMappedFile;

import java.io.File;
import java.io.IOException;

import static com.alibabacloud.polar_race.engine.common.utils.Constants.PAGE_SIZE;

public class CacheLog {
    /*映射的内存对象*/
    private MemoryMappedFile memoryMappedFile;
    private int size;

    public CacheLog(String storePath, int filename, int size) {
        /*打开文件，并将文件映射到内存*/
        try {
            File file = new File(storePath, "c" + filename);
            if (!file.exists()) {
                try {
                    file.createNewFile();
                } catch (IOException e) {
                    System.out.println("Create file" + "cache" + "failed");
                    e.printStackTrace();
                }
            }
            this.size = size;
            this.memoryMappedFile = new MemoryMappedFile(storePath + "/c" + filename, size << 12);
        } catch (Exception e) {
            System.out.println("map file " + "cache" + " Failed. ");
        }
    }

    public int getSize() {
        return size;
    }

    public void putCache(byte[] cache, int offset, int wrotePosition) {
        this.memoryMappedFile.putBytes(wrotePosition, cache, 0, 8);
        this.memoryMappedFile.putInt(wrotePosition + 8, offset);
    }

    //mappedbytebuffer读取数据,用于恢复hash
    public MemoryMappedFile getMappedBusReader() {
        return this.memoryMappedFile;
    }

    //清理memoryMappedFile
    public void close() {
//        System.out.println("===============unmap================");

        if (this.memoryMappedFile != null)
            try {
                this.memoryMappedFile.unmap();
            } catch (Exception e) {
                e.printStackTrace();
            }
    }

}
