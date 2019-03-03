package com.alibabacloud.polar_race.engine.common.impl;

import io.mappedbus.MemoryMappedFile;

import java.io.File;
import java.io.IOException;

public class MemoryLog {
    /*映射的堆外内存对象*/
    private MemoryMappedFile memoryMappedFile;

    public MemoryLog(String storePath, int filename, boolean exists) {
        /*打开文件，并将文件映射到内存*/
        try {
//            if (!file.exists()) {
            if (!exists) {
                try {
                    File file = new File(storePath, "k" + filename);
                    file.createNewFile();
                } catch (IOException e) {
                    System.out.println("Create file" + "key" + "failed");
                    e.printStackTrace();
                }
            }
            this.memoryMappedFile = new MemoryMappedFile(storePath + "/k" + filename, 12 * 1024 * 1024);
        } catch (Exception e) {
            System.out.println("map file " + "key" + " Failed. ");
        }
    }

    public void putKey(byte[] key, int offset, int wrotePosition) {
        this.memoryMappedFile.putBytes(wrotePosition, key, 0, 8);
        this.memoryMappedFile.putInt(wrotePosition + 8, offset);
    }

    //memoryMappedFile读取数据,用于恢复hash
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
