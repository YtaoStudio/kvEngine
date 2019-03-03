package com.alibabacloud.polar_race.engine.common.impl;

import com.alibabacloud.polar_race.engine.common.utils.DirectFileInputStream;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class ValueLog {

    /*映射的fileChannel对象*/
    private FileChannel fileChannel;

    private RandomAccessFile randomAccessFile;

    //    private ByteBuffer directWriteBuffer;
    private ThreadLocal<DirectFileInputStream> local;
    private String vlogPath;

    public ValueLog(String storePath, int filename, boolean exists) {
        /*打开文件*/
        try {
            vlogPath = storePath + "/" + "v" + filename;
            File file = new File(storePath, "v" + filename);
//            if (!file.exists()) {
            if (!exists) {
                try {
                    file.createNewFile();
                } catch (IOException e) {
                    System.out.println("Create file" + "valueLog" + filename + "failed");
                    e.printStackTrace();
                }
            }
            this.local = new ThreadLocal<>();
            this.randomAccessFile = new RandomAccessFile(file, "rw");
            this.fileChannel = this.randomAccessFile.getChannel();
        } catch (FileNotFoundException e) {
            System.out.println("create file channel " + "valueLog" + " Failed. ");
        }

//        this.directWriteBuffer = ByteBuffer.allocateDirect(4096);
    }

    public long getFileLength() {
        try {
            return this.randomAccessFile.length();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public void putMessageDirect(byte[] value) {
//        this.directWriteBuffer.clear();
//        this.directWriteBuffer.put(value);
//        this.directWriteBuffer.flip();
//        try {
//            this.fileChannel.write(this.directWriteBuffer);
//        } catch (IOException e){
//            e.printStackTrace();
//        }
    }

    public void putMessageDirect(ByteBuffer buffer) {
        try {
            this.fileChannel.write(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public long getWrotePosition() {
        try {
            return this.fileChannel.position();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }

    void setWrotePosition(long wrotePosition) {
        try {
            this.fileChannel.position(wrotePosition);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    byte[] getMessageDirect(long offset, ByteBuffer byteBuffer, byte[] bytes) {
        byteBuffer.clear();
        try {
            fileChannel.read(byteBuffer, offset);
        } catch (IOException e) {
            e.printStackTrace();
        }
        byteBuffer.flip();
        byteBuffer.get(bytes);
        return bytes;
    }

    byte[] getMessageDirect(long offset, byte[] bytes) {
        if (local.get() == null) {
            try {
                local.set(new DirectFileInputStream(vlogPath));
            } catch (IOException e) {
                System.out.println("ThreadLocal " + vlogPath + "failed");
                e.printStackTrace();
            }
        }
        try {
            local.get().seek(offset);
            local.get().read(bytes, 0, 4096);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bytes;
    }


    public void close() {
//        directWriteBuffer = null;
        local = null;
        try {
            this.fileChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
