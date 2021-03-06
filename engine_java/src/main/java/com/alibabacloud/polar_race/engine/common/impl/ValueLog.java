package com.alibabacloud.polar_race.engine.common.impl;

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
//    private File file;

    public ValueLog(String storePath, int filename) {
        /*打开文件*/
        try {
            File file = new File(storePath, "v" + filename);
            if (!file.exists()) {
                try {
                    file.createNewFile();
                } catch (IOException e) {
                    System.out.println("Create file" + "valueLog" + filename + "failed");
                    e.printStackTrace();
                }
            }
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

    public void putMessageDirect(ByteBuffer buffer, long offset) {
        try {
            this.fileChannel.write(buffer, offset);
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

    public byte[] getMessageDirect(long offset, ByteBuffer byteBuffer, byte[] bytes) {
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

    public void close() {
//        directWriteBuffer = null;
        try {
            this.fileChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
