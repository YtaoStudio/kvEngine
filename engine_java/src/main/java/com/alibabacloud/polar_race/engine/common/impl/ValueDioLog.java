package com.alibabacloud.polar_race.engine.common.impl;

import com.alibabacloud.polar_race.engine.common.utils.DirectFileInputStream;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class ValueDioLog {

    /*映射的fileChannel对象*/
    private FileChannel fileChannel;

    private RandomAccessFile randomAccessFile;
    private DirectFileInputStream inputStream;
    private long length;

    public ValueDioLog(String storePath, int filename) {
        /*打开文件*/
        File file = null;
        try {
            file = new File(storePath, "v" + filename);
            this.randomAccessFile = new RandomAccessFile(file, "rw");
            this.fileChannel = this.randomAccessFile.getChannel();
            length = this.randomAccessFile.length();
            if (length > 0)
                this.inputStream = new DirectFileInputStream(file.getPath());
        } catch (FileNotFoundException e) {
            System.out.println("create file channel " + "valueLog" + " Failed. ");
        } catch (IOException e) {
            System.out.println("create file dio failed, " + file.getPath());
        }

    }

    public long getFileLength() {
        return length;
    }

    byte[] getMessageDirectByDio(long offset, byte[] bytes) {
        try {
            synchronized (this) {
                inputStream.seek(offset);
                inputStream.read(bytes, 0, 4096);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bytes;
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
        try {
            if (this.inputStream != null)
                this.inputStream.close();
            this.fileChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public ByteBuffer getMessageDirect(int start, int end, int index, byte[][] bytes, ByteBuffer byteBuffer) {
        byteBuffer.clear();
        try {
            fileChannel.position(start * 4096);
        } catch (IOException e) {
            e.printStackTrace();
        }
        while (start < end) {
            try {
                fileChannel.read(byteBuffer);
            } catch (IOException e) {
                e.printStackTrace();
            }
//            byteBuffer.flip();
//            byteBuffer.get(bytes[index][start]);
            start++;
        }
        return byteBuffer;
    }
}
