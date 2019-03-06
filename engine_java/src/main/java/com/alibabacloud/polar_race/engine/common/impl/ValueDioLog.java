package com.alibabacloud.polar_race.engine.common.impl;

import com.alibabacloud.polar_race.engine.common.utils.DirectFileInputStream;
import net.smacke.jaydio.buffer.AlignedDirectByteBuffer;
import net.smacke.jaydio.channel.DirectIoByteChannel;

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
    private DirectIoByteChannel dioChannel;

    public ValueDioLog(String storePath, int filename) {
        /*打开文件*/
        File file = null;
        try {
            file = new File(storePath, "v" + filename);
            this.randomAccessFile = new RandomAccessFile(file, "rw");
            this.fileChannel = this.randomAccessFile.getChannel();
            length = this.randomAccessFile.length();
            if (length > 0) {
                this.inputStream = new DirectFileInputStream(file.getPath());
                this.dioChannel = DirectIoByteChannel.getChannel(file, true);
            }
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
            if (this.inputStream != null) {
                this.inputStream.close();
                dioChannel.close();
            }
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

    public byte[][] getRangeMessageDirect(ByteBuffer byteBuffer, byte[][] cache, int start, int end) {
        try {
            fileChannel.position(0);
            while (start < end) {
                byteBuffer.clear();
//                fileChannel.read(byteBuffer, start << 12);
                fileChannel.read(byteBuffer);
                byteBuffer.flip();
                byteBuffer.get(cache[start]);
                start++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return cache;
    }

    public byte[] getRangeMessageDirect(byte[] cache, int size) {
        try {
            inputStream.read(cache, 0, size);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return cache;
    }

    public byte[][] getRangeMessageDirect(byte[][] cache, int start, int end) {
        try {
            while (start < end) {
                inputStream.seek(start << 12);
                inputStream.read(cache[start], 0, 4096);
                start++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return cache;
    }

    public void getRangeMessageDirect(AlignedDirectByteBuffer dBuffer) {
        try {
            dioChannel.read(dBuffer, 0);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
