package com.alibabacloud.polar_race.engine.common.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;

public class KeyLog {
    /*映射的fileChannel对象*/
    private FileChannel fileChannel;
    /*映射的内存对象*/
    private MappedByteBuffer mappedByteBuffer;

    public KeyLog(int FileSize, String storePath) {
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
            this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, FileSize);
        } catch (FileNotFoundException e) {
            System.out.println("create file channel " + "key" + " Failed. ");
        } catch (IOException e) {
            System.out.println("map file " + "key" + " Failed. ");
        }
    }

    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }

    private static Method method(Object target, String methodName, Class<?>[] args)
            throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";

        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }

    void setWrotePosition(int wrotePosition) {
        this.mappedByteBuffer.position(wrotePosition);
    }

    void putKey(byte[] key, int offset, int wrotePosition) {
        ByteBuffer byteBuffer = mappedByteBuffer.slice();
        byteBuffer.position(wrotePosition);
        byteBuffer.put(key);
        byteBuffer.putInt(offset);
    }

    //mappedbytebuffer读取数据,用于恢复hash
    ByteBuffer getKeyBuffer() {
        return this.mappedByteBuffer.slice();
    }

    public void close() {
        try {
            this.fileChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        clean();
    }

    //清理mappedbuffer
    public void clean() {

        System.out.println("===============unmap================");

        ByteBuffer buffer = this.mappedByteBuffer;
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)
            return;
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }

}