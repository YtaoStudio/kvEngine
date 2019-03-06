package com.alibabacloud.polar_race.engine.common.utils;

import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

/**
 * directIO方式读取文件的InputStream, 支持文件偏移，随机访问
 */
public class DirectFileInputStream extends InputStream {

    //just a couple of them are used, here for future uses
    private static final int O_RDONLY = 00;
    private static final int O_WRONLY = 01;
    private static final int O_RDWR = 02;
    private static final int O_CREAT = 0100;
    private static final int O_EXCL = 0200;
    private static final int O_NOCTTY = 0400;
    private static final int O_TRUNC = 01000;
    private static final int O_APPEND = 02000;
    private static final int O_NONBLOCK = 04000;
    private static final int O_NDELAY = O_NONBLOCK;
    private static final int O_SYNC = 010000;
    private static final int O_ASYNC = 020000;
    private static final int O_DIRECT = 040000;
    private static final int O_DIRECTORY = 0200000;
    private static final int O_NOFOLLOW = 0400000;
    private static final int O_NOATIME = 01000000;
    private static final int O_CLOEXEC = 02000000;

    private static final int SEEK_SET = 0;
    private static final int SEEK_CUR = 1;
    private static final int SEEK_END = 2;

    //    public static final int BLOCK_SIZE = 128 * 1024;  // multiple of 512.
    public static final int BLOCK_SIZE = 4096;


    static {
        try {
            Native.register("c");
        } catch (Throwable e) {
            throw e;
        }
    }

    private native int open(String pathname, int flags);

    private native int read(int fd, Pointer buf, int count);

    private native int posix_memalign(PointerByReference memptr, int alignment, int size);

    private native void free(Pointer memptr);

    private native int close(int fd);

    static native int read(int fd, byte[] b, int length);

    static native int lseek(int fd, long offset, int whence);

    private String filePath;
    private int fd;
    private Pointer bufPnt;
    private byte[] buffer;
    private ThreadLocal<byte[]> threadLocalReadBytes = ThreadLocal.withInitial(() -> new byte[4096]);
    private int pos;
    private int available;
    private long currentOffset;  // 当前绝对偏移位置
    private final long minBoundIndex;

    public DirectFileInputStream(String pathname) throws IOException {
        this(pathname, 0);
    }

    public DirectFileInputStream(String filePath, long offset) throws IOException {
        this.minBoundIndex = offset;
        this.filePath = filePath;
        fd = open(this.filePath, O_RDONLY | O_DIRECT);
        if (fd == -1) {
            throw new FileNotFoundException("Error occurs during opening input file: " + filePath + ", Error NO is: ");
        }

        PointerByReference pntByRef = new PointerByReference();
        if (posix_memalign(pntByRef, BLOCK_SIZE, BLOCK_SIZE) != 0) {
            throw new IOException("Fail to exec posix_memalign!");
        }
        bufPnt = pntByRef.getValue();
//        buffer = new byte[BLOCK_SIZE];
        buffer = threadLocalReadBytes.get();

        int skipBlockNum = (int) (offset / BLOCK_SIZE);
        int redundantOffset = (int) (offset % BLOCK_SIZE);

        // directIO 只能跳过成块数据
        // 多余数据，通过skipRedundantBytes来进行过滤即可完整跳过offset
        long alignOffset = (long) skipBlockNum * BLOCK_SIZE;

        // 部分情况下，offset为0，如实时索引，增量索引在创建compoundFile的时候
        if (offset > 0) lseek(fd, alignOffset, SEEK_SET);
        currentOffset = alignOffset;
        fillAndSkip(redundantOffset);
    }

    private void fill() throws IOException {
        int rtn = read(fd, bufPnt, BLOCK_SIZE);
        if (rtn < 0) {
            throw new IOException("Error occurs while filling, filePath: " + filePath + ", last error no: ");
        }
        if (rtn == 0) {
            throw new EOFException();
        }
        bufPnt.read(0, buffer, 0, rtn);
        pos = 0;
        available = rtn;
    }

    private void fillAndSkip(int size) throws IOException {
        assert size >= 0 && size < BLOCK_SIZE;
        if (size == 0) {
            fill();
        } else {
            while (size > 0) {
                fill();
                int skip = size > available ? available : size;
                size -= skip;
                pos += skip;
                currentOffset += skip;
            }
        }
    }

    @Override
    public int read(byte bytes[], int off, int len) throws IOException {
//        checkBounds(off, len, bytes.length);

        if (len == 0) {
            return 0;
        }

        int b = read(), originOff = off;
        if (b == -1) return -1;
        bytes[off++] = (byte) b;
        len--;
        int overflow = pos + len - available;

        do {
            if (overflow <= 0) {
                System.arraycopy(buffer, pos, bytes, off, len);
                off += len;
                pos += len;
                currentOffset += len;
                break;
            } else {
                int bytesToCopy = len - overflow;
                System.arraycopy(buffer, pos, bytes, off, bytesToCopy);
                len -= bytesToCopy;
                off += bytesToCopy;
                pos += bytesToCopy;
                currentOffset += bytesToCopy;
                try {
                    fill();
                } catch (EOFException eof) {
                    break;
                }
                overflow = overflow - available;
            }
        } while (true);

        return off - originOff;
    }

    @Override
    public final int read() throws IOException {
        if (pos >= available) {
            try {
                fill();
            } catch (EOFException eof) {  // ignore.
                return -1;
            }
        }
        currentOffset += 1;
        return buffer[pos++] & 0xFF;
    }

    @Override
    public void close() throws IOException {
        if (fd == -1) {
            return;
        }
        if (bufPnt != null) {
            free(bufPnt);
        }
        int tmp = fd;
        fd = -1;
        if (close(tmp) < 0) {
            throw new IOException("Error occurs while closing file " + filePath + ", fd: " + tmp);
        }
    }

    private void checkBounds(int off, int len, int size) {
        if ((off | len | (off + len) | (size - (off + len))) < 0)
            throw new IndexOutOfBoundsException("offset: " + off + ", len: " + len + ", size: " + size);
    }

    public void seek(long offset) throws IOException {
        checkMinBound(offset);
        if (offset >= currentOffset - pos && offset < currentOffset - pos + BLOCK_SIZE) {
            pos = (int) (offset + pos - currentOffset);
            currentOffset = offset;
        } else {
            int skipBlockNum = (int) (offset / BLOCK_SIZE);
            int redundantOffset = (int) (offset % BLOCK_SIZE);
            long alignOffset = (long) skipBlockNum * BLOCK_SIZE;
            lseek(fd, alignOffset, SEEK_SET);
            currentOffset = alignOffset;
            fillAndSkip(redundantOffset);
        }
    }

    private void checkMinBound(long index) throws IOException {
        if (index < this.minBoundIndex) {
            throw new IOException("Cannot not go backwards before minBoundIndex! minBoundIndex: " + this.minBoundIndex + ", idx:" + index);
        }
    }

    public long getCurrentOffset() {
        return currentOffset;
    }

    public static void main(String[] args) {

    }

}
