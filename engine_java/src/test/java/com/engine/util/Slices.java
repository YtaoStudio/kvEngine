package com.engine.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.*;
import java.util.IdentityHashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public final class Slices {

    /**
     * A buffer whose capacity is {@code 0}.
     */
    public static final Slice EMPTY_SLICE = new Slice(0);

    private Slices() {
    }

    public static Slice ensureSize(Slice existingSlice, int minWritableBytes) {
        if (existingSlice == null) {
            existingSlice = EMPTY_SLICE;
        }

        if (minWritableBytes <= existingSlice.length()) {
            return existingSlice;
        }

        int newCapacity;
        if (existingSlice.length() == 0) {
            newCapacity = 1;
        } else {
            newCapacity = existingSlice.length();
        }
        int minNewCapacity = existingSlice.length() + minWritableBytes;
        while (newCapacity < minNewCapacity) {
            newCapacity <<= 1;
        }

        Slice newSlice = allocate(newCapacity);
        newSlice.setBytes(0, existingSlice, 0, existingSlice.length());
        return newSlice;
    }

    public static Slice allocate(int capacity) {
        if (capacity == 0) {
            return EMPTY_SLICE;
        }
        return new Slice(capacity);
    }

    public static Slice wrappedBuffer(byte[] array) {
        if (array.length == 0) {
            return EMPTY_SLICE;
        }
        return new Slice(array);
    }

    public static Slice copiedBuffer(ByteBuffer source, int sourceOffset, int length) {
        requireNonNull(source, "source is null");
        int newPosition = source.position() + sourceOffset;
        return copiedBuffer((ByteBuffer) source.duplicate().order(ByteOrder.LITTLE_ENDIAN).clear().limit(newPosition + length).position(newPosition));
    }

    public static Slice copiedBuffer(ByteBuffer source) {
        requireNonNull(source, "source is null");
        Slice copy = allocate(source.limit() - source.position());
        copy.setBytes(0, source.duplicate().order(ByteOrder.LITTLE_ENDIAN));
        return copy;
    }

    public static Slice copiedBuffer(String string, Charset charset) {
        requireNonNull(string, "string is null");
        requireNonNull(charset, "charset is null");

        return wrappedBuffer(string.getBytes(charset));
    }

    public static ByteBuffer encodeString(CharBuffer src, Charset charset) {
        CharsetEncoder encoder = getEncoder(charset);
        ByteBuffer dst = ByteBuffer.allocate(
                (int) ((double) src.remaining() * encoder.maxBytesPerChar()));
        try {
            CoderResult cr = encoder.encode(src, dst, true);
            if (!cr.isUnderflow()) {
                cr.throwException();
            }
            cr = encoder.flush(dst);
            if (!cr.isUnderflow()) {
                cr.throwException();
            }
        } catch (CharacterCodingException x) {
            throw new IllegalStateException(x);
        }
        dst.flip();
        return dst;
    }

    public static String decodeString(ByteBuffer src, Charset charset) {
        CharsetDecoder decoder = getDecoder(charset);
        CharBuffer dst = CharBuffer.allocate(
                (int) ((double) src.remaining() * decoder.maxCharsPerByte()));
        try {
            CoderResult cr = decoder.decode(src, dst, true);
            if (!cr.isUnderflow()) {
                cr.throwException();
            }
            cr = decoder.flush(dst);
            if (!cr.isUnderflow()) {
                cr.throwException();
            }
        } catch (CharacterCodingException x) {
            throw new IllegalStateException(x);
        }
        return dst.flip().toString();
    }

    private static final ThreadLocal<Map<Charset, CharsetEncoder>> encoders =
            new ThreadLocal<Map<Charset, CharsetEncoder>>() {
                @Override
                protected Map<Charset, CharsetEncoder> initialValue() {
                    return new IdentityHashMap<>();
                }
            };

    private static final ThreadLocal<Map<Charset, CharsetDecoder>> decoders =
            new ThreadLocal<Map<Charset, CharsetDecoder>>() {
                @Override
                protected Map<Charset, CharsetDecoder> initialValue() {
                    return new IdentityHashMap<>();
                }
            };

    /**
     * Returns a cached thread-local {@link CharsetEncoder} for the specified
     * <tt>charset</tt>.
     */
    private static CharsetEncoder getEncoder(Charset charset) {
        if (charset == null) {
            throw new NullPointerException("charset");
        }

        Map<Charset, CharsetEncoder> map = encoders.get();
        CharsetEncoder e = map.get(charset);
        if (e != null) {
            e.reset();
            e.onMalformedInput(CodingErrorAction.REPLACE);
            e.onUnmappableCharacter(CodingErrorAction.REPLACE);
            return e;
        }

        e = charset.newEncoder();
        e.onMalformedInput(CodingErrorAction.REPLACE);
        e.onUnmappableCharacter(CodingErrorAction.REPLACE);
        map.put(charset, e);
        return e;
    }

    /**
     * Returns a cached thread-local {@link CharsetDecoder} for the specified
     * <tt>charset</tt>.
     */
    private static CharsetDecoder getDecoder(Charset charset) {
        if (charset == null) {
            throw new NullPointerException("charset");
        }

        Map<Charset, CharsetDecoder> map = decoders.get();
        CharsetDecoder d = map.get(charset);
        if (d != null) {
            d.reset();
            d.onMalformedInput(CodingErrorAction.REPLACE);
            d.onUnmappableCharacter(CodingErrorAction.REPLACE);
            return d;
        }

        d = charset.newDecoder();
        d.onMalformedInput(CodingErrorAction.REPLACE);
        d.onUnmappableCharacter(CodingErrorAction.REPLACE);
        map.put(charset, d);
        return d;
    }

    // todo: the length of {byte[] b} is not always 8

    /**
     * byte[] to long
     * <p>
     * refer: https://www.cnblogs.com/wzhanke/p/4562056.html
     * <br>
     *
     * @param b
     * @return
     */
    public static long bytesToLong(byte[] b) {
        long s = 0;
        long s0 = b[0] & 0xff;// 最低位
        long s1 = b[1] & 0xff;
        long s2 = b[2] & 0xff;
        long s3 = b[3] & 0xff;
        long s4 = b[4] & 0xff;// 最低位
        long s5 = b[5] & 0xff;
        long s6 = b[6] & 0xff;
        long s7 = b[7] & 0xff;

        // s0不变
        s1 <<= 8;
        s2 <<= 16;
        s3 <<= 24;
        s4 <<= 8 * 4;
        s5 <<= 8 * 5;
        s6 <<= 8 * 6;
        s7 <<= 8 * 7;
        s = s0 | s1 | s2 | s3 | s4 | s5 | s6 | s7;
        return s;
    }

    /**
     * long to byte[]
     *
     * @param x
     * @return
     */
    public static byte[] longToBytes(long x) {
        long temp = x;
        byte[] b = new byte[8];
        for (int i = 0; i < b.length; i++) {
            b[i] = new Long(temp & 0xff).byteValue();// 将最低位保存在最低位 temp = temp
            // >> 8;// 向右移8位
        }
        return b;
    }

    /**
     * file_no + offset -> int 4
     *
     * @param vNo
     * @param vOffset
     * @return
     */
    public static int valueInt(int vNo, int vOffset) {
        return (vNo << 24) + vOffset;
    }

    /**
     * byte[] -> int
     *
     * @param src
     * @param offset
     * @return
     */
    public static int bytesToInt(byte[] src, int offset) {
        int value;
        value = (int) ((src[offset] & 0xFF)
                | ((src[offset + 1] & 0xFF) << 8)
                | ((src[offset + 2] & 0xFF) << 16)
                | ((src[offset + 3] & 0xFF) << 24));
        return value;
    }

    public static byte[] intToBytes(int vNoOffset) {
        int temp = vNoOffset;
        byte[] b = new byte[4];
        for (int i = 0; i < b.length; i++) {
            b[i] = new Integer(temp & 0xff).byteValue();// 将最低位保存在最低位
            temp = temp >> 8;// 向右移8位
        }
        return b;
    }
}
