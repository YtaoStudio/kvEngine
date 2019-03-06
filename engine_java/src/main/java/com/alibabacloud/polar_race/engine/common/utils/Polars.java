package com.alibabacloud.polar_race.engine.common.utils;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.*;
import java.util.IdentityHashMap;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Polars {
    public static int byteArrayToInt(byte[] b) {
        return b[3] & 0xFF |
                (b[2] & 0xFF) << 8 |
                (b[1] & 0xFF) << 16 |
                (b[0] & 0xFF) << 24;
    }

    public static byte[] intToByteArray(int a) {
        return new byte[]{
                (byte) ((a >> 24) & 0xFF),
                (byte) ((a >> 16) & 0xFF),
                (byte) ((a >> 8) & 0xFF),
                (byte) (a & 0xFF)
        };
    }

    public static long byteArrayToLong(byte[] b) {

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
        return s0 | s1 | s2 | s3 | s4 | s5 | s6 | s7;

    }

    public static byte[] longToByteArray(long a) {
        long temp = a;
        byte[] b = new byte[8];
        for (int i = 0; i < b.length; i++) {
            b[i] = new Long(temp & 0xff).byteValue();

            temp = temp >> 8; // 向右移8位
        }
        return b;
    }

    public static long bytesToLong(byte[] buffer) {
        long values = 0;
        for (int i = 0; i < 8; i++) {
            values <<= 8;
            values |= (buffer[i] & 0xff);
        }
        return values;
    }

    //因为offset最多需要26位，所以28位是很足够的
    public static byte[] intToVarByteArray(int value) {
        byte[] bytes = null;
        int highBitMask = 0x80;

        if (value < (1 << 7) && value >= 0) {
            bytes = new byte[1];
            bytes[0] = (byte) value;
        } else if (value < (1 << 14) && value > 0) {
            bytes = new byte[2];
            bytes[0] = (byte) (value | highBitMask);
            bytes[1] = (byte) (value >>> 7);
        } else if (value < (1 << 21) && value > 0) {
            bytes = new byte[3];
            bytes[0] = (byte) (value | highBitMask);
            bytes[1] = (byte) (value >>> 7 | highBitMask);
            bytes[2] = (byte) (value >>> 14);
        } else {
            bytes = new byte[4];
            bytes[0] = (byte) (value | highBitMask);
            bytes[1] = (byte) (value >>> 7 | highBitMask);
            bytes[2] = (byte) (value >>> 14 | highBitMask);
            bytes[3] = (byte) (value >>> 21);
        }
        return bytes;
    }

    public static int varByteArrayToInt(byte[] value) {
        int result = 0;
        for (int shift = 0; shift <= 28; shift += 7) {
            int b = value[shift / 7];

            // add the lower 7 bits to the result
            result |= ((b & 0x7f) << shift);

            // if high bit is not set, this is the last byte in the number
            if ((b & 0x80) == 0) {
                return result;
            }
        }

        return result;
    }

    public static int bytesGetTwo(byte[] key) {
        return (key[0] - 48) * 10 + (key[1] - 48);
    }

    public static int getHashCode(byte[] key) {
        return (key[0] & 0xFF) >> 3;
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

    public static PolarSlice wrappedBuffer(byte[] array) {
        return new PolarSlice(array);
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

    public int compare(byte[] left, byte[] right) {
        for (int i = 0; i < 8; i++) {
            int thisByte = 0xFF & left[i];
            int thatByte = 0xFF & right[i];
            if (thisByte != thatByte) {
                return (thisByte) - (thatByte);
            }
        }
        return 0;
    }

    private String getStringByKey(Long keyLong, ByteBuffer byteBuffer, byte[] bytes) {
        byteBuffer.clear();
        byteBuffer.putLong(keyLong);
        byteBuffer.flip();
        byteBuffer.get(bytes);
        return new String(bytes, UTF_8);
    }

}
