package com.alibabacloud.polar_race.engine.common.utils;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Polar slice of a byte array.
 */
public final class PolarSlice
        implements Comparable<PolarSlice> {
    private final byte[] data;

    public PolarSlice(byte[] data) {
        this.data = new byte[8];
        for (int i = 0; i < 8; i++) {
            this.data[i] = data[i];
        }
//        this.data = data;
    }

    /**
     * Gets the array underlying this slice.
     */
    public byte[] getRawArray() {
        return data;
    }

    /**
     * Decodes this buffer's readable bytes into a string with the specified
     * character set name.
     */
    public String toString(Charset charset) {
        return toString(0, 8, charset);
    }

    public String toString() {
        return toString(UTF_8);
    }

    /**
     * Decodes this buffer's sub-region into a string with the specified
     * character set.
     */
    public String toString(int index, int length, Charset charset) {
        if (length == 0) {
            return "";
        }

        return Polars.decodeString(toByteBuffer(index, length), charset);
    }

    /**
     * Converts this buffer's sub-region into a NIO buffer.  The returned
     * buffer shares the content with this buffer.
     */
    public ByteBuffer toByteBuffer(int index, int length) {
        return ByteBuffer.wrap(data, index, length).order(LITTLE_ENDIAN);
    }

    /**
     * Compares the content of the specified buffer to the content of this
     * buffer.  This comparison is performed byte by byte using an unsigned
     * comparison.
     */
    @Override
    public int compareTo(PolarSlice that) {
        if (this == that) {
            return 0;
        }

        for (int i = 0; i < 8; i++) {
            int thisByte = 0xFF & this.data[i];
            int thatByte = 0xFF & that.data[i];
            if (thisByte != thatByte) {
                return (thisByte) - (thatByte);
            }
        }
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PolarSlice slice = (PolarSlice) o;

        return Arrays.equals(data, slice.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }
}
