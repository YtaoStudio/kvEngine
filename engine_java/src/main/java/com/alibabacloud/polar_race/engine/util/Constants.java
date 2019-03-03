package com.alibabacloud.polar_race.engine.util;

public class Constants {
    public static long bytesToLong(byte[] buffer) {
        long values = 0;
        for (int i = 0; i < 8; i++) {
            values <<= 8;
            values |= (buffer[i] & 0xff);
        }
        return values;
    }

    public static int bytesGetTwo(byte[] key) {
        return (key[0] - 48) * 10 + (key[1] - 48);
    }


}
