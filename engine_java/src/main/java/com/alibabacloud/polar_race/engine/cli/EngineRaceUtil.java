package com.alibabacloud.polar_race.engine.cli;

import java.util.Random;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @version V1.0
 * @Package: com.alibabacloud.polar_race.engine.cli
 * @ClassName: EngineRaceUtil
 * @Description:
 * @author: tao
 * @date: Create in 2018-11-08 11:52
 **/
public class EngineRaceUtil {

    public static byte[] randomByte(Random random, int length) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) (random.nextInt(10) + 48);
        }
        return bytes;
    }

    public static String toStr(byte[] key) {
        if (key == null) {
            return null;
        }
        return new String(key, UTF_8);
    }

    public static byte[] toByteArray(String value) {
        return value.getBytes(UTF_8);
    }

    public static String randomString(Random random, int length) {
        char[] chars = new char[length];
        for (int i = 0; i < chars.length; i++) {
            chars[i] = (char) ((int) ' ' + random.nextInt(95));
        }
        return new String(chars);

    }

}
