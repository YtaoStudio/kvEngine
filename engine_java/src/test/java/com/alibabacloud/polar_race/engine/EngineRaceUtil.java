package com.alibabacloud.polar_race.engine;

import java.util.Random;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @version V1.0
 * @Package: com.alibabacloud.polar_race.engine
 * @ClassName: EngineRaceUtil
 * @Description:
 * @author: tao
 * @date: Create in 2018-11-08 11:52
 **/
public class EngineRaceUtil {

    // /media/tao/Exercise/Ubuntu/software/station/tianchi/engine/engine_/
    // /home/tao/software/station/tianchi/engine/engine_/
    public static String path = "/home/tao/software/station/tianchi/engine/engine_/";
    public static String logFilePath = "/home/tao/software/station/tianchi/engine/engine_java/src/test/java/com/alibabacloud/polar_race/engine/";
    public static String kvFilePath = "/media/tao/Exercise/Ubuntu/software/station/tianchi/engine/engine_java/src/test/java/com/alibabacloud/polar_race/engine/";
    public static String fileName = "key";
    public static String result_file = "/home/tao/result_file";
    public static String result_file_log = "/home/tao/result_file.csv";
    public static String pcPath = "/media/tao/Exercise/Ubuntu/software/station/tianchi/engine/engine_java/src/test/java/io/mappedbus";

    public static final int BUFFER_SIZE = 1024 * 1024 * 32;

    public static byte[] randomByte(Random random, int length) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) (random.nextInt(10) + 48);
        }
        return bytes;
    }

    public static byte[] randomByte(Random random, int length, int empty) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < bytes.length - empty; i++) {
            bytes[i] = (byte) (random.nextInt(10) + 48);
        }
        for (int i = length - empty; i < length; i++) {
            bytes[i] = (byte) (32);
        }
        return bytes;
    }

    public static int randomInt(Random random, int max) {
        return random.nextInt(max);
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
