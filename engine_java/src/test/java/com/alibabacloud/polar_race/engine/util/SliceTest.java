package com.alibabacloud.polar_race.engine.util;

import org.junit.Test;

import com.engine.util.Slices;
import java.util.Random;

import static com.alibabacloud.polar_race.engine.EngineRaceUtil.randomByte;
import static com.alibabacloud.polar_race.engine.EngineRaceUtil.toStr;

public class SliceTest {

    @Test
    public void testBytesToLong() {
        String s = " ";
        byte[] bb = s.getBytes();
        System.out.println(bb.length);
        Random r = new Random();
        for (int i = 0; i < 30; i++) {
            if (i % 3 == 0) {
                byte[] bytes = randomByte(r, 8, 8);
                System.out.println(toStr(bytes) + "\t" + Slices.bytesToLong(bytes));
            } else {
                byte[] bytes = randomByte(r, 8);
                System.out.println(toStr(bytes) + "\t" + Slices.bytesToLong(bytes));
            }
        }
    }

    @Test
    public void testValueInt() {
        int fileNo = 15;
        int pos = 123456;

        int value = Slices.valueInt(fileNo, pos);
        System.out.println(value);

        fileNo = value >> 24;
        pos = value & ((1 << 24) - 1);
        System.out.println(fileNo + " " + pos);

        int key = 81060999;
        fileNo = key >> 24;
        pos = (key & ((1 << 24) - 1)) - 1;
        System.out.println(fileNo + " " + pos);
    }

    @Test
    public void testByteStr() {
        byte[] key = randomByte(new Random(), 8);
        System.out.println(toStr(key));
        System.out.println(key[0] + "," + key[1]);
        System.out.println(toStr(Slices.wrappedBuffer(key).copyBytes(0, 4)));

        System.out.println(Integer.MAX_VALUE);
    }
}
