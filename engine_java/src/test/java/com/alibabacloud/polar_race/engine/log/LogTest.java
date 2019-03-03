package com.alibabacloud.polar_race.engine.log;

import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;

import static com.alibabacloud.polar_race.engine.EngineRaceUtil.*;
import static com.engine.common.EngineRace.VALUE_LEN;

public class LogTest {

    @Test
    public void testLogWriter() {
        String curVlogFile = logFilePath + fileName;
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(curVlogFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        FileChannel fileChannel = fos.getChannel();

        Random random = new Random();

        // write vLog
        ByteBuffer bbf = getByteBuffer(random);
        try {
            fileChannel.write(bbf);
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }

        bbf = getByteBuffer(random);
        try {
            fileChannel.write(bbf);
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }

//        try {
//            fileChannel.force(true);
//            fileChannel.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        try (FileInputStream fis = new FileInputStream(new File(curVlogFile));
             FileChannel fileChannel1 = fis.getChannel()) {
            int offset = 1;
            // read vLog
            try {
                fileChannel1.position(offset * VALUE_LEN);
            } catch (IOException e) {
                e.printStackTrace();
            }
            ByteBuffer buf = ByteBuffer.allocate(VALUE_LEN);
            int bytesRead = 0;
            try {
                bytesRead = fileChannel1.read(buf);
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (-1 == bytesRead) {
                System.out.println("error");
            }

            byte[] valueRead = buf.array();
            System.out.println(toStr(valueRead));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private ByteBuffer getByteBuffer(Random random) {
        byte[] value = randomByte(random, 4 * 1024);

        System.out.println(toStr(value));
        ByteBuffer bbf = ByteBuffer.wrap(value);
        bbf.put(value);
        bbf.flip();
        return bbf;
    }


}
