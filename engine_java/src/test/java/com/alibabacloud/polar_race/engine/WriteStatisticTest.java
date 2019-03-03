package com.alibabacloud.polar_race.engine;

import com.alibabacloud.polar_race.engine.common.impl.KeyLog;
import com.engine.util.Slices;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

import static com.alibabacloud.polar_race.engine.EngineRaceUtil.*;
import static com.alibabacloud.polar_race.engine.common.utils.ByteToInt.byteArrayToLong;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @version V1.0
 * @Package: com.alibabacloud.polar_race.engine
 * @ClassName: WriteStatisticTest
 * @Description:
 * @author: tao
 * @date: Create in 2018-11-11 09:25
 **/
public class WriteStatisticTest {

    @Test
    public void testStatistic() {
        List<String> keys = new ArrayList<>();
        try (BufferedReader statReader = new BufferedReader(new FileReader(result_file))) {
            String line;
            String[] lines;
            int count = 0;
            while ((line = statReader.readLine()) != null) {
                lines = line.split(",");
                if (lines.length == 4) {
                    // true
                    keys.add(lines[1]);
                }
                if (lines.length == 3) {
                    // false
                    keys.add(lines[2]);
                }
                count++;
            }
            System.out.println(keys.size());
            System.out.println(count);
        } catch (IOException e) {
            e.printStackTrace();
        }

        StringBuilder sb = new StringBuilder();
        Map<Long, Integer> map = new HashMap();
        try (BufferedWriter logWriter = new BufferedWriter(new FileWriter(result_file_log), BUFFER_SIZE)) {
            int i = 0;
            for (String keyStr : keys) {
                byte[] key = keyStr.getBytes();

                System.out.println(i + "," + keyStr + "," + Slices.bytesToLong(key) + "," + Slices.bytesToLong(key) + "," + Slices.bytesToLong(key) % 64);
//                long num = Slices.bytesToLong(key) % 64;
                long num = Integer.parseInt(keyStr.substring(0, 2)) % 64;
                if (map.get(num) == null) {
                    map.put(num, 1);
                } else {
                    map.put(num, map.get(num) + 1);
                }
                sb.append(i).append(",").append(keyStr).append(",").append(Slices.bytesToLong(key)).append("\n");
                logWriter.write(sb.toString());
                i++;
                sb = new StringBuilder();
            }
            System.out.println(map.size());
            int sum = 0;
            for (Map.Entry<Long, Integer> c : map.entrySet()) {
                sum += c.getValue();
                System.out.println(c.getKey() + "," + c.getValue());
            }
            System.out.println(sum + "," + keys.size());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testFileChannel() {
        File file = new File(kvFilePath + fileName);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        FileChannel fileChannel = null;

        int keyNo = 0;
        byte[] value = randomByte(new Random(), 8, 3);

        String str = new String(Slices.wrappedBuffer(value).copyBytes(0, 3), UTF_8);
        System.out.println(str);
        try {
            fileChannel = new RandomAccessFile(file, "rw").getChannel();
            ByteBuffer bbf = ByteBuffer.wrap(value);
//            bbf.clear();
//            bbf.put(value);
//            bbf.flip();
//            fileChannel.position(keyNo);
            fileChannel.write(bbf);

//            fileChannel.position(1 * 8);
            value = randomByte(new Random(), 8, 2);
            bbf = ByteBuffer.wrap(value);
//            bbf.clear();
//            bbf.put(value);
//            bbf.flip();
            fileChannel.write(bbf);

//            fileChannel.position(3 * 8);
            value = randomByte(new Random(), 8, 2);
            bbf = ByteBuffer.wrap(value);
//            bbf.clear();
//            bbf.put(value);
//            bbf.flip();
            fileChannel.write(bbf);

            MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 24);

            byte[] key = new byte[8];
            bbf = mappedByteBuffer.slice();
            bbf.get(key);

            System.out.println(new String(key, UTF_8));

            getBuffers(bbf);

            bbf.get(key);

            System.out.println("later:" + new String(key, UTF_8));

            bbf.get(key);

            System.out.println(new String(key, UTF_8));
            fileChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void getBuffers(ByteBuffer bbf) {
        byte[] key = new byte[8];
        bbf.position(1);
        bbf.get(key);
        System.out.println("getBuffers:" + new String(key, UTF_8));
    }

    @Test
    public void testLong() {
        long l = 1000000;
        System.out.println(Long.MAX_VALUE);
        System.out.println(Long.MAX_VALUE / 4);
        System.out.println(Long.MAX_VALUE / 4 / 1024);
        System.out.println(l);
        System.out.println(Integer.MAX_VALUE);
        FileChannel fileChannel = null;
        File file = new File(kvFilePath + fileName);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            fileChannel = new RandomAccessFile(file, "rw").getChannel();
            int count = 10;
            int keyNo = count - 1;
            int lens = 4;
            byte[] value = new byte[lens];

            for (int i = 0; i < count; i++) {
                value = randomByte(new Random(), lens);
                fileChannel.write(ByteBuffer.wrap(value));
                if (i == keyNo) {
                    System.out.println(new String(value, UTF_8));
                }
            }
            System.out.println(fileChannel.position() + "-pos");

            System.out.println(keyNo);
            int vNoOffset = Slices.valueInt(1, keyNo);
            System.out.println(vNoOffset + "-offset");

            int keyNo1 = (vNoOffset & ((1 << 24) - 1));
            System.out.println(keyNo1 + "-offset");
            int len = keyNo / lens;
            int pos = keyNo - lens * len;
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            for (int j = 0; j < keyNo; j++) {
                randomAccessFile.skipBytes(lens);
            }
//            randomAccessFile.skipBytes(pos);
            int bytesRead = randomAccessFile.read(value);
            System.out.println(new String(value, UTF_8));
            fileChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test() {
        String key = " 412738 ";
        int sum = 5;
        getResult(key, sum);
        getResult(key, sum);
        System.out.println("sum:" + sum);
        Random r = new Random();
        byte[] bb = randomByte(r, 8, 2);
        String str = new String(bb, UTF_8);
        System.out.println(str);
        System.out.println("0:" + (bb[0] - 48));
        long s0 = bb[0] & 0xff;// 最低位
        System.out.println(s0);
        long s1 = bb[1] & 0xff;
        System.out.println(s1);
        s1 <<= 8;
        System.out.println("0:" + (s0 | s1));
        int index = Integer.parseInt(new String(bb, UTF_8).substring(0, 2));

        System.out.println(index);
        System.out.println(index % 64);
        Map<Long, Integer>[] map = new HashMap[10];
        for (int i = 0; i < 10; i++) {
            map[i] = new HashMap<>();
        }
        map[0].put(1L, 2);

        getBuffer(map[1]);

        map[1].put(3L, 3);
        System.out.println(map[0].size());
        System.out.println(map[1].size());
        System.out.println(map[1].keySet().toString());
        System.out.println(map[1].values());
    }

    private void getBuffer(Map<Long, Integer> map) {
        map.put(2L, 2);
        System.out.println("getBuffer:" + map.size());
    }

    private void getResult(String key, int sum) {
        byte[] bytes = key.getBytes();
//        long keyLong = ByteBuffer.wrap(bytes).getLong();
        long keyLong = Slices.bytesToLong(bytes);
        int index = (int) (keyLong & 0x11F);
        System.out.println("index:" + index);
        if (index >= 64 || index < 0) {
            System.out.println(index);
            index = Math.abs(index) % 64;
        }

        System.out.println(Long.toBinaryString(keyLong));
        System.out.println(keyLong);
        System.out.println(keyLong & 0x11F);
        System.out.println((keyLong & 0x11F) % 64);
        System.out.println(keyLong % 64);
        sum--;
        System.out.println("sum:" + sum);
    }


    @Test
    public void testKeyLog() {
        KeyLog keyLog = new KeyLog(24, kvFilePath);
        ByteBuffer byteBuffer = keyLog.getKeyBuffer();
        byteBuffer.position(0);
        byte[] key = new byte[8];
        byteBuffer.get(key);
        System.out.println("0:" + new String(key, UTF_8));

        getKeyLog(keyLog);

        byteBuffer.get(key);
        System.out.println("2:" + new String(key, UTF_8));
    }

    private void getKeyLog(KeyLog keyLog) {
        KeyLog keyLog1 = keyLog;
        ByteBuffer byteBuffer = keyLog1.getKeyBuffer();
        byteBuffer.position(8);
        byte[] key = new byte[8];
        byteBuffer.get(key);
        System.out.println("1:" + new String(key, UTF_8));
    }

    @Test
    public void getKeyLog() {
        Random r = new Random();
        byte[] key = randomByte(r, 8, 2);
        print(key);

        key = randomByte(r, 8);
        print(key);

        key = randomByte(r, 8);
        print(key);

        key = randomByte(r, 8);
        print(key);
        System.out.println(63 / 2);
    }

    private void print(byte[] key) {
        long keyLong = ByteBuffer.wrap(key).getLong();
        System.out.println(new String(key, UTF_8));
        System.out.println(keyLong + "," + keyLong % 64);
        long keyLong1 = byteArrayToLong(key);
        int index = (int) (keyLong % 64);
        System.out.println(keyLong1 + "," + keyLong1 % 64 + "," + index);
    }
}
