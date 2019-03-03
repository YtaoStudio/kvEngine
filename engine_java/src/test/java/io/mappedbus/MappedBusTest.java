package io.mappedbus;

import com.alibabacloud.polar_race.engine.common.impl.MemoryKey;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

import static com.alibabacloud.polar_race.engine.EngineRaceUtil.randomByte;
import static io.mappedbus.MappedBudConstant.FILE_NAME;
import static io.mappedbus.MappedBudConstant.MAPPED_FILE_SIZE;

public class MappedBusTest {

    @Test
    public void testMappedBus() throws Exception {
        byte[] key = randomByte(new Random(), 8);
        MemoryMappedFile m = new MemoryMappedFile(FILE_NAME, MAPPED_FILE_SIZE);
        System.out.println(Arrays.toString(key));
        m.putBytes(0, key, 0, 8);
        m.putInt(8 + 0, 45);
        key = randomByte(new Random(), 8);
        System.out.println(Arrays.toString(key));

        m.putBytes(12, key, 0, 8);
        m.putInt(12 + 8, 450);

        m.unmap();

        MemoryMappedFile m1 = new MemoryMappedFile(FILE_NAME, MAPPED_FILE_SIZE);
        m1.getBytes(0, key, 0, 8);
        int index = m1.getInt(8);
        System.out.println(index);
        System.out.println(Arrays.toString(key));
        m1.getBytes(12, key, 0, 8);
        index = m1.getInt(20);
        System.out.println(index);
        System.out.println(Arrays.toString(key));

        m1.getBytes(24, key, 0, 8);
        System.out.println(Arrays.toString(key));

        m.unmap();
    }


    @Test
    public void testMappedBusWR() throws Exception {
        byte[] key = randomByte(new Random(), 8);
        int num = 0;
        int valueLogNo = 0;
        int offset = num | (valueLogNo << 24);

        MappedBusWriter writer = new MappedBusWriter(FILE_NAME, MAPPED_FILE_SIZE, 12, false);
        writer.open();

        MemoryKey memoryKey = new MemoryKey(key, num);
        writer.write(memoryKey, 0);
        System.out.println(memoryKey.toString());

        key = randomByte(new Random(), 8);
        num = 1;
        offset = num | (valueLogNo << 24);
        memoryKey = new MemoryKey(key, num);
        writer.write(memoryKey, 0);
        System.out.println(memoryKey.toString());

        key = randomByte(new Random(), 8);
        num = 2;
        offset = num | (valueLogNo << 24);
        memoryKey = new MemoryKey(key, num);
        writer.write(memoryKey, 0);
        System.out.println(memoryKey.toString());

        writer.close();

        MappedBusReader reader = new MappedBusReader(FILE_NAME, MAPPED_FILE_SIZE, 12);
        reader.open();

        byte[] keys = new byte[8];
        MemoryKey mk = new MemoryKey(keys);

        int pos = 0;
        long start = System.nanoTime();
        while (reader.next(pos)) {
            reader.readMessage(mk);
        }
        System.out.println("last: " + memoryKey.toString());
        long stop = System.nanoTime();
        System.out.println("Elapsed: " + ((stop - start) / 1000000) + " ms");
        System.out.println("Per op: " + ((stop - start) / 80000000) + " ns");
        System.out.println("Op/s: " + (long) (80000000 / ((stop - start) / (float) 1000000000)));
    }
}
