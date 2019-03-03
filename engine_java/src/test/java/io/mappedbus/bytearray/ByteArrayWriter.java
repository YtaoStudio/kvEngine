package io.mappedbus.bytearray;

import io.mappedbus.MemoryMappedFile;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Random;

import static com.alibabacloud.polar_race.engine.EngineRaceUtil.randomByte;
import static io.mappedbus.MappedBudConstant.FILE_NAME;
import static io.mappedbus.MappedBudConstant.MAPPED_FILE_SIZE;

public class ByteArrayWriter {

    public static void main(String[] args) {
        ByteArrayWriter writer = new ByteArrayWriter();
        writer.run(5);
    }

    public void run(int source) {
        try {
//			MappedBusWriter writer = new MappedBusWriter(FILE_NAME, 2000000L, 10, true);
//			writer.open();
//
//			byte[] buffer = new byte[10];
//
//			for (int i = 0; i < 1000; i++) {
//				Arrays.fill(buffer, (byte)source);
//				writer.write(buffer, 0, buffer.length);
//			}

            int pos = 0;
            MemoryMappedFile m = new MemoryMappedFile(FILE_NAME, 2000000L);
            for (int i = 0; i < 1000; i++) {
                byte[] key = randomByte(new Random(), 10);
                m.putBytes(pos, key, 0, 10);

                pos += 10;
                if (i == 1000 - 1) {
                    System.out.println(Arrays.toString(key));
                }
            }

            System.out.println("DONE");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSize() {
        try {
            FileChannel fileChannel = new RandomAccessFile(FILE_NAME, "rw").getChannel();
            try {
                System.out.println(fileChannel.size());
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}