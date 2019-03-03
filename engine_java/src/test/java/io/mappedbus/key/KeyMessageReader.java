package io.mappedbus.key;

import com.alibabacloud.polar_race.engine.common.impl.MemoryKey;
import io.mappedbus.MappedBusMessage;
import io.mappedbus.MappedBusReader;
import io.mappedbus.perf.PriceUpdate;

import static io.mappedbus.MappedBudConstant.*;

public class KeyMessageReader {

    public static void main(String[] args) {
        if (args.length == 0) {
            args = new String[]{FILE_NAME};
        }
        KeyMessageReader reader = new KeyMessageReader();
        reader.run(args[0]);
    }

    public void run(String fileName) {
        try {
            MappedBusReader reader = new MappedBusReader(fileName, MAPPED_FILE_SIZE, 12);
            reader.open();

            byte[] key = new byte[8];
            int num = 0;
            MemoryKey memoryKey = new MemoryKey(key);

            MappedBusMessage message = null;

            long start = System.nanoTime();
            for (int i = 0; i < ROW_NUM; i++) {
                while (true) {
                    if (reader.next()) {
//                        int type = reader.readType();
//                        switch (type) {
//                            case PriceUpdate.TYPE:
//                                message = memoryKey;
//                                break;
//                            default:
//                                throw new RuntimeException("Unknown type: " + type);
//                        }
                        reader.readMessage(memoryKey);
                        break;
                    }
                }
            }
            System.out.println(memoryKey.toString());
            long stop = System.nanoTime();
            System.out.println("Elapsed: " + ((stop - start) / 1000000) + " ms");
            System.out.println("Per op: " + ((stop - start) / 80000000) + " ns");
            System.out.println("Op/s: " + (long) (80000000 / ((stop - start) / (float) 1000000000)));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}