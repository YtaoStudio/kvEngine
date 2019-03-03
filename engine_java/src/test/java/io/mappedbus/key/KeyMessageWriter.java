package io.mappedbus.key;

import com.alibabacloud.polar_race.engine.common.impl.MemoryKey;
import io.mappedbus.MappedBusWriter;

import java.util.Random;

import static com.alibabacloud.polar_race.engine.EngineRaceUtil.randomByte;
import static io.mappedbus.MappedBudConstant.*;

public class KeyMessageWriter {

    public static void main(String[] args) {
        if (args.length == 0) {
            args = new String[]{FILE_NAME};
        }
        KeyMessageWriter writer = new KeyMessageWriter();
        writer.run(args[0]);
    }

    public void run(String fileName) {
        try {
//            new File(fileName).delete();

            MappedBusWriter writer = new MappedBusWriter(fileName, MAPPED_FILE_SIZE, 12, false);
            writer.open();

            MemoryKey memoryKey;

            byte[] key;
            int num = 0;
            int valueLogNo = 0;
            int offset;
            for (int i = 0; i < ROW_NUM; i++) {
                key = randomByte(new Random(), 8);
                num++;
                memoryKey = new MemoryKey(key, num);

                if (i == ROW_NUM - 1) {
                    System.out.println(memoryKey.toString());
                }
                writer.write(memoryKey);
            }

            System.out.println("Done");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}