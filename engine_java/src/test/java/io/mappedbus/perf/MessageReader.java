package io.mappedbus.perf;

import io.mappedbus.MappedBusMessage;
import io.mappedbus.MappedBusReader;

import static io.mappedbus.MappedBudConstant.*;

public class MessageReader {

    public static void main(String[] args) {
        if (args.length == 0) {
            args = new String[]{FILE_NAME};
        }
        MessageReader reader = new MessageReader();
        reader.run(args[0]);
    }

    public void run(String fileName) {
        try {
            MappedBusReader reader = new MappedBusReader(fileName, MAPPED_FILE_SIZE, 12);
            reader.open();

            PriceUpdate priceUpdate = new PriceUpdate();

            MappedBusMessage message = null;

            long start = System.nanoTime();
            for (int i = 0; i < ROW_NUM; i++) {
                while (true) {
                    if (reader.next()) {
                        int type = reader.readType();
                        switch (type) {
                            case PriceUpdate.TYPE:
                                message = priceUpdate;
                                break;
                            default:
                                throw new RuntimeException("Unknown type: " + type);
                        }
                        reader.readMessage(message);
                        break;
                    }
                }
            }
            System.out.println(message.toString());
            long stop = System.nanoTime();
            System.out.println("Elapsed: " + ((stop - start) / 1000000) + " ms");
            System.out.println("Per op: " + ((stop - start) / 80000000) + " ns");
            System.out.println("Op/s: " + (long) (80000000 / ((stop - start) / (float) 1000000000)));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}