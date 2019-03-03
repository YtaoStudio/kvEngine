package io.mappedbus.perf;

import io.mappedbus.MappedBusWriter;

import java.io.File;

import static io.mappedbus.MappedBudConstant.*;

public class MessageWriter {

    public static void main(String[] args) {
        if (args.length == 0) {
            args = new String[]{FILE_NAME};
        }
        MessageWriter writer = new MessageWriter();
        writer.run(args[0]);
    }

    public void run(String fileName) {
        try {
            new File(fileName).delete();

            MappedBusWriter writer = new MappedBusWriter(fileName, MAPPED_FILE_SIZE, 12, false);
            writer.open();

            PriceUpdate priceUpdate = new PriceUpdate();

            for (int i = 0; i < ROW_NUM; i++) {
                if (i == ROW_NUM - 1) {
                    priceUpdate = new PriceUpdate(1, 2, 3);
                }
                writer.write(priceUpdate);
            }

            System.out.println("Done");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}