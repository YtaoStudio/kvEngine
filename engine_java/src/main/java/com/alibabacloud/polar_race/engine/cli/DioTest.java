package com.alibabacloud.polar_race.engine.cli;

import net.smacke.jaydio.DirectRandomAccessFile;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import static com.alibabacloud.polar_race.engine.cli.EngineRaceUtil.randomByte;
import static java.nio.charset.StandardCharsets.UTF_8;

public class DioTest {
    static File inFile = new File("/home/tao/hello.txt");
    static File outFile = new File("/home/tao/world.txt");

    public static void main(String[] args) throws IOException {
//        rwDio();
        rwEngineDio();
    }

    public static void rwEngineDio() throws IOException {
        DirectRandomAccessFile fout =
                new DirectRandomAccessFile(outFile, "rw", 4096);
        System.out.println(fout.length());
        byte[] value = new byte[4096];
        Random r = new Random(1);
        for (int i = 0; i < 100; i++) {
            value = randomByte(r, 4096);
            int offset = (int) fout.length();
            fout.write(value, 0, 4096);
            System.out.println(fout.getFilePointer());
            System.out.println(fout.length());
            System.out.println(i + "," + new String(value, UTF_8));
        }
        fout.close();

        DirectRandomAccessFile fin =
                new DirectRandomAccessFile(outFile, "r", 200000 * 4096);
        int offset = 0;
        for (int i = 0; i < 10; i++) {
            System.out.println(fin.getFilePointer());
            System.out.println(fin.length());
            fin.seek(offset);
            fin.read(value);
            System.out.println(i + "," + new String(value, UTF_8));
            offset += 4096;
        }
        fin.close();
    }

    public static void rwDio() throws IOException {
        int bufferSize = 1 << 23; // Use 8 MiB buffers
        byte[] buf = new byte[bufferSize];

        if (!inFile.exists()) {
            inFile.createNewFile();
            outFile.createNewFile();
        }
        DirectRandomAccessFile fin =
                new DirectRandomAccessFile(inFile, "r", bufferSize);

        DirectRandomAccessFile fout =
                new DirectRandomAccessFile(outFile, "rw", bufferSize);

        while (fin.getFilePointer() < fin.length()) {
            int remaining = (int) Math.min(bufferSize, fin.length() - fin.getFilePointer());
            fin.read(buf, 0, remaining);
            fout.write(buf, 0, remaining);
        }

        fin.close();
        fout.close();
    }
}
