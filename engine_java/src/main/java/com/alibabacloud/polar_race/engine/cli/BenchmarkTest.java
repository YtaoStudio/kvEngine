package com.alibabacloud.polar_race.engine.cli;

import com.alibabacloud.polar_race.engine.common.EngineRace;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;

public class BenchmarkTest {

    public static String path = "/tmp/engine_data/";

    public static void main(String[] args) {
        if (args.length > 0)
            path = args[0];

        try {
            System.out.println("=======================db init=========================");
            FileUtils.deleteDirectory(new File(path));
        } catch (IOException e) {
            e.printStackTrace();
        }
        //////////////////////////////////////////
        EngineRace race = new EngineRace();
        try {
            race.open(path);
        } catch (EngineException e) {
            System.out.println(e.getMessage());
        }

        Map<KeyComparable, byte[]> kvMap = new ConcurrentSkipListMap<>();

        ////////////////////////////////////////////////
        System.out.println("=======================start write=========================");
        long startWriteTime = System.currentTimeMillis();
        Writer[] consumer = new Writer[64];
        int k = 64;
        for (int i = 0; i < k; i++) {
            consumer[i] = new Writer(race, kvMap);
            consumer[i].start();

        }
        for (int i = 0; i < k; i++) {
            try {
                consumer[i].join();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        long endWriteTime = System.currentTimeMillis();
        System.out.println("Write cost time: " + (endWriteTime - startWriteTime) + "ms");
        System.out.println("=======================end write=========================");
        race.close();

        System.out.println("kvMap size:" + kvMap.size());

        race = new EngineRace();
        try {
            race.open(path);
        } catch (EngineException e) {
            System.out.println(e.getMessage());
        }

        System.out.println("=======================start read=========================");
        long startReadTime = System.currentTimeMillis();
        Reader reader[] = new Reader[64];
        int r_num = 64;
        for (int i = 0; i < r_num; i++) {
            reader[i] = new Reader(race, kvMap, i);
            reader[i].start();
        }
        for (int i = 0; i < r_num; i++) {
            try {
                reader[i].join();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        long endReadTime = System.currentTimeMillis();
        System.out.println("Read cost time: " + (endReadTime - startReadTime) + "ms");
        System.out.println("=======================end read=========================");


        System.out.println("=======================start range=========================");
        //////////////////////////////////////////////////////
        long startTime = System.currentTimeMillis();
        Ranger[] rg = new Ranger[64];
        int rg_num = 64;
        for (int i = 0; i < rg_num; i++) {
            rg[i] = new Ranger(race, kvMap, i);
            rg[i].start();
        }
        for (int i = 0; i < rg_num; i++) {
            try {
                rg[i].join();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        long stopTime = System.currentTimeMillis();
        System.out.println("Range cost time: " + (stopTime - startTime) + "ms");
        System.out.println("=======================end range=========================");
        race.close();
        //////////////////////////////////////////////////////////
    }

}

class Writer extends Thread {
    EngineRace race;
    Map<KeyComparable, byte[]> kvMap;
    final int acesstimes = 100;

    public Writer(EngineRace race, Map<KeyComparable, byte[]> kvMap) {
        this.race = race;
        this.kvMap = kvMap;
    }

    @Override
    public void run() {
        try {
            for (int i = 0; i < acesstimes; i++) {
                byte[] key = EngineRaceUtil.randomByte(new Random(), 8);
                byte[] value = EngineRaceUtil.randomByte(new Random(), 4 * 1024);
                race.write(key, value);
                kvMap.put(new KeyComparable(key), value);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


class Reader extends Thread {
    EngineRace race;
    Map<KeyComparable, byte[]> kvMap;
    Object[] keylist;
    int t;
    final int acesstimes = 10;

    public Reader(EngineRace race, Map<KeyComparable, byte[]> kvMap, int t) {
        this.race = race;
        this.kvMap = kvMap;
        keylist = kvMap.keySet().toArray();
        this.t = t;
    }

    @Override
    public void run() {
        try {
            for (int i = 0; i < acesstimes; i++) {
                int index = new Random().nextInt(keylist.length);
                byte[] oldValue = kvMap.get(keylist[index]);

                byte[] value = race.read(((KeyComparable) keylist[index]).getKey());
                if (!Arrays.equals(value, oldValue)) {
                    try {
                        System.out.println(new BigInteger(1, value).toString(2) + "\n"
                                + new BigInteger(1, oldValue).toString(2) + "\n");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }
            }
        } catch (EngineException e) {
            System.out.println(e.getMessage());
        }
    }
}

class Ranger extends Thread {
    EngineRace race;
    Map<KeyComparable, byte[]> kvMap;
    final int acesstimes = 2;

    public Ranger(EngineRace race, Map<KeyComparable, byte[]> kvMap, int t) {
        this.race = race;
        this.kvMap = kvMap;
    }

    @Override
    public void run() {
        for (int i = 0; i < acesstimes; i++) {
            DefaultVisitor visitor = new DefaultVisitor(kvMap);
            try {
                race.range(null, null, visitor);
                System.out.println("All kv pair: (" + Thread.currentThread().getId() + "," + i + ") " + visitor.visitCount.get());
            } catch (EngineException e) {
                e.printStackTrace();
            }

        }
    }
}