package com.alibabacloud.polar_race.engine.recover;

import com.alibabacloud.polar_race.engine.common.EngineRace;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import org.junit.Test;

import com.engine.util.Slice;
import com.engine.util.Slices;
import java.io.*;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alibabacloud.polar_race.engine.EngineRaceUtil.path;
import static com.alibabacloud.polar_race.engine.EngineRaceUtil.fileName;
import static com.alibabacloud.polar_race.engine.EngineRaceUtil.kvFilePath;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

public class RecoverTest {

    public static void main(String[] args) {
        String str = new String("abcd");
        byte[] b = str.getBytes();
        Slice s = Slices.wrappedBuffer(b);
        System.out.println(s.length());
    }

    // produce-consume module
    @Test
    public void testCreateKv() {

        BlockingQueue<String> queue = new LinkedBlockingDeque<>();

        int maxNum = 10000;

        KvProducer p1 = new KvProducer(queue, kvFilePath, fileName, maxNum);
        ExecutorService service = Executors.newCachedThreadPool();
        service.execute(p1);

        EngineRace race = new EngineRace();
        try {
            race.open(path);
        } catch (EngineException e) {
            System.out.println(e.getMessage());
        }

        int threadNum = 8;
        KvConsumer[] consumers = new KvConsumer[threadNum];
        try {
            for (int i = 0; i < threadNum; i++) {
                KvConsumer pixelsKvConsumer = new KvConsumer(queue, race, maxNum);
                consumers[i] = pixelsKvConsumer;
                pixelsKvConsumer.start();
            }
            for (KvConsumer c : consumers) {
                try {
                    c.join();
                } catch (InterruptedException e) {
                    throw new Exception("KvConsumer InterruptedException, " + e.getMessage());
                }
            }
        } catch (Exception e) {
            try {
                throw new Exception("KvConsumer Error, " + e.getMessage());
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }

        p1.closeProducer();
        service.shutdown();
    }

    @Test
    public void testRecover() {
        EngineRace race = new EngineRace();
        try {
            race.open(path);
        } catch (EngineException e) {
            System.out.println(e.getMessage());
        }
        String filePath = kvFilePath + "/" + fileName;
        int i = 0;

        try (BufferedReader workloadReader = new BufferedReader(new FileReader(filePath))) {
            String line;
            String[] lines;
            String key = null;
            String value = null;
            while ((line = workloadReader.readLine()) != null) {
                lines = line.split("\t");
                key = lines[1];
                value = lines[2];

                try {
                    race.write(key.getBytes(), value.getBytes());
                } catch (EngineException e) {
                    System.out.println(e.getMessage());
                }

                try {
                    System.out.println(i + "\t" + toStr(race.read(key.getBytes())));
                    if (toStr(race.read(key.getBytes())) == null) {
                        i++;
                    }
                    assertEquals(race.read(key.getBytes()), value);
                } catch (EngineException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Error count: " + i);
        race.close();
    }

    public String toStr(byte[] key) {
        if (key == null) {
            return null;
        }
        return new String(key, UTF_8);
    }

    public byte[] randomByte(Random random, int length) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) (random.nextInt(10) + 48);
        }
        return bytes;
    }

    public String randomString(Random random, int length) {
        char[] chars = new char[length];
        for (int i = 0; i < chars.length; i++) {
            chars[i] = (char) ((int) ' ' + random.nextInt(95));
        }
        return new String(chars);
    }

    class KvProducer extends Thread {

        public static final int BUFFER_SIZE = 1024 * 1024 * 32;

        private volatile boolean isRunning = true;
        private BlockingQueue<String> queue;
        private StringBuilder data = new StringBuilder();
        final String filePath;
        final int maxNum;
        private BufferedWriter kvWriter;
        private AtomicInteger count = new AtomicInteger();
        Random random = new Random();

        public KvProducer(BlockingQueue<String> queue, String kvFilePath, String fileName, int maxNum) {
            this.queue = queue;
            File file = new File(kvFilePath);
            this.filePath = kvFilePath + "/" + fileName;
            this.maxNum = maxNum;

            if (!file.exists()) {
                file.mkdirs();

                file = new File(filePath);
                try {
                    file.createNewFile();
                } catch (IOException e) {
                    System.out.println("File Create Error.");
                }
            }
            try {
                kvWriter = new BufferedWriter(new FileWriter(filePath), BUFFER_SIZE);
            } catch (IOException e) {
                System.out.println("BufferedWriter Error.");
            }

        }

        @Override
        public void run() {
            System.out.println("start producer thread！");

            try {
                String key = null;
                String value = null;

                while (isRunning) {
                    key = toStr(randomByte(random, 8));
                    value = toStr(randomByte(random, 4 * 2014));
                    data.append(key).append("\t").append(value);

                    if (!queue.offer(data.toString(), 2, TimeUnit.SECONDS)) {
                        System.out.println("add error：" + data);
                    }

                    try {
                        kvWriter.write(count.get() + "\t" + data.toString() + "\n");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    data = new StringBuilder();

                    if (count.incrementAndGet() >= maxNum) {
                        stopProducer();
                        System.out.println("It equals count: " + maxNum);
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
                Thread.currentThread().interrupt();
            } finally {
                System.out.println("Exit producer thread！");
            }
        }

        public void stopProducer() {
            isRunning = false;
            try {
                if (kvWriter != null) {
                    kvWriter.flush();
                    kvWriter.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public void closeProducer() {
            try {
                if (kvWriter != null) {
                    kvWriter.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    class KvConsumer extends Thread {

        private BlockingQueue<String> queue;
        private EngineRace race;
        private AtomicInteger count = new AtomicInteger();
        final int maxNum;

        public KvConsumer(BlockingQueue<String> queue, EngineRace race, int maxNum) {
            this.queue = queue;
            this.race = race;
            this.maxNum = maxNum;
        }

        @Override
        public void run() {
            System.out.println("This is consumer： " + Thread.currentThread().getName());
            try {
                String key = null;
                String value = null;
                while (true) {

                    String data = queue.poll(2, TimeUnit.SECONDS);
                    if (data != null) {
                        key = data.split("\t")[0];
                        value = data.split("\t")[1];

                        try {
                            race.write(key.getBytes(), value.getBytes());
                        } catch (EngineException e) {
                            e.printStackTrace();
                        }
                    } else {
                        System.out.println("No data : " + Thread.currentThread().getName());
                        break;
                    }

                }

            } catch (InterruptedException e) {
                System.out.println("No data: " + Thread.currentThread().getName());
                System.out.println("Num: " + count.get());
                Thread.currentThread().interrupt();
            }
        }
    }


}
