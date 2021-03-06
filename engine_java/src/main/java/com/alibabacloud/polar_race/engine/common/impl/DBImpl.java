package com.alibabacloud.polar_race.engine.common.impl;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.alibabacloud.polar_race.engine.common.utils.Constants;
import com.alibabacloud.polar_race.engine.common.utils.sysinfo.SystemInfo;
import com.sun.jna.Pointer;
import net.smacke.jaydio.DirectIoLib;
import net.smacke.jaydio.buffer.AlignedDirectByteBuffer;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DBImpl {

    /*  1024个valuelog文件，初赛每个线程对应一个文件  */
//    private ValueLog valueLog[];
    // DIO 节约了Page Cache的时间
    private ValueDioLog valueReadLog[];
    private ValueLog valueWriteLog[];

    /*  1024个keylog文件  */
    private MemoryLog[] keyLog;
    private AtomicInteger keylogWrotePosition[];
    private AtomicLong valuelogWrotePosition[];
    private Lock[] writeLocks;

    /*  内存恢复hash   */
    /*  Long为存储vLogId, offset, globalPos    */
    private TSortedLongIntHashMap tmap[];

    // 预定义
    private ThreadLocal<ByteBuffer> threadLocalKeyBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(8));
    private ThreadLocal<byte[]> threadLocalKeyBytes = ThreadLocal.withInitial(() -> new byte[8]);
    private AtomicBoolean rangeFirst; // = new AtomicBoolean(false);

    /*  用于读，增加读取并发性，减小gc    */
    private ThreadLocal<ByteBuffer> threadLocalReadBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(4096));
    private ThreadLocal<byte[]> threadLocalReadBytes = ThreadLocal.withInitial(() -> new byte[4096]);
    private BlockingQueue<RangeTask> rangeQueue; // = new LinkedBlockingDeque<>(Constants.RANGE_THREAD);
    // cache
    private Semaphore[] readCacheLock;
    private Semaphore[] writeCacheLock;
    private int max = 0;
    //    private static Unsafe UNSAFE;
    private AlignedDirectByteBuffer[] dBuffer;
    // 用堆外内存代替内存, Fc读数据到堆内，本质上还是先拷到堆外，再拷到堆内

    //    private byte[][][] cacheArray; // cachePartitionNum, PartitionSize, ValueSize
    private Pointer pointer[];
    private Field bufferPointerField;

    public DBImpl(String path) {
        try {
            createDBPath(path);
        } catch (IOException e) {
            System.out.println("create path error");
            e.printStackTrace();
        }

        this.keyLog = new MemoryLog[Constants.PARTITION_SIZE];
        // 判断MemoryLog文件是否存在,如果存在，说明之前写过数据，进行内存恢复
        File dir = new File(path, "k0");
        if (dir.exists()) {
            System.out.println("---------------Start read or write append---------------");
            // 如果找不到key就会返回-1
            tmap = new TSortedLongIntHashMap[Constants.PARTITION_SIZE];

            recoverIndexMulti(path); // 多线程索引的建立
            rangeQueue = new LinkedBlockingQueue<>(Constants.RANGE_THREAD);
            rangeFirst = new AtomicBoolean(false);

            System.out.println("cache size set:" + max);

            writeCacheLock = new Semaphore[Constants.CACHE_PARTITION_NUM];
            readCacheLock = new Semaphore[Constants.CACHE_PARTITION_NUM];
            dBuffer = new AlignedDirectByteBuffer[Constants.CACHE_PARTITION_NUM];
            pointer = new Pointer[Constants.CACHE_PARTITION_NUM];
            try {
                // 反射, 获取Pointer
                bufferPointerField = AlignedDirectByteBuffer.class.getDeclaredField("pointer");
                bufferPointerField.setAccessible(true);
            } catch (NoSuchFieldException e) {
                e.printStackTrace();
            }
            for (int i = 0; i < Constants.CACHE_PARTITION_NUM; i++) {
                writeCacheLock[i] = new Semaphore(1);
                readCacheLock[i] = new Semaphore(0);
                dBuffer[i] = AlignedDirectByteBuffer.allocate(DirectIoLib.getLibForPath(path + "/lib"),
                        max * Constants.VALUE_SIZE);
                try {
                    pointer[i] = (Pointer) bufferPointerField.get(dBuffer[i]);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
            System.out.println("Recover finished");
        }

        // 如果不存在，说明是第一次open
        // 创建1024个value文件，分别命名value0--1023
        else {
            System.out.println("---------------Start first write---------------");
            this.writeLocks = new Lock[Constants.PARTITION_SIZE];
            this.valueWriteLog = new ValueLog[Constants.PARTITION_SIZE];
            this.keylogWrotePosition = new AtomicInteger[Constants.PARTITION_SIZE];
            this.valuelogWrotePosition = new AtomicLong[Constants.PARTITION_SIZE];
            for (int i = 0; i < Constants.PARTITION_SIZE; i++) {
                writeLocks[i] = new ReentrantLock();
                valueWriteLog[i] = new ValueLog(path, i);
                keyLog[i] = new MemoryLog(path, i);
                keylogWrotePosition[i] = new AtomicInteger(0);
                valuelogWrotePosition[i] = new AtomicLong(0L);
            }
            System.out.println("---------------KeyLog init--------------------");
        }
    }

    private void createDBPath(String dbPath) throws IOException {
        Path path = Paths.get(dbPath);
        if (!Files.exists(path)) {
            Files.createDirectory(path);
            path = Paths.get(dbPath + "/lib");
            Files.createDirectory(path);
        }
    }

    private void recoverIndexMulti(String path) {
        int[] valueLogWroteposition = new int[Constants.PARTITION_SIZE]; //每个valueLog文件写入了多少个数据

        this.valueReadLog = new ValueDioLog[Constants.PARTITION_SIZE];
        for (int i = 0; i < Constants.PARTITION_SIZE; i++) {
            valueReadLog[i] = new ValueDioLog(path, i);
            valueLogWroteposition[i] = (int) (valueReadLog[i].getFileLength() >> 12); // getFileLength()/4096
            if (valueLogWroteposition[i] > max) {
                max = valueLogWroteposition[i];
            }
            tmap[i] = new TSortedLongIntHashMap(
                    valueLogWroteposition[i],
                    1.0f, -1L, -1);

            // todo add keyLog consumer for each valueLog number
            keyLog[i] = new MemoryLog(path, i);
        }

        long indexCheckStart = System.currentTimeMillis();
        // https://stackoverflow.com/questions/1250643/how-to-wait-for-all-threads-to-finish-using-executorservice
        ExecutorService indexService = Executors.newFixedThreadPool(Constants.RANGE_THREAD);
        for (int i = 0; i < Constants.PARTITION_SIZE; i++) {
            indexService.submit(new MemConsumers(keyLog[i], tmap[i]));
//            indexService.submit(new MemConsumer(path, tmap[i], i));
        }
        indexService.shutdown();
        try {
            indexService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ExecutorService sortService = Executors.newFixedThreadPool(Constants.RANGE_THREAD);
        for (int i = 0; i < Constants.PARTITION_SIZE; i++) {
            int finalI = i;
            sortService.execute(() -> {
                tmap[finalI].sort();
            });
        }
        sortService.shutdown();

        long indexCheckEnd = System.currentTimeMillis();
        System.out.printf("Index Check: %d ms\n", indexCheckEnd - indexCheckStart);

        System.out.println("---------------Index finished---------------");
        System.out.println(SystemInfo.getSystemInfo());
        System.out.println(SystemInfo.getMemoryInfo(SystemInfo.SIZE_UNIT.MB));
    }

    public void write(byte[] key, byte[] value) {
        int index = ((key[0] & 0xFF) << 2) | ((key[1] & 0XFF) >> 6);
        try {
            writeLocks[index].lock();

            // 每个valueLog100w个数据，这个只占三个字节，表示该valueLog第几个数据
            int offset = (int) (valueWriteLog[index].getWrotePosition() >> 12);

            // 因为用多个keyLog，所以要有个原子量记录写在keyLog array中的位置
            keyLog[index].putKey(key, offset, keylogWrotePosition[index].getAndAdd(12));

//            valueWriteLog[index].putMessageDirect(value);
            valueWriteLog[index].putMessageDirect(threadLocalReadBuffer.get().wrap(value));
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            writeLocks[index].unlock();
        }
    }

    public byte[] read(byte[] key) throws EngineException {
        long keyLong = ByteBuffer.wrap(key).getLong();
        int index = ((key[0] & 0xFF) << 2) | ((key[1] & 0XFF) >> 6);

        long currentPos = tmap[index].get(keyLong);
        if (currentPos == -1) {
            throw new EngineException(RetCodeEnum.NOT_FOUND, "not found this key");
        }
        return valueReadLog[index].getMessageDirectByDio(currentPos << 12, threadLocalReadBytes.get());
    }

    /**
     * Applies the given AbstractVisitor: visit function to the result of every key-value pair
     * in the key range [first, last), in order.
     *
     * @param lower   lower=="" is treated as a key before all keys in the database.
     * @param upper   upper=="" is is treated as a key after all keys in the database.
     * @param visitor the following call will traverse the entire database: Range("", "", visitor)
     */
    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {
        // 阻塞测试程序的64线程
        if (rangeFirst.compareAndSet(false, true)) {
            System.out.println("---------------Range first---------------");
            initPreFetchThreads();
        }
        RangeTask task = new RangeTask(new CountDownLatch(1), visitor);
        rangeQueue.offer(task);
        try {
            task.getCountDownLatch().await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void initPreFetchThreads() {
        new Thread(() -> {
            RangeTask[] rangeTasks = new RangeTask[Constants.RANGE_THREAD];
            for (int i = 0; i < Constants.RANGE_THREAD; i++) {
                try {
                    rangeTasks[i] = rangeQueue.take();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            // 多线程Range
            long indexCheckStart = System.currentTimeMillis();

            ExecutorService service = Executors.newFixedThreadPool(Constants.CACHE_PARTITION_NUM);

            CacheReader cacheReader[] = new CacheReader[Constants.CACHE_PARTITION_NUM];
            for (int i = 0; i < Constants.CACHE_PARTITION_NUM; i++) {
                cacheReader[i] = new CacheReader(i, readCacheLock[i], writeCacheLock[i]);
                service.execute(cacheReader[i]);
            }

            byte[] key;
            long keyLong = 0L;
            byte[] value = null;
            for (int i = 0; i < tmap.length; i++) {
                try {
                    readCacheLock[i % Constants.CACHE_PARTITION_NUM].acquire();
//                    System.out.println(Thread.currentThread().getId() + " 开始消费：" + i);
                    if (tmap[i].size() > 0) {
                        for (int j = 0; j < tmap[i].keyIndexArray.length; j++) {
                            keyLong = tmap[i].getKey(j);
                            key = getByteByKey(keyLong, threadLocalKeyBuffer.get(), threadLocalKeyBytes.get());
                            value = locateBuffer(threadLocalReadBytes.get(), i % Constants.CACHE_PARTITION_NUM, tmap[i].get(keyLong));

                            for (int k = 0; k < Constants.RANGE_THREAD; k++) {
                                rangeTasks[k].getVisitor().visit(key, value);
                            }
                        }
                    }
//                    System.out.println(Thread.currentThread().getId() + " 结束消费：" + i);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    writeCacheLock[i % Constants.CACHE_PARTITION_NUM].release();
                }
            }
            service.shutdown();
            long indexCheckEnd = System.currentTimeMillis();
            System.out.printf("Range Check: %d ms\n", indexCheckEnd - indexCheckStart);

            rangeFirst.set(false);
            for (int i = 0; i < Constants.RANGE_THREAD; i++) {
                rangeTasks[i].getCountDownLatch().countDown();
            }

        }).start();
    }

    public void close() {
        if (keyLog != null)
            for (MemoryLog K : keyLog) {
                K.close();
            }
        if (valueWriteLog != null) {
            for (ValueLog V : valueWriteLog) {
                V.close();
            }
            valueWriteLog = null;
        }
        if (valueReadLog != null) {
            for (ValueDioLog V : valueReadLog) {
                V.close();
            }
            valueReadLog = null;
        }
        keyLog = null;
        tmap = null;
        threadLocalReadBuffer = null;
        threadLocalReadBytes = null;
        if (dBuffer != null) {
            for (AlignedDirectByteBuffer A : dBuffer)
                A.clear();
            dBuffer = null;
        }
    }

    private byte[] getByteByKey(Long keyLong, ByteBuffer byteBuffer, byte[] bytes) {
        byteBuffer.clear();
        byteBuffer.putLong(keyLong);
        byteBuffer.flip();
        byteBuffer.get(bytes);
        return bytes;
    }

    class RangeTask {
        private CountDownLatch countDownLatch;
        private AbstractVisitor visitor;

        public RangeTask(CountDownLatch countDownLatch, AbstractVisitor visitor) {
            this.countDownLatch = countDownLatch;
            this.visitor = visitor;
        }

        public CountDownLatch getCountDownLatch() {
            return countDownLatch;
        }

        public AbstractVisitor getVisitor() {
            return visitor;
        }

    }

    class CacheReader implements Runnable {
        private int cacheIndex;
        private Semaphore readCacheLock;
        private Semaphore writeCacheLock;

        public CacheReader(int cacheIndex, Semaphore readCacheLock, Semaphore writeCacheLock) {
            this.cacheIndex = cacheIndex;
            this.readCacheLock = readCacheLock;
            this.writeCacheLock = writeCacheLock;
        }

        @Override
        public void run() {
            for (int i = cacheIndex; i < Constants.PARTITION_SIZE;
                 i += Constants.CACHE_PARTITION_NUM) {
                try {
                    writeCacheLock.acquire();
//                    System.out.println(Thread.currentThread().getId() + " begin producing:" + i);
                    if (tmap[i].size() > 0) {
                        // todo get all the value in file v{i}
                        valueReadLog[i].getRangeMessageDirect(dBuffer[cacheIndex]);
                    }
//                    System.out.println(Thread.currentThread().getId() + " end producing:" + i);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    readCacheLock.release();
                }
            }
        }
    }

    private byte[] locateBuffer(byte[] value, int cacheIndex, int cachePos) {
//        dBuffer[cacheIndex].position(cachePos << 12);
//        dBuffer[cacheIndex].get(value, 0, Constants.VALUE_SIZE);
//        return value;
        pointer[cacheIndex].read(cachePos << 12, value, 0, Constants.VALUE_SIZE);
        return value;
    }

}