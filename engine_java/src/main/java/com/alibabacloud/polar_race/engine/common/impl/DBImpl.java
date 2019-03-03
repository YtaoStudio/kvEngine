package com.alibabacloud.polar_race.engine.common.impl;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import gnu.trove.map.hash.TLongIntHashMap;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alibabacloud.polar_race.engine.common.utils.ByteToInt.byteArrayToLong;

public class DBImpl {

    /*  每个线程对应一个valuelog文件  */
    private ValueLog valueLog[];
    //线程号与valuelog文件的对应
    private ConcurrentHashMap<Long, Integer> threadValueLog;
    private AtomicInteger whichValueLog = new AtomicInteger(0);

    /*  用64个keylog文件  */
    private MemoryLog keyLog[];
    // 因为用了64个keylog文件，记录位置
    private AtomicInteger keylogWrotePosition[];

    /*  内存恢复hash   */
    private TLongIntHashMap[] tmap;
    /*  keyLog多线程 */
    private MemConsumer[] consumers;

    /*  用于读，增加读取并发性，减小gc    */
    private ThreadLocal<ByteBuffer> threadLocalReadBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(4096));
    private ThreadLocal<byte[]> threadLocalReadBytes = ThreadLocal.withInitial(() -> new byte[4096]);

    public DBImpl(String path) {
        try {
            createDBPath(path);
        } catch (IOException e) {
            System.out.println("create path error");
            e.printStackTrace();
        }

        //创建64个key/value文件，分别命名k/v0--63
        this.valueLog = new ValueLog[64];
        this.keyLog = new MemoryLog[64];
        //判断KeyLog文件是否存在,如果存在，说明之前写过数据，进行内存恢复
        File dir = new File(path, "k0");
        if (dir.exists()) {
            System.out.println("---------------Start read or write append---------------");
            //如果找不到key就会返回-1
            this.tmap = new TLongIntHashMap[64];
            this.consumers = new MemConsumer[64];
            recoverHashtableMulti(path); // TLongIntHashMap多线程恢复
            System.out.println("Recover finished");
        }

        //如果不存在，说明是第一次open
        else {
            this.threadValueLog = new ConcurrentHashMap<>(64);
            this.keylogWrotePosition = new AtomicInteger[64];
            System.out.println("---------------Start first write---------------");
            for (int i = 0; i < 64; i++) {
                valueLog[i] = new ValueLog(path, i, false);
                keyLog[i] = new MemoryLog(path, i, false);
                keylogWrotePosition[i] = new AtomicInteger(0);
            }
        }
    }

    private void createDBPath(String dbPath) throws IOException {
        Path path = Paths.get(dbPath);
        if (!Files.exists(path))
            Files.createDirectory(path);
    }

    private void recoverHashtableMulti(String path) {
        for (int i = 0; i < 64; i++) {
            valueLog[i] = new ValueLog(path, i, true);
            keyLog[i] = new MemoryLog(path, i, true);//keylog恢复
            tmap[i] = new TLongIntHashMap();
//            tmap[i] = new TLongIntHashMap(1200000, 1.0F, -1L, -1);
            // todo add comsumer for each valueLog number
            consumers[i] = new MemConsumer(keyLog[i], tmap[i]);
            consumers[i].start();
        }
        //Step: 多线程索引的建立
        long indexCheckStart = System.currentTimeMillis();
        for (MemConsumer consumer : consumers) {
            try {
                consumer.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        long indexCheckEnd = System.currentTimeMillis();
        System.out.printf("Index Check: %d ms \n", indexCheckEnd - indexCheckStart);
    }

    public void write(byte[] key, byte[] value) {

        long id = Thread.currentThread().getId();
        if (!threadValueLog.containsKey(id))
            threadValueLog.put(id, whichValueLog.getAndAdd(1));
        int valueLogNo = threadValueLog.get(id);

        //每个valuelog100w个数据，这个只占三个字节，表示该valuelog第几个数据
        int num = (int) (valueLog[valueLogNo].getWrotePosition() / 4096);
        //offset 第一个字节 表示这个key对应的存在哪个valuelog中，后三个字节表示这个value是该valuelog的第几个数据
        int offset = num | (valueLogNo << 24);

//        long keyLong = ByteBuffer.wrap(key).getLong();
//        long keyLong = bytesToLong(key);
        long keyLong = byteArrayToLong(key);
        int index = (int) (Math.abs(keyLong) % 64);
//        int index = bytesGetTwo(key) % 64;

        //因为只用一个keylog，所以要有个原子量记录写在keylog中的位置
        keyLog[index].putKey(key, offset, keylogWrotePosition[index].getAndAdd(12));

        valueLog[valueLogNo].putMessageDirect(threadLocalReadBuffer.get().wrap(value));
//        valueLog[valueLogNo].putMessageDirect(value);
    }

    public byte[] read(byte[] key) throws EngineException {
//        long keyLong = bytesToLong(key);
//        long keyLong = ByteBuffer.wrap(key).getLong();
        long keyLong = byteArrayToLong(key);
//        int index = (int) (keyLong & 0xFF);
//        index = Math.abs(index) % 64;
        int index = (int) (Math.abs(keyLong) % 64);
//        int index = bytesGetTwo(key) % 64;
        int currentPos = tmap[index].get(keyLong);
//        int currentPos = tmap.get(ByteBuffer.wrap(key).getLong());
        if (currentPos == -1) {
            throw new EngineException(RetCodeEnum.NOT_FOUND, "not found this key");
        }
        return valueLog[currentPos >> 24].getMessageDirect(((long) (currentPos & 0x00FFFFFF)) * 4096, threadLocalReadBytes.get());
    }

    public void close() {
//        keyLog.close();
        for (ValueLog V : valueLog) {
            V.close();
        }
        for (MemoryLog K : keyLog) {
            K.close();
        }
        keyLog = null;
        valueLog = null;
        tmap = null;
//        threadLocalReadBuffer = null;
        threadLocalReadBytes = null;
        threadValueLog = null;
        System.out.println("===============close end================");
    }
}