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

public class DBImpl {

    /*  每个线程对应一个valuelog文件  */
    private ValueLog valueLog[];
    //线程号与valuelog文件的对应
    private ConcurrentHashMap<Long, Integer> threadValueLog;
    private AtomicInteger whichValueLog = new AtomicInteger(0);

    /*  仅用一个keylog文件  */
    private KeyLog keyLog;
    //因为只用了一个keylog文件，记录位置
    private AtomicInteger kelogWrotePosition = new AtomicInteger(0);

    /*  内存恢复hash   */
    private TLongIntHashMap tmap;

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

        this.threadValueLog = new ConcurrentHashMap<>(64);
        //创建64个value文件，分别命名value0--63
        this.valueLog = new ValueLog[64];
        for (int i = 0; i < 64; i++) {
            valueLog[i] = new ValueLog(path, i);
        }
        //判断KeyLog文件是否存在,如果存在，说明之前写过数据，进行内存恢复
        File dir = new File(path, "key");
        if (dir.exists()) {
            System.out.println("---------------Start read or write append---------------");
            //如果找不到key就会返回-1
            tmap = new TLongIntHashMap();
//            tmap = new TLongIntHashMap(64000000, 1.0F, -1L, -1);
            keyLog = new KeyLog(12 * 64 * 1024 * 1024, path);//keylog恢复
            recoverHashtable();//hashtable恢复和wroteposition恢复
//            recoverHashtableMulti();
            System.out.println("Recover finished");
        }

        //如果不存在，说明是第一次open
        else {
            System.out.println("---------------Start first write---------------");
            keyLog = new KeyLog(12 * 64 * 1024 * 1024, path);
        }
    }

    private void createDBPath(String dbPath) throws IOException {
        Path path = Paths.get(dbPath);
        if (!Files.exists(path))
            Files.createDirectory(path);
    }

    private void recoverHashtableMulti() {
//        ByteBuffer byteBuffer = keyLog.getKeyBuffer();
//        byteBuffer.position(0);
        int sum = 0;//总共写入了多少个数据
        int[] valueLogWroteposition = new int[64];//每个valuelog文件写入了多少个数据
        Thread[] consumers = new Thread[64];

        for (int i = 0; i < 64; i++) {
            valueLogWroteposition[i] = (int) (valueLog[i].getFileLength() / 4096);
            sum += valueLogWroteposition[i];
        }
        System.out.println("sum:" + sum);

        for (int i = 0; i < 64; i++) {
//            tmaps[i] = new TLongIntHashMap();
//            tmaps[i] = new TLongIntHashMap(1200000, 1.0F, -1L, -1);
//            tmaps[i] = new TLongIntHashMap(64000000, 1.0F, -1L, -1);
            // todo add comsumer for each valueLog number
//            consumers[i] = new Thread(new KeyConsumer(keyLog, tmaps[i], i, sum));
        }
        System.out.println("sum1:" + sum);
        //Step: 多线程索引的建立
        long indexCheckStart = System.currentTimeMillis();
        for (int i = 0; i < 64; i++) {
            consumers[i].start();
        }

        for (int i = 0; i < 64; i++) {
            try {
                consumers[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        long indexCheckEnd = System.currentTimeMillis();
        System.out.printf("Index Check: %d ms \n", indexCheckEnd - indexCheckStart);
    }

    private void recoverHashtable() {
        ByteBuffer byteBuffer = keyLog.getKeyBuffer();
        byteBuffer.position(0);
        int sum = 0;//总共写入了多少个数据
        int[] valueLogWroteposition = new int[64];//每个valuelog文件写入了多少个数据
        for (int i = 0; i < 64; i++) {
            valueLogWroteposition[i] = (int) (valueLog[i].getFileLength() / 4096);
            sum += valueLogWroteposition[i];
        }
        System.out.println("sum:" + sum);
        byte[] key = new byte[8];
        while (sum > 0) {
            byteBuffer.get(key);
            tmap.put(ByteBuffer.wrap(key).getLong(), byteBuffer.getInt());
            sum--;
        }
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

        //因为只用一个keylog，所以要有个原子量记录写在keylog中的位置
        keyLog.putKey(key, offset, kelogWrotePosition.getAndAdd(12));

        valueLog[valueLogNo].putMessageDirect(value);
    }

    public byte[] read(byte[] key) throws EngineException {
//        long keyLong = ByteBuffer.wrap(key).getLong();
//        long keyLong = Constants.bytesToLong(key);
//        String keyStr = new String(key, UTF_8);
//        int index = bytesGetTwo(key) % 64;
//        int index = (int) (keyLong % 64);
//        int currentPos = tmaps[index].get(keyLong);
//        int currentPos = tmaps[index].get(keyLong);
//        System.out.println("r:" + keyStr + "," + currentPos + "," + index);
        int currentPos = tmap.get(ByteBuffer.wrap(key).getLong());
        if (currentPos == -1) {
            throw new EngineException(RetCodeEnum.NOT_FOUND, "not found this key");
        }
//        int valueLogNo = currentPos >> 24;
//        int num = currentPos & 0x00FFFFFF;
//        long value_file_wrotePosition = ((long)num) * 4096;
        return valueLog[currentPos >> 24].getMessageDirect(((long) (currentPos & 0x00FFFFFF)) * 4096, threadLocalReadBuffer.get(), threadLocalReadBytes.get());
    }

    public void close() {
        keyLog.close();
        for (ValueLog V : valueLog) {
            V.close();
        }
        keyLog = null;
        valueLog = null;
        tmap = null;
//        if (tmaps != null) {
//            for (int i = 0; i < 64; i++) {
//                if (tmaps[i] != null)
//                    tmaps[i] = null;
//            }
//            tmaps = null;
//        }
        threadLocalReadBuffer = null;
        threadLocalReadBytes = null;
        threadValueLog = null;
    }
}
