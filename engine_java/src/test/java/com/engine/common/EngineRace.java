package com.engine.common;

import com.engine.util.Slices;
import gnu.trove.map.hash.TLongIntHashMap;

import com.engine.common.exceptions.EngineException;
import com.engine.common.exceptions.RetCodeEnum;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Three key points for the race:
 * 1. handle concurrent operations correctly
 * 2. handle process exit gracefully, make sure no data corruption or loss
 * 3. achieve best performance on SSD devices
 */
public class EngineRace extends AbstractEngine {
    private static final AtomicInteger nextVLogNumber = new AtomicInteger(0);

    public static final String INDEX_FILE_NAME = "index";

    /**
     * Length of value
     */
    public static final int VALUE_LEN = 4 * 1024;

    /**
     * Number of file
     */
    public static final int LOG_NUM = 64;

    /**
     * Length of key, fileNo, offset
     */
    public static final int KEY_LEN = 12;

    /**
     * Size of a single file
     */
    public static final long SINGLE_File_SIZE = 4 * 1024 * 3 * 1024;

    /**
     * used in MappedByteBuffer
     */
    public static final long PAGE_SIZE = 12 * 64 * 1024 * 1024; // 12 * 64 * 1024 * 1024

    public static final byte SIZE_OF_BYTE = 1;
    public static final byte SIZE_OF_SHORT = 2;
    public static final byte SIZE_OF_INT = 4;
    public static final byte SIZE_OF_LONG = 8;

    private File databaseDir;
    private ThreadLocal<FileKey> vLog;
    private FileKey[] keyOffsetFiles;
    private MMapFile[] mmapFiles;
    //    private CASLock[] writeLocks; // no use
    private TLongIntHashMap keyFileMap;
    private FileChannel fileChannel;
    private MappedByteBuffer mappedByteBuffer;

    @Override
    public void open(String path)
            throws EngineException {
        this.databaseDir = new File(path);
        if (!databaseDir.exists()) {
            databaseDir.mkdirs();
        }

        this.vLog = new ThreadLocal<>();
        this.keyOffsetFiles = new FileKey[LOG_NUM];
        this.mmapFiles = new MMapFile[LOG_NUM];
//        this.writeLocks = new CASLock[LOG_NUM];
        this.keyFileMap = new TLongIntHashMap(64000000, 1.0F, -1L, -1);
//        this.keyFileMap = new TLongIntHashMap();
        File currentFile = new File(databaseDir, INDEX_FILE_NAME);
        if (!currentFile.exists()) {
            try {
                currentFile.createNewFile();
            } catch (IOException e) {
                throw new EngineException(RetCodeEnum.INVALID_ARGUMENT, "Unable to create log file ");
            }
        }
        try {
            this.fileChannel = new RandomAccessFile(currentFile, "rw").getChannel();
            this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, PAGE_SIZE);
        } catch (IOException e) {
            e.printStackTrace();
        }

        long keySize = 0;
        File curVlogFile = null;
        for (int i = 0; i < LOG_NUM; i++) {
            curVlogFile = new File(databaseDir, i + "" + ".log");
            if (!curVlogFile.exists()) {
                try {
                    curVlogFile.createNewFile();
//                    writeLocks[i] = new CASLock();
                    keyOffsetFiles[i] = new FileKey(curVlogFile, i);
                } catch (IOException e) {
                    throw new EngineException(RetCodeEnum.CORRUPTION, "Unable to create new keyOffsetFiles");
                }
            } else {
                keySize += curVlogFile.length() / VALUE_LEN;
                try {
                    mmapFiles[i] = new MMapFile(curVlogFile, i);
                } catch (IOException e) {
                    throw new EngineException(RetCodeEnum.CORRUPTION, "Unable to create new mmapFiles");
                }
            }
        }
        System.out.println("size：" + keySize);
        if (keySize > 0) {
            currentFile = new File(databaseDir, INDEX_FILE_NAME);
            System.out.println("KEY:" + currentFile.length());
            try {
                // create index
                recoverLogFile(currentFile, keySize);
                System.out.println("Main:" + Thread.currentThread().getName() + "," + keyFileMap.size());
            } catch (IOException e) {
                throw new EngineException(RetCodeEnum.CORRUPTION, "Unable to recover log file ");
            }
        } else {
            System.out.println("No vLog when open()");
        }
    }

    /**
     * recover from 'KEY', to load into memory, the size of map is according to the {value length / 4K}
     *
     * @param file
     * @param keySize
     * @throws IOException
     */
    private void recoverLogFile(File file, long keySize) throws IOException {
        try (FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel()) {
            MappedByteBuffer indexBuf = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, PAGE_SIZE);
            // Read all the records and add to a TLongIntHashMap
            long offset = 0L;
            boolean running = true;
            while (running) {
                if (indexBuf.remaining() == 0) {
                    running = false;
                }
                // read vLog
                byte[] key = new byte[SIZE_OF_LONG];
                indexBuf.get(key, 0, SIZE_OF_LONG);

                byte[] value = new byte[SIZE_OF_INT];
                indexBuf.get(value, 0, SIZE_OF_INT);

                long keys = Slices.bytesToLong(key);
                int vNoOffset = Slices.bytesToInt(value, 0);
                keyFileMap.put(keys, vNoOffset);

                offset++;
                if (offset >= keySize) {
                    running = false;
                    System.out.println("offset==keySize," + keyFileMap.size());
                }
            }
        }
    }

    /**
     * read key in open() according to the length of value
     * <ol>
     * <li>key, fileno, offset load into memory</li>
     * <li>value load into vLog by filechannel</li>
     * <ol/>
     *
     * @param key
     * @param value
     * @throws EngineException
     */
    @Override
    public void write(byte[] key, byte[] value) throws EngineException {
        FileKey fileKey = null;
        int keyNo = 0;
        int vNoOffset = 0;
        int vLogNumber = 0;
        try {
            if (vLog.get() == null) {
                vLogNumber = getNextLogNumber();
                fileKey = keyOffsetFiles[vLogNumber]; // from 0
                vLog.set(fileKey);
            } else {
                fileKey = vLog.get();
                vLogNumber = fileKey.getFileNo();
            }
            try {
                keyNo = fileKey.getNextKeyNumber(); // from 0
            } catch (Exception e) {
                throw new EngineException(RetCodeEnum.INVALID_ARGUMENT, e.getMessage());
            }
            // write key, fileNo, offset
            try {
                vNoOffset = Slices.valueInt(vLogNumber, keyNo);
            } catch (Exception e) {
                throw new EngineException(RetCodeEnum.INVALID_ARGUMENT, e.getMessage());
            }
            addKeyFile(key, vNoOffset);
            // write vLog
            fileKey.appendValue(value);
//            System.out.println(vLogNumber + "," + keyNo);
        } catch (Exception e) {
            throw new EngineException(RetCodeEnum.CORRUPTION, "W:" + vLogNumber + "," + keyNo + e.getMessage());
        }

    }

    @Override
    public byte[] read(byte[] key) throws EngineException {
        long keys = Slices.bytesToLong(key);
        // read from memory
        int values = keyFileMap.get(keys);
        // values not null
        if (values != 0) {
            int vLogNumber = values >> 24;
            int keyNo = (values & ((1 << 24) - 1)); // from 0
            // read vLog
            byte[] value = mmapFiles[vLogNumber].read(keyNo, vLogNumber);
            return value;
        } else {
            // value ""
            throw new EngineException(RetCodeEnum.NOT_FOUND, new String(key, UTF_8));
        }

    }

    public synchronized void addKeyFile(byte[] key, int vNoOffset) {
        try {
            mappedByteBuffer.put(key);
            mappedByteBuffer.put(Slices.intToBytes(vNoOffset));
        } catch (Exception e) {
            System.out.println("addKeyFile");
        }
    }

    @Override
    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {
    }

    @Override
    public void close() {

    }

    public int getNextLogNumber() {
        return nextVLogNumber.getAndIncrement();
    }

    class MMapFile {
        private final FileChannel fileChannel;
        //    private final MappedByteBuffer mappedByteBuffer;
        private int fileNo;

        public MMapFile(File curFile, int fileNo) throws IOException {
            this.fileNo = fileNo;
            this.fileChannel = new RandomAccessFile(curFile, "rw").getChannel();
//        this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, SINGLE_File_SIZE);
        }

        public byte[] read(int offset, int logNo) throws EngineException {
            ByteBuffer byteBuffer = ByteBuffer.allocate(VALUE_LEN);
            try {
                fileChannel.position(offset * VALUE_LEN);
                int bytesRead = fileChannel.read(byteBuffer);
                if (-1 == bytesRead) {
                    throw new EngineException(RetCodeEnum.NOT_FOUND, offset + "," + logNo);
                }
            } catch (IOException e) {
                throw new EngineException(RetCodeEnum.NOT_FOUND, e.getMessage());
            }
            return byteBuffer.array();
        }
    }

    /**
     * //todo not need, for each thread is sequence for one file
     */
    class CASLock implements Lock {

        private AtomicBoolean atomicBoolean = new AtomicBoolean(false);

        @Override
        public void lock() {
            while (true) {
                if (atomicBoolean.get() == true) {
                    continue;
                }
                if (atomicBoolean.compareAndSet(false, true)) {
                    break;
                }
            }
        }

        @Override
        public void unlock() {
            atomicBoolean.set(false);
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {

        }

        @Override
        public boolean tryLock() {
            return false;
        }

        @Override
        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            return false;
        }

        @Override
        public Condition newCondition() {
            return null;
        }

    }
}
