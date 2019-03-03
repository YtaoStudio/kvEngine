## SimpleEngine
```
保证数据不丢失
64线程， 每个线程访问100W次
key 1000,000 * 64 * 8bit = 512M 同时key的key需要long存储，也需要512M，
value 1000,000 * 64 * 4k = 256G
为防止线程同步问题，可考虑threadLocal，数据在磁盘中排序？

可以采用LSM算法
也可以直接将key进行存储

针对value， 可以将几个线程进行merge，数据达到64k，一起落盘
不然64线程同时4k数据落盘IO是否会是瓶颈有待测试

假设写入入读1.2G/S
1,200,000 k = 300,000 * 4 k
大约每个线程每毫秒4687.5次才能达到IO极限。
```

```java
public class SimpleEngine extends AbstractEngine {

    // new TLongIntHashMap(32000000, 1.0f, -1, -1)
    private FileChannel keyChannel, valueChannel, exchangeChannel;

    // 标志位，用于判断之前是存储阶段还是读取阶段.
    private AtomicBoolean readWriteSign = new AtomicBoolean(false);

    /**
     * 创建三个文件，一个用于存储value，一个用于存储key+position，一个用于后续交换文件
     *
     * @param path the path of engine store data.
     * 
     */
    @Override
    public void open(String path) throws EngineException {

        Path directoryPath = Paths.get(path);
        if (!Files.exists(directoryPath)) {
            try {
                Files.createDirectory(directoryPath);
            } catch (IOException e) {
                throw new EngineException(RetCodeEnum.IO_ERROR, e.toString());
            }
        }
        File keyFile = new File(path, "key.log");
        File valueFile = new File(path, "value.log");
        if (keyFile.exists()) {
            // 需要初始化数据
        } else {
            try {
                keyChannel = new RandomAccessFile(keyFile, "rw").getChannel();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        String keyLog = path + "/key.log";
        String valueLog = path + "/value.log";
        String exchangeLog = path + "/exchange.log";
        try {
            keyChannel = new RandomAccessFile(keyLog, "rw").getChannel();
            valueChannel = new RandomAccessFile(valueLog, "rw").getChannel();
        } catch (FileNotFoundException e) {
            throw new EngineException(RetCodeEnum.NOT_FOUND, e.toString());
        }
    }

    @Override
    public void write(byte[] key, byte[] value) throws EngineException {

    }

    @Override
    public byte[] read(byte[] key) throws EngineException {

        return null;
    }

    @Override
    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {

    }

    @Override
    public void close() {

    }
}
```

## KvEngine

