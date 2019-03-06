# Engine Configuration

## JVM 
- GC 
```
-server -Xms1500m -Xmx1500m -XX:MaxDirectMemorySize=1024m -XX:MaxMetaspaceSize=300m -XX:NewRatio=4 -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:-UseBiasedLockin -XX:+PrintGCDateStamps
```

## 内存结构
应用于 branch`1024partitions_2buffers_64threads`, 在此方法中删除
```
private void initCurCacheMuti(TSortedLongIntHashMap map, int index, int cacheIndex) {
    // split to n partition
    int splitLen = (map.size() % Constants.SPLIT_SIZE == 0) ? (map.size() / Constants.SPLIT_SIZE) : (map.size() / Constants.SPLIT_SIZE + 1);
    CacheTask[] cacheTasks = new CacheTask[Constants.SPLIT_SIZE];
    int start = 0;
    int end = 0;
    for (int i = 0; i < Constants.SPLIT_SIZE; i++) {
        end += splitLen;
        if (end > map.size()) {
            end = map.size();
        }
        cacheTasks[i] = new CacheTask(start, end, map, index, cacheIndex);
        start = end;
    }

    // cache多线程
//        long indexCheckStart = System.currentTimeMillis();
    for (int i = 0; i < Constants.SPLIT_SIZE; i++) {
        cacheTasks[i].start();
    }
    for (int i = 0; i < Constants.SPLIT_SIZE; i++) {
        try {
            cacheTasks[i].join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
//        long indexCheckEnd = System.currentTimeMillis();
//        System.out.printf("Cache Check: %d ms\n", indexCheckEnd - indexCheckStart);
}

class CacheTask extends Thread {
    private int start;
    private int end;
    private TSortedLongIntHashMap map;
    private int index;
    private int cacheIndex;

    public CacheTask(int start, int end, TSortedLongIntHashMap map, int index, int cacheIndex) {
        this.start = start;
        this.end = end;
        this.map = map;
        this.index = index;
        this.cacheIndex = cacheIndex;
    }

    @Override
    public void run() {
//            for (int i = start; i < end; i++) {
//                try {
//                    System.arraycopy(read(map.get(map.getKey(i)), index), 0, cacheArray[cacheIndex][i], 0, Constants.VALUE_SIZE);
////                    UNSAFE.copyMemory(read(map.get(map.getKey(i)), index), 0, cacheArray[cacheIndex][i], 0, Constants.VALUE_SIZE + 16);
//                } catch (EngineException e) {
//                    e.printStackTrace();
//                }
//            }
        // todo sequence read
//            try {
//                read(start, end, index, cacheArray[cacheIndex]);
//            } catch (EngineException e) {
//                e.printStackTrace();
//            }
    }
}

    public byte[] read(long currentPos, int index) throws EngineException {
        return valueReadLog[index].getMessageDirect(currentPos << 12, threadLocalReadBuffer.get(), threadLocalReadBytes.get());
    }
```

