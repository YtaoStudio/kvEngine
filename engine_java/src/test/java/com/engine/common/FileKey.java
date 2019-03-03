package com.engine.common;

import com.engine.common.exceptions.EngineException;
import com.engine.common.exceptions.RetCodeEnum;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

import static com.engine.common.EngineRace.VALUE_LEN;

/**
 * @version V1.0
 * @Package: com.alibabacloud.polar_race.engine.common
 * @ClassName: FileKey
 * @Description:
 * @author: tao
 * @date: Create in 2018-11-08 16:47
 **/
public class FileKey {

    private final AtomicInteger nextKeyNumber = new AtomicInteger(0);
    private final FileChannel fileChannel;
    private int fileNo;
    private long fileOffset = 0L;

    public FileKey(File curFile, int fileNo) throws FileNotFoundException {
        this.fileNo = fileNo;
        this.fileChannel = new RandomAccessFile(curFile, "rw").getChannel();
    }

    public int getFileNo() {
        return fileNo;
    }

    public int getNextKeyNumber() {
        return nextKeyNumber.getAndIncrement();
    }

    public long appendValue(byte[] value) {
        int writtenLength = 0;
        try {
            writtenLength = fileChannel.write(ByteBuffer.wrap(value));
        } catch (IOException e) {
            try {
                throw new EngineException(RetCodeEnum.CORRUPTION, "W:" + e.getMessage());
            } catch (EngineException e1) {
                e1.printStackTrace();
            }
        }
        long tmp = fileOffset;
        fileOffset += writtenLength;
        return tmp / VALUE_LEN;
    }
}
