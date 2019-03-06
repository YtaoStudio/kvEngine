package com.alibabacloud.polar_race.engine.common.simple;

import net.smacke.jaydio.DirectRandomAccessFile;

import java.io.File;
import java.io.IOException;

public class ValueDioReadLog {

    private DirectRandomAccessFile readDio;

    public ValueDioReadLog(String storePath, int filename) {
        try {
            File file = new File(storePath, "v" + filename);
            this.readDio = new DirectRandomAccessFile(file, "r");
        } catch (IOException e) {
            System.out.println("create file channel " + "valueLog" + " Failed. ");
        }
    }

    public byte[] getMessageDirect(long offset, byte[] bytes) {
        synchronized (this) {
            try {
                readDio.seek(offset);
                readDio.read(bytes);
                return bytes;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return bytes;
    }

    public long getFileLength() {
        return this.readDio.length();
    }

    public void close() {
        try {
            this.readDio.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
