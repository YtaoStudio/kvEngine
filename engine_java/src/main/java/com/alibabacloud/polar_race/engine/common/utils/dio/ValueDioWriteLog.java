package com.alibabacloud.polar_race.engine.common.utils.dio;

import net.smacke.jaydio.DirectRandomAccessFile;

import java.io.File;
import java.io.IOException;


/**
 *
 */
public class ValueDioWriteLog {

    private DirectRandomAccessFile writeDio;

    public ValueDioWriteLog(String storePath, int filename) {
        try {
            File file = new File(storePath, "v" + filename);
            if (!file.exists()) {
                try {
                    file.createNewFile();
                } catch (IOException e) {
                    System.out.println("Create file" + "valueLog" + filename + "failed");
                    e.printStackTrace();
                }
            }
            this.writeDio =
                    new DirectRandomAccessFile(file, "rw");
        } catch (IOException e) {
            System.out.println("create file channel " + "valueLog" + " Failed. ");
        }
    }

    public void putMessageDirect(byte[] value) {
        try {
            this.writeDio.write(value, 0, 4096);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    long getWrotePosition() {
        return this.writeDio.getFilePointer();
    }

    public void close() {
        try {
            this.writeDio.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
