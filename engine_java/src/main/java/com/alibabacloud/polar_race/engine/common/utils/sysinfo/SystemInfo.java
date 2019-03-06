package com.alibabacloud.polar_race.engine.common.utils.sysinfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemInfo {
    public enum SIZE_UNIT {
        B(1), KB(1024), MB(1024 * 1024);

        private int size;

        SIZE_UNIT(int size) {
            this.size = size;
        }
    }

    public static long getFreeMemory(SIZE_UNIT unit) {
        return Runtime.getRuntime().freeMemory() / unit.size;
    }

    public static long getMaxMemory(SIZE_UNIT unit) {
        return Runtime.getRuntime().maxMemory() / unit.size;
    }

    public static long getTotalMemory(SIZE_UNIT unit) {
        return Runtime.getRuntime().totalMemory() / unit.size;
    }

    public static String getSystemInfo() {
        return "System Infomation:\n" + "Java version: " + System.getProperty("java.version") + "\nJava VM Version: " +
                System.getProperty("java.vm.version") + "\nOS info:" + System.getProperty("os.name") + "_"
                + System.getProperty("os.arch") + "_" + System.getProperty("os.version");
    }

    public static String getMemoryInfo(SIZE_UNIT unit) {
        long totalMemory = getTotalMemory(unit);
        long maxMemory = getMaxMemory(unit);
        long freeMemory = getFreeMemory(unit);
        return "Max Memory(" + unit + "): " + maxMemory + "\nTotal Memory(" + unit + "): " + totalMemory +
                "\nFree Memory(" + unit + "): " + freeMemory;
    }

    public static void main(String[] args) {
        Logger log = LoggerFactory.getLogger(SystemInfo.class);
        log.info("Free memory(KB): " + getFreeMemory(SIZE_UNIT.KB));
        log.info("Free memory(MB): " + getFreeMemory(SIZE_UNIT.MB));
        log.info(getSystemInfo());
        log.info("MemoryInfo: \n" + getMemoryInfo(SIZE_UNIT.MB));
    }
}
