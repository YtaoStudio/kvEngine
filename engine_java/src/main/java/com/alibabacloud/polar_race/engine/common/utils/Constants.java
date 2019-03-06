package com.alibabacloud.polar_race.engine.common.utils;

public class Constants {
    public static int KEY_OFFSET_BYTE_SIZE = 12;
    public static final int SPLIT_SIZE = 64;
    public static final int VALUE_SIZE = 4096;
    public static final int RANGE_THREAD = 64;
    public static final int KEY_SIZE = 8;
    public static final int PARTITION_SIZE = 1024;  // 256
    public static final int CACHE_SIZE = 2 * 1024;
    public static final int CACHE_KEYARRY_SIZE = 1024;
    public static final int KEY_OFFSET_MAP_SIZE = 102000; // 64000000 / 256 = 250000
    public static final long PAGE_SIZE = 12000000;
    public static final int CACHE_PARTITION_NUM = 2;  // 4
}
