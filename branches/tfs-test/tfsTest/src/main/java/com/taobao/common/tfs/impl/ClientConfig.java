package com.taobao.common.tfs.impl;

import com.taobao.common.tfs.common.TfsConstant;

public class ClientConfig {
    // timeout
    public static int TIMEOUT = TfsConstant.DEFAULT_TIMEOUT;

    // small and large file io
    public static int MAX_SMALL_IO_LENGTH = 1 << 19; // 512k
    public static int MAX_LARGE_IO_LENGTH = 1 << 23; // 8M

    // segment
    public static int BATCH_COUNT = TfsConstant.MAX_BATCH_COUNT;
    public static int SEGMENT_LENGTH = TfsConstant.MAX_SEGMENT_LENGTH;
    public static int BATCH_WRITE_LENGTH = BATCH_COUNT * SEGMENT_LENGTH;

    // cache
    public static int CACHEITEM_COUNT = 500000;
    public static int CACHE_TIME = 600000; // 10min

    // timeworker interval
    public static int GC_INTERVAL = 43200000;       // 0.5 day
    public static int CACHEMETRIC_INTERVAL = 60000; // 60s

    // gc expired time
    public static int GC_EXPIRED_TIME = TfsConstant.MIN_GC_EXPIRED_TIME; // 1 day
}