/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.impl;

import java.util.Timer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ManagerTimerTask {
    private static final Log log = LogFactory.getLog(ManagerTimerTask.class);

    private static final ManagerTimerTask managerTimerTask = new ManagerTimerTask();

    private Timer timer = null;

    private GcWorker gcWorker = new GcWorker();
    private CacheMetric cacheMetric = new CacheMetric();

    private ManagerTimerTask() {
    }

    public static ManagerTimerTask getInstance() {
        return managerTimerTask;
    }

    public synchronized void init() {
        if (timer == null) {
            timer = new Timer();
            timer.schedule(gcWorker, 0, ClientConfig.GC_INTERVAL);
            timer.schedule(cacheMetric, 0, ClientConfig.CACHEMETRIC_INTERVAL);
        }
    }

    public synchronized void destroy() {
        if (timer != null) {
            timer.cancel();
            timer = null;
        }
    }

    public GcWorker getGcWorkerTask() {
        return gcWorker;
    }

    public CacheMetric getCacheMetricTask() {
        return cacheMetric;
    }

    public int getCacheHitRatio() {
        if (timer == null) {
            log.warn("cachemetric timer not start. no cache hit ratio");
            return 0;
        }
        return cacheMetric.getHitRatio();
    }
}

