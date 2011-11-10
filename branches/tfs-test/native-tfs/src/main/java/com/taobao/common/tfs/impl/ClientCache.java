/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.impl;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.LinkedHashMap;

public class ClientCache {

    class BlockCacheItem {
        private long lastTime;
        private List<Long> dsList;

        public BlockCacheItem(long lastTime, List<Long> dsList) {
            this.lastTime = lastTime;
            this.dsList = dsList;
        }

        public long getLastTime() {
            return lastTime;
        }

        public void setLastTime(long lastTime) {
            this.lastTime = lastTime;
        }

        public List<Long> getDsList() {
            return dsList;
        }

        public void setDsList(List<Long> dsList) {
            this.dsList = dsList;
        }
    }

    private int maxCacheItemCount = ClientConfig.CACHEITEM_COUNT;
    private int maxCacheTime = ClientConfig.CACHE_TIME;
    private CacheMetric cacheMetric = ManagerTimerTask.getInstance().getCacheMetricTask();

    private LinkedHashMap cacheMap =
        new LinkedHashMap(maxCacheItemCount/2, 0.75f, true) {
            protected boolean removeEldestEntry(Map.Entry eldest) {
                return size() > maxCacheItemCount;
            }
        };

    public ClientCache() {
    }

    public ClientCache(int maxCacheItemCount, int maxCacheTime) {
        this.maxCacheItemCount = maxCacheItemCount;
        this.maxCacheTime = maxCacheTime;
    }

    public int getMaxCacheItemCount() {
        return maxCacheItemCount;
    }

    public void setMaxCacheItemCount(int maxCacheItemCount) {
        this.maxCacheItemCount = maxCacheItemCount;
    }

    public int getMaxCacheTime() {
        return maxCacheTime;
    }

    public void setMaxCacheTime(int maxCacheTime) {
        this.maxCacheTime = maxCacheTime;
    }

    public synchronized void clear() {
        cacheMap.clear();
    }

    public synchronized void remove(int blockId) {
        cacheMap.remove(blockId);
        cacheMetric.addRemoveCount();
    }

    public synchronized List<Long> get(int blockId) {
        BlockCacheItem cacheItem = (BlockCacheItem)cacheMap.get(blockId);
        if (cacheItem != null &&
            System.currentTimeMillis() - cacheItem.getLastTime() < maxCacheTime) {
            cacheMetric.addHitCount();
            // copy
            return new ArrayList<Long>(cacheItem.getDsList());
        } else {
            cacheMetric.addMissCount();
        }
        return null;
    }

    public synchronized void put(int blockId, List<Long> dsList) {
        if (dsList == null) {
            return;
        }
        cacheMap.put(blockId,
                     (new BlockCacheItem(
                         System.currentTimeMillis(),
                         (new ArrayList<Long>(dsList))))); // copy
    }
}

