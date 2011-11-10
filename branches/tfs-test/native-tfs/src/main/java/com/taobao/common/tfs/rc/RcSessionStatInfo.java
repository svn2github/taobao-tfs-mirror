/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.rc;

import java.nio.ByteBuffer;

import java.util.Map;
import java.util.HashMap;
import java.util.Map.Entry;

import com.taobao.common.tfs.common.TfsConstant;

public class RcSessionStatInfo {
    private long cacheHitRatio;
    private Map<Integer, RcAppOperInfo> statInfos;

    RcSessionStatInfo() {
        cacheHitRatio = 0;
        statInfos = new HashMap<Integer, RcAppOperInfo>();
    }

    RcSessionStatInfo(RcSessionStatInfo sessionStatInfo) {
        this.cacheHitRatio = sessionStatInfo.getCacheHitRatio();
        this.statInfos = new HashMap<Integer, RcAppOperInfo>(sessionStatInfo.getStatInfos());
    }

    public long getCacheHitRatio() {
        return cacheHitRatio;
    }

    public void setCacheHitRatio(long cacheHitRatio) {
        this.cacheHitRatio = cacheHitRatio;
    }

    public Map<Integer, RcAppOperInfo> getStatInfos() {
        return statInfos;
    }

    public void setStatInfos(Map<Integer, RcAppOperInfo> statInfos) {
        this.statInfos = statInfos;
    }

    public void addStatInfo(RcAppOperInfo appOperInfo) {
        RcAppOperInfo curAppOperInfo = statInfos.get(appOperInfo.getOperType());
        if (curAppOperInfo == null) {
            statInfos.put(appOperInfo.getOperType(), appOperInfo);
        } else {
            curAppOperInfo.add(appOperInfo);
        }
    }

    public void add(RcSessionStatInfo sessionStatInfo) {
        for (Entry entry : statInfos.entrySet()) {
            addStatInfo((RcAppOperInfo)entry.getValue());
        }
    }

    public void clear() {
        cacheHitRatio = 0;
        statInfos.clear();
    }

    public int length() {
        int length = TfsConstant.INT_SIZE + TfsConstant.LONG_SIZE;
        for (Entry entry : statInfos.entrySet()) {
            length += ((RcAppOperInfo)entry.getValue()).length();
        }
        return length;
    }

    public boolean encode(ByteBuffer byteBuffer) {
        byteBuffer.putInt(statInfos.size());
        for (Entry entry : statInfos.entrySet()) {
            byteBuffer.putInt((Integer)entry.getKey());
            ((RcAppOperInfo)entry.getValue()).encode(byteBuffer);
        }
        byteBuffer.putLong(cacheHitRatio);
        return true;
    }

    public String toString() {
        String str = "cacheHitRatio: " + cacheHitRatio +
            "\nappOperInfo: " + statInfos.size();
        for (Entry entry : statInfos.entrySet()) {
            str += "\n" + (RcAppOperInfo)entry.getValue();
        }
        return str;
    }
}
