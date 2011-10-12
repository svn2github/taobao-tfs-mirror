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

import com.taobao.common.tfs.common.TfsConstant;

public class RcKeepAliveInfo {
    private long lastReportTime;
    private RcSessionBaseInfo sessionBaseInfo;
    private RcSessionStatInfo sessionStatInfo;

    public long getLastReportTime() {
        return lastReportTime;
    }

    public void setLastReportTime(long lastReportTime) {
        this.lastReportTime = lastReportTime;
    }

    public RcSessionBaseInfo getSessionBaseInfo() {
        return sessionBaseInfo;
    }

    public void setSessionBaseInfo(RcSessionBaseInfo sessionBaseInfo) {
        this.sessionBaseInfo = sessionBaseInfo;
    }

    public RcSessionStatInfo getSessionStatInfo() {
        return sessionStatInfo;
    }

    public void setSessionStatInfo(RcSessionStatInfo sessionStatInfo) {
        this.sessionStatInfo = sessionStatInfo;
    }

    public int length() {
        return sessionBaseInfo.length() + sessionStatInfo.length() + TfsConstant.LONG_SIZE;
    }

    public boolean encode(ByteBuffer byteBuffer) {
        sessionBaseInfo.encode(byteBuffer);
        sessionStatInfo.encode(byteBuffer);
        byteBuffer.putLong(lastReportTime);
        return true;       
    }

    public String toString() {
        return "lastReportTime: " + lastReportTime +
            "\nsessionBaseInfo:\n" + sessionBaseInfo +
            "\nsessionStatInfo:\n" + sessionStatInfo;
    }

}
