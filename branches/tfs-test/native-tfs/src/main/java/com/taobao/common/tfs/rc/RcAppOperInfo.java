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

public class RcAppOperInfo {
    private int operType;          // type
    private long operTimes;        // times
    private long operResponseTime; // response time
    private long operSuccessCount; // success count
    private long operSize;         // operated size

    public static int RC_OPER_READ = 1;
    public static int RC_OPER_WRITE = 2;
    public static int RC_OPER_UNIQUE_WRITE = 3;
    public static int RC_OPER_UNLINK = 4;
    public static int RC_OPER_UNIQUE_UNLINK = 5;

    public int getOperType() {
        return operType;
    }

    public void setOperType(int operType) {
        this.operType = operType;
    }

    public long getOperTimes() {
        return operTimes;
    }

    public void setOperTimes(long operTimes) {
        this.operTimes = operTimes;
    }

    public long getOperResponseTime() {
        return operResponseTime;
    }

    public void setOperResponseTime(long operResponseTime) {
        this.operResponseTime = operResponseTime;
    }

    public long getOperSuccessCount() {
        return operSuccessCount;
    }

    public void setOperSuccessCount(long operSuccessCount) {
        this.operSuccessCount = operSuccessCount;
    }

    public long getOperSize() {
        return operSize;
    }

    public void setOperSize(long operSize) {
        this.operSize = operSize;
    }

    public void add(RcAppOperInfo appOperInfo) {
        this.operTimes += appOperInfo.getOperTimes();
        this.operSize += appOperInfo.getOperSize();
        this.operResponseTime += appOperInfo.getOperResponseTime();
        this.operSuccessCount += appOperInfo.getOperSuccessCount();
    }

    public int length() {
        return TfsConstant.INT_SIZE + TfsConstant.LONG_SIZE * 4;
    }

    public boolean encode(ByteBuffer byteBuffer) {
        byteBuffer.putInt(operType);
        byteBuffer.putLong(operTimes);
        byteBuffer.putLong(operSize);
        byteBuffer.putLong(operResponseTime);
        byteBuffer.putLong(operSuccessCount);
        return true;
    }

    public String toString() {
        return "operType: " + operType + " operTimes: " + operTimes +
            " operResponseTime: "  + operResponseTime + " operSucCount: " + operSuccessCount +
            " operSize: " + operSize;
    }
}
