/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.impl;

import java.nio.ByteBuffer;

import com.taobao.common.tfs.common.TfsConstant;

public class SegmentHead {
    private int segmentCount = 0;
    private long segmentLength = 0;
    private byte[] reserve;     // reserve. not use now
    private static final int RESERVE_LENGTH = 64;

    public void setSegmentCount(int segmentCount) {
        this.segmentCount = segmentCount;
    }

    public int getSegmentCount() {
        return segmentCount;
    }

    public void setSegmentLength(long segmentLength) {
        this.segmentLength = segmentLength;
    }

    public long getSegmentLength() {
        return segmentLength;
    }

    public void increment(long length) {
        segmentCount++;
        segmentLength += length;
    }

    public void increment(int count, long length) {
        segmentCount += count;
        segmentLength += length;
    }

    public void decrement(long length) {
        segmentCount--;
        segmentLength -= length;
    }

    public void decrement(int count, long length) {
        segmentCount -= count;
        segmentLength -= length;
    }

    public void serialize(ByteBuffer data) {
        data.order(TfsConstant.TFS_PROTOCOL_BYTE_ORDER);
        data.putInt(segmentCount);
        data.putLong(segmentLength);
        data.position(data.position() + RESERVE_LENGTH); // just reserve
    }

    public void deserialize(ByteBuffer data) {
        data.order(TfsConstant.TFS_PROTOCOL_BYTE_ORDER);
        segmentCount = data.getInt();
        segmentLength = data.getLong();
        data.position(data.position() + RESERVE_LENGTH); // just reserve
    }

    public void clear() {
        segmentCount = 0;
        segmentLength = 0;
    }

    public String toString() {
        return "count: " + segmentCount + " length: " + segmentLength;
    }

    public static int size() {
        return TfsConstant.INT_SIZE + TfsConstant.LONG_SIZE + RESERVE_LENGTH;
    }

}
