/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.impl;

import java.util.Comparator;
import java.nio.ByteBuffer;

import com.taobao.common.tfs.common.TfsConstant;

public class SegmentInfo {
    private int blockId;
    private long fileId;
    private long offset;
    private int length;
    private int crc;

    public void reset() {
        blockId = 0;
        fileId = 0;
        offset = 0;
        length = 0;
        crc = 0;
    }

    public void setBlockId(int blockId) {
        this.blockId = blockId;
    }

    public int getBlockId() {
        return blockId;
    }

    public void setFileId(long fileId) {
        this.fileId = fileId;
    }

    public long getFileId() {
        return fileId;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getOffset() {
        return offset;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public int getLength() {
        return length;
    }

    public void setCrc(int crc) {
        this.crc = crc;
    }

    public int getCrc() {
        return crc;
    }

    public void serialize(ByteBuffer data) {
        data.order(TfsConstant.TFS_PROTOCOL_BYTE_ORDER);
        data.putInt(blockId);
        data.putLong(fileId);
        data.putLong(offset);
        data.putInt(length);
        data.putInt(crc);
    }

    public void deserialize(ByteBuffer data) {
        data.order(TfsConstant.TFS_PROTOCOL_BYTE_ORDER);
        blockId = data.getInt();
        fileId = data.getLong();
        offset = data.getLong();
        length = data.getInt();
        crc = data.getInt();
    }

    public String toString() {
        return "blockId: " + blockId + " fileId: " + fileId +
            " offset: " + offset + " length: " + length +
            " crc: " + crc;
    }

    public static int size() {
        return TfsConstant.INT_SIZE * 3 + TfsConstant.LONG_SIZE * 2;
    }

    public static class SegmentInfoComparator implements Comparator<SegmentInfo> {
        public int compare(SegmentInfo segmentInfoL, SegmentInfo segmentInfoR) {
            return segmentInfoL.getOffset() < segmentInfoR.getOffset() ? -1 :
                segmentInfoL.getOffset() == segmentInfoR.getOffset() ? 0 : 1;
        }
    }

}
