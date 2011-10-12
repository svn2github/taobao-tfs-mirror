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

import com.taobao.common.tfs.impl.SegmentInfo;
import com.taobao.common.tfs.common.TfsUtil;
import com.taobao.common.tfs.packet.DsListWrapper;

public class SegmentData {
    public enum SegmentStatus {
        SEG_STATUS_NOT_INIT,          // not initialized
        SEG_STATUS_OPEN_OVER,         // all is completed
        SEG_STATUS_CREATE_OVER,       // create file is completed
        SEG_STATUS_BEFORE_CLOSE_OVER, // all before final close is completed
        SEG_STATUS_ALL_OVER;          // all is completed
    }

    private SegmentInfo segmentInfo;
    private SegmentStatus segmentStatus;
    private DsListWrapper dsListWrapper;
    private int primaryDsIndex;
    private long fileNumber;
    private int innerOffset;
    private byte[] data;
    private int dataOffset;
    private int dataLength;

    public SegmentData() {
        reset();
        segmentInfo = new SegmentInfo();
    }

    public SegmentData(SegmentInfo segmentInfo) {
        reset();
        this.segmentInfo = segmentInfo;
    }

    public void reset() {
        if (segmentInfo != null) {
            segmentInfo.reset();
        }
        segmentStatus = SegmentStatus.SEG_STATUS_NOT_INIT;
        dsListWrapper = null;
        primaryDsIndex = 0;
        fileNumber = 0;
        innerOffset = 0;
        data = null;
        dataOffset = 0;
        dataLength = 0;
    }

    public void readReset() {
        // for read retry, not modify SegmentStatus
        setPrimaryDsIndex(++primaryDsIndex);
    }

    public void writeReset() {
        segmentInfo.setBlockId(0);
        segmentInfo.setFileId(0);
        segmentInfo.setCrc(0);
        segmentStatus = SegmentStatus.SEG_STATUS_NOT_INIT;
        dsListWrapper = null;
        fileNumber = 0;
    }

    public SegmentInfo getSegmentInfo() {
        return segmentInfo;
    }

    public void setSegmentInfo(SegmentInfo segmentInfo) {
        this.segmentInfo = segmentInfo;
    }

    public SegmentStatus getSegmentStatus() {
        return segmentStatus;
    }

    public void setSegmentStatus(SegmentStatus segmentStatus) {
        this.segmentStatus = segmentStatus;
    }

    public int getBlockId() {
        return segmentInfo.getBlockId();
    }

    public void setBlockId(int blockId) {
        segmentInfo.setBlockId(blockId);
    }

    public long getFileId() {
        return segmentInfo.getFileId();
    }

    public void setFileId(long fileId) {
        segmentInfo.setFileId(fileId);
    }

    public int getLength() {
        return segmentInfo.getLength();
    }

    public void setLength(int length) {
        segmentInfo.setLength(length);
    }

    public long getOffset() {
        return segmentInfo.getOffset();
    }

    public void setOffset(long offset) {
        segmentInfo.setOffset(offset);
    }

    public int getCrc() {
        return segmentInfo.getCrc();
    }

    public void setCrc(int length) {
        segmentInfo.setCrc(length);
    }

    public long getFileNumber() {
        return fileNumber;
    }

    public void setFileNumber(long fileNumber) {
        this.fileNumber = fileNumber;
    }

    public int getInnerOffset() {
        return innerOffset;
    }

    public void setInnerOffset(int innerOffset) {
        this.innerOffset = innerOffset;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
        this.dataOffset = 0;
        this.dataLength = data.length;
    }

    public void setData(byte[] data, int dataOffset, int dataLength) {
        this.data = data;
        this.dataOffset = dataOffset;
        this.dataLength = dataLength;
    }

    public int getDataOffset() {
        return dataOffset;
    }

    public void setDataOffset(int dataOffset) {
        this.dataOffset = dataOffset;
    }

    public int getDataLength() {
        return dataLength;
    }

    public void setDataLength(int dataLength) {
        this.dataLength = dataLength;
    }

    public DsListWrapper getDsListWrapper() {
        return dsListWrapper;
    }

    public void setDsListWrapper(DsListWrapper dsListWrapper) {
        this.dsListWrapper = dsListWrapper;
    }

    public int getPrimaryDsIndex() {
        return primaryDsIndex;
    }

    public void setPrimaryDsIndex(int primaryDsIndex) {
        // ensure valid positive primaryDsIndex and invalid negtive
        // ds size == 0 ?
        this.primaryDsIndex = primaryDsIndex % dsListWrapper.getDsList().size();
    }

    public long getPrimaryDs() {
        return dsListWrapper.getDsList().get(primaryDsIndex);
    }

    public int getReadPrimaryDsIndex() {
        return (int)Math.abs(segmentInfo.getFileId()) % dsListWrapper.getDsList().size();
    }

    public void setReadPrimaryDsIndex() {
        primaryDsIndex = (int)Math.abs(segmentInfo.getFileId()) % dsListWrapper.getDsList().size();
    }

    public void putData(byte[] data) {
        ByteBuffer.wrap(this.data, this.dataOffset, this.dataLength).
            put(data);
    }

    public void putData(byte[] data, int dataOffset, int dataLength) {
        ByteBuffer.wrap(this.data, this.dataOffset, this.dataLength).
            put(data, dataOffset, dataLength);
    }

    public String toString() {
        return segmentInfo +
            " status: " + segmentStatus +
            " innerOffset: " + innerOffset +
            " fileNumber: " + fileNumber +
            " dataOffset: " + dataOffset +
            " dataLength: " + dataLength +
            " server: " + getDs();
    }

    private String getDs() {
        // primaryDsIndex < 0, server is the last retry one
        int index = primaryDsIndex < 0 ? getReadPrimaryDsIndex() : primaryDsIndex;
        index = index == 0 ? (dsListWrapper.getDsList().size() - 1) : (index - 1);
        return TfsUtil.longToHost(dsListWrapper.getDsList().get(index));
    }
}
