/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.packet;

import java.nio.ByteBuffer;

import com.taobao.common.tfs.common.TfsConstant;

public class WriteDataInfo implements TfsPacketObject {
    private int blockId;
    private long fileId;
    private int offset;
    private int length;
    private int isServer;
    private long fileNumber;

    public WriteDataInfo() {

    }

    public WriteDataInfo(int blockId, long fileId, int offset, int length,
                         int isServer, long fileNumber) {
        super();
        this.blockId = blockId;
        this.fileId = fileId;
        this.offset = offset;
        this.length = length;
        this.isServer = isServer;
        this.fileNumber = fileNumber;
    }
    public int getBlockId() {
        return blockId;
    }
    public void setBlockId(int blockId) {
        this.blockId = blockId;
    }
    public long getFileId() {
        return fileId;
    }
    public void setFileId(long fileId) {
        this.fileId = fileId;
    }
    public int getOffset() {
        return offset;
    }
    public void setOffset(int offset) {
        this.offset = offset;
    }
    public int getLength() {
        return length;
    }
    public void setLength(int length) {
        this.length = length;
    }
    public int getIsServer() {
        return isServer;
    }
    public void setIsServer(int isServer) {
        this.isServer = isServer;
    }
    public long getFileNumber() {
        return fileNumber;
    }
    public void setFileNumber(long fileNumber) {
        this.fileNumber = fileNumber;
    }

    public void writeToStream(ByteBuffer byteBuffer) {
        byteBuffer.putInt(blockId);
        byteBuffer.putLong(fileId);
        byteBuffer.putInt(offset);
        byteBuffer.putInt(length);
        byteBuffer.putInt(isServer);
        byteBuffer.putLong(fileNumber);
    }

    public void readFromStream(ByteBuffer byteBuffer) {
        this.blockId = byteBuffer.getInt();
        this.fileId = byteBuffer.getLong();
        this.offset = byteBuffer.getInt();
        this.length = byteBuffer.getInt();
        this.isServer = byteBuffer.getInt();
        this.fileNumber = byteBuffer.getLong();
    }

    public int streamLength() {
        return TfsConstant.INT_SIZE * 4 + TfsConstant.LONG_SIZE * 2;
    }


}
