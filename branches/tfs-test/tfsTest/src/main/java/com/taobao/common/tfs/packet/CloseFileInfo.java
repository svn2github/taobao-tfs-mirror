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

public class CloseFileInfo implements TfsPacketObject {
    private int blockId;
    private long fileId;
    private int mode;
    private int crc;
    private long fileNumber;

    public CloseFileInfo() {

    }

    public CloseFileInfo(int blockId, long fileId, int mode, int crc,
                         long fileNumber) {
        super();
        this.blockId = blockId;
        this.fileId = fileId;
        this.mode = mode;
        this.crc = crc;
        this.fileNumber = fileNumber;
    }

    public void writeToStream(ByteBuffer byteBuffer) {
        byteBuffer.putInt(blockId);
        byteBuffer.putLong(fileId);
        byteBuffer.putInt(mode);
        byteBuffer.putInt(crc);
        byteBuffer.putLong(fileNumber);
    }



    public void readFromStream(ByteBuffer byteBuffer) {
        this.blockId = byteBuffer.getInt();
        this.fileId = byteBuffer.getLong();
        this.mode = byteBuffer.getInt();
        this.crc = byteBuffer.getInt();
        this.fileNumber = byteBuffer.getLong();
    }

    /**
     * two of long, three of integer.
     */
    public int streamLength() {
        return TfsConstant.INT_SIZE * 3 + TfsConstant.LONG_SIZE * 2;
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

    public int getMode() {
        return mode;
    }

    public void setMode(int mode) {
        this.mode = mode;
    }

    public int getCrc() {
        return crc;
    }

    public void setCrc(int crc) {
        this.crc = crc;
    }

    public long getFileNumber() {
        return fileNumber;
    }

    public void setFileNumber(long fileNumber) {
        this.fileNumber = fileNumber;
    }

}
