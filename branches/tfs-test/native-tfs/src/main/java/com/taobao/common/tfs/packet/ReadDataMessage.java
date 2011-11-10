/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.packet;

import com.taobao.common.tfs.common.TfsConstant;

public class ReadDataMessage extends BasePacket {

    private int blockId;
    private long fileId;
    private int offset;
    private int length;
    private byte flag;

    public ReadDataMessage(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TfsConstant.READ_DATA_MESSAGE;
    }

    @Override
    public int getPacketLength() {
        return TfsConstant.INT_SIZE * 3 + TfsConstant.LONG_SIZE ;
    }

    @Override
    public void writePacketStream() {
        byteBuffer.putInt(blockId);
        byteBuffer.putLong(fileId);
        byteBuffer.putInt(offset);
        byteBuffer.putInt(length);
        byteBuffer.put(flag);
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

    public byte getFlag() {
        return flag;
    }

    public void setFlag(byte flag) {
        this.flag = flag;
    }
}









