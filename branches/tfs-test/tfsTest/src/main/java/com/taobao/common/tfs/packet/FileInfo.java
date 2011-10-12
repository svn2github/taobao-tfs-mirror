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
import com.taobao.common.tfs.common.TfsUtil;

public class FileInfo implements TfsPacketObject {
    private long id;            // file id
    private int offset;         // file offset in block
    private long length;          // file length. support largefile length. use LONG
    private long occupyLength;    // file occupied length in block, support largefile length, use LONG
    private int modifyTime;     // modifiy time
    private int createTime;     // creat time
    private int flag;           // status flag
    private int crc;            // crc checksum

    public void readFromStream(ByteBuffer byteBuffer) {
        this.id = byteBuffer.getLong();
        this.offset = byteBuffer.getInt();
        this.length = byteBuffer.getInt();
        this.occupyLength = byteBuffer.getInt();
        this.modifyTime = byteBuffer.getInt();
        this.createTime = byteBuffer.getInt();
        this.flag = byteBuffer.getInt();
        this.crc = byteBuffer.getInt();
    }

    public int streamLength() {
        return TfsConstant.LONG_SIZE + TfsConstant.INT_SIZE * 7;
    }

    public void writeToStream(ByteBuffer byteBuffer) {
        byteBuffer.putLong(id);
        byteBuffer.putInt(offset);
        byteBuffer.putInt((int)length);
        byteBuffer.putInt((int)occupyLength);
        byteBuffer.putInt(modifyTime);
        byteBuffer.putInt(createTime);
        byteBuffer.putInt(flag);
        byteBuffer.putInt(crc);
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public long getLength() {
        return length;
    }

    public void setLength(long length) {
        this.length = length;
    }

    public long getOccupyLength() {
        return occupyLength;
    }

    public void setOccupyLength(long occupyLength) {
        this.occupyLength = occupyLength;
    }

    public int getModifyTime() {
        return modifyTime;
    }

    public void setModifyTime(int modifyTime) {
        this.modifyTime = modifyTime;
    }

    public int getCreateTime() {
        return createTime;
    }

    public void setCreateTime(int createTime) {
        this.createTime = createTime;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public int getCrc() {
        return crc;
    }

    public void setCrc(int crc) {
        this.crc = crc;
    }

    public String toString() {
        return "\nfileid: " + id + "\noffset: " + offset + "\nlength: " + length +
            "\noccupyLength: " + occupyLength + "\nmodifyTime: " + TfsUtil.timeToString(modifyTime) +
            "\ncreateTime: " + TfsUtil.timeToString(createTime) + "\nstatus: " + flag +
            "\ncrc: " + crc;
    }

}
