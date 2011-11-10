/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.namemeta;

import java.nio.ByteBuffer;

import com.taobao.common.tfs.common.TfsConstant;

public class FragMeta {
    private int blockId;
    private long fileId;
    private long offset;
    private int length;

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

    public static int length() {
        return TfsConstant.INT_SIZE * 2 + TfsConstant.LONG_SIZE * 2;
    }

    public boolean serialize(ByteBuffer byteBuffer) {
        byteBuffer.putInt(blockId);
        byteBuffer.putLong(fileId);
        byteBuffer.putLong(offset);
        byteBuffer.putInt(length);
        return true;
    }

    public boolean deserialize(ByteBuffer byteBuffer) {
        blockId = byteBuffer.getInt();
        fileId = byteBuffer.getLong();
        offset = byteBuffer.getLong();
        length = byteBuffer.getInt();
        return true;
    }

    public String toString() {
        return "blockId: " + blockId + " fileId: " + fileId + " offset: " + offset + " length: " + length;
    }
}
