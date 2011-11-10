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

public class FileInfoMessage extends BasePacket {

    private int blockId;
    private long fileId;
    private int mode;

    public FileInfoMessage(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TfsConstant.FILE_INFO_MESSAGE;
    }

    @Override
    public int getPacketLength() {
        return TfsConstant.INT_SIZE * 2 + TfsConstant.LONG_SIZE;
    }

    @Override
    public void writePacketStream() {
        byteBuffer.putInt(blockId);
        byteBuffer.putLong(fileId);
        byteBuffer.putInt(mode);
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


}
