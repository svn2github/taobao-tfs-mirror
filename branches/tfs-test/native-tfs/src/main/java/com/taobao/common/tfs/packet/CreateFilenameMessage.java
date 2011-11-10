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

public class CreateFilenameMessage extends BasePacket {

    private int blockId;
    private long fileId;

    public CreateFilenameMessage(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TfsConstant.CREATE_FILENAME_MESSAGE;
    }

    public CreateFilenameMessage(Transcoder transcoder, int blockId, long fileId) {
        super(transcoder);
        this.pcode = TfsConstant.CREATE_FILENAME_MESSAGE;
        this.blockId = blockId;
        this.fileId = fileId;
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

    public void setFileId(int fileId) {
        this.fileId = fileId;
    }

    @Override
    public int getPacketLength() {
        return TfsConstant.INT_SIZE  + TfsConstant.LONG_SIZE;
    }

    @Override
    public void writePacketStream() {
        byteBuffer.putInt(blockId);
        byteBuffer.putLong(fileId);
    }


}
