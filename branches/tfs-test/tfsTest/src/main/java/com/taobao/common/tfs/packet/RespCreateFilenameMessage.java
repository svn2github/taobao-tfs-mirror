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

public class RespCreateFilenameMessage extends BasePacket {
    private int blockId;
    private long fileId;
    private long fileNumber;

    public RespCreateFilenameMessage(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TfsConstant.RESP_CREATE_FILENAME_MESSAGE;
    }

    @Override
    public boolean decode() {
        blockId = byteBuffer.getInt();
        fileId = byteBuffer.getLong();
        fileNumber = byteBuffer.getLong();
        return true;
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

    public long getFileNumber() {
        return fileNumber;
    }

    public void setFileNumber(long fileNumber) {
        this.fileNumber = fileNumber;
    }


}
