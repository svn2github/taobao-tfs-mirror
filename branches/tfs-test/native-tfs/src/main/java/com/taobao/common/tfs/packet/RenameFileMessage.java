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

public class RenameFileMessage extends BasePacket {

    private int blockId;
    private long fileId;
    private long newFileId;
    private int isServer;
    private int optionFlag;
    private DsListWrapper dsListWrapper;

    public RenameFileMessage(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TfsConstant.RENAME_FILE_MESSAGE;
    }

    @Override
    public int getPacketLength() {
        return TfsConstant.INT_SIZE * 3 + TfsConstant.LONG_SIZE * 2 + dsListWrapper.streamLength();
    }

    @Override
    public void writePacketStream() {
        byteBuffer.putInt(blockId);
        byteBuffer.putLong(fileId);
        byteBuffer.putLong(newFileId);
        byteBuffer.putInt(isServer);
        dsListWrapper.writeToStream(byteBuffer);
        byteBuffer.putInt(optionFlag);
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

    public long getNewFileId() {
        return newFileId;
    }

    public void setNewFileId(long newFileId) {
        this.newFileId = newFileId;
    }

    public int getIsServer() {
        return isServer;
    }

    public void setIsServer(int isServer) {
        this.isServer = isServer;
    }

    public int getOptionFlag() {
        return optionFlag;
    }

    public void setOptionFlag(int optionFlag) {
        this.optionFlag = optionFlag;
    }

    public DsListWrapper getDsListWrapper() {
        return dsListWrapper;
    }

    public void setDsListWrapper(DsListWrapper dsListWrapper) {
        this.dsListWrapper = dsListWrapper;
    }


}
