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

public class UnlinkFileMessage extends BasePacket {

    public static final int DELETE = 0x0;
    public static final int UNDELETE = 0x2;
    public static final int CONCEAL = 0x4;
    public static final int REVEAL = 0x6;

    private int blockId;
    private long fileId;
    private int isServer;

    private int optionFlag;
    private DsListWrapper dsListWrapper;

    public UnlinkFileMessage(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TfsConstant.UNLINK_FILE_MESSAGE;
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

    public void setDel() { this.isServer |= DELETE;}
    public void setUndel() { this.isServer |= UNDELETE; }
    public void setConceal() { this.isServer |= CONCEAL; }
    public void setReveal() { this.isServer |= REVEAL; }
    public void setUnlinkType(int action) { this.isServer |= action; }

    @Override
    public int getPacketLength() {
        return (Integer.SIZE/8) * 3 + (Long.SIZE/8) + dsListWrapper.streamLength();
    }

    @Override
    public void writePacketStream() {
        byteBuffer.putInt(blockId);
        byteBuffer.putLong(fileId);
        byteBuffer.putInt(isServer);
        this.dsListWrapper.writeToStream(byteBuffer);
        byteBuffer.putInt(optionFlag);
    }


}
