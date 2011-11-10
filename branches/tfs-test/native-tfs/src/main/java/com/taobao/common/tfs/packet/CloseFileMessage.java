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

public class CloseFileMessage extends BasePacket {
    private CloseFileInfo closeFileInfo;
    private DsListWrapper dsListWrapper;
    private int optionFlag;

    public CloseFileMessage(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TfsConstant.CLOSE_FILE_MESSAGE;
        this.optionFlag = 0;
    }   

    @Override
    public int getPacketLength() {
        if (closeFileInfo == null || dsListWrapper == null)
            return 0;
        return closeFileInfo.streamLength() + dsListWrapper.streamLength()
            + TfsConstant.INT_SIZE * 3;

    }

    @Override
    public void writePacketStream() {
        closeFileInfo.writeToStream(byteBuffer);
        dsListWrapper.writeToStream(byteBuffer);
        byteBuffer.putInt(0);   // block info size
        byteBuffer.putInt(0);   // file info size
        byteBuffer.putInt(optionFlag);
    }

    public CloseFileInfo getCloseFileInfo() {
        return closeFileInfo;
    }

    public void setCloseFileInfo(CloseFileInfo closeFileInfo) {
        this.closeFileInfo = closeFileInfo;
    }

    public DsListWrapper getDsListWrapper() {
        return dsListWrapper;
    }

    public void setDsListWrapper(DsListWrapper dsListWrapper) {
        this.dsListWrapper = dsListWrapper;
    }

    public int getOptionFlag() {
        return optionFlag;
    }

    public void setOptionFlag(int optionFlag) {
        this.optionFlag = optionFlag;
    }


}
