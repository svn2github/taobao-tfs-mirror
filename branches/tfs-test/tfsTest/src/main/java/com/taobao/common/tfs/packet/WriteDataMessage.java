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
import com.taobao.common.tfs.common.StreamTranscoderUtil;

public class WriteDataMessage extends BasePacket {

    private WriteDataInfo writeDataInfo ;
    private DsListWrapper dsListWrapper ;
    byte[] writeData;
    int sourceOffset;

    public WriteDataMessage(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TfsConstant.WRITE_DATA_MESSAGE;
    }

    public WriteDataInfo getWriteDataInfo() {
        return writeDataInfo;
    }

    public void setWriteDataInfo(WriteDataInfo writeDataInfo) {
        this.writeDataInfo = writeDataInfo;
    }

    public DsListWrapper getDsListWrapper() {
        return dsListWrapper;
    }

    public void setDsListWrapper(DsListWrapper dsListWrapper) {
        this.dsListWrapper = dsListWrapper;
    }

    public byte[] getWriteData() {
        return writeData;
    }

    public void setWriteData(byte[] writeData) {
        this.writeData = writeData;
    }

    public int getSourceOffset() {
        return sourceOffset;
    }

    public void setSourceOffset(int sourceOffset) {
        this.sourceOffset = sourceOffset;
    }

    @Override
    public int getPacketLength() {
        if (writeDataInfo == null || dsListWrapper == null || writeData == null)
            return 0;
        if (writeDataInfo.getLength() + sourceOffset > writeData.length)
            return 0;
        int length = writeDataInfo.streamLength() +  dsListWrapper.streamLength() ;
        if (writeDataInfo.getLength() > 0) {
            length += writeDataInfo.getLength();
        }
        return length;
    }

    @Override
    public void writePacketStream() {
        writeDataInfo.writeToStream(byteBuffer);
        dsListWrapper.writeToStream(byteBuffer);
        if (writeDataInfo.getLength() > 0) {
            StreamTranscoderUtil.putByteArray(byteBuffer, writeData, sourceOffset, writeDataInfo.getLength());
        }
    }


}
