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

public class SetBlockInfoMessage extends BasePacket {

    private int blockId;
    private DsListWrapper dsListWrapper;

    public SetBlockInfoMessage(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TfsConstant.SET_BLOCK_INFO_MESSAGE;
        blockId = 0;
        dsListWrapper  = new DsListWrapper();
    }

    public int getBlockId() {
        return blockId;
    }

    public void setBlockId(int blockId) {
        this.blockId = blockId;
    }

    @Override
    public boolean decode() {
        blockId = byteBuffer.getInt();
        dsListWrapper.readFromStream(byteBuffer);
        return true;
    }

    public DsListWrapper getDsListWrapper() {
        return dsListWrapper;
    }

    public void setDsListWrapper(DsListWrapper dsListWrapper) {
        this.dsListWrapper = dsListWrapper;
    }


}
