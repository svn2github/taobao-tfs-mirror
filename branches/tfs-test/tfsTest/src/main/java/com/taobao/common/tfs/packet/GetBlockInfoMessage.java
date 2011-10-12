/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.packet;

import java.util.List;
import java.util.ArrayList;

import com.taobao.common.tfs.common.TfsConstant;
import com.taobao.common.tfs.common.StreamTranscoderUtil;

public class GetBlockInfoMessage extends BasePacket {
    private int blockId;
    private int mode;
    private List<Long> failServer;

    public GetBlockInfoMessage(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TfsConstant.GET_BLOCK_INFO_MESSAGE;
        blockId = 0;
        mode = 0;
        failServer = new ArrayList<Long>();
    }

    /*
     * length = sizeof (int) * 3 + failServer.size() * 8
     * <table>
     * <tr>
     * <td>[int=4] mode
     * <td>[int=4] blockId
     * <td>[int=4] failServer.size()
     * <td>[long=8] element of failServer
     * </tr>
     * </table>
     * @see com.taobao.common.tfs.tbnet.BasePacket#getPacketLength()
     */
    public int getPacketLength() {
        return TfsConstant.INT_SIZE * 3 + failServer.size() * TfsConstant.LONG_SIZE;
    }

    @Override
    public void writePacketStream() {
        byteBuffer.putInt(mode);
        byteBuffer.putInt(blockId);
        StreamTranscoderUtil.putVL(byteBuffer, failServer);
    }

    public int getBlockId() {
        return blockId;
    }

    public void setBlockId(int blockId) {
        this.blockId = blockId;
    }

    public int getMode() {
        return mode;
    }

    public void setMode(int mode) {
        this.mode = mode;
    }

    public List<Long> getFailServer() {
        return failServer;
    }

    public void setFailServer(List<Long> failServer) {
        this.failServer = failServer;
    }


}
