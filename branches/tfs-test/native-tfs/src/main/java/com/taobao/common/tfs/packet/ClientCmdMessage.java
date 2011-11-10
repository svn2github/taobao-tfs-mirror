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

public class ClientCmdMessage extends BasePacket {

    private int type;
    private long serverId;
    private int blockId;
    private int version;
    private long fromServerId;

    public ClientCmdMessage(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TfsConstant.CLIENT_CMD_MESSAGE;
    }

    @Override
    public int getPacketLength() {
        return TfsConstant.INT_SIZE * 3 + TfsConstant.LONG_SIZE * 2;
    }

    @Override
    public void writePacketStream() {
        byteBuffer.putInt(type);
        byteBuffer.putLong(serverId);
        byteBuffer.putInt(blockId);
        byteBuffer.putInt(version);
        byteBuffer.putLong(fromServerId);
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public long getServerId() {
        return serverId;
    }

    public void setServerId(long serverId) {
        this.serverId = serverId;
    }

    public int getBlockId() {
        return blockId;
    }

    public void setBlockId(int blockId) {
        this.blockId = blockId;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public long getFromServerId() {
        return fromServerId;
    }

    public void setFromServerId(long fromServerId) {
        this.fromServerId = fromServerId;
    }


}
