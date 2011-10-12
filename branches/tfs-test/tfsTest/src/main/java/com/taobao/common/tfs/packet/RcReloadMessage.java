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

public class RcReloadMessage extends BasePacket {
    // reload type
    private static final int RC_RELOAD_CONFIG = 1;
    private static final int RC_RELOAD_RESOURCE = 2;

    private int reloadType = RC_RELOAD_RESOURCE;

    public RcReloadMessage(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TfsConstant.REQ_RC_RELOAD_MESSAGE;
    }   

    @Override
    public int getPacketLength() {
        return TfsConstant.INT_SIZE;
    }

    @Override
    public void writePacketStream() {
        byteBuffer.putInt(reloadType);
    }

    public int getReloadType() {
        return reloadType;
    }

    public void setReloadType(int reloadType) {
        this.reloadType = reloadType;
    }
}
