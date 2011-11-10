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
import com.taobao.common.tfs.rc.RcKeepAliveInfo;

public class RcKeepAliveMessage extends BasePacket {
    private RcKeepAliveInfo kaInfo;

    public RcKeepAliveMessage(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TfsConstant.REQ_RC_KEEPALIVE_MESSAGE;
    }   

    @Override
    public int getPacketLength() {
        return kaInfo.length();
    }

    @Override
    public void writePacketStream() {
        kaInfo.encode(byteBuffer);
    }

    public RcKeepAliveInfo getKaInfo() {
        return kaInfo;        
    }

    public void setKaInfo(RcKeepAliveInfo kaInfo) {
        this.kaInfo = kaInfo;
    }
}
