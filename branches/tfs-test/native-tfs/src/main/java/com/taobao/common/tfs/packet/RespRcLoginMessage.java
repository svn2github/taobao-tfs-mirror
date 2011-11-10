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

import com.taobao.common.tfs.rc.RcBaseInfo;

public class RespRcLoginMessage extends BasePacket {
    private String sessionId;
    private RcBaseInfo baseInfo;

    public RespRcLoginMessage(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TfsConstant.RESP_RC_LOGIN_MESSAGE;
    }

    @Override
    public boolean decode() {
        super.decode();
        sessionId = StreamTranscoderUtil.getString(byteBuffer);
        baseInfo = new RcBaseInfo();
        return baseInfo.decode(byteBuffer);
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public RcBaseInfo getBaseInfo() {
        return baseInfo;
    }

    public void setBaseInfo(RcBaseInfo baseInfo) {
        this.baseInfo = baseInfo;        
    }
}
