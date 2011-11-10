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

public class RcLoginMessage extends BasePacket {

    private String appKey;
    private long appIp;

    public RcLoginMessage(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TfsConstant.REQ_RC_LOGIN_MESSAGE;
    }

    @Override
    public int getPacketLength() {
        return TfsConstant.LONG_SIZE + StreamTranscoderUtil.getStringLength(appKey);
    }

    @Override
    public void writePacketStream() {
        StreamTranscoderUtil.putString(byteBuffer, appKey);
        byteBuffer.putLong(appIp);
    }

    public String getAppKey() {
        return appKey;        
    }

    public void setAppKey(String appKey) {
        this.appKey = appKey;
    }

    public long getAppIp() {
        return appIp;        
    }

    public void setAppIp(long appIp) {
        this.appIp = appIp;
    }

}
