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
import com.taobao.common.tfs.rc.RcBaseInfo;

public class RespRcKeepAliveMessage extends BasePacket {
    private RcBaseInfo baseInfo;
    private boolean isUpdate = false;

    public RespRcKeepAliveMessage(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TfsConstant.RESP_RC_KEEPALIVE_MESSAGE;
    }   

    @Override
    public boolean decode() {
        isUpdate = (int)byteBuffer.get() != 0 ? true : false;
        if (isUpdate) {
            baseInfo = new RcBaseInfo();                
            return baseInfo.decode(byteBuffer);
        }
        return true;
    }

    public RcBaseInfo getBaseInfo() {
        return baseInfo;
    }

    public void setBaseInfo(RcBaseInfo baseInfo) {
        this.baseInfo = baseInfo;        
    }

    public boolean getIsUpdate() {
        return isUpdate;
    }

    public void setIsUpdate(boolean isUpdate) {
        this.isUpdate = isUpdate;
    }
}

