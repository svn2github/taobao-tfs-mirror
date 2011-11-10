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
import com.taobao.common.tfs.namemeta.FragInfo;

public class RespNameMetaReadMessage extends BasePacket {
    private boolean hasNext;
    private FragInfo fragInfo;

    public RespNameMetaReadMessage(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TfsConstant.RESP_NAME_META_READ_MESSAGE;
    }

    @Override
    public boolean decode() {
        hasNext =(int)byteBuffer.get() != 0;
        fragInfo = new FragInfo();
        return fragInfo.deserialize(byteBuffer);
    }

    public void setHasNext(boolean hasNext) {
        this.hasNext = hasNext;
    }

    public boolean hasNext() {
        return hasNext;
    }

    public void setFragInfo(FragInfo fragInfo) {
        this.fragInfo = fragInfo;
    }

    public FragInfo getFragInfo() {
        return fragInfo;
    }
}