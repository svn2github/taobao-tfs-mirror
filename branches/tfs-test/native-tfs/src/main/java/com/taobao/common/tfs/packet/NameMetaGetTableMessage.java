/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.packet;

import com.taobao.common.tfs.common.StreamTranscoderUtil;
import com.taobao.common.tfs.common.TfsConstant;
import com.taobao.common.tfs.namemeta.NameMetaUserInfo;

public class NameMetaGetTableMessage extends BasePacket {

    private byte reserve;
    public NameMetaGetTableMessage(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TfsConstant.NAME_META_GET_TABLE_MESSAGE;
        reserve = 0;
    }
    
    @Override
    public int getPacketLength() {
        return TfsConstant.BYTE_SIZE;
    }  
    
    @Override
    public void writePacketStream() {
        byteBuffer.put(reserve);
    }
}
