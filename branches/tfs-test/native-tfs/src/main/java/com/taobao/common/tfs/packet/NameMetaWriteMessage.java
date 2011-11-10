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
import com.taobao.common.tfs.namemeta.NameMetaUserInfo;
import com.taobao.common.tfs.namemeta.FragInfo;

public class NameMetaWriteMessage extends BasePacket {
    private NameMetaUserInfo userInfo;
    private String filePath;
    private FragInfo fragInfo;
    private long version;
    
    public NameMetaWriteMessage(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TfsConstant.NAME_META_WRITE_MESSAGE;
    }

    @Override
    public int getPacketLength() {
        return NameMetaUserInfo.length() +
            StreamTranscoderUtil.getStringLength(filePath) +
            TfsConstant.LONG_SIZE + 
            fragInfo.length();
    }

    @Override
    public void writePacketStream() {
        userInfo.serialize(byteBuffer);
        StreamTranscoderUtil.putString(byteBuffer, filePath);
        byteBuffer.putLong(version);
        fragInfo.serialize(byteBuffer);
    }

    public void setUserInfo(NameMetaUserInfo userInfo) {
        this.userInfo = userInfo;
    }

    public NameMetaUserInfo getUserInfo() {
        return userInfo;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setVersion(long version) {
        this.version = version;
    }
    
    public long getVersion() {
        return version;
    }
    
    public void setFragInfo(FragInfo fragInfo) {
        this.fragInfo = fragInfo;
    }

    public FragInfo getFragInfo() {
        return fragInfo;
    }

}