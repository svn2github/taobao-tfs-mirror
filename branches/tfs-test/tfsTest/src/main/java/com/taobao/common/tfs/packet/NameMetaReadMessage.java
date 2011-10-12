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
import com.taobao.common.tfs.namemeta.NameMetaUserInfo;

public class NameMetaReadMessage extends BasePacket {
    private NameMetaUserInfo userInfo;
    private String filePath;
    private long version;
    private long offset;
    private long length;

    public NameMetaReadMessage(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TfsConstant.NAME_META_READ_MESSAGE;
    }

    @Override
    public int getPacketLength() {
        return NameMetaUserInfo.length() +
            StreamTranscoderUtil.getStringLength(filePath) +
            TfsConstant.LONG_SIZE * 3;
    }

    @Override
    public void writePacketStream() {
        userInfo.serialize(byteBuffer);
        StreamTranscoderUtil.putString(byteBuffer, filePath);
        byteBuffer.putLong(version);
        byteBuffer.putLong(offset);
        byteBuffer.putLong(length);
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
    
    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getOffset() {
        return offset;
    }

    public void setLength(long length) {
        this.length = length;
    }

    public long getLength() {
        return length;
    }

}
