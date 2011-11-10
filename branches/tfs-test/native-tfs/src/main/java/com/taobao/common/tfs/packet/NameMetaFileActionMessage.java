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

public class NameMetaFileActionMessage extends BasePacket {
    private NameMetaUserInfo userInfo;
    private long version;
    private byte type;
    private String srcFilePath;
    private String destFilePath;

    public NameMetaFileActionMessage(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TfsConstant.NAME_META_FILE_ACTION_MESSAGE;
    }

    @Override
    public int getPacketLength() {
        return NameMetaUserInfo.length() + TfsConstant.BYTE_SIZE +
            TfsConstant.LONG_SIZE +
            StreamTranscoderUtil.getStringLength(srcFilePath) +
            StreamTranscoderUtil.getStringLength(destFilePath);
    }

    @Override
    public void writePacketStream() {
        userInfo.serialize(byteBuffer);
        StreamTranscoderUtil.putString(byteBuffer, srcFilePath);
        byteBuffer.putLong(version);
        // destFilePath is not MUST
        StreamTranscoderUtil.putString(byteBuffer, destFilePath);
        byteBuffer.put(type);
    }

    public void setUserInfo(NameMetaUserInfo userInfo) {
        this.userInfo = userInfo;
    }

    public NameMetaUserInfo getUserInfo() {
        return userInfo;
    }

    public void setType(byte type) {
        this.type = type;
    }

    public byte getType() {
        return type;
    }

    public void setSrcFilePath(String srcFilePath) {
        this.srcFilePath = srcFilePath;
    }

    public String getSrcFilePath() {
        return srcFilePath;
    }

    public void setVersion(long version) {
        this.version = version;
    }
    
    public long getVersion() {
        return version;
    }
    
    public void setDestFilePath(String destFilePath) {
        this.destFilePath = destFilePath;
    }

    public String getDestFilePath() {
        return destFilePath;
    }
}

