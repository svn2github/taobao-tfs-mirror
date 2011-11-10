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

public class NameMetaLsMessage extends BasePacket {
    private NameMetaUserInfo userInfo;
    private long pid;
    private String filePath;
    private byte fileType;
    private long version;

    public NameMetaLsMessage(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TfsConstant.NAME_META_LS_MESSAGE;
    }

    @Override
    public int getPacketLength() {
        return NameMetaUserInfo.length() +
            TfsConstant.LONG_SIZE*2 + TfsConstant.BYTE_SIZE +
            StreamTranscoderUtil.getStringLength(filePath);
    }

    @Override
    public void writePacketStream() {
        userInfo.serialize(byteBuffer);
        byteBuffer.putLong(pid);
        StreamTranscoderUtil.putString(byteBuffer, filePath);
        byteBuffer.put(fileType);
        byteBuffer.putLong(version);
    }

    public void setUserInfo(NameMetaUserInfo userInfo) {
        this.userInfo = userInfo;
    }

    public NameMetaUserInfo getUserInfo() {
        return userInfo;
    }

    public long getPid() {
        return pid;
    }

    public void setPid(long pid) {
        this.pid = pid;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public void setVersion(long version) {
        this.version = version;
    }
    
    public long getVersion() {
        return version;
    }
    
    public byte getFileType() {
        return fileType;
    }

    public void setFileType(byte fileType) {
        this.fileType = fileType;
    }

}
