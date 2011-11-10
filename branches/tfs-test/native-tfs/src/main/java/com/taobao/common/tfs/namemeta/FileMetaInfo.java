/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.namemeta;

import java.nio.ByteBuffer;

import com.taobao.common.tfs.common.TfsConstant; 
import com.taobao.common.tfs.common.TfsUtil;
import com.taobao.common.tfs.common.StreamTranscoderUtil;

public class FileMetaInfo {
    private String fileName;
    private byte fileType;
    private long pid;
    private long id;
    private int createTime;
    private int modifyTime;
    private long length;
    private short version;

    public void setFileName(String fileName) {
        this.fileName = fileName;        
    }

    public String getFileName() {
        return fileName;
    }

    public boolean isFile() {
        return (pid & (((long)1) << 63)) != 0;
    }

    public long getPid() {
        return pid;        
    }

    public void setPid(long pid) {
        this.pid = pid;
    }

    public long getId() {
        return id;      
    }

    public void setId(long id) {
        this.id = id;
    }

    public void setModifyTime(int modifyTime) {
        this.modifyTime = modifyTime;
    }

    public int getModifyTime() {
        return modifyTime;
    }

    public void setCreateTime(int createTime) {
        this.createTime = createTime;
    }

    public int getCreateTime() {
        return createTime;
    }

    public void setLength(long length) {
        this.length = length;
    }

    public long getLength() {
        return length;
    }

    public void setVersion(short version) {
        this.version = version;
    }

    public short getVersion() {
        return version;
    }

    public int length() {
        return TfsConstant.LONG_SIZE * 3 + TfsConstant.INT_SIZE * 2 +
            TfsConstant.SHORT_SIZE  +
            StreamTranscoderUtil.getStringLength(fileName);
    }

    public boolean deserialize(ByteBuffer byteBuffer) {
        fileName = StreamTranscoderUtil.getString(byteBuffer);
        pid = byteBuffer.getLong();
        id = byteBuffer.getLong();
        createTime = byteBuffer.getInt();
        modifyTime = byteBuffer.getInt();
        length = byteBuffer.getLong();
        version = byteBuffer.getShort();

        return true;
    }

    public String toString() {
        return "fileName: " + fileName + " fileType: " + (isFile() ? "file" : "dir")  +
            " pid: " + pid + " id: " + id + 
            " createTime: " + TfsUtil.timeToString(createTime) +
            " modifyTime: " + TfsUtil.timeToString(modifyTime) +
            " length: " + length + " version: " + version;
    }
}