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

public class RespFileInfoMessage extends BasePacket {
    private FileInfo fileInfo;

    public RespFileInfoMessage(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TfsConstant.RESP_FILE_INFO_MESSAGE;
    }

    @Override
    public boolean decode() {
        int size = byteBuffer.getInt();
        fileInfo = new FileInfo();
        if (size > 0) fileInfo.readFromStream(byteBuffer);
        return true;
    }

    public FileInfo getFileInfo() {
        return fileInfo;
    }

    public void setFileInfo(FileInfo fileInfo) {
        this.fileInfo = fileInfo;
    }



}
