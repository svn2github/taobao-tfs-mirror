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

public class RespReadDataMessageV2 extends RespReadDataMessage {

    private FileInfo fileInfo;

    public RespReadDataMessageV2(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TfsConstant.RESP_READ_DATA_MESSAGE_V2;
    }

    public FileInfo getFileInfo() {
        return fileInfo;
    }

    public void setFileInfo(FileInfo fileInfo) {
        this.fileInfo = fileInfo;
    }

    public boolean decode() {
        super.decode();
        int size = byteBuffer.getInt();
        if (size > 0) {
            fileInfo = new FileInfo();
            fileInfo.readFromStream(byteBuffer);
        }
        return true;
    }
}
