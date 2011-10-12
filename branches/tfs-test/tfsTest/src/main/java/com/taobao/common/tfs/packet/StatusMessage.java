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

public class StatusMessage extends BasePacket {
    public static final int STATUS_MESSAGE_OK = TfsConstant.TFS_SUCCESS;
    public static final int STATUS_MESSAGE_ERROR = TfsConstant.TFS_ERROR;
    public static final int STATUS_MESSAGE_PING = 3;

    protected int status ;
    protected String error;

    public StatusMessage(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TfsConstant.STATUS_MESSAGE;
    }

    /**
     * length = sizeof(int) + sizeof(error.length) + error.length
     */
    public int getPacketLength() {
        int length = Integer.SIZE / 8;
        if (error != null) length += error.length();
        return length;
    }

    @Override
    public void writePacketStream() {
        byteBuffer.putInt(status);
        StreamTranscoderUtil.putString(byteBuffer, error);
    }

    @Override
    public boolean decode() {
        status = byteBuffer.getInt();
        error = StreamTranscoderUtil.getString(byteBuffer);
        return true;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }


}
