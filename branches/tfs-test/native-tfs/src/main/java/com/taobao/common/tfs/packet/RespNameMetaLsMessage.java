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
import com.taobao.common.tfs.namemeta.FileMetaInfo;

public class RespNameMetaLsMessage extends BasePacket {
    private boolean hasNext;
    private List<FileMetaInfo> metaInfoList;

    public RespNameMetaLsMessage(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TfsConstant.RESP_NAME_META_LS_MESSAGE;
    }

    @Override
    public boolean decode() {
        hasNext = (int)byteBuffer.get() != 0;
        metaInfoList = new ArrayList<FileMetaInfo>();
        int size = byteBuffer.getInt();
        for (int i = 0; i < size; i++) {
            FileMetaInfo metaInfo = new FileMetaInfo();
            metaInfo.deserialize(byteBuffer);
            metaInfoList.add(metaInfo);
        }
        return true;
    }

    public boolean hasNext() {
        return hasNext;
    }

    public void setMetaInfoList(List<FileMetaInfo> metaInfoList) {
        this.metaInfoList = metaInfoList;
    }

    public List<FileMetaInfo> getMetaInfoList() {
        return metaInfoList;
    }

}