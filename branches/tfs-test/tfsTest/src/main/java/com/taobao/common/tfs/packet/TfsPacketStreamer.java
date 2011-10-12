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

public class TfsPacketStreamer extends AbstractPacketStreamer {

    private Transcoder transcoder = null;

    public TfsPacketStreamer(Transcoder transcoder) {
        this.transcoder = transcoder;
    }

    @Override
    public BasePacket createPacket(int pcode) {
        BasePacket packet = null;

        switch (pcode) {
        case TfsConstant.STATUS_MESSAGE:
            packet = new StatusMessage(transcoder);
            break;
        case TfsConstant.SET_BLOCK_INFO_MESSAGE:
            packet = new SetBlockInfoMessage(transcoder);
            break;
        case TfsConstant.BATCH_SET_BLOCK_INFO_MESSAGE:
            packet = new BatchSetBlockInfoMessage(transcoder);
            break;
        case TfsConstant.RESP_CREATE_FILENAME_MESSAGE:
            packet = new RespCreateFilenameMessage(transcoder);
            break;
        case TfsConstant.RESP_FILE_INFO_MESSAGE:
            packet = new RespFileInfoMessage(transcoder);
            break;
        case TfsConstant.RESP_READ_DATA_MESSAGE:
            packet = new RespReadDataMessage(transcoder);
            break;
        case TfsConstant.RESP_READ_DATA_MESSAGE_V2:
            packet = new RespReadDataMessageV2(transcoder);
            break;
        case TfsConstant.UNIQUE_FILE_MESSAGE:
            packet = new UniqueFileMessage(transcoder);
            break;
        case TfsConstant.RESP_RC_LOGIN_MESSAGE:
            packet = new RespRcLoginMessage(transcoder);
            break;
        case TfsConstant.RESP_RC_KEEPALIVE_MESSAGE:
            packet = new RespRcKeepAliveMessage(transcoder);
            break;
        case TfsConstant.RESP_NAME_META_READ_MESSAGE:
            packet = new RespNameMetaReadMessage(transcoder);
            break;
        case TfsConstant.RESP_NAME_META_LS_MESSAGE:
            packet = new RespNameMetaLsMessage(transcoder);
            break;
        case TfsConstant.RESP_NAME_META_GET_TABLE_MESSAGE:
            packet = new RespNameMetaGetTableMessage(transcoder);
            break;       
        default:
        }

        if ((packet != null) && (packet.getPcode() != pcode)) {
            packet = null;
        }

        return packet;
    }


}
