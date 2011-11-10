/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.namemeta;

import java.util.List;
import java.util.ArrayList;

import com.taobao.common.tfs.TfsException;
import com.taobao.common.tfs.exception.ErrorStatusException;
import com.taobao.common.tfs.exception.UnexpectMessageException;
import com.taobao.common.tfs.common.TfsConstant;
import com.taobao.common.tfs.net.ClientManager;
import com.taobao.common.tfs.impl.ClientConfig;
import com.taobao.common.tfs.packet.BasePacket;
import com.taobao.common.tfs.packet.StatusMessage;
import com.taobao.common.tfs.packet.NameMetaFileActionMessage;
import com.taobao.common.tfs.packet.NameMetaReadMessage;
import com.taobao.common.tfs.packet.RespNameMetaReadMessage;
import com.taobao.common.tfs.packet.NameMetaWriteMessage;
import com.taobao.common.tfs.packet.NameMetaLsMessage;
import com.taobao.common.tfs.packet.RespNameMetaLsMessage;
import com.taobao.common.tfs.packet.NameMetaGetTableMessage;
import com.taobao.common.tfs.packet.RespNameMetaGetTableMessage;

public class NameMetaSessionHelper {
    public static final ClientManager clientManager = new ClientManager();    

    static {
        clientManager.setTimeout(ClientConfig.TIMEOUT);
        clientManager.init();
    }

    public static int doFileAction(long serverId, NameMetaUserInfo userInfo, byte type,
                                   String srcFilePath, String destFilePath, long version)
        throws TfsException {
        NameMetaFileActionMessage nmfam = new NameMetaFileActionMessage(clientManager.getTranscoder());
        nmfam.setUserInfo(userInfo);
        nmfam.setType(type);
        nmfam.setSrcFilePath(srcFilePath);
        nmfam.setDestFilePath(destFilePath);
        nmfam.setVersion(version);

        return clientManager.sendPacketNoReturn(serverId, nmfam);
    }

    public static RespNameMetaLsMessage nameMetaLs(long serverId, NameMetaUserInfo userInfo,
                                                   String filePath, byte fileType, long filePid, long version)
        throws TfsException {
        NameMetaLsMessage nmlm = new NameMetaLsMessage(clientManager.getTranscoder());
        nmlm.setUserInfo(userInfo);
        nmlm.setPid(filePid);
        nmlm.setFilePath(filePath);
        nmlm.setFileType(fileType);
        nmlm.setVersion(version);
        
        BasePacket retPacket = clientManager.sendPacket(serverId, nmlm);
        if (retPacket.getPcode() == TfsConstant.RESP_NAME_META_LS_MESSAGE) {
            return (RespNameMetaLsMessage)retPacket;
        }

        if (retPacket.getPcode() == TfsConstant.STATUS_MESSAGE) {
            throw new ErrorStatusException(serverId, (StatusMessage)retPacket);
        }

        throw new UnexpectMessageException(serverId, retPacket);
    }

    public static RespNameMetaReadMessage nameMetaRead(long serverId, NameMetaUserInfo userInfo, String filePath,
                                                       long offset, long length, long version)
        throws TfsException {
        NameMetaReadMessage nmrm = new NameMetaReadMessage(clientManager.getTranscoder());
        nmrm.setUserInfo(userInfo);
        nmrm.setFilePath(filePath);
        nmrm.setOffset(offset);
        nmrm.setLength(length);
        nmrm.setVersion(version);
        
        BasePacket retPacket = clientManager.sendPacket(serverId, nmrm);
        if (retPacket.getPcode() == TfsConstant.RESP_NAME_META_READ_MESSAGE) {
            return (RespNameMetaReadMessage)retPacket;
        }

        if (retPacket.getPcode() == TfsConstant.STATUS_MESSAGE) {
            throw new ErrorStatusException(serverId, (StatusMessage)retPacket);
        }

        throw new UnexpectMessageException(serverId, retPacket);
    }

    public static int nameMetaWrite(long serverId, NameMetaUserInfo userInfo, String filePath,
                                    FragInfo fragInfo, long version)
        throws TfsException {
        NameMetaWriteMessage nmwm = new NameMetaWriteMessage(clientManager.getTranscoder());
        nmwm.setUserInfo(userInfo);
        nmwm.setFilePath(filePath);
        nmwm.setFragInfo(fragInfo);
        nmwm.setVersion(version);
        
        return clientManager.sendPacketNoReturn(serverId, nmwm);
    }
    
    public static RespNameMetaGetTableMessage getTable(long serverId)
    
    	throws TfsException {
        NameMetaGetTableMessage nmgtm = new NameMetaGetTableMessage(clientManager.getTranscoder());
        
        BasePacket retPacket = clientManager.sendPacket(serverId, nmgtm);
        if (retPacket.getPcode() == TfsConstant.RESP_NAME_META_GET_TABLE_MESSAGE) {
            return (RespNameMetaGetTableMessage)retPacket;
        }

        if (retPacket.getPcode() == TfsConstant.STATUS_MESSAGE) {
            throw new ErrorStatusException(serverId, (StatusMessage)retPacket);
        }

        throw new UnexpectMessageException(serverId, retPacket);
    }

}
