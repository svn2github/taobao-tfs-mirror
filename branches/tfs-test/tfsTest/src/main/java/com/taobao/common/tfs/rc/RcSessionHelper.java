/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.rc;

import com.taobao.common.tfs.TfsException;
import com.taobao.common.tfs.common.TfsConstant;
import com.taobao.common.tfs.net.ClientManager;
import com.taobao.common.tfs.impl.ClientConfig;
import com.taobao.common.tfs.exception.ErrorStatusException;
import com.taobao.common.tfs.exception.UnexpectMessageException;
import com.taobao.common.tfs.packet.BasePacket;
import com.taobao.common.tfs.packet.StatusMessage;
import com.taobao.common.tfs.packet.RcLoginMessage;
import com.taobao.common.tfs.packet.RcKeepAliveMessage;
import com.taobao.common.tfs.packet.RcReloadMessage;
import com.taobao.common.tfs.packet.RcLogoutMessage;
import com.taobao.common.tfs.packet.RespRcLoginMessage;
import com.taobao.common.tfs.packet.RespRcKeepAliveMessage;

public class RcSessionHelper {
    public static final ClientManager clientManager = new ClientManager();

    static {
        clientManager.setTimeout(ClientConfig.TIMEOUT);
        clientManager.init();
    }

    public static RespRcLoginMessage login(long serverId, String appId, long appIp)
        throws TfsException {
        RcLoginMessage rlm = new RcLoginMessage(clientManager.getTranscoder());
        rlm.setAppKey(appId);
        rlm.setAppIp(appIp);

        BasePacket retPacket = clientManager.sendPacket(serverId, rlm);

        if (retPacket.getPcode() == TfsConstant.RESP_RC_LOGIN_MESSAGE) {
            return (RespRcLoginMessage)retPacket;
        }

        if (retPacket.getPcode() == TfsConstant.STATUS_MESSAGE) {
            throw new ErrorStatusException(serverId, (StatusMessage)retPacket);
        }

        throw new UnexpectMessageException(serverId, retPacket);
    }

    public static RespRcKeepAliveMessage keepAlive(long serverId, RcKeepAliveInfo kaInfo)
        throws TfsException {
        RcKeepAliveMessage rkam = new RcKeepAliveMessage(clientManager.getTranscoder());
        rkam.setKaInfo(kaInfo);
        BasePacket retPacket = clientManager.sendPacket(serverId, rkam);

        if (retPacket.getPcode() == TfsConstant.RESP_RC_KEEPALIVE_MESSAGE) {
            return (RespRcKeepAliveMessage)retPacket;
        }

        if (retPacket.getPcode() == TfsConstant.STATUS_MESSAGE) {
            throw new ErrorStatusException(serverId, (StatusMessage)retPacket);
        }

        throw new UnexpectMessageException(serverId, retPacket);
    }

    public static int reload(long serverId, int reloadType)
        throws TfsException {
        RcReloadMessage rrm = new RcReloadMessage(clientManager.getTranscoder());
        rrm.setReloadType(reloadType);

        return clientManager.sendPacketNoReturn(serverId, rrm);
    }

    public static int logout(long serverId, RcKeepAliveInfo kaInfo)
        throws TfsException {
        RcLogoutMessage rlm = new RcLogoutMessage(clientManager.getTranscoder());
        rlm.setKaInfo(kaInfo);

        return clientManager.sendPacketNoReturn(serverId, rlm);
    }

}