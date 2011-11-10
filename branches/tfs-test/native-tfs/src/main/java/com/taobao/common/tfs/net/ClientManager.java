/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Generl Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.net;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.common.tfs.net.TfsClient;
import com.taobao.common.tfs.net.TfsClientFactory;
import com.taobao.common.tfs.net.AsyncSender;
import com.taobao.common.tfs.net.RequestCommand;
import com.taobao.common.tfs.common.TfsUtil;
import com.taobao.common.tfs.common.TfsConstant;
import com.taobao.common.tfs.exception.ConnectTimeoutException;
import com.taobao.common.tfs.exception.ConnectionException;
import com.taobao.common.tfs.exception.ErrorStatusException;
import com.taobao.common.tfs.TfsException;
import com.taobao.common.tfs.exception.UnexpectMessageException;
import com.taobao.common.tfs.packet.BasePacket;
import com.taobao.common.tfs.packet.StatusMessage;
import com.taobao.common.tfs.packet.TfsPacketConstant;
import com.taobao.common.tfs.packet.TfsPacketStreamer;
import com.taobao.common.tfs.packet.Transcoder;

public class ClientManager {

    private static final Log log = LogFactory.getLog(ClientManager.class);

    private int timeout = TfsConstant.DEFAULT_TIMEOUT;
    private TfsPacketStreamer packetStreamer = null;
    private Transcoder transcoder = null;
    private Map<Integer, AsyncSender> asyncSenderMap = new HashMap<Integer, AsyncSender>();
    private static AtomicInteger globalAsyncId = new AtomicInteger(0);

    /**
     * initialize method, start [transport] && set streamer
     */
    public void init() {
        if (packetStreamer == null) {
            packetStreamer = new TfsPacketStreamer(transcoder);
        }
    }

    /**
     * stop [transport]
     */
    public void destroy() {
        TfsClientFactory.getInstance().destroy();
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public Transcoder getTranscoder() {
        return transcoder;
    }

    public void setTranscoder(Transcoder transcoder) {
        this.transcoder = transcoder;
    }

    /////////////////////////////////
    //        send packet          //
    /////////////////////////////////
    public BasePacket sendPacket(long server, BasePacket packet) throws ConnectionException {
        TfsClient client = null;
        try {
            client = TfsClientFactory.getInstance().get(server, timeout, packetStreamer);
        } catch (TfsException e) {
            log.error("",e);
        }
        if (client == null) {
            log.warn("cannot get Connection:" + TfsUtil.idToAddress(server).toString());
            throw new ConnectionException(server);
        }

        long startTime = System.currentTimeMillis();
        BasePacket returnPacket = null;
        packet.encode();

        try {
            returnPacket = (BasePacket)client.invoke(packet, timeout);
        } catch (TfsException e) {
            throw new ConnectionException(server, e);
        }

        if (returnPacket == null) {
            log.error("send packet [" + packet.getChid() + "] timeout error. timeout: "
                      + timeout + ", used: " + (System.currentTimeMillis() - startTime)+ " (ms)");
            throw new ConnectTimeoutException(server);
        } else if (log.isDebugEnabled()) {
            log.debug("send packet [" + packet.getChid() + "] timeout: " + timeout + ", used: "
                      + (System.currentTimeMillis() - startTime) + " (ms)");
        }

        return returnPacket;
    }

    public int sendPacketNoReturn(long server, BasePacket packet) throws ConnectionException {
        BasePacket retPacket = sendPacket(server, packet);
        if (retPacket.getPcode() == TfsConstant.STATUS_MESSAGE) {
            StatusMessage sm = (StatusMessage)retPacket;
            if (sm.getStatus() != StatusMessage.STATUS_MESSAGE_OK) {
                throw new ErrorStatusException(server, sm);
            }
            return sm.getStatus();
        }
        log.warn("send packet expect status message but not, pcode: " + retPacket.getPcode());
        throw new UnexpectMessageException(server, retPacket);
    }


    /////////////////////////////////
    //        post packet          //
    /////////////////////////////////

    private AsyncSender getAsyncSender(int id) {
        AsyncSender asyncSender = asyncSenderMap.get(id);
        if (asyncSender == null ) {
            asyncSender = new AsyncSender(packetStreamer, timeout);
            asyncSenderMap.put(id, asyncSender);
        }
        return asyncSender;
    }

    public int getAsyncId() {
        return globalAsyncId.incrementAndGet();
    }

    public boolean postPacket(List<RequestCommand> requestList, int id) {
        return getAsyncSender(id).sendPacket(requestList);
    }

    public boolean postPacket(long addr, BasePacket request, int id, int seqId) {
        return getAsyncSender(id).sendPacket(addr, request, seqId);
    }

    public boolean await(int id) {
        return getAsyncSender(id).await();
    }

    public List<BasePacket> getResponseList(int id) {
        AsyncSender asyncSender = asyncSenderMap.get(id);

        if (asyncSender == null) {
            log.error("invalid async id to get response");
            return null;
        }

        // avoid insert returned response packet after get, copy
        List<BasePacket> responseList = new ArrayList<BasePacket>(asyncSender.getResponseList());
        log.debug("get response size : " + responseList.size() + " async id : " + id);
        return responseList;
    }

    public List<Integer> getFailIdList(int id) {
        AsyncSender asyncSender = asyncSenderMap.get(id);

        if (asyncSender == null) {
            log.error("invalid async id to get failid list");
            return null;
        }
        return asyncSender.getFailIdList();
    }

    public void destroyAsync(int id) {
        asyncSenderMap.remove(id);
    }
}
