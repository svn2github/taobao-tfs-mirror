/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.net;

import java.io.IOException;

import com.taobao.common.tfs.packet.BasePacket;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mina.common.IoHandlerAdapter;
import org.apache.mina.common.IoSession;

public class TfsClientProcessor extends IoHandlerAdapter{

    private static final Log log = LogFactory.getLog(TfsClientProcessor.class);

    private TfsClient client = null;

    private TfsClientFactory factory = null;

    private Long key = null;

    public void setClient(TfsClient client){
        this.client = client;
    }

    public void setFactory(TfsClientFactory factory,Long targetId){
        this.factory = factory;
        key = targetId;
    }

    @Override
    public void messageReceived(IoSession session, Object message)
        throws Exception {
        BasePacket response = (BasePacket)message;
        Integer requestId = response.getChid();

        if (client.isCallbackTask(requestId)) {
            client.putCallbackResponse(requestId, response);
        } else {
            client.putResponse(requestId, response);
        }
    }

    @Override
    public void exceptionCaught(IoSession session, Throwable cause)
        throws Exception {
        if (log.isDebugEnabled())
            log.debug("connection exception occured", cause);

        if(!(cause instanceof IOException)){
            session.close();
        }
    }

    public void sessionClosed(IoSession session) throws Exception {
        factory.removeClient(key);
    }


}
