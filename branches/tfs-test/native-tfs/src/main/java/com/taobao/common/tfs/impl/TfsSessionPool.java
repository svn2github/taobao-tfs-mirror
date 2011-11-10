/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.impl;

import java.util.Map;
import java.util.HashMap;

import com.taobao.common.tfs.TfsException;

public class TfsSessionPool {
    private static Map<String, TfsSession> sessionPool = new HashMap<String, TfsSession>();

    public synchronized static TfsSession setSession(String nsAddr) throws TfsException {
        TfsSession session = new TfsSession();
        session.setNameServerIp(nsAddr);
        if (session.init()) {
            sessionPool.put(nsAddr, session);
            return session;
        }
        throw new TfsException("get tfs session fail: " + nsAddr);
    }

    public static TfsSession getSession(String nsAddr) throws TfsException {
        TfsSession session = sessionPool.get(nsAddr);
        if (session == null) {
            return setSession(nsAddr);
        }
        return session;
    }

}
