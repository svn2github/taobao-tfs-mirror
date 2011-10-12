/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.net;

import java.net.SocketAddress;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mina.common.ConnectFuture;
import org.apache.mina.common.IoSession;
import org.apache.mina.common.ThreadModel;
import org.apache.mina.transport.socket.nio.SocketConnector;
import org.apache.mina.transport.socket.nio.SocketConnectorConfig;

import com.taobao.common.tfs.TfsException;
import com.taobao.common.tfs.common.TfsUtil;
import com.taobao.common.tfs.packet.PacketStreamer;

public class TfsClientFactory {

    private static final Log log = LogFactory.getLog(TfsClientFactory.class);

    private static final int processorCount = Runtime.getRuntime()
        .availableProcessors() + 1;

    private static final String CONNECTOR_THREADNAME = "TFSCLIENT";

    private static final ThreadFactory CONNECTOR_TFACTORY = new NamedThreadFactory(
            CONNECTOR_THREADNAME);

    private static final TfsClientFactory factory = new TfsClientFactory();

    private static final int MIN_CONN_TIMEOUT = 1000;

    private final SocketConnector ioConnector;

    private final ConcurrentHashMap<Long, FutureTask<TfsClient>> clients = new ConcurrentHashMap<Long, FutureTask<TfsClient>>();

    private ExecutorService executors = (ExecutorService)Executors.newCachedThreadPool(CONNECTOR_TFACTORY);

    private TfsClientFactory() {
        ioConnector = new SocketConnector(processorCount, executors);
    }

    public static TfsClientFactory getInstance() {
        return factory;
    }

    public void destroy() {
        Iterator iter = clients.entrySet().iterator();
        try {
            while (iter.hasNext()) {
                ((FutureTask<TfsClient>)((Entry)iter.next()).getValue()).get().destroy();
            }
        } catch (Exception e) {
            log.error("", e);
        }
        ioConnector.setWorkerTimeout(0);
        executors.shutdown();
    }

    public TfsClient get(Long targetId, final int connectionTimeout, final PacketStreamer pstreamer)
        throws TfsException {
        final Long key = targetId;
        if (clients.containsKey(key)) {
            try {
                return clients.get(key).get();
            } catch (Exception e) {
                removeClient(key);
                throw new TfsException(
                        "get tfs connection error,targetAddress is "
                        + targetId, e);
            }
        } else {
            FutureTask<TfsClient> task = new FutureTask<TfsClient>(
                    new Callable<TfsClient>() {
                        public TfsClient call() throws Exception {
                            return createClient(key, connectionTimeout, pstreamer);
                        }
                    });
            FutureTask<TfsClient> existTask = clients.putIfAbsent(key, task);
            if (existTask == null) {
                existTask = task;
                task.run();
            }
            try {
                return existTask.get();
            } catch (Exception e) {
                removeClient(key);
                throw new TfsException(
                        "get tfs connection error,targetAddress is "
                        + targetId, e);
            }
        }
    }

    protected void removeClient(Long key) {
        clients.remove(key);
    }

    private TfsClient createClient(Long targetId, int connectionTimeout, PacketStreamer pstreamer)
        throws Exception {
        SocketConnectorConfig cfg = new SocketConnectorConfig();
        cfg.setThreadModel(ThreadModel.MANUAL);
        if (connectionTimeout < MIN_CONN_TIMEOUT)
            connectionTimeout = MIN_CONN_TIMEOUT;
        cfg.setConnectTimeout((int) connectionTimeout / 1000);
        cfg.getSessionConfig().setTcpNoDelay(true);
        cfg.getFilterChain().addLast("objectserialize",
                new TfsProtocolCodecFilter(pstreamer));
        SocketAddress targetAddress = TfsUtil.idToAddress(targetId);
        TfsClientProcessor processor = new TfsClientProcessor();
        ConnectFuture connectFuture = ioConnector.connect(targetAddress, null,
                processor, cfg);

        connectFuture.join();

        IoSession ioSession = connectFuture.getSession();
        if ((ioSession == null) || (!ioSession.isConnected())) {
            throw new TfsException(
                    "create tfs connection error,targetaddress is "
                    + targetId);
        }
        if (log.isTraceEnabled()) {
            log.trace("create tfs connection success,targetaddress is "
                    + targetId);
        }

        TfsClient client = new TfsClient(ioSession,targetId);
        processor.setClient(client);
        processor.setFactory(this, targetId);
        return client;
    }

}
