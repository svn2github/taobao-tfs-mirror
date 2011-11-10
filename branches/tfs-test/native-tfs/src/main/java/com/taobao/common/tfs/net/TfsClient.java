/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.net;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mina.common.IoFuture;
import org.apache.mina.common.IoFutureListener;
import org.apache.mina.common.IoSession;
import org.apache.mina.common.WriteFuture;

import com.taobao.common.tfs.TfsException;
import com.taobao.common.tfs.packet.BasePacket;


public class TfsClient {

    private static final Log log = LogFactory.getLog(TfsClient.class);

    private static final boolean isDebugEnabled = log.isDebugEnabled();

    private static ConcurrentHashMap<Integer, ResponseCallbackTask> callbackTasks =
        new ConcurrentHashMap<Integer, ResponseCallbackTask>();

    private static long minTimeout = 100L;

    private static ConcurrentHashMap<Integer, ArrayBlockingQueue<Object>> responses =
        new ConcurrentHashMap<Integer, ArrayBlockingQueue<Object>>();

    private final IoSession session;

    private Long key;

    static {
        Thread callbackThread = new Thread(new CallbackTasksScan());
        callbackThread.setDaemon(true);
        callbackThread.start();
    }

    protected TfsClient(IoSession session,Long key) {
        this.session = session;
        this.key = key;
    }

    public Object invoke(final BasePacket packet, final long timeout)
        throws TfsException {
        if (isDebugEnabled) {
            log.debug("send request [" + packet.getChid() + "],time is:"
                    + System.currentTimeMillis());
        }
        ArrayBlockingQueue<Object> queue = new ArrayBlockingQueue<Object>(1);
        responses.put(packet.getChid(), queue);
        ByteBuffer bb = packet.getByteBuffer();
        bb.flip();
        byte[] data = new byte[bb.remaining()];
        bb.get(data);
        WriteFuture writeFuture = session.write(data);
        writeFuture.addListener(new IoFutureListener() {

            public void operationComplete(IoFuture future) {

                WriteFuture wfuture = (WriteFuture) future;
                if (wfuture.isWritten()) {
                    return;
                }
                String error = "send message to tfs server error ["
                    + packet.getChid() + "], tfs server: "
                    + session.getRemoteAddress()
                    + ", maybe because this connection closed: "
                    + !session.isConnected();

                try {
                    putResponse(packet.getChid(), new TfsException(error));
                } catch (TfsException e) {
                    // should never happen
                    log.error("put response fail", e);
                }

                // close this session
                if(session.isConnected()) {
                    session.close();
                } else {
                    TfsClientFactory.getInstance().removeClient(key);
                }
            }

        });

        Object response = null;
        try {
            response = queue.poll(timeout, TimeUnit.MILLISECONDS);
            if (response == null) { // timeout
                return null;
            } else if (response instanceof TfsException) {
                throw (TfsException) response;
            }
        } catch (InterruptedException e) {
            throw new TfsException("tfs client invoke error", e);
        } finally {
            responses.remove(packet.getChid());
            // For GC
            queue = null;
        }
        if (isDebugEnabled) {
            log.debug("return response [" + packet.getChid() + "],time is:"
                    + System.currentTimeMillis());
        }

        // do decode here
        if (response instanceof BasePacket) {
            ((BasePacket)response).decode();
        }
        return response;
    }

    public void invokeAsync(final BasePacket packet, final long timeout, ResponseListener listener) {
        if (isDebugEnabled) {
            log.debug("send request ["+packet.getChid()+"] async,time is:"+System.currentTimeMillis());
        }

        if (minTimeout > timeout) {
            minTimeout = timeout;
        }

        final ResponseCallbackTask callbackTask = new ResponseCallbackTask(packet.getSeqId(), listener, timeout);
        callbackTasks.put(packet.getChid(), callbackTask);

        ByteBuffer bb = packet.getByteBuffer();
        bb.flip();
        byte[] data = new byte[bb.remaining()];
        bb.get(data);
        WriteFuture writeFuture = session.write(data);
        writeFuture.addListener(new IoFutureListener() {

                public void operationComplete(IoFuture future) {
                    WriteFuture wfuture = (WriteFuture)future;
                    if (wfuture.isWritten()) {
                        return;
                    }
                    String error = "send message to tfs server error [" + packet.getChid() + "], tfs server: " + session.getRemoteAddress()+", maybe because this connection closed: " + !session.isConnected();
                    callbackTask.setResponse(new TfsException(error));

                    // close this session
                    if (session.isConnected()) {
                        session.close();
                    } else {
                        TfsClientFactory.getInstance().removeClient(key);
                    }
                }

            });
    }

    protected void putResponse(Integer requestId, Object response)
        throws TfsException {
        if (responses.containsKey(requestId)) {
            try {
                ArrayBlockingQueue<Object> queue = responses.get(requestId);
                if (queue != null) {
                    queue.put(response);
                    if (isDebugEnabled) {
                        log.debug("put response [" + requestId
                                  + "],time is:" + System.currentTimeMillis());
                    }
                } else if (isDebugEnabled) {
                    log.debug("give up the response, maybe because timeout, requestId is:"
                              + requestId);
                }

            } catch (InterruptedException e) {
                throw new TfsException("put response error", e);
            }
        } else {
                log.warn("give up the response, invalid request Id: "
                          + requestId);
        }
    }

    protected boolean isCallbackTask(Integer requestId) {
        return callbackTasks.containsKey(requestId);
    }

    protected void putCallbackResponse(Integer requestId, Object response)
        throws TfsException {
        ResponseCallbackTask task = callbackTasks.get(requestId);

        if (task == null) {
            log.error("request id is not in callback task: " + requestId);
        } else {
            task.setResponse(response);
        }
    }

    static class CallbackTasksScan implements Runnable {
        static final long DEFAULT_SLEEPTIME = 10L;
        final TfsException timeoutException = new TfsException("receive response timeout");

        public void run() {
            while (true) {
                List<Integer> removeIds = new ArrayList<Integer>();
                for (Entry<Integer, ResponseCallbackTask> entry: callbackTasks.entrySet()) {
                    long currentTime = System.currentTimeMillis();
                    ResponseCallbackTask task = entry.getValue();

                    if((task.getIsDone().get())) {
                        removeIds.add(entry.getKey());
                    } else if(task.getTimeout() < currentTime) {
                        removeIds.add(entry.getKey());
                        task.setResponse(timeoutException);
                    }
                }
                for (Integer removeId : removeIds) {
                    callbackTasks.remove(removeId);
                }

                long sleepTime = DEFAULT_SLEEPTIME;
                if(callbackTasks.size() == 0) {
                    sleepTime = minTimeout;
                }

                try {
                    Thread.sleep(sleepTime*10);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    public String toString() {
        if (this.session != null)
            return this.session.toString();
        return "null session client";
    }

    public void destroy() {
        if (session != null && session.isConnected()) {
            session.close();
        }
    }
}
