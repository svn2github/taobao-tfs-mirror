/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.net;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.taobao.common.tfs.TfsException;
import com.taobao.common.tfs.common.TfsConstant;
import com.taobao.common.tfs.packet.BasePacket;
import com.taobao.common.tfs.packet.PacketStreamer;
import com.taobao.common.tfs.packet.TfsPacketConstant;

public class AsyncSender {
    public class MultiReceiveListener implements ResponseListener {
        private ReentrantLock lock = new ReentrantLock();
        private Condition cond = lock.newCondition();
        private int doneCount = 0;

        private List<BasePacket> responseList = new ArrayList<BasePacket>();
        private List<Integer> failIdList = new ArrayList<Integer>();

        public MultiReceiveListener() {
        }

        public void responseReceived(Object response) {
            lock.lock();

            try {
                // decode here
                ((BasePacket)response).decode();
                responseList.add((BasePacket)response);
                this.doneCount++;
                cond.signal();
            } finally {
                lock.unlock();
            }
        }

        public void exceptionCaught(int seqId, TfsException exception) {
            lock.lock();
            this.doneCount++;
            failIdList.add(seqId);
            cond.signal();
            lock.unlock();
            log.error("listener exception caught. seq Id: " + seqId, exception);
        }

        public boolean await(int count, int timeout) {
            long t = TimeUnit.MILLISECONDS.toNanos(timeout);

            lock.lock();

            try {
                while (this.doneCount < count) {
                    if ((t = cond.awaitNanos(t)) <= 0) {
                        return false;
                    }
                }
            } catch (InterruptedException e) {
                log.error("interrupted while waiting", e);
                return false;
            } finally {
                lock.unlock();
            }

            return true;
        }
    }

    private static final Log log = LogFactory.getLog(AsyncSender.class);

    private PacketStreamer packetStreamer = null;
    private MultiReceiveListener listener = new MultiReceiveListener();

    private int timeout = TfsConstant.DEFAULT_TIMEOUT;
    private AtomicInteger sendCount = new AtomicInteger(0);

    public AsyncSender(PacketStreamer packetStreamer, int timeout) {
        this.packetStreamer = packetStreamer;
        this.timeout = timeout;
    }

    public boolean sendPacket(List<RequestCommand> requestList) {
        TfsClient client = null;
        int sucCount = 0;

        for (RequestCommand rc : requestList) {
            if (rc.getSeqId() > TfsPacketConstant.MAX_SEQ_ID) {
                log.error("seq id is over MAX value, skip: " + rc.getSeqId() + " > " + TfsPacketConstant.MAX_SEQ_ID);
                continue;
            }

            try {
                client = TfsClientFactory.getInstance().get(rc.getAddr(), timeout, packetStreamer);
            } catch (TfsException e) {
                log.error("get client fail.", e);
            }

            if (client == null) {
                log.error("async send packet fail, seqId: " + rc.getSeqId());
                continue;
            }

            BasePacket request = rc.getRequest();
            request.setSeqId(rc.getSeqId());
            request.encode();
            client.invokeAsync(request, timeout, listener);
            sucCount++;
        }

        sendCount.addAndGet(sucCount);
        // TODO: only consider if all success or not
        return sucCount == requestList.size();
    }

    public boolean sendPacket(long addr, BasePacket request, int seqId) {
        if (seqId > TfsPacketConstant.MAX_SEQ_ID) {
            log.error("seq id is over MAX value: " + seqId + " > " + TfsPacketConstant.MAX_SEQ_ID);
            return false;
        }

        TfsClient client = null;
        try {
            client = TfsClientFactory.getInstance().get(addr, timeout, packetStreamer);
        } catch (TfsException e) {
            log.error("get client fail", e);
        }

        if (client == null) {
            log.error("async send packet fail, seqId: " + seqId);
            return false;
        }

        request.setSeqId(seqId);
        request.encode();
        client.invokeAsync(request, timeout, listener);
        sendCount.incrementAndGet();
        return true;
    }

    public boolean await() {
        int waitCount = sendCount.get();
        boolean ret;
        if (!(ret = listener.await(waitCount, timeout))) {
            log.warn("not get all response. request count: " + waitCount +
                     " response count: " + listener.doneCount);
        }
        return ret;
    }

    public List<BasePacket> getResponseList() {
        return listener.responseList;
    }

    public List<Integer> getFailIdList() {
        return listener.failIdList;
    }


}
