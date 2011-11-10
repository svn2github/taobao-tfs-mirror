/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.net;

import java.util.concurrent.atomic.AtomicBoolean;

import com.taobao.common.tfs.TfsException;

public class ResponseCallbackTask {

    private int requestId;

    private ResponseListener listener;

    private AtomicBoolean isDone = new AtomicBoolean(false);


    private long timeout;

    public ResponseCallbackTask(Integer requestId, ResponseListener listener, long timeout) {
        this.requestId = requestId;
        this.listener = listener;
        this.timeout = System.currentTimeMillis() + timeout;
    }

    public int getRequestId() {
        return requestId;
    }

    public ResponseListener getListener() {
        return listener;
    }

    public long getTimeout() {
        return timeout;
    }

    public AtomicBoolean getIsDone() {
        return isDone;
    }

    public void setResponse(Object response) {
        if(!isDone.compareAndSet(false, true)) {
            return;
        }

        if(response instanceof TfsException) {
            listener.exceptionCaught(requestId, (TfsException) response);
        } else {
            listener.responseReceived(response);
        }
    }


}
