/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.exception;

import com.taobao.common.tfs.common.TfsUtil;
import com.taobao.common.tfs.TfsException;

public class ConnectionException extends TfsException {
    private static final long serialVersionUID = -5579024880405807891L;
    private long server;

    public ConnectionException(long server) {
        this.server = server;
    }

    public ConnectionException(long server, String message) {
        super(message);
        this.server = server;
    }

    public ConnectionException(long server, Throwable cause) {
        super(cause);
        this.server = server;
    }

    public ConnectionException(long server, String message, Throwable cause) {
        super(message, cause);
        this.server = server;
    }

    @Override
    public String getMessage() {
        return "Connect server:" + TfsUtil.idToAddress(server).toString() + " error. " + super.getMessage();
    }

    public long getServer() {
        return server;
    }

    public void setServer(long server) {
        this.server = server;
    }


}
