/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.exception;

public class ConnectTimeoutException extends ConnectionException {
    private static final long serialVersionUID = 4004040653743478697L;

    public ConnectTimeoutException(long server) {
        super(server, "call message timeout");
    }


}
