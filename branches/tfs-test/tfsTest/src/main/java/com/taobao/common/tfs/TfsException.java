/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs;

/**
 * specified tfs exception
 *
 */
public class TfsException extends Exception {
    private static final long serialVersionUID = -7366858604097861658L;

    public TfsException() {
    }

    public TfsException(String message) {
        super(message);
    }

    public TfsException(Throwable cause) {
        super(cause);
    }

    public TfsException(String message, Throwable cause) {
        super(message, cause);
    }


}
