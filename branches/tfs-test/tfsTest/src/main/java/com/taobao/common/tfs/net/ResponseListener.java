/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.net;

import com.taobao.common.tfs.TfsException;

public interface ResponseListener {

    /**
     *
     *
     * @param response
     * @param exception
     */
    public void responseReceived(Object response);

    /**
     *
     *
     * @param exception
     */
    public void exceptionCaught(int seqId, TfsException exception);


}
