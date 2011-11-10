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
import com.taobao.common.tfs.packet.StatusMessage;

public class ErrorStatusException extends ConnectionException {
    private static final long serialVersionUID = -5798332922782946065L;
    private StatusMessage packet;

    public ErrorStatusException(long server, StatusMessage packet) {
        super(server);
        this.packet = packet;
    }

    public int getStatus() {
        if (this.packet != null) {
            return packet.getStatus();
        }
        return StatusMessage.STATUS_MESSAGE_ERROR;
    }

    @Override
    public String getMessage() {
        if (this.packet != null)
            return "receive wrong status message, code:[" + packet.getStatus() + "] from "
                + TfsUtil.idToAddress(getServer()).toString() + ",errmsg:" + packet.getError();
        return "receive wrong packet null from "
            + TfsUtil.idToAddress(getServer()).toString();
    }


}
