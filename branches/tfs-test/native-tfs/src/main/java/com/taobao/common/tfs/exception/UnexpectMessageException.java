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
import com.taobao.common.tfs.packet.BasePacket;

public class UnexpectMessageException extends ConnectionException {
    private static final long serialVersionUID = -5630257280950703107L;

    private BasePacket packet ;

    public UnexpectMessageException(long server, BasePacket packet) {
        super(server);
        this.packet = packet;
    }

    @Override
    public String getMessage() {
        if (packet != null) {
            return "receive wrong packet code:" + packet.getPcode() + " from "
                + TfsUtil.idToAddress(getServer()).toString();
        }
        return "receive wrong packet null from "
            + TfsUtil.idToAddress(getServer()).toString();
    }


}
