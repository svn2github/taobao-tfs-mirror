/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.packet;

import com.taobao.common.tfs.common.TfsConstant;

public class RcLogoutMessage extends RcKeepAliveMessage {
    public RcLogoutMessage(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TfsConstant.REQ_RC_LOGOUT_MESSAGE;
    }
}
