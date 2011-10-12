/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.net;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mina.common.ByteBuffer;
import org.apache.mina.common.IoSession;
import org.apache.mina.filter.codec.ProtocolEncoderAdapter;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;


public class TfsProtocolEncoder extends ProtocolEncoderAdapter {
    private static final Log LOGGER = LogFactory.getLog(TfsProtocolEncoder.class);
    @SuppressWarnings("unused")
        private static final boolean isDebugEnabled = LOGGER.isDebugEnabled();

    /* (non-Javadoc)
     * @see org.apache.mina.filter.codec.ProtocolEncoder#encode(org.apache.mina.common.IoSession, java.lang.Object, org.apache.mina.filter.codec.ProtocolEncoderOutput)
     */
    public void encode(IoSession session, Object message,
            ProtocolEncoderOutput out) throws Exception {
        if (!(message instanceof byte[])) {
            throw new Exception("must send byte[]");
        }
        byte[] payload = (byte[]) message;
        ByteBuffer buf = ByteBuffer.allocate(payload.length, false);
        buf.put(payload);
        buf.flip();
        out.write(buf);
    }


}
