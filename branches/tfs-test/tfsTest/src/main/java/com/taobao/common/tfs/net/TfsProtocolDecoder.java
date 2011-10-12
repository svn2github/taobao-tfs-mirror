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
import org.apache.mina.filter.codec.CumulativeProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolDecoderOutput;

import com.taobao.common.tfs.packet.BasePacket;
import com.taobao.common.tfs.packet.PacketStreamer;

public class TfsProtocolDecoder extends CumulativeProtocolDecoder {

    private static final Log LOGGER = LogFactory.getLog(TfsProtocolDecoder.class);

    @SuppressWarnings("unused")
    private static final boolean isDebugEnabled=LOGGER.isDebugEnabled();

    private PacketStreamer pstreamer;

    public TfsProtocolDecoder(PacketStreamer pstreamer) {
        this.pstreamer = pstreamer;

    }

    /* (non-Javadoc)
     * @see org.apache.mina.filter.codec.CumulativeProtocolDecoder#doDecode(org.apache.mina.common.IoSession, org.apache.mina.common.ByteBuffer, org.apache.mina.filter.codec.ProtocolDecoderOutput)
     */
    @Override
    protected boolean doDecode(IoSession session, ByteBuffer in, ProtocolDecoderOutput out)
        throws Exception {
        BasePacket returnPacket = pstreamer.decodePacket(in);
        if (null == returnPacket) {
            return false;
        }
        out.write(returnPacket);

        return true;
    }


}
