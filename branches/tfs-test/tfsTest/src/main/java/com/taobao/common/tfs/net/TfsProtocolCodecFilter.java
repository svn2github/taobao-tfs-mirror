/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.net;

import org.apache.mina.filter.codec.ProtocolCodecFilter;

import com.taobao.common.tfs.packet.PacketStreamer;

public class TfsProtocolCodecFilter extends ProtocolCodecFilter {

    public TfsProtocolCodecFilter(PacketStreamer pstreamer) {
        super(new TfsProtocolEncoder(), new TfsProtocolDecoder(pstreamer));
    }


}
