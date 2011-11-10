/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.packet;

import java.io.IOException;

import org.apache.mina.common.ByteBuffer;


public interface PacketStreamer {
    /**
     * decode packet header
     *
     * @param buffer
     *
     * @return
     *
     * @throws IOException
     */
    public BasePacket decodePacket(ByteBuffer buffer) throws IOException;


}
