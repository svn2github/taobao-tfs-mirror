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

import com.taobao.common.tfs.common.TfsConstant;
import com.taobao.common.tfs.common.TfsUtil;

public abstract class AbstractPacketStreamer implements PacketStreamer {
    public BasePacket decodePacket(ByteBuffer buffer) throws IOException {

        final int origonPos = buffer.position();
        if (buffer.remaining() < TfsPacketConstant.TFS_PACKET_HEADER_SIZE) {
            buffer.position(origonPos);
            return null;
        }

        buffer.order(TfsConstant.TFS_PROTOCOL_BYTE_ORDER);
        // packet header
        int flag = buffer.getInt();
        int len = buffer.getInt();
        int pcode = buffer.getShort();
        int version = buffer.getShort();
        long id = buffer.getLong();

        int crc = buffer.getInt();

        if ((flag != TfsConstant.TFS_PACKET_FLAG) || (len > 0x4000000)) {
            throw new IOException("streamer error. flag=" + flag
                                  + ",TfsConstant.TFS_PACKET_FLAG="
                                  + TfsConstant.TFS_PACKET_FLAG + ", len=" + len
                                  + ",buffer.remaining():" + buffer.remaining()
                                  + ",position:" + buffer.position() + ",version:" + version);
        }

        if (buffer.remaining() < len) {
            buffer.position(origonPos);
            return null;
        }

        byte[] bytes = new byte[len];
        buffer.get(bytes);
        int confirmCrc = TfsUtil.crc32(TfsConstant.TFS_PACKET_FLAG, bytes, 0, len);
        if (crc != confirmCrc) {
            throw new IOException("receive packet crc error: crc:" + crc
                                  + ",calc:" + confirmCrc);
        }

        // pcode
        BasePacket packet = createPacket(pcode);

        // channel id 0~24 bit
        packet.setChid((int)id);
        // seq id 24~32 bit
        packet.setSeqId((int)(id >> 24));
        packet.setLen(len);
        packet.setByteBuffer(java.nio.ByteBuffer.wrap(bytes));

        return packet;
    }

    public abstract BasePacket createPacket(int pcode);


}
