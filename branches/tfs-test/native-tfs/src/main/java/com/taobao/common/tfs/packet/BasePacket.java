/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.packet;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicInteger;

import com.taobao.common.tfs.common.TfsConstant;
import com.taobao.common.tfs.common.TfsUtil;

public class BasePacket {
    protected Exception          exception    = null;
    protected ByteBuffer         byteBuffer   = null;
    protected int                chid         = 0;
    protected int                seqId        = 0;
    protected int                pcode        = 0;
    protected int                len          = 0;
    private BasePacket           returnPacket = null;
    private static AtomicInteger globalChid   = new AtomicInteger(0);
    protected Transcoder         transcoder   = null;
    private long                 startTime    = 0;


    /**
     * @param transcoder
     */
    public BasePacket(Transcoder transcoder) {
        this.transcoder = transcoder;
    }

    /**
     *    byteBuffer
     *
     * @return
     */
    public ByteBuffer getByteBuffer() {
        if (byteBuffer == null) {
            encode();
        }

        return byteBuffer;
    }

    public int getPacketLength() { return 0; }

    public void writePacketStream() {}

    public int encode() {
        int length = getPacketLength();
        if (length <= 0) return 0;
        writePacketBegin(length);
        writePacketStream();
        writePacketEnd();
        return 0;
    }

    public boolean decode() {
        System.out.println("decode base " + byteBuffer.remaining() + " len " + len);
        if ((byteBuffer == null) || (byteBuffer.remaining() < len)) {
            return false;
        }

        // byte[] tmp = new byte[len];
        // byteBuffer.get(tmp);
        return true;
    }

    protected void writePacketBegin(int capacity) {
        setChid(globalChid.incrementAndGet());

        // packet header
        byteBuffer = ByteBuffer.allocate(capacity + 256);
        byteBuffer.order(TfsConstant.TFS_PROTOCOL_BYTE_ORDER);
        byteBuffer.putInt(TfsConstant.TFS_PACKET_FLAG); // packet flag
        byteBuffer.putInt(0); // body len position, write by writePacketEnd later
        byteBuffer.putShort((short)pcode); // packet code
        byteBuffer.putShort((short)TfsPacketConstant.TFS_PACKET_VERSION); // packet version
        byteBuffer.putLong(seqId << 24 | chid); // channel id
        byteBuffer.putInt(0); // crc position, write by writePacketEnd later.
    }

    protected void writePacketEnd() {
        int len = byteBuffer.position() - TfsPacketConstant.TFS_PACKET_HEADER_SIZE;
        byteBuffer.putInt(TfsPacketConstant.TFS_PACKET_HEADER_BLPOS, len);
        // crc
        int crc = TfsUtil.crc32(TfsConstant.TFS_PACKET_FLAG, byteBuffer.array(),
                                TfsPacketConstant.TFS_PACKET_HEADER_SIZE, len);
        byteBuffer.putInt(TfsPacketConstant.TFS_PACKET_HEADER_CRCPOS, crc);
    }

    public BasePacket getReturnPacket() {
        return returnPacket;
    }

    public void setReturnPacket(BasePacket returnPacket) {
        this.returnPacket = returnPacket;
    }

    public void setByteBuffer(ByteBuffer byteBuffer) {
        this.byteBuffer = ByteBuffer.allocate(len);
        this.byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer.get(this.byteBuffer.array());
    }

    public int getPcode() {
        return pcode;
    }

    public void setPcode(int pcode) {
        this.pcode = pcode;
    }

    public void setChid(int chid) {
        this.chid = chid & TfsPacketConstant.MAX_CHANNEL_ID;
    }

    public int getChid() {
        return chid;
    }

    public void setSeqId(int seqId) {
        this.seqId = seqId & TfsPacketConstant.MAX_SEQ_ID;
    }

    public int getSeqId() {
        return seqId;
    }

    public int getLen() {
        return len;
    }

    public void setLen(int len) {
        this.len = len;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public String toString() {
        return "basepacket: chid=" + chid + ", pcode=" + pcode + ", len=" + len;
    }

    /**
     *
     * @return the exception
     */
    public Exception getException() {
        return exception;
    }

    /**
     *
     * @param exception the exception to set
     */
    public void setException(Exception exception) {
        this.exception = exception;
    }


}
