/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.packet;

import java.util.List;
import java.util.ArrayList;

import com.taobao.common.tfs.common.TfsConstant;
import com.taobao.common.tfs.common.StreamTranscoderUtil;

public class BatchGetBlockInfoMessage extends BasePacket {
    private int mode;    
    private int blockCount;
    List<Integer> blockIds;

    public BatchGetBlockInfoMessage(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TfsConstant.BATCH_GET_BLOCK_INFO_MESSAGE;
        mode = 0;
        blockIds = new ArrayList<Integer>();
    }

    public int getPacketLength() {
        return ((mode & TfsConstant.WRITE_MODE) != 0) ? TfsConstant.INT_SIZE * 2 : TfsConstant.INT_SIZE * (2 + blockIds.size()); 
    }

    public void writePacketStream() {
        byteBuffer.putInt(mode);
        if ((mode & TfsConstant.WRITE_MODE) != 0) {
            byteBuffer.putInt(blockCount);
        } else {
            StreamTranscoderUtil.putV(byteBuffer, blockIds);
        }
    }

    public int getMode() {
        return mode;
    }

    public void setMode(int mode) {
        this.mode = mode;
    }

    public int getBlockCount() {
        return blockCount;
    }

    public void setBlockCount(int blockCount) {
        this.blockCount = blockCount;
    }

    public void addBlockId(int blockId) {
        this.blockIds.add(blockId);        
    }

    public List<Integer> getBlockId() {
        return blockIds;        
    }

    public void setBlockId(List<Integer> blockIds) {
        this.blockIds = blockIds;
    }

}
