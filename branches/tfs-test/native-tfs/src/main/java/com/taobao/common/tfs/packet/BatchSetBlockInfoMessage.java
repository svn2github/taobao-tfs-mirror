/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.packet;

import java.util.Map;
import java.util.HashMap;

import com.taobao.common.tfs.common.TfsConstant;

public class BatchSetBlockInfoMessage extends BasePacket {
    private Map<Integer, DsListWrapper> blockDsMap;

    public BatchSetBlockInfoMessage(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TfsConstant.BATCH_SET_BLOCK_INFO_MESSAGE;
        blockDsMap = new HashMap<Integer, DsListWrapper>();
    }

    public boolean decode() {
        int count = byteBuffer.getInt();
        for (int i = 0; i < count; i++) {
            int blockId = byteBuffer.getInt();
            DsListWrapper dsListWrapper = new DsListWrapper();
            dsListWrapper.readFromStream(byteBuffer);
            blockDsMap.put(blockId, dsListWrapper);
        }
        return true;
    }

    public Map<Integer, DsListWrapper> getBlockDsMap() {
        return blockDsMap;
    }
}
