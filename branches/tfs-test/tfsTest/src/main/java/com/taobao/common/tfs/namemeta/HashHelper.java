/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.namemeta;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.taobao.common.tfs.common.TfsConstant;
import com.taobao.common.tfs.common.StreamTranscoderUtil;

public class HashHelper {
    private int length = TfsConstant.LONG_SIZE*2;
    private ByteBuffer byteBuffer;

    public HashHelper(long appId, long userId) {
    	byteBuffer.order(ByteOrder.BIG_ENDIAN);
        byteBuffer.putLong(appId);
        byteBuffer.putLong(userId);
    }
    
    public byte[] getByteArray()
    {
    	return StreamTranscoderUtil.getByteArray(byteBuffer, length);
    }
    
    public int length()
    {
    	return this.length;
    }
    
}