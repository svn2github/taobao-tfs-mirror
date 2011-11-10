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

public interface TfsPacketObject {
    public void writeToStream(ByteBuffer byteBuffer);
    public void readFromStream(ByteBuffer byteBuffer);
    public int streamLength();


}
