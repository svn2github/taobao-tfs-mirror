/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.packet;

public class TfsPacketConstant {
    public static final int MAX_SEQ_ID = 0xff;
    public static final int MAX_CHANNEL_ID = 0xffffff;

    public static final int TFS_PACKET_HEADER_SIZE = 24;
    public static final int TFS_PACKET_HEADER_BLPOS = 4;
    public static final int TFS_PACKET_HEADER_CRCPOS = 20;
    public static final int TFS_PACKET_VERSION = 2;

    // buffer size
    public static final int INOUT_BUFFER_SIZE = 8192;

}
