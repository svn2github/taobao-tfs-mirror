/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.unique;

import java.nio.ByteBuffer;

/**
 * Deprecated Class
 *
 */
public class TfsUniqueKey {
    
    public static class Pair {
        public int crc;
        public int size;
        public Pair(int crc, int size) {
            super();
            this.crc = crc;
            this.size = size;
        }
        public Pair() {
            
        }
    }

    public static byte[] encode(int crc, int size) {
        ByteBuffer bf = ByteBuffer.allocate(64);
        bf.putInt(crc);
        bf.putInt(size);
        return bf.array();
    }
    
    public static Pair decode(byte[] array) {
        ByteBuffer bf = ByteBuffer.wrap(array);
        Pair pair = new Pair();
        pair.crc = bf.getInt();
        pair.size = bf.getInt();
        return pair;
    }
    
}
