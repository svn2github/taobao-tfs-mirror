/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.unique;

import com.taobao.common.tfs.TfsException;
import com.taobao.common.tfs.unique.UniqueValue;

public interface UniqueStore {
    public void init();

    public void reset();

    public byte[] getKey(byte[] data, int offset, int length);

    public UniqueValue query(byte[] key) throws TfsException;

    public UniqueValue insert(byte[] key, UniqueValue value)  throws TfsException;

    public UniqueValue delete(byte[] key)  throws TfsException;

    public UniqueValue increment(byte[] key, UniqueValue value)  throws TfsException;

    public UniqueValue decrement(byte[] key, UniqueValue value)  throws TfsException;
}
