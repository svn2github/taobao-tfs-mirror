/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.namemeta;

import java.util.ArrayList;

import com.taobao.common.tfs.common.TfsUtil;

public class MetaTable {
    private long version;
    private ArrayList<Long> table = new ArrayList<Long>();

    public void dump() {
		System.out.println("meta table version: " + version + ", size: " + table.size());
		for (int i = 0; i < table.size(); i++) {
			System.out.println("index: " + i + ", metaServer: " + TfsUtil.longToHost(table.get(i)));
		}
    }
    
    public void clear() {
    	table.clear();
    	version = 0;
    }
    
    public void setVersion(long version) {
        this.version = version;
    }

    public long getVersion() {
        return version;
    }
    
    public int getTableSize()
    {
    	return table.size();
    }
    
    public long getMetaServerId(int bucketId)
    {
    	return table.get(bucketId);
    }
    
    public void push(long metaServerId)
    {
    	table.add(metaServerId);
    }
}