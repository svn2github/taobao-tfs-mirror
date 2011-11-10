/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.namemeta;

import java.util.List;
import java.util.ArrayList;
import java.nio.ByteBuffer;

import com.taobao.common.tfs.common.TfsConstant;

public class FragInfo {
    private int clusterId;
    private List<FragMeta> fragMetaList = new ArrayList<FragMeta>();

    public void setClusterId(int clusterId) {
        this.clusterId = clusterId;
    }

    public int getClusterId() {
        return clusterId;
    }

    public void setFragMetaList(List<FragMeta> fragMetaList) {
        this.fragMetaList = fragMetaList;
    }

    public List<FragMeta> getFragMetaList() {
        return fragMetaList;
    }

    public void merge(FragInfo fragInfo) {
        if (fragInfo != null || clusterId == fragInfo.getClusterId()) {
            fragMetaList.addAll(fragInfo.getFragMetaList());
        }
    }

    public void add(FragMeta fragMeta) {
        if (fragMeta != null) {
            fragMetaList.add(fragMeta);        
        }
    }

    public void clear() {
        fragMetaList.clear();        
    }

    public long getLength() {
        int size = fragMetaList.size();
        if (size > 0) {
            return fragMetaList.get(size - 1).getOffset() + fragMetaList.get(size - 1).getLength();
        }
        return 0;
    }

    public long getOffset() {
        if (fragMetaList.size() > 0) {
            return fragMetaList.get(0).getOffset();
        }
        return 0;
    }

    public int length() {
        return TfsConstant.INT_SIZE * 2 +
            fragMetaList.size() * FragMeta.length();
    }

    public boolean serialize(ByteBuffer byteBuffer) {
        byteBuffer.putInt(clusterId);
        byteBuffer.putInt(fragMetaList.size());
        for (FragMeta fragMeta : fragMetaList) {
            fragMeta.serialize(byteBuffer);
        }

        return true;
    }

    public boolean deserialize(ByteBuffer byteBuffer) {
        clusterId = byteBuffer.getInt();

        int size = byteBuffer.getInt();
        if (size * FragMeta.length() > byteBuffer.remaining()) {
            return false;
        }

        for (int i = 0; i < size; i++) {
            FragMeta fragMeta = new FragMeta();
            fragMeta.deserialize(byteBuffer);
            fragMetaList.add(fragMeta);
        }
        return true;
    }

    public String toString() {
        String str =  "clusterId: " + clusterId + " fragMeta count: " + fragMetaList.size();
        for (FragMeta fragMeta : fragMetaList) {
            str += "[" + fragMeta + "]";
        }
        return str;
    }
}