/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.unique;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.io.*;
import java.nio.ByteBuffer;

import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.TairManager;
import com.taobao.tair.impl.DefaultTairManager;

import com.taobao.common.tfs.common.TfsConstant;
import com.taobao.common.tfs.TfsException;

public class TairUniqueStore implements UniqueStore {
    private int namespace;
    private TairManager tairManager;
    private List<String> configServerList;
    private String groupName;

    public List<String> getConfigServerList() {
        return configServerList;
    }

    public void setConfigServerList(List<String> configServerList) {
        this.configServerList = configServerList;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public int getNamespace() {
        return namespace;
    }

    public void setNamespace(int namespace) {
        this.namespace = namespace;
    }

    public TairManager getTairManager() {
        return tairManager;
    }

    public void setTairManager(TairManager tairManager) {
        this.tairManager = tairManager;
    }

    public synchronized void init() {
        if (tairManager == null) {
            tairManager = new DefaultTairManager();
            ((DefaultTairManager)tairManager).setConfigServerList(configServerList);
            ((DefaultTairManager)tairManager).setGroupName(groupName);
            ((DefaultTairManager)tairManager).init();
        }
    }

    public synchronized void reset() {
        TairManager tairManager = new DefaultTairManager();
        ((DefaultTairManager)tairManager).setConfigServerList(configServerList);
        ((DefaultTairManager)tairManager).setGroupName(groupName);
        ((DefaultTairManager)tairManager).init();
        this.tairManager = tairManager;
    }

    public UniqueValue decrement(byte[] key, UniqueValue value) throws TfsException {
        if (value.getReferenceCount() <= 0) {
            throw new TfsException("Referencecount invalid error: " + value.getReferenceCount());
        }
        value.subRef(1);
        try {
            if (value.getReferenceCount() == 0) {
                return delete (key);
            } else {
                return insert(key, value);
            }
        } catch (TfsException e) {
            throw new TfsException("TAIR delete failed" + e.getMessage());
        }
    }

    public UniqueValue delete(byte[] key) throws TfsException {
        ResultCode code = tairManager.delete(this.namespace, key);

        if (code.isSuccess()) {
            return new UniqueValue("", 0);
        } else {
            throw new TfsException("TAIR delete failed:" + code.getMessage());
        }
    }

    public byte[] getKey(byte[] data, int offset, int length) {
        try {
            MessageDigest algo = MessageDigest.getInstance("MD5");
            algo.update(data, offset, length);
            ByteBuffer bf = ByteBuffer.allocate(4 + algo.getDigestLength()); // algo.getDigestLength() == 16
            bf.put(algo.digest());
            bf.putInt(algo.getDigestLength(), length);

            return bf.array();
        } catch (NoSuchAlgorithmException e) {
        }
        return null;
    }

    public UniqueValue increment(byte[] key, UniqueValue value) throws TfsException {
        value.addRef(1);
        try {
            return insert(key, value);
        } catch (TfsException e) {
            throw new TfsException("TAIR increment failed:" + e.getMessage());
        }
    }

    public UniqueValue insert(byte[] key, UniqueValue value) throws TfsException {
        int i = 0;
        ResultCode code;
        do {
            i++;
            code = tairManager.put(this.namespace, key, encode(value), value.getVersion());
        } while (i < 3 && (! code.isSuccess()) && (! code.equals(ResultCode.VERERROR)));

        if (code.isSuccess()) {
            return value;
        } else if (code.equals(ResultCode.VERERROR)) {
            value.setReferenceCount(TfsConstant.UNIQUE_INSERT_VERSION_ERROR);
            return value;
        } else {
            throw new TfsException("TAIR insert failed:" + code.getMessage());
        }
    }

    public UniqueValue query(byte[] key) throws TfsException {
        UniqueValue value = null;
        DataEntry data = null;

        Result<DataEntry> ret = tairManager.get(this.namespace, key);
        if (ret.getRc().isSuccess()) {
            if (ret.getRc() == ResultCode.DATANOTEXSITS) {
                value = new UniqueValue("", TfsConstant.UNIQUE_QUERY_NOT_EXIST);
            } else if ((data = ret.getValue()) != null) {
                value =  decode((byte[])data.getValue());
                value.setVersion(data.getVersion());
            }
        } else {
            throw new TfsException("TAIR query failed" + ret.getRc().getMessage());
        }
        return value;
    }

    public UniqueValue decode(byte[] data) {
        ByteBuffer bf = ByteBuffer.wrap(data);
        int tfsNameLen = data.length - 4;
        byte[] tfsName = new byte[tfsNameLen];

        UniqueValue value = new UniqueValue();
        value.setReferenceCount(bf.getInt());
        bf.get(tfsName, 0, tfsNameLen);
        value.setTfsName(new String(tfsName));
        return value;
    }

    public byte[] encode(UniqueValue value) {
        ByteBuffer bf = ByteBuffer.allocate(4 + value.getTfsName().length());
        bf.putInt(value.getReferenceCount());
        try {
            bf.put(value.getTfsName().getBytes("US-ASCII"));
        } catch (UnsupportedEncodingException e) {
        }
        return bf.array();
    }


}
