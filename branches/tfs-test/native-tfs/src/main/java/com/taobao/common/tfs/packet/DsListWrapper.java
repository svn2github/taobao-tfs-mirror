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
import java.util.List;
import java.util.ArrayList;

import com.taobao.common.tfs.common.TfsConstant;
import com.taobao.common.tfs.common.StreamTranscoderUtil;

public class DsListWrapper implements TfsPacketObject {
    private int version;
    private int leaseId;
    private boolean hasLease;
    private List<Long> dsList;


    public DsListWrapper() {
        super();
        version = 0;
        leaseId = 0;
        hasLease = false;
    }

    public DsListWrapper(List<Long> dsList) {
        super();
        version = 0;
        leaseId = 0;
        hasLease = false;
        this.dsList = dsList;
    }

    public DsListWrapper(int version, int leaseId, boolean hasLease,
                         List<Long> dsList) {
        super();
        this.version = version;
        this.leaseId = leaseId;
        this.hasLease = hasLease;
        this.dsList = dsList;
    }

    public int getVersion() {
        return version;
    }
    public void setVersion(int version) {
        this.version = version;
    }
    public int getLeaseId() {
        return leaseId;
    }
    public void setLeaseId(int leaseId) {
        this.leaseId = leaseId;
    }
    public boolean isHasLease() {
        return hasLease;
    }
    public void setHasLease(boolean hasLease) {
        this.hasLease = hasLease;
    }
    public List<Long> getDsList() {
        return dsList;
    }
    public void setDsList(List<Long> dsList) {
        this.dsList = dsList;
    }

    public boolean parseV13DsList(List<Long> dsList) {
        if (dsList.size() > 3) {
            int flagIndex = dsList.size() - 3;
            if (dsList.get(flagIndex) == -1) {
                this.version = dsList.get(flagIndex + 1).intValue();
                this.leaseId = dsList.get(flagIndex + 2).intValue();
                this.hasLease = true;
                this.dsList = new ArrayList<Long>();
                for (int i = 0; i < flagIndex; ++i) {
                    this.dsList.add(dsList.get(i));
                }
                return true;
            }
        }
        this.dsList = dsList;
        return false;
    }

    public List<Long> getSendList() {
        List<Long> ret = new ArrayList<Long>(this.dsList);
        if (this.hasLease) {
            ret.add(new Long(-1));
            ret.add(new Long(version));
            ret.add(new Long(leaseId));
        }
        return ret;
    }

    public void writeToStream(ByteBuffer byteBuffer) {
        StreamTranscoderUtil.putVL(byteBuffer, getSendList());
    }

    public void readFromStream(ByteBuffer byteBuffer) {
        List<Long> dsList = StreamTranscoderUtil.getVL(byteBuffer);
        parseV13DsList(dsList);
    }

    public int streamLength() {
        int length = this.dsList.size() * TfsConstant.LONG_SIZE;
        if (this.hasLease) {
            length += TfsConstant.LONG_SIZE * 3;
        }
        return length;
    }


}
