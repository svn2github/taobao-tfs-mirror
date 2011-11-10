/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.rc;

import java.util.List;
import java.util.ArrayList;

import java.nio.ByteBuffer;

import com.taobao.common.tfs.common.TfsUtil;
import com.taobao.common.tfs.common.StreamTranscoderUtil;

public class RcBaseInfo {
    private int reportInterval = 0;
    private List<Long> serverAddrs;
    private List<RcClusterRackInfo> clusterRackInfos;

    public int getReportInterval() {
        return reportInterval;
    }

    public void setReportInterval(int reportInterval) {
        this.reportInterval = reportInterval;
    }

    public List<Long> getServerAddrs() {
        return serverAddrs;
    }

    public void setServerAddrs(List<Long> serverAddrs) {
        this.serverAddrs = serverAddrs;
    }

    public List<RcClusterRackInfo> getClusterRackInfos() {
        return clusterRackInfos;
    }

    public void setClusterRackInfos(List<RcClusterRackInfo> clusterRackInfos) {
        this.clusterRackInfos = clusterRackInfos;
    }

    public boolean decode(ByteBuffer byteBuffer) {
        serverAddrs = StreamTranscoderUtil.getVL(byteBuffer);

        int clusterRackInfoSize = byteBuffer.getInt();
        clusterRackInfos = new ArrayList<RcClusterRackInfo>(clusterRackInfoSize);
        for (int i = 0; i < clusterRackInfoSize; i++) {
            RcClusterRackInfo clusterRackInfo = new RcClusterRackInfo();
            clusterRackInfo.decode(byteBuffer);
            clusterRackInfos.add(clusterRackInfo);
        }
        reportInterval = byteBuffer.getInt();
        return true;
    }

    public String toString() {
        String str = "reportInterval: " + reportInterval + "\nserverList: ";
        if (serverAddrs != null) {
            str += serverAddrs.size();
            for (Long serverAddr : serverAddrs) {
                str += " " + TfsUtil.longToHost(serverAddr);
            }
        } else {
            str += "NONE";
        }

        str += "\nclusterRackInfo: ";
        if (clusterRackInfos != null && clusterRackInfos.size() != 0) {
            for (RcClusterRackInfo clusterRackInfo : clusterRackInfos) {
                str += "\n" + clusterRackInfo;
            }
        } else {
            str += "NONE";
        }
        return str;
    }
}
