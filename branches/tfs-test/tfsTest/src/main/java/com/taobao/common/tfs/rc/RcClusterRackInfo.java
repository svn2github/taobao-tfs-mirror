/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.rc;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.ArrayList;

import com.taobao.common.tfs.common.StreamTranscoderUtil;

public class RcClusterRackInfo {

    public class DuplicateInfo {
        private String masterAddr;
        private String slaveAddr;
        private String groupName;
        private int namespace;

        public String getMasterAddr() {
            return masterAddr;
        }

        public void setMaserAddr(String masterAddr) {
            this.masterAddr = masterAddr;
        }

        public String getSlaveAddr() {
            return slaveAddr;
        }

        public void setSlaveAddr(String slaveAddr) {
            this.slaveAddr = slaveAddr;
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

        public boolean decode(ByteBuffer byteBuffer) {
            String str = StreamTranscoderUtil.getString(byteBuffer);
            String[] info = str.split(";");
            if (info.length != 4) {
                return false;
            }
            masterAddr = info[0];
            slaveAddr = info[1].length() == 0 ? info[0] : info[1];
            groupName = info[2];
            namespace = Integer.parseInt(info[3]);
            return true;
        }

        public String toString() {
            return "masterAddr: " + masterAddr + " slaveAddr: " + slaveAddr +
                " group: " + groupName + " namespace: " + namespace;
        }

    }

    public class RcClusterInfo {
        private int clusterStat;
        private int accessType;
        private boolean isMaster;
        private int clusterId;
        private String vipAddr;

        public int getClusterStat() {
            return clusterStat;
        }

        public void setClusterStat(int clusterStat) {
            this.clusterStat = clusterStat;            
        }

        public int getAccessType() {
            return accessType;
        }

        public void setAccessType(int accessType) {
            this.accessType = accessType;
        }

        public int getClusterId() {
            return clusterId;
        }

        public void setClusterId(int clusterId) {
            this.clusterId = clusterId;            
        }

        public boolean getIsMaster() {
            return isMaster;
        }

        public void setIsMaster(boolean isMaster) {
            this.isMaster = isMaster;
        }

        public String getVipAddr() {
            return vipAddr;
        }

        public void setVipAddr(String vipAddr) {
            this.vipAddr = vipAddr;            
        }

        public boolean decode(ByteBuffer byteBuffer) {
            clusterStat = byteBuffer.getInt();
            accessType = byteBuffer.getInt();
            parseClusterId(StreamTranscoderUtil.getString(byteBuffer));
            vipAddr = StreamTranscoderUtil.getString(byteBuffer);
            return true;       
        }

        public String toString() {
            return "clusterStat: " + clusterStat + " accessType: " + accessType +
                " clusterId: " + clusterId + " isMaster: " + isMaster + " vipAddr: " + vipAddr;
        }

        private boolean parseClusterId(String clusterIdStr) {
            // like T1M T2B ...
            if (clusterIdStr == null) {
                return false;
            }
            if (clusterIdStr.length() < 3) {
                return false;
            }

            clusterId = Integer.parseInt(clusterIdStr.substring(1,2));
            isMaster = (clusterIdStr.charAt(2) == 'M' || clusterIdStr.charAt(2) == 'm');
            return true;
        }
    }

    private boolean isDuplicate = false;
    private DuplicateInfo duplicateInfo = null;
    private List<RcClusterInfo> clusterInfos = null;

    public static int CLUSTER_ACCESS_TYPE_READ_ONLY = 1;
    public static int CLUSTER_ACCESS_TYPE_READ_WRITE = 2;

    public boolean getIsDuplicate() {
        return isDuplicate;        
    }

    public void setIsDuplicate(boolean isDuplicate) {
        this.isDuplicate = isDuplicate;
    }

    public DuplicateInfo getDuplicateInfo() {
        return duplicateInfo;
    }

    public void setDuplicateInfo(DuplicateInfo duplicateInfo) {
        this.duplicateInfo = duplicateInfo;        
    }

    public List<RcClusterInfo> getClusterInfos() {
        return clusterInfos;
    }

    public void setClusterInfos(List<RcClusterInfo> clusterInfos) {
        this.clusterInfos = clusterInfos;
    }

    public boolean decode(ByteBuffer byteBuffer) {
        isDuplicate = (int)byteBuffer.get() != 0 ? true : false;
        if (isDuplicate) {
            duplicateInfo = new DuplicateInfo();
            duplicateInfo.decode(byteBuffer);
        }

        int clusterInfoSize = byteBuffer.getInt();
        clusterInfos = new ArrayList<RcClusterInfo>(clusterInfoSize);
        for (int i = 0; i < clusterInfoSize; i++) {
            RcClusterInfo clusterInfo = new RcClusterInfo();
            clusterInfo.decode(byteBuffer);
            clusterInfos.add(clusterInfo);
        }
        return true;
    }

    public String toString() {
        String str = "isDuplicate: " + isDuplicate +
            " duplicateInfo: " + ((isDuplicate && duplicateInfo != null) ? duplicateInfo : "NONE") +
            "\nclusterInfo: ";

        if (clusterInfos != null) {
            str += clusterInfos.size();
            for (RcClusterInfo clusterInfo : clusterInfos) {
                str += "\n" + clusterInfo;
            }
        } else {
            str += "NONE";
        }
        return str;
    }
}
