/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.rc;

import java.util.Timer;
import java.util.TimerTask;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReentrantLock;
import java.io.OutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.common.tfs.TfsException;
import com.taobao.common.tfs.common.TfsConstant;
import com.taobao.common.tfs.common.TfsUtil;
import com.taobao.common.tfs.packet.FileInfo;
import com.taobao.common.tfs.packet.UnlinkFileMessage;
import com.taobao.common.tfs.packet.RespRcLoginMessage;
import com.taobao.common.tfs.packet.RespRcKeepAliveMessage;
import com.taobao.common.tfs.rc.RcClusterRackInfo.DuplicateInfo;
import com.taobao.common.tfs.rc.RcClusterRackInfo.RcClusterInfo;
import com.taobao.common.tfs.impl.ClientConfig;
import com.taobao.common.tfs.impl.TfsManagerLite;
import com.taobao.common.tfs.impl.FSName;
import com.taobao.common.tfs.impl.TfsSession;
import com.taobao.common.tfs.unique.UniqueStore;
import com.taobao.common.tfs.unique.TairUniqueStore;
import com.taobao.common.tfs.namemeta.FragMeta;

// RcManager is AppId level instance.
// One AppId need one RcManager instance.
public class RcManager {

    private class KeepAliveTask extends TimerTask {
        public void run() {
            log.debug("rc keepalive run.");
            RcKeepAliveInfo kaInfo = getKaInfo();
            kaInfo.getSessionStatInfo().setCacheHitRatio(getCacheHitRatio());
            RespRcKeepAliveMessage rrkam = null;
            try {
                rrkam = RcSessionHelper.keepAlive(activeRcAddr, kaInfo);
            } catch (TfsException e) {
                log.warn("keep alive fail.", e);
                retryNextActiveRc();
                rollBackStatInfo(kaInfo.getSessionStatInfo());
                return;
            }

            log.debug("rc keepalive success, isupdate: " + rrkam.getIsUpdate());

            if (rrkam.getIsUpdate()) {
                log.warn("keepalive update baseinfo. " + rrkam.getBaseInfo());
                long oldReportInterval = baseInfo.getReportInterval();
                // update new base info
                updateBaseInfo(rrkam.getBaseInfo());
                // update new cluster info
                updateNsInfo();
                if (oldReportInterval != baseInfo.getReportInterval()) {
                    reScheduleKeepAlive(0);
                }
            }
        }
    }

    private static final Log log = LogFactory.getLog(RcManager.class);

    // tfsmanager lite implement
    TfsManagerLite tfsManager = new TfsManagerLite();

    // unique store manager
    private boolean isDuplicate = false;
    UniqueStore uniqueStore = null;

    // manager status
    private static final int MANAGER_NOT_INIT = 0;
    private static final int MANAGER_INITED = 1;
    private int status = MANAGER_NOT_INIT;

    // mutex lock
    // for atomic sessionStat update
    private ReentrantLock lock = new ReentrantLock();

    // keepalive timer
    private Timer timer = null;

    // current active rc server address
    // convenient use, actually is baseInfo.getServerAddrs().get(currentActiveRcIndex)
    private long activeRcAddr = 0;
    // current active rc server index in server list
    private int currentActiveRcIndex = 0;

    // writable ns address
    // master && slave
    private String[] writableNs = new String[2];
    // readable ns address
    // master && slave
    private Map<Integer, String>[] readableChoice = new HashMap[2];

    // local address(with reverse byte sequence). calculate ns distance
    private int localIp = 0;

    // rc baseinfo. rc server address, cluster info
    private RcBaseInfo baseInfo;
    // rc session baseinfo
    // sessionId, clientVersion, cache config, modifytime etc.
    private RcSessionBaseInfo sessionBaseInfo = new RcSessionBaseInfo();
    // rc session statinfo.
    // cache hit ratio, sort of client operate statistics
    private RcSessionStatInfo sessionStatInfo = new RcSessionStatInfo();

    // init stuff
    private String rcAddr = null;
    private long appId;
    private String appIp = null;

    public String getRcAddr() {
        return rcAddr;
    }

    public void setRcAddr(String rcAddr) {
        this.rcAddr = rcAddr;
        this.activeRcAddr = TfsUtil.hostToLong(rcAddr);
    }

    public long getAppId() {
        return appId;
    }

    public void setAppId(long appId) {
        this.appId = appId;
    }

    public String getAppIp() {
        return appIp;
    }

    public void setAppIp(String appIp) {
        this.appIp = appIp;
    }

    public synchronized boolean init() {
        if (status != MANAGER_NOT_INIT) {
            log.warn("rc manager is already inited");
            return true;
        }

        localIp = Integer.reverseBytes(TfsUtil.getLocalIp());

        if (!checkConfig()) {
            return false;
        }

        for (int i = 0; i < 2; i++) {
            if (readableChoice[i] == null) {
                readableChoice[i] = new HashMap<Integer, String>();
            }
        }

        log.warn("init RcManager: " + dumpConfig());

        if (login()) {
            tfsManager.init();
            reScheduleKeepAlive(5000); // wait 5s
            status = MANAGER_INITED;
            return true;
        }
        return false;
    }

    public synchronized boolean destroy() {
        if (timer != null) {
            timer.cancel();
            timer = null;
        }
        return logout();
    }

    // local file
    public String saveFile(String localFileName, String tfsFileName, String tfsSuffix,
                           boolean simpleName) {
        if (!checkInit()) {
            return null;
        }
        if (localFileName == null) {
            log.error("local file is null");
            return null;
        }

        String ret = null;
        String nsAddr = null;
        int retry = 0;
        long startTime = TfsUtil.currentTimeMicros();

        FileInputStream input = null;
        long length = -1;
        int type = RcAppOperInfo.RC_OPER_WRITE;
        try {
            input = new FileInputStream(localFileName);
            length = input.getChannel().size();
            if (length <= 0) {
                log.error("local file is empty: " + localFileName);
                return null;
            }

            do {
                nsAddr = getWriteNsAddr(tfsFileName, retry++);

                log.debug("@@ save get ns addr " + retry + " " + nsAddr);
                if (nsAddr == null) {
                    break;
                }

                if (isDuplicate) {
                    ret = tfsManager.
                        saveUniqueFile(uniqueStore, nsAddr, tfsFileName, tfsSuffix, input, (int)length, simpleName);
                    type = RcAppOperInfo.RC_OPER_UNIQUE_WRITE;
                } else {
                    ret = tfsManager.
                        saveFile(TfsManagerLite.TFS_SMALL_FILE_TYPE, nsAddr, tfsFileName, tfsSuffix,
                                 input, length, simpleName, localFileName);
                }
            } while (ret == null);
        } catch (Exception e) {
            log.error("local file exception.", e);
            return null;
        } finally {
            try {
                if (input != null) {
                    input.close();
                }
            } catch (IOException e) {
                log.error("close local file fail ", e);
            }
        }

        addStatInfo(type, length,
                    TfsUtil.currentTimeMicros() - startTime, ret != null);
                    
        return ret;
    }

    // byte[]
    public String saveFile(String tfsFileName, String tfsSuffix,
                           byte[] data, int offset, int length,
                           boolean simpleName) {
        if (!checkInit()) {
            return null;
        }
        if (data == null) {
            log.error("null data");
            return null;
        }
        if (offset < 0 || length <= 0) {
            log.error("invalid offset: " + offset + " or length: " + length);
            return null;
        }

        String ret = null;
        String nsAddr = null;
        int retry = 0;
        long startTime = TfsUtil.currentTimeMicros();
        int type = RcAppOperInfo.RC_OPER_WRITE;
        do {
            nsAddr = getWriteNsAddr(tfsFileName, retry++);

            if (nsAddr == null) {
                break;
            }

            if (isDuplicate) {
                ret = tfsManager.
                    saveUniqueFile(uniqueStore, nsAddr, tfsFileName, tfsSuffix, data, offset, length, simpleName);
                type = RcAppOperInfo.RC_OPER_UNIQUE_WRITE;
            } else {
                ret = tfsManager.
                    saveFile(TfsManagerLite.TFS_SMALL_FILE_TYPE, nsAddr, tfsFileName, tfsSuffix,
                             data, offset, length, simpleName, null);
            }
        } while (ret == null);

        addStatInfo(type, length,
                    TfsUtil.currentTimeMicros() - startTime, ret != null);

        return ret;
    }

    // large localfile 
    public String saveLargeFile(String localFileName, String tfsFileName, String tfsSuffix) {
        if (!checkInit()) {
            return null;
        }

        if (localFileName == null) {
            log.error("local file is null");
            return null;
        }

        String ret = null;
        String nsAddr = null;
        int retry = 0;
        long startTime = TfsUtil.currentTimeMicros();
        FileInputStream input = null;
        long length = 0;

        try {
            input = new FileInputStream(localFileName);
            length = input.getChannel().size();
            if (length <= 0) {
                log.error("local file is empty: " + localFileName);
                return null;
            }

            do {
                nsAddr = getWriteNsAddr(tfsFileName, retry++);

                if (nsAddr == null) {
                    break;
                }

                // large file not support unique store. no check isDuplicate
                ret = tfsManager.saveFile(TfsManagerLite.TFS_LARGE_FILE_TYPE, nsAddr, tfsFileName, tfsSuffix,
                                          input, length, false, localFileName);
            } while (ret == null);
        } catch (Exception e) {
            log.error("local file exception.", e);
            return null;
        } finally {
            try {
                if (input != null) {
                    input.close();
                }
            } catch (IOException e) {
                log.error("close local file fail ", e);
            }
        }

        addStatInfo(RcAppOperInfo.RC_OPER_WRITE, length,
                    TfsUtil.currentTimeMicros() - startTime, ret != null);

        return ret;
    }

    // large byte[]
    public String saveLargeFile(String tfsFileName, String tfsSuffix,
                                byte[] data, int offset, int length, String key) {
        if (!checkInit()) {
            return null;
        }
        if (data == null) {
            log.error("null data");
            return null;
        }
        if (offset < 0 || length <= 0) {
            log.error("invalid offset: " + offset + " or length: " + length);
            return null;
        }
        if (key == null) {
            log.error("null key");
            return null;
        }

        String ret = null;
        String nsAddr = null;
        int retry = 0;
        long startTime = TfsUtil.currentTimeMicros();
        do {
            nsAddr = getWriteNsAddr(tfsFileName, retry++);

            if (nsAddr == null) {
                break;
            }

            // large file not support unique store. no check isDuplicate
            ret = tfsManager.saveFile(TfsManagerLite.TFS_LARGE_FILE_TYPE, nsAddr, tfsFileName, tfsSuffix,
                                      data, offset, length, false, key);
        } while (ret == null);

        addStatInfo(RcAppOperInfo.RC_OPER_WRITE, length,
                    TfsUtil.currentTimeMicros() - startTime, ret != null);

        return ret;
    }

    // stat file
    public FileInfo statFile(String tfsFileName, String tfsSuffix) {
        if (!checkInit()) {
            return null;
        }
        FileInfo ret = null;
        String nsAddr = null;
        int retry = 0;
        do {
            nsAddr = getReadNsAddr(tfsFileName, retry++);

            if (nsAddr == null) {
                break;
            }

            ret = tfsManager.statFile(nsAddr, tfsFileName, tfsSuffix);
        } while (ret == null);

        return ret;
    }

    public boolean unlinkFile(String tfsFileName, String tfsSuffix, int action) {
        if (!checkInit()) {
            return false;
        }

        long retLength = -1;
        String nsAddr = null;
        int retry = 0;
        long startTime = TfsUtil.currentTimeMicros();
        int type = RcAppOperInfo.RC_OPER_UNLINK;

        do {
            nsAddr = getReadNsAddr(tfsFileName, retry++);

            if (nsAddr == null) {
                break;
            }

            if (isDuplicate && action == UnlinkFileMessage.DELETE) {
                retLength = tfsManager.
                    unlinkUniqueFile(uniqueStore, nsAddr, tfsFileName, tfsSuffix);
                type = RcAppOperInfo.RC_OPER_UNIQUE_UNLINK;
            } else {
                retLength = tfsManager.
                    unlinkFile(nsAddr, tfsFileName, tfsSuffix, action);
            }
            log.debug("@@ unlink ret length " + retLength);
        } while (retLength < 0);

        boolean ret = retLength >= 0;
        if (ret) {
            // REVEAL and CONCEAL have no effect on capacity
            if (action == UnlinkFileMessage.CONCEAL || action == UnlinkFileMessage.REVEAL) {
                retLength = 0;
            } else if (action == UnlinkFileMessage.UNDELETE) { // UNDELETE rollback capacity
                retLength = 0 - retLength;
            }
        }

        addStatInfo(type, retLength,
                    TfsUtil.currentTimeMicros() - startTime, ret);

        return ret;
    }

    // delete file
    public boolean unlinkFile(String tfsFileName, String tfsSuffix) {
        return unlinkFile(tfsFileName, tfsSuffix, UnlinkFileMessage.DELETE);
    }

    // hide[unhide] file
    public boolean hideFile(String tfsFileName, String tfsSuffix, int option) {
        int action = 0;
        if (option == 0) {
            action = UnlinkFileMessage.REVEAL;
        } else if (option == 1) {
            action = UnlinkFileMessage.CONCEAL;
        } else {
            log.error("invalid tfs hide option");
            return false;
        }

        return unlinkFile(tfsFileName, tfsSuffix, action);
    }

    // fetch localfile
    public boolean fetchFile(String tfsFileName, String tfsSuffix, String localFileName) {
        if (!checkInit()) {
            return false;
        }
        if (localFileName == null) {
            log.error("local file is null");
            return false;
        }

        long retLength = -1;
        String nsAddr = null;
        int retry = 0;
        long startTime = TfsUtil.currentTimeMicros();

        do {
            nsAddr = getReadNsAddr(tfsFileName, retry++);

            if (nsAddr == null) {
                break;
            }

            retLength = tfsManager.fetchFile(nsAddr, tfsFileName, tfsSuffix, localFileName);
        } while (retLength < 0);

        boolean ret = retLength >= 0;
        addStatInfo(RcAppOperInfo.RC_OPER_READ, retLength,
                    TfsUtil.currentTimeMicros() - startTime, ret);

        return ret;
    }

    // fetch outputstream
    public boolean fetchFile(String tfsFileName, String tfsSuffix,
                             long fileOffset, long length, OutputStream output) {
        if (!checkInit()) {
            return false;
        }
        if (output == null) {
            log.error("output stream is null");
            return false;
        }
        if (fileOffset < 0 || length <= 0) {
            log.error("invalid fileoffset: " + fileOffset + " or length: " + length);
            return false;
        }

        long retLength = -1;
        String nsAddr = null;
        int retry = 0;
        long startTime = TfsUtil.currentTimeMicros();
        do {
            nsAddr = getReadNsAddr(tfsFileName, retry++);

            if (nsAddr == null) {
                break;
            }

            retLength = tfsManager.fetchFile(nsAddr, tfsFileName, tfsSuffix, fileOffset, length, output);
        } while (retLength < 0);

        boolean ret = retLength >= 0;
        addStatInfo(RcAppOperInfo.RC_OPER_READ, retLength,
                    TfsUtil.currentTimeMicros() - startTime, ret);

        return ret;
    }

    // open file
    public int openFile(String tfsFileName, String tfsSuffix, int openMode, String key) {
        if (!checkInit()) {
            return TfsConstant.EXIT_INVALIDFD_ERROR;
        }
        int ret = TfsConstant.EXIT_INVALIDFD_ERROR;
        String nsAddr = null;
        int retry = 0;

        do {
            if (openMode == TfsConstant.READ_MODE) {
                nsAddr = getReadNsAddr(tfsFileName, retry++);
            } else {
                nsAddr = getWriteNsAddr(tfsFileName, retry++);
            }

            if (nsAddr == null) {
                return TfsConstant.EXIT_INVALIDFD_ERROR;
            }

            ret = tfsManager.openFile(nsAddr, tfsFileName, tfsSuffix, openMode, key);
        } while (ret == TfsConstant.EXIT_INVALIDFD_ERROR);

        return ret;
    }

    // read file
    public int readFile(int fd, byte[] data, int offset, int length) {
        long startTime = TfsUtil.currentTimeMicros();
        int retLength = tfsManager.readFile(fd, data, offset, length);

        addStatInfo(RcAppOperInfo.RC_OPER_READ, retLength,
                    TfsUtil.currentTimeMicros() - startTime, retLength >= 0);

        return retLength;
    }

    // pread file
    public int readFile(int fd, long fileOffset, byte[] data, int offset, int length) {
        long startTime = TfsUtil.currentTimeMicros();
        int retLength = tfsManager.readFile(fd, fileOffset, data, offset, length);

        addStatInfo(RcAppOperInfo.RC_OPER_READ, retLength,
                    TfsUtil.currentTimeMicros() - startTime, retLength >= 0);
        return retLength;
    }

    // write file
    public int writeFile(int fd, byte[] data, int offset, int length) {
        long startTime = TfsUtil.currentTimeMicros();
        int retLength = tfsManager.writeFile(fd, data, offset, length);

        addStatInfo(RcAppOperInfo.RC_OPER_WRITE, retLength,
                    TfsUtil.currentTimeMicros() - startTime, retLength >= 0);
        return retLength;
    }

    // close file
    public String closeFile(int fd) {
        return tfsManager.closeFile(fd);
    }

    private boolean checkInit() {
        if (status != MANAGER_INITED) {
            log.error("rc manager not init");
            return false;
        }
        return true;
    }

    private boolean checkConfig() {
        boolean ret = (rcAddr != null && appIp != null && localIp != 0);
        if (!ret) {
            log.error("invalid config. rcServerAddr: " + rcAddr +
                      " appId: " + appId + " appIp: " + appIp +
                      " localIp: " + localIp);

        }
        return ret;
    }

    private boolean login() {
        try {
            // TODO: appId should be long ..
           RespRcLoginMessage rrlm =
                RcSessionHelper.login(TfsUtil.hostToLong(rcAddr), ((Long)appId).toString(), TfsUtil.hostToLong(appIp));

            log.debug("@@ baseinfo: " + rrlm.getBaseInfo() + " sessionid: " + rrlm.getSessionId());

            if (rrlm.getSessionId() == null || rrlm.getBaseInfo() == null) {
                log.error("get invalid session id and rc baseinfo");
                return false;
            }

            // update baseinfo
            updateBaseInfo(rrlm.getBaseInfo());

            // update session base info
            updateSessionBaseInfo(rrlm.getSessionId());

            // update ns cluster info
            updateNsInfo();
        } catch (TfsException e) {
            log.error("login fail.", e);
            return false;
        }
        log.warn("login success:\n" + baseInfo + "\n" + sessionBaseInfo);
        return true;
    }

    private boolean logout() {
        log.debug("@@ logout start " + status);
        if (status == MANAGER_NOT_INIT) {
            return true;
        }

        RcKeepAliveInfo kaInfo = getKaInfo();
        kaInfo.getSessionBaseInfo().setIsLogout(true);
        try {
            if (RcSessionHelper.logout(activeRcAddr, kaInfo) == TfsConstant.TFS_SUCCESS) {
                status = MANAGER_NOT_INIT;
                log.warn("logout success");
                return true;
            }
        } catch (TfsException e) {
            log.error("logout fail.", e);
        }

        return false;
    }

    private String getWriteNsAddr(String tfsFileName, int retry) {
        if (retry > 1) {
            return null;
        }

        // support update, just recognize read cluster to be ENABLE TO UPDATE
        if (tfsFileName != null && tfsFileName.length() != 0) {
            return getReadNsAddr(tfsFileName, retry);
        }

        return writableNs[retry];
    }

    private String getReadNsAddr(String tfsFileName, int retry) {
        if (retry > 1) {
            return null;
        }

        int clusterId = 0;
        try {
            clusterId = getClusterId(tfsFileName);
        } catch (TfsException e) {
            log.error("invalid tfs file name: " + tfsFileName);
            return null;
        }

        return readableChoice[retry].get(clusterId);
    }

    private int getClusterId(String tfsFileName) throws TfsException {
        FSName fsname = new FSName(tfsFileName, null);
        return fsname.getTfsClusterIndex() - '0';
    }

    private RcKeepAliveInfo getKaInfo() {
        RcKeepAliveInfo kaInfo = new RcKeepAliveInfo();
        kaInfo.setLastReportTime(TfsUtil.currentTimeMicros());
        kaInfo.setSessionBaseInfo(this.sessionBaseInfo);

        lock.lock();
        // copy
        kaInfo.setSessionStatInfo(new RcSessionStatInfo(this.sessionStatInfo));
        this.sessionStatInfo.clear();
        lock.unlock();

        return kaInfo;
    }

    private synchronized void retryNextActiveRc() {
        int serverSize = baseInfo.getServerAddrs().size();
        if (serverSize == 0) {
            log.error("rc server address list is emtpy");
        } else if (serverSize > 1) {
            currentActiveRcIndex = (currentActiveRcIndex + 1) % serverSize;
            activeRcAddr = baseInfo.getServerAddrs().get(currentActiveRcIndex);
        }
    }

    // if dest1 is far from src than dest2
    private boolean distanceFarFrom(int src, String dest1, String dest2) {
        int destId1 = Integer.reverseBytes(TfsUtil.ipStringToId(dest1));
        int destId2 = Integer.reverseBytes(TfsUtil.ipStringToId(dest2));

        return (destId1 - src) > (destId2 - src);
    }

    private void updateWritableNS(String vipAddr, String[] tmpWritableNs) {
        if (vipAddr == null || tmpWritableNs.length != 2) {
            return;
        }

        if (tmpWritableNs[0] == null) {
            tmpWritableNs[0] = vipAddr;
        } else if (distanceFarFrom(localIp, tmpWritableNs[0], vipAddr)) {
            tmpWritableNs[1] = tmpWritableNs[0];
            tmpWritableNs[0] = vipAddr;
        } else {
            tmpWritableNs[1] = vipAddr;
        }
    }

    private void updateReadableChoice(int clusterId, String vipAddr, Map<Integer, String>[] tmpReadableChoice) {
        if (vipAddr == null || tmpReadableChoice.length != 2) {
            return;
        }

        String addr = tmpReadableChoice[0].get(clusterId);
        if (addr == null) {
            tmpReadableChoice[0].put(clusterId, vipAddr);
        } else if (distanceFarFrom(localIp, addr, vipAddr)) {
            tmpReadableChoice[1].put(clusterId, addr);
            tmpReadableChoice[0].put(clusterId, vipAddr);
        } else {
            tmpReadableChoice[1].put(clusterId, vipAddr);   
        }
    }

    private void updateUniqueStore(boolean update, boolean isDuplicate, DuplicateInfo duplicateInfo) {
        if (update) {
            if (this.isDuplicate = isDuplicate) {
                if (uniqueStore == null) {
                    uniqueStore = new TairUniqueStore();
                }
                List<String> addrList = new ArrayList<String>();
                addrList.add(duplicateInfo.getMasterAddr());
                addrList.add(duplicateInfo.getSlaveAddr());
                ((TairUniqueStore)uniqueStore).setConfigServerList(addrList);
                ((TairUniqueStore)uniqueStore).setGroupName(duplicateInfo.getGroupName());
                ((TairUniqueStore)uniqueStore).setNamespace(duplicateInfo.getNamespace());
                // not init
            } else {            // reset uniquestore
                uniqueStore = null;
            }
        } else {                // reset uniquestore
            uniqueStore = null;
        }
    }

    private synchronized void updateNsInfo() {
        String[] tmpWritableNs = new String[2];
        Map<Integer, String>[] tmpReadableChoice = new HashMap[2];
        tmpReadableChoice[0] = new HashMap<Integer, String>();
        tmpReadableChoice[1] = new HashMap<Integer, String>();

        for (RcClusterRackInfo clusterRackInfo : baseInfo.getClusterRackInfos()) {
            boolean writable = false;
            for (RcClusterInfo clusterInfo : clusterRackInfo.getClusterInfos()) {
                if (clusterInfo.getAccessType() == RcClusterRackInfo.CLUSTER_ACCESS_TYPE_READ_WRITE) {
                    writable = true;
                    // update write ns
                    updateWritableNS(clusterInfo.getVipAddr(), tmpWritableNs);
                }

                // update read ds
                updateReadableChoice(clusterInfo.getClusterId(), clusterInfo.getVipAddr(), tmpReadableChoice);
            }

            // only one rack can write, so only one duplicate info
            // update duplicate info.
            updateUniqueStore(writable, clusterRackInfo.getIsDuplicate(), clusterRackInfo.getDuplicateInfo());
        }

        // To avoid lock everytime when get meta info from writableNs and readableChoice,
        // just swap readied one with original one,
        // info may be conflicted at this point, but tolerable
        this.writableNs = tmpWritableNs;
        this.readableChoice = tmpReadableChoice;

        log.debug("@@ update nsinfo: writablens: [0]: " + writableNs[0] + " [1]: " + writableNs[1]);
        for (int i = 0; i < 2; i++) {
            log.debug("@@ readableChoice:[" + i + "] ");
            for (Entry entry : readableChoice[i].entrySet()) {
                System.out.println("cluster: " + entry.getKey() + " vip: " + entry.getValue());
            }
        }
    }

    private void updateBaseInfo(RcBaseInfo baseInfo) {
        this.baseInfo = baseInfo;
        if ((currentActiveRcIndex = this.baseInfo.getServerAddrs().indexOf(activeRcAddr)) == -1) {
            currentActiveRcIndex = 0;
            activeRcAddr = baseInfo.getServerAddrs().get(currentActiveRcIndex);
        }
        log.debug("@@ update base info " + this.baseInfo + " activercaddr " + TfsUtil.longToHost(activeRcAddr) + " currentActiveRcIndex " + currentActiveRcIndex);
    }

    private void updateSessionBaseInfo(String sessionId) {
        sessionBaseInfo.setSessionId(sessionId);
        sessionBaseInfo.setClientVersion(TfsConstant.CLIENT_VERSION);
        sessionBaseInfo.setCacheSize(ClientConfig.CACHEITEM_COUNT);
        sessionBaseInfo.setCacheTime(ClientConfig.CACHE_TIME);
        sessionBaseInfo.setIsLogout(false);
        sessionBaseInfo.setModifyTime(TfsUtil.currentTimeMicros());
        log.debug("@@ update session stat info: " + sessionBaseInfo);
    }

    private void addStatInfo(int operType, long operSize, long operResponseTime, boolean isSuccess) {
        RcAppOperInfo appOperInfo = new RcAppOperInfo();
        appOperInfo.setOperType(operType);
        appOperInfo.setOperTimes(1);
        appOperInfo.setOperResponseTime(operResponseTime);
        if (isSuccess) {
            appOperInfo.setOperSize(operSize);
            appOperInfo.setOperSuccessCount(1);
        }

        lock.lock();
        sessionStatInfo.addStatInfo(appOperInfo);
        lock.unlock();
        log.debug("@@ add stat info : " + appOperInfo);
    }

    private void rollBackStatInfo(RcSessionStatInfo statInfo) {
        lock.lock();
        this.sessionStatInfo.add(statInfo);
        lock.unlock();
        log.debug("@@ rollback statinfo " + statInfo);
    }

    private synchronized void reScheduleKeepAlive(int delay) {
        if (delay < 0) {
            delay = 0;
        }
        if (timer != null) {
            timer.cancel();
        } else {
            timer = new Timer();
        }

        timer.schedule((new KeepAliveTask()), delay, baseInfo.getReportInterval() * 1000); // (ms)
    }

    private long getCacheHitRatio() {
        return tfsManager.getCacheHitRatio();
    }

    private String dumpConfig() {
        return "rcAddr: " + rcAddr + " appId: " + appId +
            " appIp: " + appIp + " localIp: " + TfsUtil.getLocalIpString() +
            " cacheTime: " + ClientConfig.CACHE_TIME +
            " cacheItemCount: " + ClientConfig.CACHEITEM_COUNT + " timeout: " + ClientConfig.TIMEOUT;
    }

    ////////////////////////////////////////
    //  for NameMetaServer
    ////////////////////////////////////////

    public int getWriteClusterId(int clusterId) {
        if (clusterId < 0) {
            log.error("invalid clusterId: " + clusterId);
            return clusterId;
        }
        int retClusterId = -1;

        // 1. just use default write cluster
        if (clusterId == 0) {
            for (String nsAddr : writableNs) {
                retClusterId = tfsManager.getClusterId(nsAddr);
                if (retClusterId > 0) {
                    break;
                }
            }
            return retClusterId;
        }

        // 2. clusterId > 0, wanna update, ALLOW now here, just return readable cluster
        for (int i = 0; i < readableChoice.length; i++) {
            retClusterId = tfsManager.getClusterId(readableChoice[i].get(clusterId));
            if (retClusterId > 0) {
                break;
            }
        }

        return retClusterId;
    }

    public boolean read(int clusterId, int blockId, long fileId, long offset, long length, OutputStream output) {
        return fetchFile(new FSName(clusterId, blockId, fileId).get(), null, offset, length, output);
    }

    public FragMeta write(int clusterId, long offset, byte[] data, int dataOffset, int length) {
        String retName = saveFile(new FSName(clusterId, 0, 0).get(), null, data, dataOffset, length, false);

        if (retName == null) {
            return null;
        }

        FSName name = null;
        try {
            // FSName seems to get too much hit ..
            name = new FSName(retName);
        } catch (TfsException e) {
            // should not happen, cause saveFile already return.
            return null;
        }
        FragMeta fragMeta = new FragMeta();
        fragMeta.setBlockId(name.getBlockId());
        fragMeta.setFileId(name.getFileId());
        fragMeta.setOffset(offset);
        fragMeta.setLength(length);

        return fragMeta;
    }

    public boolean unlink(int clusterId, int blockId, long fileId) {
        return unlinkFile(new FSName(clusterId, blockId, fileId).get(), null);
    }
        
}
