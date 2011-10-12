/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs;

import java.util.List;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.common.tfs.common.TfsConstant;
import com.taobao.common.tfs.packet.FileInfo;
import com.taobao.common.tfs.impl.ClientConfig;
import com.taobao.common.tfs.rc.RcManager;
import com.taobao.common.tfs.namemeta.FileMetaInfo;
import com.taobao.common.tfs.namemeta.NameMetaManager;

public class DefaultTfsManager implements TfsManager {
    private static final Log log = LogFactory.getLog(DefaultTfsManager.class);

    // rc manager
    private RcManager rcManager = new RcManager();

    // nameMetaManager
    private boolean useNameMeta = false;
    private NameMetaManager nameMetaManager = null;

    public String getRcAddr() {
        return rcManager.getRcAddr();
    }

    public void setRcAddr(String rcAddr) {
        rcManager.setRcAddr(rcAddr);
    }

    public long getAppId() {
        return rcManager.getAppId();
    }

    public void setAppId(long appId) {
        rcManager.setAppId(appId);
    }

    public String getAppIp() {
        return rcManager.getAppIp();
    }

    public void setAppIp(String appIp) {
        rcManager.setAppIp(appIp);
    }

    public void setUseNameMeta(boolean useNameMeta) {
        this.useNameMeta = useNameMeta;
    }

    public boolean getUseNameMeta() {
        return useNameMeta;
    }

    // sorted of config
    /**
     * timeout for one request
     *
     * @param timeout
     */
    public void setTimeout(int timeout) {
        if (timeout > 0) {
            ClientConfig.TIMEOUT = timeout;
        }
    }

    public int getTimeout() {
        return ClientConfig.TIMEOUT;
    }

    /**
     * max amount of blockid==>serverlist cache intems
     *
     * @param maxCacheItemCount
     */
    public void setMaxCacheItemCount(int maxCacheItemCount) {
        if (maxCacheItemCount > 0) {
            ClientConfig.CACHEITEM_COUNT = maxCacheItemCount;
        }
    }

    public int getMaxCacheItemCount() {
        return ClientConfig.CACHEITEM_COUNT;
    }

    /**
     * max effecting time of cache
     *
     * @param maxCacheTime
     */
    public void setMaxCacheTime(int maxCacheTime) {
        if (maxCacheTime > 0) {
            ClientConfig.CACHE_TIME = maxCacheTime;
        }
    }

    public int getMaxCacheTime() {
        return ClientConfig.CACHE_TIME;
    }

    /**
     * max io length when op small file
     *
     * @param maxSmallIOLength
     */
    public void setMaxSmallIOLength(int maxSmallIOLength) {
        if (maxSmallIOLength > 0) {
            ClientConfig.MAX_SMALL_IO_LENGTH = maxSmallIOLength;
        }
    }

    public int getMaxSmallIOLength() {
        return ClientConfig.MAX_SMALL_IO_LENGTH;
    }

    /**
     * max io length when op large file
     *
     * @param maxLargeIOLength
     */
    public void setMaxLargeIOLength(int maxLargeIOLength) {
        if (maxLargeIOLength > 0) {
            ClientConfig.MAX_LARGE_IO_LENGTH = maxLargeIOLength;
        }
    }

    public int getMaxLargeIOLength() {
        return ClientConfig.MAX_LARGE_IO_LENGTH;
    }

    /**
     * gc time worker interval
     *
     * @param gcInternal (ms)
     */
    public void setGcInterval(int gcInternal) {
        if (gcInternal > 0) {
            ClientConfig.GC_INTERVAL = gcInternal;
        }
    }

    public int getGcInterval() {
        return ClientConfig.GC_INTERVAL;
    }

    /**
     * cache metric statistics timer worker interval
     *
     * @param cacheMetricInterval
     */
    public void setCacheMetricInterval(int cacheMetricInterval) {
        if (cacheMetricInterval > 0) {
            ClientConfig.CACHEMETRIC_INTERVAL = cacheMetricInterval;
        }
    }

    public int getCacheMetricInterval() {
        return ClientConfig.CACHEMETRIC_INTERVAL;
    }

    /**
     * gc localkey and gcfile expired time
     *
     * @param gcExpiredTime
     */
    public void setGcExpiredTime(int gcExpiredTime) {
        if (gcExpiredTime >= TfsConstant.MIN_GC_EXPIRED_TIME) {
            ClientConfig.GC_EXPIRED_TIME = gcExpiredTime;
        }
    }

    public long getGcExpiredTime() {
        return ClientConfig.GC_EXPIRED_TIME;
    }

    /**
     * segment length
     *
     * @param segmentLength
     */
    public void setSegmentLength(int segmentLength) {
        if (segmentLength > 0 && segmentLength <= TfsConstant.MAX_SEGMENT_LENGTH) {
            ClientConfig.SEGMENT_LENGTH = segmentLength;
            ClientConfig.BATCH_WRITE_LENGTH = ClientConfig.SEGMENT_LENGTH * ClientConfig.BATCH_COUNT;
        }
    }

    public int getSegmentLength() {
        return ClientConfig.SEGMENT_LENGTH;
    }

    /**
     * batch operate count
     *
     * @param batchCount
     */
    public void setBatchCount(int batchCount) {
        if (batchCount > 0 && batchCount <= TfsConstant.MAX_BATCH_COUNT) {
            ClientConfig.BATCH_COUNT = batchCount;
            ClientConfig.BATCH_WRITE_LENGTH = ClientConfig.SEGMENT_LENGTH * ClientConfig.BATCH_COUNT;
        }
    }

    public int getBatchCount() {
        return ClientConfig.BATCH_COUNT;
    }

    private List<String> rootServerAddrList = null;
    public void setRootServerAddrList(List<String> rootServerAddrList) {
        this.rootServerAddrList = rootServerAddrList;
    }

    public List<String> getRootServerAddrList() {
        return this.rootServerAddrList;
    }

    ////////////////////////////////
    // interface implement
    ////////////////////////////////

    public boolean init() {
        log.warn("TfsManager [ " + TfsConstant.CLIENT_VERSION + " ] initing ... ");
        if (rcManager == null) {
            throw new RuntimeException("rcManager are all not configed");
        }

        // maybe retry.
        // but it fail when init, there must be something wrong happened.
        if (!rcManager.init()) {
            throw new RuntimeException("rcManager init fail");
        }

        // nameMetaManager configed
        if (useNameMeta) {
            if (rootServerAddrList == null) {
                throw new RuntimeException("use NameMeta need config rootServerAddrList");
            }
            nameMetaManager = new NameMetaManager();
            nameMetaManager.setRootServerAddrList(rootServerAddrList);
            nameMetaManager.setRcManager(rcManager);
            if (!nameMetaManager.init()) {
                throw new RuntimeException("nameMetaManager init fail");
            }
        }
    
        log.warn("TfsManager (RcManager" +
                 (nameMetaManager != null ? " and NameMetaManager" : "") +
                 ") init OK: [ " + TfsConstant.CLIENT_VERSION + " ]");
        return true;
    }

    public void destroy() {
        if (nameMetaManager != null) {
            nameMetaManager.destroy();
        }
        if (rcManager != null) {
            rcManager.destroy();
        }
    }

    public String saveFile(String localFileName, String tfsFileName, String tfsSuffix) {
        return rcManager.saveFile(localFileName, tfsFileName, tfsSuffix, false);
    }

    public String saveFile(String localFileName, String tfsFileName, String tfsSuffix, boolean simpleName) {
        return rcManager.saveFile(localFileName, tfsFileName, tfsSuffix, simpleName);
    }

    public String saveFile(String tfsFileName, String tfsSuffix, byte[] data, int offset, int length) {
        return rcManager.saveFile(tfsFileName, tfsSuffix, data, offset, length, false);
    }

    public String saveFile(String tfsFileName, String tfsSuffix,
                           byte[] data, int offset, int length, boolean simpleName) {
        return rcManager.saveFile(tfsFileName, tfsSuffix, data, offset, length, simpleName);
    }

    public String saveFile(byte[] data, String tfsFileName, String tfsSuffix) {
        if (data != null) {
            return rcManager.saveFile(tfsFileName, tfsSuffix, data, 0, data.length, false);
        }
        return null;
    }

    public String saveFile(byte[] data, String tfsFileName, String tfsSuffix, boolean simpleName) {
        if (data != null) {
            return rcManager.saveFile(tfsFileName, tfsSuffix, data, 0, data.length, simpleName);
        }
        return null;
    }

    public String saveLargeFile(String localFileName, String tfsFileName, String tfsSuffix) {
        return rcManager.saveLargeFile(localFileName, tfsFileName, tfsSuffix);
    }

    public String saveLargeFile(byte[] data, String tfsFileName, String tfsSuffix, String key) {
        if (data != null) {
            return rcManager.saveLargeFile(tfsFileName, tfsSuffix, data, 0, data.length, key);
        }
        return null;
    }

    public String saveLargeFile(String tfsFileName, String tfsSuffix,
                                byte[] data, int offset, int length, String key) {
        return rcManager.saveLargeFile(tfsFileName, tfsSuffix, data, offset, length, key);
    }

    public FileInfo statFile(String tfsFileName, String tfsSuffix) {
        return rcManager.statFile(tfsFileName, tfsSuffix);
    }

    public boolean unlinkFile(String tfsFileName, String tfsSuffix, int action) {
        return rcManager.unlinkFile(tfsFileName, tfsSuffix, action);        
    }

    public boolean unlinkFile(String tfsFileName, String tfsSuffix) {
        return rcManager.unlinkFile(tfsFileName, tfsSuffix);
    }

    public boolean hideFile(String tfsFileName, String tfsSuffix, int option) {
        return rcManager.hideFile(tfsFileName, tfsSuffix, option);
    }

    public boolean fetchFile(String tfsFileName, String tfsSuffix, String localFileName) {
        return rcManager.fetchFile(tfsFileName, tfsSuffix, localFileName);
    }

    public boolean fetchFile(String tfsFileName, String tfsSuffix, OutputStream output) {
        return rcManager.fetchFile(tfsFileName, tfsSuffix, 0, Long.MAX_VALUE, output);
    }

    public boolean fetchFile(String tfsFileName, String tfsSuffix, long offset, OutputStream output) {
        return rcManager.fetchFile(tfsFileName, tfsSuffix, offset, Long.MAX_VALUE, output);
    }

    public boolean fetchFile(String tfsFileName, String tfsSuffix, long offset, long length, OutputStream output) {
        return rcManager.fetchFile(tfsFileName, tfsSuffix, offset, length, output);
    }

    public int openWriteFile(String tfsFileName, String tfsSuffix, String key) {
        return rcManager.openFile(tfsFileName, tfsSuffix, TfsConstant.WRITE_MODE, key);
    }

    public int openReadFile(String tfsFileName, String tfsSuffix) {
        return rcManager.openFile(tfsFileName, tfsSuffix, TfsConstant.READ_MODE, null);
    }

    public int readFile(int fd, byte[] data, int offset, int length) {
        return rcManager.readFile(fd, data, offset, length);
    }

    public int readFile(int fd, long fileOffset, byte[] data, int offset, int length) {
        return rcManager.readFile(fd, fileOffset, data, offset, length);
    }

    public int writeFile(int fd, byte[] data, int offset, int length) {
        return rcManager.writeFile(fd, data, offset, length);
    }

    public String closeFile(int fd) {
        return rcManager.closeFile(fd);
    }


    /********************************
     *     
     *     Namemeta interface
     *
     *******************************/
    public boolean createDir(long userId, String filePath) {
        return nameMetaManager.createDir(userId, filePath);
    }

    public boolean mvDir(long userId, String srcFilePath, String destFilePath) {
        return nameMetaManager.mvDir(userId, srcFilePath, destFilePath);
    }

    public boolean rmDir(long userId, String filePath) {
        return nameMetaManager.rmDir(userId, filePath);
    }

    public List<FileMetaInfo> lsDir(long userId, String filePath) {
        return nameMetaManager.lsDir(userId, filePath);
    }

    public List<FileMetaInfo> lsDir(long userId, String filePath, boolean isRecursive) {
        return nameMetaManager.lsDir(userId, filePath, isRecursive);
    }

    public boolean createFile(long userId, String filePath) {
        return nameMetaManager.createFile(userId, filePath);
    }

    public boolean mvFile(long userId, String srcFilePath, String destFilePath) {
        return nameMetaManager.mvFile(userId, srcFilePath, destFilePath);
    }

    public boolean rmFile(long userId, String filePath) {
        return nameMetaManager.rmFile(userId, filePath);
    }

    public FileMetaInfo lsFile(long userId, String filePath) {
        return nameMetaManager.lsFile(userId, filePath);
    }

    public long read(long userId, String filePath, long fileOffset, long length, OutputStream output) {
        return nameMetaManager.read(userId, filePath, fileOffset, length, output);
    }

    public long write(long userId, String filePath, byte[] data) {
        return nameMetaManager.write(userId, filePath, data);
    }

    public long write(long userId, String filePath, byte[] data, long dataOffset, long length) {
        return nameMetaManager.write(userId, filePath, data, dataOffset, length);
    }

    public long write(long userId, String filePath, long fileOffset, byte[] data) {
        return nameMetaManager.write(userId, filePath, fileOffset, data);        
    }

    public long write(long userId, String filePath, long fileOffset,
                      byte[] data, long dataOffset, long length) {
        return nameMetaManager.write(userId, filePath, fileOffset, data, dataOffset, length);
    }

    public boolean saveFile(long userId, String localFile, String fileName) {
        return nameMetaManager.saveFile(userId, localFile, fileName);
    }

    public boolean fetchFile(long userId, String localFile, String fileName) {
        return nameMetaManager.fetchFile(userId, localFile, fileName);
    }
}
