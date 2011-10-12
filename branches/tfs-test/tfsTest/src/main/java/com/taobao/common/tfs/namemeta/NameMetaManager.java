/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.namemeta;

import java.lang.Math;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.DataFormatException;

import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.io.FileOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.common.tfs.TfsException;
import com.taobao.common.tfs.exception.ConnectionException;
import com.taobao.common.tfs.exception.ErrorStatusException;
import com.taobao.common.tfs.impl.ClientConfig;
import com.taobao.common.tfs.rc.RcManager;
import com.taobao.common.tfs.common.TfsConstant;
import com.taobao.common.tfs.common.TfsUtil;
import com.taobao.common.tfs.packet.RespNameMetaReadMessage;
import com.taobao.common.tfs.packet.RespNameMetaLsMessage;
import com.taobao.common.tfs.packet.RespNameMetaGetTableMessage;

// NameMetaManager is AppId level instance.
// One AppId need one NameMetaManager instance.
public class NameMetaManager {
    private static final Log log = LogFactory.getLog(NameMetaManager.class);

    // retry threshold
    private static int MAX_TRY_TIME = 2;
    private static int MAX_CONNECT_SERVER_FAIL_COUNT = 5;

    // file type
    private static byte FILE_TYPE = (byte)0x1;
    private static byte DIR_TYPE = (byte)0x2;

    // action type
    private static byte CREATE_DIR = (byte)0x1;
    private static byte CREATE_FILE = (byte)0x2;
    private static byte RM_DIR = (byte)0x3;
    private static byte RM_FILE = (byte)0x4;
    private static byte MV_DIR = (byte)0x5;
    private static byte MV_FILE = (byte)0x6;

    // appid
    private long appId;
    // rc manager
    private RcManager rcManager = null;

    // root server addr
    private long rootServerId = 0;
    private List<String> rootServerAddrList = new ArrayList<String>();
    // rootServerId ==> metaTable
    private HashMap<Long, MetaTable> metaTables = new HashMap<Long, MetaTable>();
    // read write lock TODO: only one lock here for one table from one rootServer
    ReentrantReadWriteLock metaTableRWLock = new ReentrantReadWriteLock();
    // for connecting fail count
    private int failCount = 0;

    public synchronized boolean init() {

        // TODO. rcManager should insert here to get namemetaserver(rootserver) address
        //       should exit if init fail.
        if (rcManager == null) {
            log.error("rc manager not config");
            return false;
        }

        // set appId
        this.appId = rcManager.getAppId();

        if (rootServerAddrList == null) {
        	throw new RuntimeException("use NameMeta need config rootServerAddrList");
        }
        rootServerId = TfsUtil.hostToLong(rootServerAddrList.get(0));
        if (0 == rootServerId) {
        	log.error("invalid root server addr!");
        	return false;
        }
        MetaTable metaTable = new MetaTable();
        metaTables.put(rootServerId, metaTable);
        
        boolean bRet = getTable();
        if (false == bRet) {
            log.error("get table failed!");
            return false;
        }

        log.debug("init NameMetaManager. root server addr: " + rootServerAddrList.get(0));
        return true;
    }

    public void destroy() {
        // do nothing now
    }

    public void setRcManager(RcManager rcManager) {
        this.rcManager = rcManager;
        this.appId = rcManager.getAppId();
    }

    public RcManager getRcManager() {
        return this.rcManager;        
    }

    public void setRootServerAddrList(List<String> rootServerAddrList) {
    	this.rootServerAddrList = rootServerAddrList;
    }
    
    private boolean getTable() {
    	boolean bRet = false;
        RespNameMetaGetTableMessage rnmgtm = getTableFromRootServer(rootServerId);
        if (null != rnmgtm) {
        	// get table from packet then store in the map
        	try {
            	MetaTable metaTable = metaTables.get(rootServerId);
            	if (null == metaTable) {
        	    	log.error("meta table not found by rootServerId: " + rootServerId);
        	    	return false;
            	}
            	metaTableRWLock.writeLock().lock();
        	    bRet = rnmgtm.getTable(metaTable);
        	    metaTableRWLock.writeLock().unlock();
        		metaTable.dump();
        	}
        	catch (DataFormatException e) {
        		log.error("decompress table failed! catch DataFormatException!");
        		return false;
        	}
        }
        return bRet;
    }
   
    private long getNameMetaServerIdFromTable(long userId) {
    	long metaServerId = 0;
    	MetaTable metaTable = metaTables.get(rootServerId);
    	if (metaTable != null) {
    		metaTableRWLock.readLock().lock();
    		int tableSize = metaTable.getTableSize();
    		if (tableSize <= 0) {
    			log.error("meta table empty!");
    			metaServerId = 0;
    		}
    		else {
	    		HashHelper hashHelper = new HashHelper(appId, userId);
	    		int bucketId = TfsUtil.murMurHash(hashHelper.getByteArray(), hashHelper.length()) % tableSize;
	    		metaServerId = metaTable.getMetaServerId(bucketId);
    		}
    		metaTableRWLock.readLock().unlock();
    	}
    	return metaServerId;
    }

    // Interface Implementation
    public boolean createDir(long userId, String filePath) {
        return doFileAction(userId, CREATE_DIR, filePath);
    }

    public boolean mvDir(long userId, String srcFilePath, String destFilePath) {
        if (isSubDir(destFilePath, srcFilePath)) {
            log.error("invalid file path or dest file path is subdir of src file path, or they are the same dir"
                      + " src file path: " + srcFilePath + " dest file path: " + destFilePath);
            return false;
        }

        return doFileAction(userId, MV_DIR, srcFilePath, destFilePath);
    }

    // directory must be empty
    public boolean rmDir(long userId, String filePath) {
        return doFileAction(userId, RM_DIR, filePath);
    }

    // list directory non-recursive
    public List<FileMetaInfo> lsDir(long userId, String filePath) {
        return lsDir(userId, filePath, DIR_TYPE, -1, false);
    }

    // list directory isRecursive-ed
    public List<FileMetaInfo> lsDir(long userId, String filePath, boolean isRecursive) {
        return lsDir(userId, filePath, DIR_TYPE, -1, isRecursive);
    }

    public boolean createFile(long userId, String filePath) {
        return doFileAction(userId, CREATE_FILE, filePath);
    }

    public boolean mvFile(long userId, String srcFilePath, String destFilePath) {
        return doFileAction(userId, MV_FILE, srcFilePath, destFilePath);
    }

    public boolean rmFile(long userId, String filePath) {
        // 1. read all fragmeta,
        // 2. delete metainfo.
        // 3. delete data
        FragInfo fragInfo = readFileFragMeta(userId, filePath);
        if (fragInfo == null) {
            log.error("read all fragmeta fail.");
            return false;
        }
        if (!doFileAction(userId, RM_FILE, filePath)) {
            log.error("rm metainfo fail.");
            return false;
        }
        // ignore return status, just remain garbage data when unlink fail.
        unlinkFile(fragInfo);
        return true;
    }

    // list file meta information
    public FileMetaInfo lsFile(long userId, String filePath) {
        if ((filePath = canonicalFilePath(filePath)) == null) {
            log.error("invalid file path");
            return null;
        }

        RespNameMetaLsMessage rnmlm = doLs(userId, filePath, FILE_TYPE, -1);

        if (rnmlm == null || rnmlm.getMetaInfoList().size() == 0) {
            log.error("get null file meta info. filePath: " + filePath);
            return null;
        }

        log.debug("@@ ls file: " + filePath + ": " + rnmlm.getMetaInfoList().get(0));
        return rnmlm.getMetaInfoList().get(0);
    }

    public long read(long userId, String filePath, long offset, long length, OutputStream output) {
        if ((filePath = canonicalFilePath(filePath)) == null) {
            log.error("invalid file path.");
            return -1;
        }
        if (output == null) {
            log.error("null output stream");
            return -1;
        }
        if (offset < 0 || length < 0) {
            log.error("invalid offset or length. offset: " + offset + " length: " + length);
            return -1;
        }
        if (length == 0) {
            return 0;
        }

        long readLength = 0;
        long curReadLength = 0;

        log.debug("@@ read start offset: " + offset + " len: " + length);
        RespNameMetaReadMessage rnmrm = null;
        do {
            rnmrm = doRead(userId, filePath, offset + readLength, length - readLength);
            if (rnmrm == null || rnmrm.getFragInfo().getClusterId() < 0) { // clustid < 0 mean error
                log.error("read meta info fail. filePath: " + filePath +
                          " offset: " + (offset + readLength) + " length: " + (length - readLength));
                if (readLength == 0) {
                    readLength = -1;
                }
                break;
            }

            log.debug("@@ get read frag: " + rnmrm.getFragInfo() + " hasnext: " + rnmrm.hasNext());
            curReadLength = readData(offset + readLength, length - readLength, rnmrm.getFragInfo(), output);

            // read fail or read over
            if (curReadLength <= 0 || rnmrm.getFragInfo().getClusterId() <= 0) {
                log.debug("read fail.");
                break;
            }

            readLength += curReadLength;
        } while (readLength < length && rnmrm.hasNext());

        return readLength;
    }

    public long write(long userId, String filePath, byte[] data) {
        if (data == null) {
            log.error("null data");
            return -1;
        }
        return write(userId, filePath, -1, data, 0, data.length);
    }

    // append write
    public long write(long userId, String filePath, byte[] data, long dataOffset, long length) {
        return write(userId, filePath, -1, data, dataOffset, length);
    }

    public long write(long userId, String filePath, long offset, byte[] data) {
        if (data == null) {
            log.error("null data");
            return -1;
        }
        return write(userId, filePath, offset, data, 0, data.length);
    }

    public long write(long userId, String filePath, long offset, byte[] data, long dataOffset, long length) {
        if ((filePath = canonicalFilePath(filePath)) == null) {
            log.error("invalid file path.");
            return -1;
        }
        if (data == null) {
            log.error("null input data");
            return -1;
        }
        if ((offset < 0 && offset != -1) || dataOffset < 0 || length < 0 || dataOffset + length > data.length) {
            log.error("invalid offset or length. offset: " + offset +
                      " dataoffset: " + dataOffset + " length: " + length +
                      " datalength: " + data.length);
            return -1;
        }
        if (length == 0) {
            return 0;
        }

        long writeLength = 0;
        long curLength = 0;
        long curWriteLength = 0;
        long curOffset = offset;
        long leftLength = length;

        int clusterId = 0;

        RespNameMetaReadMessage rnmrm = doRead(userId, filePath, 0, 0);
        if (rnmrm == null) {
            log.error("get clusterid fail.");
            return -1;
        }

        clusterId = rnmrm.getFragInfo().getClusterId();

        log.debug("@@ get clusterid : " + clusterId);
        FragInfo fragInfo = new FragInfo();
        int count = 0;
        do {
            // write and update metainfo batch mode.
            fragInfo.clear();
            curLength = Math.min(leftLength, ClientConfig.BATCH_WRITE_LENGTH);
            curOffset += offset < 0 ? 0 : writeLength;
            curWriteLength = writeData(curOffset, data,
                                       dataOffset + writeLength , curLength, fragInfo);
            if (curWriteLength <= 0) {
                log.debug("@@ write fail. ");
                break;
            }

            log.debug("@@ write data once. offset: " + curOffset + " length: " + curLength
                + " retLength " + curWriteLength
                + " fraginfo " + fragInfo);

            if (!doWrite(userId, filePath, fragInfo)) {
                log.error("write meta data fail. offset: " + curOffset + " dataoffset: " +
                          (dataOffset + writeLength) + " length: " + curLength);
                // update metainfo fail. then rollback data alread write. ignore return status.
                unlinkFile(fragInfo);
                break;
            }
            log.debug("@@ write meta server once. offset: " + curOffset
                      + " length: " + curLength + " retLength" + curWriteLength);

            writeLength += curWriteLength;
            leftLength -= curWriteLength;
            count++;

            log.debug("@@ write success " + count + ". offset: " + curOffset + " length: " + curLength
                      + " retLength: " + curWriteLength + " leftLength: " + leftLength);
        } while (curWriteLength == curLength && writeLength < length); // break once occuring write fail

        return writeLength;
    }

    // utility interface
    // if filename's directory not exist, saveFile will fail.
    public boolean saveFile(long userId, String localFile, String fileName) {
        if (localFile == null || (fileName = canonicalFilePath(fileName)) == null) {
            log.error("null filename. localFile: " + localFile + " filename: " + fileName);
            return false;
        }

        if (!createFileIfNotExist(userId, fileName)) {
            log.error("create file fail: " + fileName);
            return false;
        }

        FileInputStream input = null;
        try {
            input = new FileInputStream(localFile);
            long length = input.getChannel().size();
            // read from local at most
            int perLength = (int)Math.min(ClientConfig.BATCH_WRITE_LENGTH, length);
            int readLength = 0;
            byte[] data = new byte[perLength];
            long fileOffset = 0;

            while (length > 0) {
                readLength = input.read(data, 0, perLength);
                if (readLength > 0) {
                    if ((write(userId, fileName, fileOffset, data, 0, readLength)) != readLength) {
                        log.error("write occure fail.");
                        return false; // rollback ? .. retry ..
                    }
                    length -= readLength;
                    fileOffset += readLength;
                } else {
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            log.error("save file " + localFile + " => " + fileName + " fail.", e);
        } finally {
            try {
                if (input != null) {
                    input.close();
                }
            } catch (Exception e) {
                log.error("close localfile fail");
            }
        }
        return false;
    }

    public boolean fetchFile(long userId, String localFile, String fileName) {
        if (localFile == null || (fileName = canonicalFilePath(fileName)) == null) {
            log.error("null filename. localFile: " + localFile + " filename: " + fileName);
            return false;
        }

        FileOutputStream output = null;
        try {
            output = new FileOutputStream(localFile);
            if (read(userId, fileName, 0, Long.MAX_VALUE - 1, output) < 0) {
                log.error("read file fail. " + fileName);
                return false;
            }

            return true;
        } catch (Exception e) {
            log.error("fetch file " + fileName + " => " + localFile + " fail.", e);
        } finally {
            try {
                if (output != null) {
                    output.close();
                }
            } catch (Exception e) {
                log.error("close localfile fail");
            }
        }
        return false;
    }

    // if filePid is -1, then filePath is the path that should be listed.
    // or filePath is the start point to continue listed under path whose id is filePid,
    // null filePath indicate just start from beginning.
    private List<FileMetaInfo> lsDir(long userId, String filePath, byte fileType, long filePid, boolean isRecursive) {
        if (filePid == -1 && (filePath = canonicalFilePath(filePath)) == null) {
            log.error("null file path and invalid pid");
            return null;
        }

        List<FileMetaInfo> metaInfoList = new ArrayList<FileMetaInfo>();
        FileMetaInfo lastFileMetaInfo = null;
        RespNameMetaLsMessage rnmlm = null;

        do {
            rnmlm = doLs(userId, filePath, fileType, filePid);

            if (rnmlm == null) {
                log.error("ls directory fail. filePath: " + filePath + " fileType: " + fileType +
                          " filePid: " + filePid);
                return null;
            }

            if (rnmlm.getMetaInfoList().size() == 0) {
                log.debug("@@ get ls metainfo empty. maybe over.");
                break;
            }

            log.debug("@@ " + filePath + " once get ls count: " + rnmlm.getMetaInfoList().size() + " hasnext: " + rnmlm.hasNext());
            for (FileMetaInfo metaInfo : rnmlm.getMetaInfoList()) {
                log.debug("@@ [ " + metaInfo + " ]");
            }

            metaInfoList.addAll(rnmlm.getMetaInfoList());
            lastFileMetaInfo = rnmlm.getMetaInfoList().get(rnmlm.getMetaInfoList().size() - 1);

            filePid = lastFileMetaInfo.getPid();
            filePath = lastFileMetaInfo.getFileName();
            fileType = lastFileMetaInfo.isFile() ? FILE_TYPE : DIR_TYPE;
        } while (rnmlm.hasNext());

        if (!metaInfoList.isEmpty() && isRecursive) {
            log.debug("@@ recursive list ");
            List<FileMetaInfo> extraMetaInfoList = new ArrayList<FileMetaInfo>();
            for (FileMetaInfo metaInfo : metaInfoList) {
                // if (metaInfo.getFileType() == DIR_TYPE) {
                if (!metaInfo.isFile()) {
                    // recursive ls directory. filePid is the id of this filePath
                    List<FileMetaInfo> innerMetaInfoList = lsDir(userId, null, DIR_TYPE, metaInfo.getId(), true);
                    if (innerMetaInfoList == null) {
                        break;  // once fail, return
                    }
                    extraMetaInfoList.addAll(innerMetaInfoList);
                }
            }

            // add all file metaInfo
            // TODO: resort metainfo to sorted file hierarchy
            metaInfoList.addAll(extraMetaInfoList);
        }

        return metaInfoList;
    }

    private boolean doFileAction(long userId, byte type, String filePath) {
        if ((filePath = canonicalFilePath(filePath)) == null) {
            log.error("invalid file path.");
            return false;
        }

        return doFileActionEx(userId, type, filePath, null);
    }

    private boolean doFileAction(long userId, byte type, String srcFilePath, String destFilePath) {
        if ((srcFilePath = canonicalFilePath(srcFilePath)) == null ||
            (destFilePath = canonicalFilePath(destFilePath)) == null) {
            log.error("invalid file path.");
            return false;
        }

        if (srcFilePath.equals(destFilePath)) {
            log.error("source file path equals to destination file path: " +
                      srcFilePath + " == " + destFilePath);
            return false;
        }

        return doFileActionEx(userId, type, srcFilePath, destFilePath);
    }

    private boolean doFileActionEx(long userId, byte type, String srcFilePath, String destFilePath) {
        int retry = MAX_TRY_TIME;
        int ret = TfsConstant.TFS_ERROR;
        long metaServerId = getNameMetaServerIdFromTable(userId);
    	boolean metaTableObsoleted = false;
    	
        do {
        	metaTableObsoleted = false;
        	long version = metaTables.get(rootServerId).getVersion();
            try {
                ret = NameMetaSessionHelper.doFileAction(metaServerId, new NameMetaUserInfo(appId, userId),
                                                         type, srcFilePath, destFilePath, version);
            } catch (ErrorStatusException e) {
            	if (e.getStatus() == TfsConstant.EXIT_TABLE_VERSION_ERROR) {
            		metaTableObsoleted = true;
            		boolean bRet = getTable();
            		if (false == bRet) {
            			metaTableObsoleted = false;
            			log.error("update table failed! retry: " + (MAX_TRY_TIME - retry));
            		}
            		else {
            			metaServerId = getNameMetaServerIdFromTable(userId);
            		}
            	}
            	else {
            		log.error("do file action fail. type: " + type + " srcFilePath: " + srcFilePath +
            				" destFilePath: " + destFilePath);
            		return false;
            	}
            } catch (ConnectionException e) {
                log.warn("connect fail. retry: " + (MAX_TRY_TIME - retry), e);
            } catch (TfsException e) {
                log.error("do file action fail. type: " + type + " srcFilePath: " + srcFilePath +
                          " destFilePath: " + destFilePath);
                return false;
            }
        } while (ret != TfsConstant.TFS_SUCCESS && (metaTableObsoleted || (!metaTableObsoleted && --retry > 0)));

        return ret != TfsConstant.TFS_SUCCESS ? false : true;
    }

    private RespNameMetaLsMessage doLs(long userId, String filePath, byte fileType, long filePid) {
        int retry = MAX_TRY_TIME;
        RespNameMetaLsMessage rnmlm = null;
        long metaServerId = getNameMetaServerIdFromTable(userId);
    	boolean metaTableObsoleted = false;
    	
        do {
        	metaTableObsoleted = false;
        	long version = metaTables.get(rootServerId).getVersion();
            try {
                rnmlm = NameMetaSessionHelper.nameMetaLs(metaServerId, new NameMetaUserInfo(appId, userId),
                                                         filePath, fileType, filePid, version);
            } catch (ErrorStatusException e) {
            	if (e.getStatus() == TfsConstant.EXIT_TABLE_VERSION_ERROR) {
            		metaTableObsoleted = true;
            		boolean bRet = getTable();
            		if (false == bRet) {
            			metaTableObsoleted = false;
            			log.error("update table failed! retry: " + (MAX_TRY_TIME - retry));
            		}
            		else {
            			metaServerId = getNameMetaServerIdFromTable(userId);
            		}
            	}
            	else {
            		log.error("get file meta info fail.", e);
            		return null;
            	}
            } catch (ConnectionException e) { // retry if connect fail.
                log.warn("connect fail. retry: " + (MAX_TRY_TIME - retry), e);
            } catch (TfsException e) {
                log.error("get file meta info fail.", e);
                return null;
            }
        } while (rnmlm == null && (metaTableObsoleted || (!metaTableObsoleted && --retry > 0)));

        return rnmlm;
    }

    private RespNameMetaReadMessage doRead(long userId, String filePath, long offset, long length) {
        RespNameMetaReadMessage rnmrm = null;
        int retry = MAX_TRY_TIME;
        long metaServerId = getNameMetaServerIdFromTable(userId);
    	boolean metaTableObsoleted = false;
    	
        do {
        	metaTableObsoleted = false;
        	long version = metaTables.get(rootServerId).getVersion();
            try {
                rnmrm = NameMetaSessionHelper.nameMetaRead(metaServerId, new NameMetaUserInfo(appId, userId),
                                                           filePath, offset, length, version);
            } catch (ErrorStatusException e) {
            	if (e.getStatus() == TfsConstant.EXIT_TABLE_VERSION_ERROR) {
            		metaTableObsoleted = true;
            		boolean bRet = getTable();
            		if (false == bRet) {
            			metaTableObsoleted = false;
            			log.error("update table failed! retry: " + (MAX_TRY_TIME - retry));
            		}
            		else {
            			metaServerId = getNameMetaServerIdFromTable(userId);
            		}
            	}
            	else {
                    log.error("read file fail. " + filePath, e);
            		return null;
            	}
            } catch (ConnectionException e) { // retry if connect fail.
                log.warn("connect fail. retry: " + (MAX_TRY_TIME - retry), e);
            } catch (TfsException e) {
                log.error("get file meta info fail. " + filePath, e);
                return null;
            }
        } while (rnmrm == null && (metaTableObsoleted || (!metaTableObsoleted && --retry > 0)));

        return rnmrm;
    }

    private boolean doWrite(long userId, String filePath, FragInfo fragInfo) {
        int ret = TfsConstant.TFS_ERROR;
        int retry = MAX_TRY_TIME;
        long metaServerId = getNameMetaServerIdFromTable(userId);
    	boolean metaTableObsoleted = false;
    	
        do {
        	metaTableObsoleted = false;
        	long version = metaTables.get(rootServerId).getVersion();
            try {
                ret = NameMetaSessionHelper.nameMetaWrite(metaServerId, new NameMetaUserInfo(appId, userId),
                                                          filePath, fragInfo, version);
            } catch (ErrorStatusException e) {
            	if (e.getStatus() == TfsConstant.EXIT_TABLE_VERSION_ERROR) {
            		metaTableObsoleted = true;
            		boolean bRet = getTable();
            		if (false == bRet) {
            			metaTableObsoleted = false;
            			log.error("update table failed! retry: " + (MAX_TRY_TIME - retry));
            		}
            		else {
            			metaServerId = getNameMetaServerIdFromTable(userId);
            		}
            	}
            	else {
                    log.error("write meta data fail.", e);
            		return false;
            	}
            } catch (ConnectionException e) { // retry if connect fail.
                log.warn("connect fail. retry: " + (MAX_TRY_TIME - retry), e);
            } catch (TfsException e) {
                log.error("write meta data fail.");
                return false;
            }
        } while (ret != TfsConstant.TFS_SUCCESS && (metaTableObsoleted || (!metaTableObsoleted && --retry > 0)));

        return ret != TfsConstant.TFS_SUCCESS ? false : true;
    }

    // read data based on fraginfo and write to output. read as much as possible,
    // return once read fail, so data already readed before fail is OK
    private long readData(long offset, long length, FragInfo fragInfo, OutputStream output) {
        List<FragMeta> fragMetaList = fragInfo.getFragMetaList();
        if (fragMetaList.size() == 0) { // read over
            return 0;
        }

        long curLength = 0;
        long curOffset = offset;
        long leftLength = length;

        for (FragMeta fragMeta : fragMetaList) {
            try {
                // should never happen
                if (curOffset  >= fragMeta.getOffset() + fragMeta.getLength()) {
                    continue;
                }

                if (curOffset < fragMeta.getOffset()) {
                    curLength = Math.min(leftLength, fragMeta.getOffset() - curOffset);
                    // file hole, just fill and continue
                    output.write(new byte[(int)curLength]);
                    leftLength -= curLength;
                    curOffset += curLength;
                    if (leftLength <= 0) {
                        break;
                    }
                }

                curLength = Math.min(leftLength, fragMeta.getLength() - (curOffset - fragMeta.getOffset()));

                log.debug("@@ read tfs data, offset: " + (curOffset - fragMeta.getOffset()) + " curLength: " + curLength
                          + " fragmeta: " + fragMeta);
                if (!rcManager.read(fragInfo.getClusterId(), fragMeta.getBlockId(), fragMeta.getFileId(),
                                       curOffset - fragMeta.getOffset(), curLength, output)) {
                    log.error("read fail. " + fragMeta);
                    break;
                }

                leftLength -= curLength;
                curOffset += curLength;
                if (leftLength <= 0) {
                    break;
                }
            } catch (Exception e) {
                log.error("read fail. " + fragMeta, e);
                // set clusterId to -1 as read fail
                fragInfo.setClusterId(-1);
                break;
            }
        }

        return (length - leftLength);
    }

    private long writeData(long offset, byte[] data, long dataOffset, long length, FragInfo fragInfo) {
        // cluster may be 0 when first write, get writable cluster then use it.
        int clusterId = rcManager.getWriteClusterId(fragInfo.getClusterId());
        if (clusterId <= 0) {
            log.error("invalid clusterid for write: " + fragInfo.getClusterId());
            return -1;
        }
        fragInfo.setClusterId(clusterId);

        long curOffset = offset;
        long curDataOffset = dataOffset;
        long leftLength = length;

        long curLength = 0;
        do {
            curLength = Math.min(leftLength, ClientConfig.SEGMENT_LENGTH);

            log.debug("@@ write data. curOffset: " + curOffset
                      + " curDataOffset: " + curDataOffset
                      + " curLength: " + curLength);

            FragMeta fragMeta =
                rcManager.write(fragInfo.getClusterId(), curOffset, data, (int)curDataOffset, (int)curLength);
            if (fragInfo == null) {
                log.error("write fail. offset: " + curOffset +
                          " dataoffset: " + curDataOffset +
                          " leftlength: " + leftLength +
                          " clusterid: " + clusterId);
                break;
            }
            fragInfo.add(fragMeta);

            curOffset += curOffset < 0 ? 0 : curLength;
            curDataOffset += curLength;
            leftLength -= curLength;
        } while (leftLength > 0);

        return (length - leftLength);
    }

    private boolean unlinkFile(FragInfo fragInfo) {
        for (FragMeta fragMeta : fragInfo.getFragMetaList()) {
            if (!rcManager.unlink(fragInfo.getClusterId(), fragMeta.getBlockId(), fragMeta.getFileId())) {
                // just ignore.
                log.warn("unlink fail. blockid: " + fragMeta.getBlockId() +
                         " fileid: " + fragMeta.getFileId() +
                         " clusterId: " + fragInfo.getClusterId());
            }
        }

        return true;
    }

    private FragInfo readFileFragMeta(long userId, String filePath) {
        if ((filePath = canonicalFilePath(filePath)) == null) {
            log.error("invalid file path.");
            return null;
        }

        FragInfo fragInfo = new FragInfo();
        long offset = 0;
        RespNameMetaReadMessage rnmrm = null;

        do {
            rnmrm = doRead(userId, filePath, offset, Long.MAX_VALUE);
            if (rnmrm == null) {
                log.error("do read file meta info fail. offset: " + offset);
                return null;
            }

            fragInfo.setClusterId(rnmrm.getFragInfo().getClusterId());
            fragInfo.merge(rnmrm.getFragInfo());
            offset += rnmrm.getFragInfo().getLength();
        } while (rnmrm.getFragInfo().getLength() > 0 && rnmrm.hasNext());

        return fragInfo;
    }

    private boolean createFileIfNotExist(long userId, String filePath) {
        if (lsFile(userId, filePath) == null) { // maybe fail, maybe not exist
            return createFile(userId, filePath);
        }
        return true;
    }
    
    private RespNameMetaGetTableMessage getTableFromRootServer(long rootServerId) {
        int retry = MAX_TRY_TIME;
        RespNameMetaGetTableMessage rnmgtm = null;
        do {
	        try {
	        	rnmgtm = NameMetaSessionHelper.getTable(rootServerId);
	        } catch (ErrorStatusException e) {
	            log.error("get table from root server " + rootServerId + " failed!", e);
	            return null;
	        } catch (ConnectionException e) { // retry if connect fail.
	            log.warn("connect fail. retry: " + (MAX_TRY_TIME - retry), e);
	        } catch (TfsException e) {
	            log.error("get table from root server " + rootServerId + " failed!", e);
	            return null;
	        }
        } while (rnmgtm == null && --retry > 0);
        return rnmgtm;
    }

    // canonical file path.
    // 1. trim whitespace.
    // 2. trim adjacent "/"
    // 3. trim tailing "/"
    private String canonicalFilePath(String filePath) {
        String origFilePath = filePath;
        if (filePath == null || filePath.length() <= 0) {
            log.error("invalid filePath: " + origFilePath);
            return null;
        }

        filePath = filePath.trim();
        if (filePath.length() <= 0 || filePath.charAt(0) != '/') {
            log.error("invalid filePath: " + origFilePath);
            return null;
        }
        filePath = filePath.replaceAll("/{2,}", "/");
        int length = filePath.length();
        if (length <= 0) {
            log.error("invalid filePath: " + origFilePath);
            return null;
        }

        if (length > 1 &&       // not "/"
            filePath.endsWith("/")) {
            filePath = filePath.substring(0, length - 1);
        }

        return filePath;
    }

    private boolean isSubDir(String subDir, String parentDir) {
        if ((subDir = canonicalFilePath(subDir)) == null ||
            (parentDir = canonicalFilePath(parentDir)) == null) {
            return true;
        }

        if (subDir.equals(parentDir) || subDir.indexOf(parentDir + "/") == 0) {
            return true;
        }

        return false;
    }

}
