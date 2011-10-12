/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.impl;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.common.tfs.TfsException;
import com.taobao.common.tfs.common.TfsConstant;
import com.taobao.common.tfs.common.TfsUtil;
import com.taobao.common.tfs.packet.UnlinkFileMessage;
import com.taobao.common.tfs.packet.FileInfo;
import com.taobao.common.tfs.unique.UniqueStore;
import com.taobao.common.tfs.namemeta.FragMeta;

public class TfsManagerLite {

    private class FilePool {
        // reuse once operated file. threadsafe.
        private ThreadLocal<TfsSmallFile> fileLocal = new ThreadLocal<TfsSmallFile>();
        private ThreadLocal<TfsLargeFile> largeFileLocal = new ThreadLocal<TfsLargeFile>();

        // large file use fd to support manipulating multifile simultaneously
        // used in synchronized method, no atomic need
        private Map<Integer, TfsLargeFile> largeFileMap = new HashMap<Integer, TfsLargeFile>();
        private int globalFd = 0;
        private static final int MAX_FILE_FD = Integer.MAX_VALUE;
        private static final int MAX_OPEN_FD_COUNT = MAX_FILE_FD - 1;

        public TfsSmallFile getFile(String nsAddr) throws TfsException {
            TfsSmallFile tfsFile = (TfsSmallFile)fileLocal.get();

            if (tfsFile == null) {
                tfsFile = new TfsSmallFile();
                fileLocal.set(tfsFile);
            }

            tfsFile.setSession(TfsSessionPool.getSession(nsAddr));
            return tfsFile;
        }

        public TfsLargeFile getLargeFile(String nsAddr) throws TfsException {
            TfsLargeFile tfsFile = (TfsLargeFile)largeFileLocal.get();

            if (tfsFile == null) {
                tfsFile = new TfsLargeFile();
                tfsFile.setSession(TfsSessionPool.getSession(nsAddr));
                largeFileLocal.set(tfsFile);
            } else {
                tfsFile.cleanUp();
            }
            return tfsFile;
        }

        public synchronized TfsLargeFile getLargeFile(int fd) throws TfsException {
            TfsLargeFile tfsLargeFile = largeFileMap.get(fd);
            if (tfsLargeFile == null) {
                throw new TfsException("invaild fd: " + fd);
            }
            return tfsLargeFile;
        }

        public synchronized int getLargeFileFd(String nsAddr) throws TfsException {
            if (largeFileMap.size() >= MAX_OPEN_FD_COUNT) {
                throw new TfsException("too much file opened.");
            }

            if (globalFd == MAX_FILE_FD) {
                globalFd = 0;
            }

            boolean fdConflict = true;
            int retry = MAX_OPEN_FD_COUNT;
            // fd overlap
            while (retry-- > 0 && (fdConflict = largeFileMap.containsKey(++globalFd))) {
                if (globalFd == MAX_FILE_FD) {
                    globalFd = 0;
                }
            }

            if (fdConflict) {
                throw new TfsException("too much file opened.");
            }

            TfsLargeFile tfsLargeFile = new TfsLargeFile();
            tfsLargeFile.setSession(TfsSessionPool.getSession(nsAddr));
            largeFileMap.put(globalFd, tfsLargeFile);

            return globalFd;
        }

        public synchronized void removeFile(int fd) {
            TfsLargeFile tfsFile = largeFileMap.get(fd);
            if (tfsFile != null) {
                // cleanup for reuse
                tfsFile.cleanUp();
                tfsFile = null;
            }

            largeFileMap.remove(fd);
        }
    }

    private static final Log log = LogFactory.getLog(TfsManagerLite.class);

    // check file type
    public static final int TFS_INVALID_FILE_TYPE = 1;
    public static final int TFS_SMALL_FILE_TYPE = 2;
    public static final int TFS_LARGE_FILE_TYPE = 3;

    // file pool
    private FilePool filePool = new FilePool();

    // timer
    private boolean runTimer = true;

    public TfsManagerLite() {
    }

    public TfsManagerLite(boolean runTimer) {
        this.runTimer = runTimer;
    }

    public void init() {
        if (runTimer) {
            ManagerTimerTask.getInstance().init();
        }
    }

    public void destroy() {
        ManagerTimerTask.getInstance().destroy();
    }

    public int getCacheHitRatio() {
        return ManagerTimerTask.getInstance().getCacheHitRatio();
    }

    public String saveFile(int type, String nsAddr, String tfsFileName, String tfsSuffix,
                           FileInputStream input, long length, boolean simpleName, String key) {
        TfsFile file = null;

        try {
            if (type == TFS_LARGE_FILE_TYPE) {
                file = filePool.getLargeFile(nsAddr);
                ((TfsLargeFile)file).open(tfsFileName, tfsSuffix, TfsConstant.WRITE_MODE, key);
            } else if (type == TFS_SMALL_FILE_TYPE) {
                file = filePool.getFile(nsAddr);
                file.open(tfsFileName, tfsSuffix, TfsConstant.WRITE_MODE);
            } else {
                log.error("invalid file type: " + type);
                return null;
            }

            int perLength = (int)Math.min(ClientConfig.MAX_LARGE_IO_LENGTH, length);
            byte[] data = new byte[perLength];
            int readLength = 0;

            while (length > 0) {
                readLength = input.read(data, 0, perLength);
                if (readLength > 0) {
                    file.write(data, 0, readLength);
                    length -= readLength;
                } else {
                    break;
                }
            }

            file.close();
            String retName = file.getFileName(simpleName);
            return (simpleName ? retName + (null == tfsSuffix ? "" : tfsSuffix) :
                    retName);
        } catch (Exception e) {
            log.error("save file fail: => " + tfsFileName + " , "
                      + tfsSuffix , e);
        }
        return null;
    }

    public String saveFile(int type, String nsAddr, String tfsFileName, String tfsSuffix,
                           byte[] data, int offset, int length, boolean simpleName, String key) {
        TfsFile file = null;
        try {
            if (type == TFS_LARGE_FILE_TYPE) {
                file = filePool.getLargeFile(nsAddr);
                ((TfsLargeFile)file).open(tfsFileName, tfsSuffix, TfsConstant.WRITE_MODE, key);
            } else if (type == TFS_SMALL_FILE_TYPE) {
                file = filePool.getFile(nsAddr);
                file.open(tfsFileName, tfsSuffix, TfsConstant.WRITE_MODE);
            } else {
                log.error("local file is empty or too large. not support now.");
                return null;
            }

            file.write(data, offset, length);
            file.close();

            String retName = file.getFileName(simpleName);
            return (simpleName ? retName + (null == tfsSuffix ? "" : tfsSuffix) :
                    retName);
        } catch (TfsException e) {
            log.error("save file fail: => " + tfsFileName + " , "
                      + tfsSuffix , e);
        }

        return null;
    }

    public long fetchFile(String nsAddr, String tfsFileName, String tfsSuffix,
                          String localFileName) {
        try {
            BufferedOutputStream output =
                new BufferedOutputStream(new FileOutputStream(localFileName));
            long retLength = fetchFile(nsAddr, tfsFileName, tfsSuffix, 0, Long.MAX_VALUE, output);
            output.close();
            return retLength;
        } catch (IOException e) {
            log.error("fetch: " + tfsFileName + "," + tfsSuffix + "=>"
                      + localFileName, e);
        }

        return -1;
    }

    public long fetchFile(String nsAddr, String tfsFileName, String tfsSuffix, long fileOffset,
                             long length, OutputStream output) {
        int type;
        if ((type = checkFileType(tfsFileName)) == TFS_LARGE_FILE_TYPE) {
            return fetchLargeFile(nsAddr, tfsFileName, tfsSuffix, fileOffset, length, output);
        }
        if (type == TFS_SMALL_FILE_TYPE) {
            return fetchSmallFile(nsAddr, tfsFileName, tfsSuffix, fileOffset, length, output);
        }
        log.error("file name is invalid: " + tfsFileName + " suffix: " + tfsSuffix);

        return -1;
    }

    // default mode is FORCE_STAT mode
    public FileInfo statFile(String nsAddr, String tfsFileName, String tfsSuffix) {
        if (nsAddr == null) {
            log.error("ns address is null");
            return null;
        }
        int type;
        if ((type = checkFileType(tfsFileName)) == TFS_LARGE_FILE_TYPE) {
            return statLargeFile(nsAddr, tfsFileName, tfsSuffix);
        }
        if (type == TFS_SMALL_FILE_TYPE) {
            return statSmallFile(nsAddr, tfsFileName, tfsSuffix);
        }
        log.error("file name is invalid: " + tfsFileName + " suffix: " + tfsSuffix);

        return null;
    }

    public long unlinkFile(String nsAddr, String tfsFileName, String tfsSuffix, int action) {
        int type;
        if ((type = checkFileType(tfsFileName)) == TFS_LARGE_FILE_TYPE) {
            return unlinkLargeFile(nsAddr, tfsFileName, tfsSuffix, action);
        }
        if (type == TFS_SMALL_FILE_TYPE) {
            return unlinkSmallFile(nsAddr, tfsFileName, tfsSuffix, action);
        }

        log.error("file name is invalid: " + tfsFileName + " suffix: " + tfsSuffix);
        return -1;
    }

    /***************************************
     *    unique store
     ***************************************/
    public String saveUniqueFile(UniqueStore uniqueStore, String nsAddr, String tfsFileName,
                                 String tfsSuffix, InputStream input, int length, boolean simpleName) {
        try {
            // once read, just small file
            byte[] data = new byte[length];
            if (input.read(data) != length) {;
                return null;
            }
            return saveUniqueFile(uniqueStore, nsAddr, tfsFileName, tfsSuffix, data, 0, length, simpleName);
        } catch (IOException e) {
            log.error("io error.", e);
        }
        return null;
    }

    public String saveUniqueFile(UniqueStore uniqueStore, String nsAddr,
                                 String tfsFileName, String tfsSuffix, byte[] data,
                                 int offset, int length, boolean simpleName) {
        boolean reSave = false;
        try {
            TfsFile file = filePool.getFile(nsAddr);

            // lazy init
            uniqueStore.init();

            String retName = file.saveUnique(uniqueStore, tfsFileName, tfsSuffix, data, offset, length);

            // unique store name may be simple type. refactor
            retName = new FSName(retName).get(false, simpleName);

            // simple name type, then add suffix.
            return simpleName ? (retName + (null == tfsSuffix ? "" : tfsSuffix)) : retName;
        } catch (RuntimeException e) {
            // tair may throw runtimeexception if first init fail.
            // must reinit again
            uniqueStore.reset();
            log.warn("saveUniqueFile fail, just saveFile. runtime exception: ", e);
            reSave = true;
        } catch (Exception e) {
            log.warn("saveUniqueFile fail, just saveFile.", e);
            reSave = true;
        }

        if (reSave) {
            String retName = saveFile(TFS_SMALL_FILE_TYPE, nsAddr, tfsFileName, tfsSuffix, data, 0, data.length, simpleName, null);
            // not simple type, add suffix
            if (retName != null && !simpleName) {
                retName = retName + (tfsSuffix == null ? "" : tfsSuffix);
            }
            return retName;
        }
        return null;
    }

    public long unlinkUniqueFile(UniqueStore uniqueStore, String nsAddr, String tfsFileName, String tfsSuffix) {
        try {
            // large file not support unique store
            if (checkFileType(tfsFileName) == TFS_LARGE_FILE_TYPE) {
                return unlinkFile(nsAddr, tfsFileName, tfsSuffix, UnlinkFileMessage.DELETE);
            }

            TfsFile file = filePool.getFile(nsAddr);

            // lazy init
            uniqueStore.init();

            return file.unlinkUnique(uniqueStore, tfsFileName, tfsSuffix);
        } catch (RuntimeException e) {
            // tair may throw runtimeexception if first init fail.
            // must reinit again. little ugly ..
            log.error("unlink unique fail, runtime exception caught: " + tfsFileName + "," + tfsSuffix, e);
            uniqueStore.reset();
        } catch (Exception e) {
            // unlink unique, tair must alive ..
            log.error("unlink fail: " + tfsFileName + "," + tfsSuffix, e);
        }

        return -1;
    }

    // stream way for large file,
    // oepnReadFile  ==> readFile  ==> closeFile
    // openWriteFile ==> writeFile ==> closeFile
    public int openFile(String nsAddr, String tfsFileName, String tfsSuffix, int mode, String key) {
        if (nsAddr == null) {
            log.error("ns address is null");
            return TfsConstant.EXIT_INVALIDFD_ERROR;
        }

        int fd = TfsConstant.EXIT_INVALIDFD_ERROR;

        TfsLargeFile file = null;
        try {
            fd = filePool.getLargeFileFd(nsAddr);
            file = filePool.getLargeFile(fd);
        } catch (TfsException e) {
            log.error("get fd fail.", e);
            return TfsConstant.EXIT_INVALIDFD_ERROR;
        }

        try {
            file.open(tfsFileName, tfsSuffix, mode, key);
        } catch (TfsException e) {
            log.error("open tfs file fail. ", e);
            filePool.removeFile(fd);
            return TfsConstant.EXIT_INVALIDFD_ERROR;
        }

        return fd;
    }

    public int readFile(int fd, byte[] data, int offset, int length) {
        try {
            TfsLargeFile file = filePool.getLargeFile(fd);
            return file.read(data, offset, length);
        } catch (TfsException e) {
            log.error("read tfs fail fd: " + fd, e);
        }

        return TfsConstant.EXIT_INVALIDFD_ERROR;
    }

    public int readFile(int fd, long fileOffset, byte[] data, int offset, int length) {
        try {
            TfsLargeFile file = filePool.getLargeFile(fd);
            return file.read(data, offset, length, fileOffset);
        } catch (TfsException e) {
            log.error("read tfs fail fd: " + fd, e);
        }

        return TfsConstant.EXIT_INVALIDFD_ERROR;
    }

    public int writeFile(int fd, byte[] data, int offset, int length) {
        try {
            TfsLargeFile file = filePool.getLargeFile(fd);
            return file.write(data, offset, length);
        } catch (TfsException e) {
            log.error("save file fail", e);
        }

        return TfsConstant.EXIT_INVALIDFD_ERROR;
    }

    public String closeFile(int fd) {
        try {
            TfsLargeFile file = filePool.getLargeFile(fd);
            file.close();
            String tfsName = file.getFileName();
            return tfsName;
        } catch (TfsException e) {
            log.error("close file fail", e);
            return null;
        } finally {
            filePool.removeFile(fd);
        }
    }

    /************************************
     * sort of specified implementation *
     ************************************/
    private long fetchSmallFile(String nsAddr, String tfsFileName, String tfsSuffix, long offset,
                                   long length, OutputStream output) {
        long totalLength = 0;
        try {
            TfsFile file = filePool.getFile(nsAddr);
            file.open(tfsFileName, tfsSuffix, TfsConstant.READ_MODE);
            if (offset > 0) {
                file.seek(offset, TfsFile.SEEK_SET);
            }
            if (length == Long.MAX_VALUE) {
                while (!file.isEof()) {
                    byte[] data = file.readV2(ClientConfig.MAX_SMALL_IO_LENGTH);
                    output.write(data);
                    totalLength += data.length;
                }
            } else {
                totalLength = length;
                while (length > 0 && !file.isEof()) {
                    byte[] data = file.readV2(ClientConfig.MAX_SMALL_IO_LENGTH);
                    output.write(data);
                    length -= data.length;
                }
            }
            file.close();
            return totalLength;
        } catch (TfsException e) {
            log.error("fetch to outputStream fail: " + tfsFileName + "," + tfsSuffix, e);
        } catch (IOException e) {
            log.error("fetch to outputStream fail: " + tfsFileName + "," + tfsSuffix, e);
        }

        return -1;
    }

    private FileInfo statSmallFile(String nsAddr, String tfsFileName, String tfsSuffix) {
        try {
            TfsFile file = filePool.getFile(nsAddr);
            file.open(tfsFileName, tfsSuffix, TfsConstant.READ_MODE);
            FileInfo info = file.stat(TfsFile.FORCE_STAT);
            file.close();
            return info;
        } catch (TfsException e) {
            log.error("stat: " + tfsFileName + "," + tfsSuffix, e);
        }

        return null;
    }

    private long unlinkSmallFile(String nsAddr, String tfsFileName, String tfsSuffix, int action) {
        try {
            TfsFile file = filePool.getFile(nsAddr);
            file.open(tfsFileName, tfsSuffix, TfsConstant.UNLINK_MODE);
            long retLength = file.unlink(action);
            file.close();
            return retLength;
        } catch (TfsException e) {
            log.error("unlink: " + tfsFileName + "," + tfsSuffix, e);
        }

        return -1;
    }

    private long fetchLargeFile(String nsAddr, String tfsFileName, String tfsSuffix, long offset,
                                long length, OutputStream output) {
        byte[] data  = new byte[ClientConfig.MAX_LARGE_IO_LENGTH];
        TfsFile file = null;
        long totalLength = 0;

        try {
            file = filePool.getLargeFile(nsAddr);
            file.open(tfsFileName, tfsSuffix, TfsConstant.READ_MODE);
            if (offset > 0) {
                file.seek(offset, TfsFile.SEEK_SET);
            }
            int readLength = 0;
            if (length == Long.MAX_VALUE) {
                while (!file.isEof()) {
                    readLength = file.read(data, 0, ClientConfig.MAX_LARGE_IO_LENGTH);
                    output.write(data, 0, readLength);
                    totalLength += readLength;
                }
            } else {            // read specified length
                totalLength = length;
                while (length > 0 && !file.isEof()) {
                    readLength = file.read(data, 0, ClientConfig.MAX_LARGE_IO_LENGTH);
                    output.write(data, 0, readLength);
                    length -= readLength;
                }
            }
            return totalLength;
        } catch (TfsException e) {
            log.error("fetch to outputStream fail: " + tfsFileName + "," + tfsSuffix, e);
        } catch (IOException e) {
            log.error("fetch to outputStream fail: " + tfsFileName + "," + tfsSuffix, e);
        } finally {
            try {
                file.close();
            } catch (TfsException e) {
                log.error("close tfs file fail");
            }
        }

        return -1;
    }

    private long unlinkLargeFile(String nsAddr, String tfsFileName, String tfsSuffix, int action) {
        try {
            if (action == UnlinkFileMessage.UNDELETE) {
                log.error("tfs large file not support undelete now");
                return -1;
            }

            // CONCEAL | REVEAL only operate over meta file
            if (action != UnlinkFileMessage.DELETE) {
                return unlinkSmallFile(nsAddr, tfsFileName, tfsSuffix, action);
            }

            TfsFile file = filePool.getLargeFile(nsAddr);
            file.open(tfsFileName, tfsSuffix, TfsConstant.UNLINK_MODE);
            long retLength = file.unlink(action);
            file.close();
            return retLength;
        } catch (TfsException e) {
            log.error("unlink: " + tfsFileName + "," + tfsSuffix, e);
        }
        return -1;
    }

    private FileInfo statLargeFile(String nsAddr, String tfsFileName, String tfsSuffix) {
        FileInfo info;
        try {
            TfsFile file = filePool.getLargeFile(nsAddr);
            file.open(tfsFileName, tfsSuffix, TfsConstant.STAT_MODE);
            info = file.stat(TfsFile.FORCE_STAT);
            file.close();
            return info;
        } catch (TfsException e) {
            log.error("stat: " + tfsFileName + "," + tfsSuffix, e);
        }

        return null;
    }

    /**************************************
     *     Assorted utility               *
     **************************************/
    // only check first char, serious name check is FSName's duty
    static public int checkFileType(String tfsFileName) {
        if (tfsFileName != null && !tfsFileName.isEmpty()) {
            char keyChar = tfsFileName.charAt(0);
            if (keyChar == FSName.TFS_SMALL_KEY_CHAR) {
                return TFS_SMALL_FILE_TYPE;
            }
            if (keyChar == FSName.TFS_LARGE_KEY_CHAR) {
                return TFS_LARGE_FILE_TYPE;
            }
        }
        return TFS_INVALID_FILE_TYPE;
    }

    // for gc
    static public boolean unlinkFile(int blockId, long fileId, long serverId) {
        try {
            TfsSession session = TfsSessionPool.getSession(TfsUtil.longToHostPure(serverId));
            TfsSmallFile file = new TfsSmallFile();
            file.setSession(session);

            file.open(blockId, fileId, TfsConstant.UNLINK_MODE);
            file.unlink(UnlinkFileMessage.DELETE);
            file.close();
            return true;
        } catch (TfsException e) {
            log.error("unlink: blockId: " + blockId + " fileId: " + fileId + " serverId: " + serverId, e);
        }

        return false;
    }

    static public int getClusterId(String nsAddr) {
        try {
            return TfsSessionPool.getSession(nsAddr).getTfsClusterIndex() - '0';
        } catch (TfsException e) {
            log.error("get cluster id fail.",e);
        }
        return -1;
    }
}
