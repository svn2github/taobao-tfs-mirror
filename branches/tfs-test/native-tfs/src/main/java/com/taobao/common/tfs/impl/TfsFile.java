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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.common.tfs.TfsException;
import com.taobao.common.tfs.common.TfsConstant;
import com.taobao.common.tfs.common.TfsUtil;
import com.taobao.common.tfs.exception.ConnectionException;
import com.taobao.common.tfs.packet.DsListWrapper;
import com.taobao.common.tfs.packet.SetBlockInfoMessage;
import com.taobao.common.tfs.packet.RespReadDataMessageV2;
import com.taobao.common.tfs.packet.RespCreateFilenameMessage;
import com.taobao.common.tfs.packet.FileInfo;
import com.taobao.common.tfs.unique.UniqueStore;

public abstract class TfsFile {
    class FilePos {
        private long offset;
        private int crc;
        private boolean fail;
        private boolean eof;

        public FilePos() {
            offset = 0;
            crc = 0;
            eof = false;
            fail = false;
        }

        public FilePos(long offset, int eof, int crc) {
            this.offset = offset;
            this.crc = crc;
            this.eof = false;
            this.fail = false;
        }

        public long getOffset() {
            return offset;
        }

        public void setOffset(long offset) {
            this.offset = offset;
        }

        public int getCrc() {
            return crc;
        }

        public void setCrc(int crc) {
            this.crc = crc;
        }

        public boolean isEof() {
            return eof;
        }

        public void setEof() {
            this.eof = true;
        }

        public boolean isFail() {
            return fail;
        }

        public void setFail(boolean fail) {
            this.fail = fail;
        }

        public void advance(int offset) {
            this.offset += offset;
        }

        public void reset() {
            offset = 0;
            crc = 0;
            eof = false;
            fail = false;
        }
    }

    private static final Log log = LogFactory.getLog(TfsFile.class);

    // stat flag
    public static final int NORMAL_STAT = 0;
    public static final int FORCE_STAT= 1;

    // read flag
    public static final int NORMAL_READ = 0;
    public static final int FORCE_READ = 1;

    // option flag
    public static final int DEFAULT_OPTION_FLAG = 0;
    public static final int NO_SYNC_LOG_OPTION_FLAG = 1;
    public static final int CLOSE_WRITE_FAIL_OPTION_FLAG = 2;

    public static final int SEEK_SET = 0;
    public static final int SEEK_CUR = 1;

    protected FilePos filePos = new FilePos();
    protected FSName fsName;
    protected FileInfo fileInfo;
    protected TfsSession session;
    protected int mode = 0;
    protected boolean isOpen = false;
    protected long fileNumber = 0;
    protected int optionFlag = DEFAULT_OPTION_FLAG;

    protected DsListWrapper dsListWrapper;

    public TfsSession getSession() {
        return session;
    }

    public void setSession(TfsSession session) {
        this.session = session;
    }

    public int getBlockId() {
        return fsName.getBlockId();
    }

    public long getFileId() {
        return fsName.getFileId();
    }

    public int getOptionFlag() {
        return optionFlag;
    }

    public void setOptionFlag(int optionFlag) {
        this.optionFlag = optionFlag;
    }

    public boolean isEof() {
        return this.filePos.isEof();
    }

    public long getOffset() {
        return this.filePos.getOffset();
    }

    public FileInfo getFileInfo() {
        return this.fileInfo;
    }

    public FilePos getFilePos() {
        return this.filePos;
    }

    private long getPrimaryDs() throws TfsException {
        return dsListWrapper.getDsList().get(0);
    }

    public long seek(long offset, long whence) {
        long current = getOffset();
        if (whence == SEEK_SET)
            current = offset;
        else if (whence == SEEK_CUR)
            current += offset;
        filePos.setOffset(current);
        return current;
    }

    public void open(int blockId, long fileId, int mode) throws TfsException {
        fsName = new FSName(blockId, fileId);
        open(mode);
    }

    protected void open(int mode) throws TfsException {
        if (session == null) {
            throw new TfsException("session not set!!");
        }

        this.mode = mode;
        int blockId = fsName.getBlockId();
        long fileId = fsName.getSeqId();

        if ((mode & (TfsConstant.READ_MODE | TfsConstant.STAT_MODE)) != 0) {
            if (blockId == 0) {
                throw new TfsException("invalid block id for read: " + blockId);
            }
            dsListWrapper = session.getReadBlockInfo(blockId);
        } else if ((mode & TfsConstant.UNLINK_MODE) != 0) {
            dsListWrapper = session.getUnlinkBlockInfo(blockId);
        } else { // for write.
            SetBlockInfoMessage message = session.getWriteBlockInfo(blockId, mode);
            blockId = message.getBlockId();
            fsName.setBlockId(blockId);
            dsListWrapper = message.getDsListWrapper();
        }

        // check return value;
        if (fsName.getBlockId() == 0 || dsListWrapper == null
            || dsListWrapper.getDsList() == null
            || dsListWrapper.getDsList().size() == 0) {
            throw new TfsException("cannot get block ds list:"
                                   + fsName.getBlockId());
        }

        // if write_mode, must get write number from master!
        if ((mode & TfsConstant.WRITE_MODE) != 0) {
            RespCreateFilenameMessage rcfm =
                session.createFileName(blockId, fileId, getPrimaryDs());

            if (rcfm.getFileId() == 0 || rcfm.getFileNumber() == 0) {
                throw new TfsException("create file name failed:"
                                       + rcfm.getBlockId());
            }

            fsName.setSeqId((int)rcfm.getFileId());
            fileNumber = rcfm.getFileNumber();
        }

        filePos.reset();
        isOpen = true;
    }

    protected byte[] read(int length)
    throws TfsException {
        return read(length, NORMAL_READ);
    }

    protected byte[] read(int length, int readFlag)
        throws TfsException {
        if (!isOpen) {
            throw new TfsException("file not opened.");
        }
        if ((mode &
             (TfsConstant.READ_MODE | TfsConstant.STAT_MODE | TfsConstant.UNLINK_MODE)) == 0) {
            throw new TfsException("file open without read permitted mode: " + mode);
        }
        if (filePos.isEof()) {
            throw new TfsException("end of file: " + getFileName());
        }
        if (fileInfo == null) {
            throw new TfsException("must stat before read file: "
                                   + getFileName());
        }

        List<Long> dsList = dsListWrapper.getDsList();
        int blockId = fsName.getBlockId();
        long fileId = fsName.getFileId();
        int dsSize = dsList.size();

        for (int index = Math.abs((int) (fileId % dsSize)), retry = 0;
             retry < dsSize; ++retry, ++index) {
            try {
                index = index % dsSize;
                long server = dsList.get(index);
                byte[] data = session.readData(blockId, fileId, (int)filePos.getOffset(), // just small file offset
                                               length, server, readFlag);
                if (data != null && data.length > 0) {
                    filePos.advance(data.length);
                    if (filePos.getOffset() >= fileInfo.getLength()) {
                        filePos.setEof();
                    }
                    return data;
                }
            } catch (TfsException e) {
                log.warn("read data fail: " + e.getMessage() + " retry: "
                         + (retry + 1));
            }
        }

        throw new TfsException("read data fail: file not exist in all dataserver");
    }

    protected byte[] readV2(int length)
        throws TfsException {
        return readV2(length, NORMAL_READ);
    }

    protected byte[] readV2(int length, int readFlag)
        throws TfsException {
        if (!isOpen) {
            throw new TfsException("file not opened.");
        }
        if ((mode &
             (TfsConstant.READ_MODE | TfsConstant.STAT_MODE | TfsConstant.UNLINK_MODE)) == 0) {
            throw new TfsException("file open without read permitted mode: " + mode);
        }
        if (filePos.isEof()) {
            throw new TfsException("read end of file:" + getFileName());
        }

        List<Long> dsList = dsListWrapper.getDsList();
        int blockId = fsName.getBlockId();
        long fileId = fsName.getFileId();
        int dsSize = dsList.size();

        for (int index = Math.abs((int) (fileId % dsSize)), retry = 0;
             retry < dsSize; ++retry, ++index) {
            try {
                index = index % dsSize;
                long server = dsList.get(index);
                RespReadDataMessageV2 message =
                    session.readDataV2(blockId, fileId, (int)filePos.getOffset(), // just small file offset
                                       length, server, readFlag);

                byte[] data = message.getData();


                if (filePos.getOffset() == 0) {
                    fileInfo = message.getFileInfo();
                }

                if (fileInfo == null) {
                  fileInfo = statEx(NORMAL_STAT);
                }

                if (data != null && data.length > 0) {
                    filePos.advance(data.length);

                    if (filePos.getOffset() >= fileInfo.getLength()) {
                        filePos.setEof();
                    }
                    return data;
                }
            } catch (TfsException e) {
                log.warn("read data fail: " + e.getMessage() + " retry: " + (retry + 1));
            }
        }

        throw new TfsException("read data fail: file not exist in all dataserver");
    }

    protected FileInfo statEx(int type) throws TfsException {
        if (!isOpen) {
            throw new TfsException("file not opened.");
        }
        if ((mode & (TfsConstant.READ_MODE | TfsConstant.STAT_MODE | TfsConstant.UNLINK_MODE)) == 0) {
            throw new TfsException("file open without read permitted mode: " + mode);
        }

        int blockId = fsName.getBlockId();
        long fileId = fsName.getFileId();
        List<Long> dsList = dsListWrapper.getDsList();
        fileInfo = null;
        int dsSize = dsList.size();

        for (int index = Math.abs((int) (fileId % dsSize)), retry = 0;
             null == fileInfo && retry < dsSize;
             ++retry, ++index) {
            try {
                index = index % dsSize;
                fileInfo = session.getFileInfo(blockId, fileId,
                                               type, dsList.get(index));
                return fileInfo;
            } catch (ConnectionException e) {
                log.warn("stat file fail: " + e.getMessage() + " retry: " + (retry + 1));
            }
        }

        throw new TfsException("stat file fail in all dataserver");
    }

    public static FileInfo stat(TfsSession session, int blockId, long fileId) {
        try {
            TfsSmallFile file = new TfsSmallFile();
            file.setSession(session);
            file.open(blockId, fileId, TfsConstant.READ_MODE);
            return file.stat(NORMAL_STAT);
        } catch (TfsException e) {
            log.warn("stat fail. server: " + session.getNameServerIp() +
                     " blockid " + blockId + " fileid: " + fileId, e);
        }
        return null;
    }

    public abstract String createFileName() throws TfsException;

    public abstract String getFileName();

    public abstract String getFileName(boolean simpleName);

    public abstract void open(String mainName, String suffix, int mode)
        throws TfsException;

    public abstract FileInfo stat()
        throws TfsException;

    public abstract FileInfo stat(int type)
        throws TfsException;

    public abstract int read(byte[] data, int offset, int length)
        throws TfsException;

    public abstract int write(byte[] data, int offset, int length)
        throws TfsException;

    public abstract long unlink(int action)
        throws TfsException;

    public abstract String saveUnique(UniqueStore uniqueStore, String mainName,
                                      String suffix, byte[] data, int offset, int length)
        throws TfsException;

    public abstract long unlinkUnique(UniqueStore uniqueStore, String mainName, String suffix)
        throws TfsException;

    public abstract int close()
        throws TfsException;
}
