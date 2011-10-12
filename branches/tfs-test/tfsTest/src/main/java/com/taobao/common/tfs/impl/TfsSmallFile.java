/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.taobao.common.tfs.common.TfsUtil;
import com.taobao.common.tfs.common.TfsConstant;
import com.taobao.common.tfs.TfsException;
import com.taobao.common.tfs.packet.CloseFileInfo;
import com.taobao.common.tfs.packet.FileInfo;
import com.taobao.common.tfs.packet.UnlinkFileMessage;
import com.taobao.common.tfs.packet.WriteDataInfo;
import com.taobao.common.tfs.unique.UniqueStore;
import com.taobao.common.tfs.unique.UniqueValue;

public class TfsSmallFile extends TfsFile {

    private static final Log log = LogFactory.getLog(TfsSmallFile.class);

    public String createFileName() throws TfsException {
        int retry = 0;
        while (retry++ < 3) {
            try {
                open(null, null, TfsConstant.WRITE_MODE | TfsConstant.NOLEASE_MODE);
            } catch (TfsException e) {
                log.error(e.getMessage());
                if (retry >= 3)
                    throw e;
            }
        }
        if (isOpen)
            isOpen = false;
        return getFileName();
    }

    public String getFileName() {
        return fsName.get(session.getTfsClusterIndex());
    }

    public String getFileName(boolean simpleName) {
        return fsName.get(session.getTfsClusterIndex(), false, simpleName);
    }

    public void open(String mainName, String suffix, int mode)
        throws TfsException {
        fsName = new FSName(mainName, suffix);
        open(mode);
    }

    public FileInfo stat() throws TfsException {
        return stat(NORMAL_STAT);
    }

    public FileInfo stat(int type)
        throws TfsException {
        return statEx(type);
    }

    private FileInfo stat(String mainName, String suffix, int type) throws TfsException {
        open(mainName, suffix, TfsConstant.READ_MODE);
        return stat(type);
    }

    // use read(int length)
    public int read(byte[] data, int offset, int length) throws TfsException {
        if (!isOpen) {
            throw new TfsException("file not opened.");
        }
        if ((mode & TfsConstant.READ_MODE) == 0) {
            throw new TfsException("file not opened with READ_MODE: " + mode);
        }
        if (data == null || offset < 0 || length < 0) {
            throw new TfsException("read data: " + data + " length : " + length
                                   + " offset " + offset + " illegal.");
        }
        if (data.length < offset + length) {
            throw new TfsException("read data length " + data.length +
                                   " less than required " + (offset + length));
        }

        byte[] readData = read(length);
        System.arraycopy(readData, 0, data, offset, length);
        return readData.length;
    }

    private int writeOneTime(byte[] data, int offset, int length) throws TfsException {
        WriteDataInfo writeDataInfo = new WriteDataInfo();
        writeDataInfo.setBlockId(fsName.getBlockId());
        writeDataInfo.setFileId(fsName.getFileId());
        writeDataInfo.setFileNumber(fileNumber);
        writeDataInfo.setOffset((int)filePos.getOffset()); // just small file offset
        writeDataInfo.setLength(length);
        writeDataInfo.setIsServer(0);
        int ret = session.writeData(writeDataInfo, dsListWrapper, data, offset);
        if (ret == TfsConstant.TFS_SUCCESS) {
            filePos.advance(length);
            filePos.setCrc(TfsUtil.crc32(filePos.getCrc(), data, offset, length));
        }
        return ret;
    }

    public int write(byte[] data, int offset, int length) throws TfsException {
        if (!isOpen) {
            throw new TfsException("file not opened.");
        }
        if ((mode & TfsConstant.WRITE_MODE) == 0) {
            throw new TfsException("file not opened with WRITE_MODE: " + mode);
        }
        if (data == null || offset < 0 || length < 0) {
            throw new TfsException("write data: " + data + " length : " + length
                                   + " offset " + offset + " illegal.");
        }
        if (data.length < length + offset) {
            throw new TfsException("write data length : " + data.length
                                   + " less than input:" + (length + offset));
        }

        int start = offset;
        int left = length;
        while (left > 0) {
            int writeLengthPerTime = left > ClientConfig.MAX_SMALL_IO_LENGTH ? ClientConfig.MAX_SMALL_IO_LENGTH : left;
            writeOneTime(data, start, writeLengthPerTime);
            start += writeLengthPerTime;
            left -= writeLengthPerTime;
        }
        return length;
    }

    public int close() throws TfsException {
        if (!isOpen) {
            log.error("file not open, close anyway");
            return TfsConstant.TFS_SUCCESS;
        }
        if ((mode & TfsConstant.WRITE_MODE) == 0) {
            isOpen = false;
            return TfsConstant.TFS_SUCCESS;
        }
        if (filePos.getOffset() == 0) {
            isOpen = false;
            // throw exception cause no tfs name will return
            throw new TfsException("there's no any data written into tfsfile.");
        }

        CloseFileInfo closeFileInfo = new CloseFileInfo();

        closeFileInfo.setBlockId(fsName.getBlockId());
        closeFileInfo.setFileId(fsName.getFileId());
        closeFileInfo.setFileNumber(fileNumber);
        closeFileInfo.setCrc(filePos.getCrc());
        closeFileInfo.setMode(0);
        int ret = session.closeFile(closeFileInfo, dsListWrapper, optionFlag);

        isOpen = false;
        return ret;
    }

    public long unlink(int action) throws TfsException {
        if (!isOpen) {
            throw new TfsException("file not opened.");
        }
        if ((mode & TfsConstant.UNLINK_MODE) == 0) {
            throw new TfsException("not unlink mode: " + mode);
        }

        return session.unlinkFile(fsName.getBlockId(), fsName.getFileId(), action,
                                  optionFlag, dsListWrapper);
    }

    private long unlink(String mainName, String suffix, int action)
        throws TfsException {
        open(mainName, suffix, TfsConstant.UNLINK_MODE);
        return unlink(action);
    }

    /**
     * compare tfsName's suffix and suffix
     *
     * @param tfsName
     * @param suffix
     * @return
     */
    private boolean checkSuffix(String tfsName, String suffix) {
        if (suffix == null || suffix.length() == 0) { // no supply suffix
            return true;
        }
        if (tfsName == null || tfsName.length() <= FSName.MAIN_FILENAME_LENGTH) { // orig no suffix, but supply suffix
            return false;
        }

        // orig has suffix and supply suffix
        String origSuffix = tfsName.substring(FSName.MAIN_FILENAME_LENGTH);
        if (origSuffix.length() >= FSName.STANDARD_SUFFIX_LENGTH) {
            origSuffix = origSuffix.substring(origSuffix.length()
                                              - FSName.STANDARD_SUFFIX_LENGTH);
        }
        if (suffix.length() >= FSName.STANDARD_SUFFIX_LENGTH) {
            suffix = suffix.substring(suffix.length() - FSName.STANDARD_SUFFIX_LENGTH);
        }
        return origSuffix.equals(suffix);
    }

    private String saveUniqueToTair(UniqueStore uniqueStore, byte[] key, UniqueValue value) {
        UniqueValue valueRet = null;
        String retName = value.getTfsName();

        try {
            int i = 0;
            for (; i < 3; i++) {
                valueRet = uniqueStore.insert(key, value);
                if (valueRet.getReferenceCount() == TfsConstant.UNIQUE_INSERT_VERSION_ERROR) { // version error
                    valueRet = uniqueStore.query(key);
                    valueRet.addRef(1);
                    value = valueRet;
                } else {
                    break;
                }
            }
            if (i > 0) {        // concurrent insert occurs
                FSName fsNameTfs = new FSName(retName);
                FSName fsNameTair = new FSName(valueRet.getTfsName());
                retName = valueRet.getTfsName();
                // new tfs file has insert, unlink old tfs file
                // unlink fail, return tfsname stored in tair
                if (fsNameTfs.getBlockId() != fsNameTair.getBlockId() ||
                    fsNameTfs.getFileId() != fsNameTair.getFileId()) {
                    unlink(retName, null, UnlinkFileMessage.DELETE);
                }
            }
        } catch (TfsException e) {
            // save to tair fail or unlink tfs file fail, just ignore
            log.warn("save unique fail, error: " + e.getMessage());
        }

        return retName;
    }

    /**
     * save unique file to tfs
     *
     * @param uniqueStore
     * @param mainName
     * @param suffix
     * @param data
     * @param offset
     * @param length
     * @param simpleName
     * @return
     * @exception
     */
    public String saveUnique(UniqueStore uniqueStore, String mainName,
                             String suffix, byte[] data, int offset, int length)
        throws TfsException {

        byte[] key = uniqueStore.getKey(data, offset, length);
        int reSave = 0x0;

        UniqueValue value = uniqueStore.query(key);
        String retName = value.getTfsName();

        if (value.getReferenceCount() == TfsConstant.UNIQUE_QUERY_NOT_EXIST) { // first save
            reSave = 0x3;     // need save tfs and tair
            value.setReferenceCount(1);
            value.setVersion(TfsConstant.FIRST_INSERT_UNIQUE_MAGIC_VERSION);
        } else if (! checkSuffix(retName, suffix)) { // suffix conflict, no saveunique
            reSave = 0x1;                                // need save tfs
            log.warn("suffix no match " + retName + " " + retName.length() + " <> " + suffix);
        } else {                // all ok, stat tfs file
            int status = TfsConstant.UNLINK_STATUS;
            try {
                FileInfo info = stat(retName, null, NORMAL_STAT);
                status = info.getFlag();
            } catch (TfsException e) {
                log.warn("stat unique file fail, need resave. " + retName + " error: " + e.getMessage());
            }

            value.addRef(1);

            if (status != TfsConstant.NORMAL_STATUS) {
                reSave = 0x3;   // save tfs and tair
            } else {
                reSave = 0x2;  // just save tair
            }
        }

        if ((reSave & 0x1) != 0) {
            open(mainName, suffix, TfsConstant.WRITE_MODE);
            write(data, offset, length);
            close();
            // return name has suffix
            retName = getFileName() + (null == suffix ? "" : suffix);
        }

        if ((reSave & 0x2) != 0) {
            value.setTfsName(retName);
            retName = saveUniqueToTair(uniqueStore, key, value);
        }

        return retName;
    }

    /**
     * unlink unique tfs file
     *
     * @param uniqueStore
     * @return
     * @exception
     */
    public long unlinkUnique(UniqueStore uniqueStore) throws TfsException {
        FileInfo info = stat();

        if (info.getFlag() == TfsConstant.NORMAL_STATUS) {
            byte[] data = read((int)info.getLength());
            byte[] key = uniqueStore.getKey(data, 0, data.length);
            int i = 0;
            UniqueValue value = new UniqueValue();
            do {
                i++;
                value = uniqueStore.query(key);
                value = uniqueStore.decrement(key, value);
                if (value.getReferenceCount() == 0) {
                    // open(TfsConstant.UNLINK_MODE)
                    this.mode = TfsConstant.UNLINK_MODE;
                    unlink(UnlinkFileMessage.DELETE);
                    break;
                }
            } while (i < 3 && value.getReferenceCount() == TfsConstant.UNIQUE_INSERT_VERSION_ERROR);
            log.debug("unlinkunique refcnt: " + value.getReferenceCount());
            return info.getLength();
        }
        throw new TfsException("unlinkunique fail. file not in normal status: " + info.getFlag());
    }

    public long unlinkUnique(UniqueStore uniqueStore, String mainName, String suffix) throws TfsException {
        open(mainName, suffix, TfsConstant.READ_MODE);
        return unlinkUnique(uniqueStore);
    }

}
