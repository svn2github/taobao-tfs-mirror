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
import java.util.ArrayList;
import java.util.TreeSet;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.common.tfs.TfsException;
import com.taobao.common.tfs.exception.ConnectionException;
import com.taobao.common.tfs.common.TfsConstant;
import com.taobao.common.tfs.unique.UniqueStore;
import com.taobao.common.tfs.packet.FileInfo;
import com.taobao.common.tfs.packet.UnlinkFileMessage;
import com.taobao.common.tfs.impl.SegmentData.SegmentStatus;

public class TfsLargeFile extends TfsFile {

    private static final Log log = LogFactory.getLog(TfsLargeFile.class);

    private static final int MAX_TRY_TIME = 2;
    public static final int MAX_SMALL_FILE_LENGTH = 2 * (1 << 20); // 2M

    private LocalKey localKey = new LocalKey();
    private List<SegmentData> segList = new ArrayList<SegmentData>();

    public String createFileName()
        throws TfsException {
        throw new TfsException("large file not support deprecated createFileName method");
    }

    public String getFileName() {
        return fsName.get(session.getTfsClusterIndex(), true, false);
    }

    public String getFileName(boolean simpleName) {
        return fsName.get(session.getTfsClusterIndex(), true, simpleName);
    }

    public void open(String tfsFileName, String tfsSuffix, int mode)
        throws TfsException {
        open(tfsFileName, tfsSuffix, mode, null);
    }

    public boolean open(String tfsFileName, String tfsSuffix, int mode, String key)
        throws TfsException {
        if ((mode & TfsConstant.WRITE_MODE) != 0) {
            if (tfsFileName != null && tfsFileName.length() != 0) {
                throw new TfsException("update large file not supported now");
            }
            if (key == null || key.length() == 0) {
                throw new TfsException("large file write with null(empty) key: " + key);
            }

            localKey.init(key, session.getNameServerId());
            fsName = new FSName(null, tfsSuffix);
        } else {                // stat | read | unlink
            localKey.clear();
            fsName = new FSName(tfsFileName, tfsSuffix);
            if ((mode & (TfsConstant.READ_MODE | TfsConstant.UNLINK_MODE)) != 0) { // read or unlink
                loadMeta(mode);
            } else {
                open(mode);
            }
        }

        segList.clear();
        this.mode = mode;
        isOpen = true;
        filePos.reset();
        localKey.setSession(getSession());
        return true;
    }

    public int write(byte[] data, int offset, int length)
        throws TfsException {
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

        int checkLength = 0;
        int curLength = 0;
        int retry = 0;
        int ret = TfsConstant.TFS_ERROR;
        filePos.setFail(false);

        while (checkLength < length) {
            curLength = localKey.getSegmentForWrite(segList, filePos.getOffset() + checkLength,
                                                    data, offset, length - checkLength);
            if (curLength < 0) {
                throw new TfsException("get segment error, ret: " + curLength);
            }

            if (segList.size() == 0) {
                log.info("segment has written, data offset: " + offset + " length: " + curLength);
            } else {
                retry = MAX_TRY_TIME;
                do {
                    try {
                        ret = writeProcess();
                        finishWriteProcess(ret);
                    } catch (ConnectionException e) {
                        log.error("", e);
                        continue;
                    } catch (IOException e) { // save localkey fail
                        throw new TfsException(e);
                    }
                } while (ret != TfsConstant.TFS_SUCCESS && --retry > 0);

                if (ret != TfsConstant.TFS_SUCCESS) {
                    filePos.setFail(true);
                    throw new TfsException("write data fail. offset: " +
                                           (filePos.getOffset() + checkLength) +
                                           ", length: " + curLength);
                }
            }
            checkLength += curLength;
        }

        filePos.advance(checkLength);
        return checkLength;
    }

    private int writeProcess() throws TfsException {
        // get block info exception, must exit
        session.getWriteBlockInfo(segList);

        int ret = TfsConstant.TFS_SUCCESS;
        if ((ret = session.process(segList, FilePhase.FILE_PHASE_CREATE_FILE)) ==
            TfsConstant.EXIT_ALL_SEGMENT_FAIL_ERROR) {
            return ret;
        }
        if ((ret = session.process(segList, FilePhase.FILE_PHASE_WRITE_FILE)) ==
            TfsConstant.EXIT_ALL_SEGMENT_FAIL_ERROR) {
            return ret;
        }
        if ((ret = session.process(segList, FilePhase.FILE_PHASE_CLOSE_FILE)) ==
            TfsConstant.EXIT_ALL_SEGMENT_FAIL_ERROR) {
            return ret;
        }

        return ret;
    }

    private void finishWriteProcess(int ret) throws TfsException, IOException {
        // deal with different condition separately,
        // a little dummy code
        if (ret == TfsConstant.TFS_SUCCESS) { // all success
            for (SegmentData segmentData : segList) {
                localKey.addSegment(segmentData.getSegmentInfo());
            }
            segList.clear();
            localKey.save();
        } else if (ret == TfsConstant.EXIT_ALL_SEGMENT_FAIL_ERROR) { // all fail
            for (SegmentData segmentData : segList) {
                segmentData.writeReset();
            }
        } else {                // not all fail
            int i = 0;
            SegmentData segmentData;
            while (i < segList.size()) {
                segmentData = segList.get(i);
                if (segmentData.getSegmentStatus() == SegmentStatus.SEG_STATUS_ALL_OVER) {
                    localKey.addSegment(segmentData.getSegmentInfo());
                    segList.remove(i);
                } else {
                    segmentData.writeReset();
                    i++;
                }
            }
            localKey.save();
        }
    }

    public int close()
        throws TfsException {
        if (!isOpen) {
            throw new TfsException("file not open, close anyway");
        }
        if ((mode & TfsConstant.WRITE_MODE) == 0) {
            isOpen = false;
            return TfsConstant.TFS_SUCCESS;
        }
        if (filePos.getOffset() == 0) {
            isOpen = false;
            throw new TfsException("there's no any data written into tfsfile.");
        }
        if (filePos.isFail()) {
            isOpen = false;
            log.error("write fail, close");
            return TfsConstant.TFS_SUCCESS;
        }
        uploadKey();
        removeKey();

        return TfsConstant.TFS_SUCCESS;
    }

    public void cleanUp() {
        localKey.cleanUp();
    }

    private void uploadKey() throws TfsException {
        if (!localKey.validate(filePos.getOffset())) {
            throw new TfsException("validate local key fail");
        }
        byte[] data = localKey.dump();
        SegmentData segmentData = new SegmentData();
        segmentData.setLength(data.length);
        segmentData.setData(data, 0, data.length);
        segList.clear();
        segList.add(segmentData);

        int tryTime = MAX_TRY_TIME;
        int ret = TfsConstant.TFS_ERROR;

        while (ret != TfsConstant.TFS_SUCCESS && tryTime-- > 0) {
            segmentData.writeReset();
            try {
                // get block info exception, must exit
                session.getWriteBlockInfo(segList);
            } catch (ConnectionException e) {
                continue;
            }

            if ((ret = session.process(segList, FilePhase.FILE_PHASE_CREATE_FILE)) ==
                TfsConstant.EXIT_ALL_SEGMENT_FAIL_ERROR) {
                log.error("close fail: create meta file fail");
                continue;
            }

            fsName.setBlockId(segmentData.getBlockId());
            fsName.setSeqId((int)segmentData.getFileId());
            segmentData.setFileId(fsName.getFileId());

            if ((ret = session.process(segList, FilePhase.FILE_PHASE_WRITE_FILE)) ==
                TfsConstant.EXIT_ALL_SEGMENT_FAIL_ERROR) {
                log.error("close fail: write data fail");
                continue;
            }
            if ((ret = session.process(segList, FilePhase.FILE_PHASE_CLOSE_FILE)) ==
                TfsConstant.EXIT_ALL_SEGMENT_FAIL_ERROR) {
                log.error("close fail: close meta file fail");
                continue;
            }
        }
        if (ret != TfsConstant.TFS_SUCCESS) {
            throw new TfsException("close fail: upload key fail");
        }
    }

    private void removeKey() throws TfsException {
        try {
            localKey.remove();
        } catch (IOException e) {
            throw new TfsException("", e);
        }
    }

    private void loadMeta(int mode) throws TfsException {
        int saveMode = this.mode;
        open(TfsConstant.READ_MODE);
        int readFlag = NORMAL_READ;
        if ((mode & TfsConstant.UNLINK_MODE) != 0) {
            readFlag = FORCE_READ;
        }

        // avoid to stat first, use readV2 directly
        // first loop will success over in most case.
        byte[] metaData;
        byte[] data = super.readV2(MAX_SMALL_FILE_LENGTH, readFlag);
        if (fileInfo.getLength() <= MAX_SMALL_FILE_LENGTH) {
            metaData = data;
        } else {
            int leftLength = (int)fileInfo.getLength() - MAX_SMALL_FILE_LENGTH;
            byte[] leftData = super.read(leftLength, readFlag);
            metaData = new byte[(int)fileInfo.getLength()];
            System.arraycopy(data, 0, metaData, 0, data.length);
            System.arraycopy(leftData, 0, metaData, data.length, leftData.length);
        }

        localKey.load(metaData);

        this.mode = saveMode;
    }

    public int read(byte[] data, int offset, int length, long fileOffset, boolean modify)
        throws TfsException {
        if (!isOpen) {
            throw new TfsException("file not opened.");
        }
        if ((mode & (TfsConstant.READ_MODE | TfsConstant.UNLINK_MODE)) == 0) {
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
        if (modify && filePos.isEof()) {
            log.info("read reach file end");
            return 0;
        }

        int checkLength = 0;
        int curLength = 0;
        int retry = MAX_TRY_TIME;
        int ret = TfsConstant.TFS_ERROR;

        while (checkLength < length) {
            curLength = localKey.getSegmentForRead(segList, fileOffset + checkLength,
                                                   data, offset, length - checkLength);
            if (curLength < 0) {
                throw new TfsException("get segment for read fail, offset: " +
                                       (fileOffset + checkLength) +
                                       ", length: " + (length - checkLength));
            }
            if (curLength == 0) {
                log.debug("read reach file end, offset: " + (fileOffset + checkLength));
                if (modify) {
                    filePos.setEof();
                }
                break;
            }

            // read should retry twice at most to fixup dirty cache ?
            // min(MAX_TRY_TIME, 2)
            retry = MAX_TRY_TIME;
            ret = TfsConstant.TFS_ERROR;
            // outer retry get blockinfo.
            // inner retry ds list
            do {
                try {
                    ret = readProcess();
                } catch (ConnectionException e) {
                    log.error("", e);
                    continue;
                }
            } while (ret != TfsConstant.TFS_SUCCESS && --retry > 0);

            if (ret != TfsConstant.TFS_SUCCESS) {
                throw new TfsException("read data fail. offset: " +
                                       (fileOffset + checkLength) +
                                       ", length: " + curLength);
            }
            checkLength += curLength;
        }

        if (modify) {
            filePos.advance(checkLength);
        }
        return checkLength;
    }

    public int read(byte[] data, int offset, int length)
        throws TfsException {
        return read(data, offset, length, filePos.getOffset(), true);
    }

    public int read(byte[] data, int offset, int length, long fileOffset)
        throws TfsException {
        return read(data, offset, length, fileOffset, false);
    }

    private int readProcess() throws TfsException {
        session.getReadBlockInfo(segList);

        int ret = TfsConstant.TFS_ERROR;
        // segList.size() != 0
        // just use first ds list size to be retry times. maybe random ..
        int retry = segList.get(0).getDsListWrapper().getDsList().size();
        do {
            ret = session.process(segList, FilePhase.FILE_PHASE_READ_FILE);
            finishReadProcess(ret);
        } while (ret != TfsConstant.TFS_SUCCESS && --retry > 0);

        return ret;
    }

    private void finishReadProcess(int ret) {
        if (ret == TfsConstant.TFS_SUCCESS) { // all success
            segList.clear();
        } else if (ret != TfsConstant.EXIT_ALL_SEGMENT_FAIL_ERROR) { // not all fail
            int i = 0;
            SegmentData segmentData;
            while (i < segList.size()) {
                segmentData = segList.get(i);
                if (segmentData.getSegmentStatus() == SegmentStatus.SEG_STATUS_ALL_OVER) {
                    segList.remove(i);
                } else {
                    // readReset already done in readProcess
                    i++;
                }
            }
        }
    }

    public FileInfo stat() throws TfsException {
        return stat(NORMAL_STAT);
    }

    public FileInfo stat(int type)
        throws TfsException {
        statEx(type);
        if (fileInfo.getFlag() == TfsConstant.NORMAL_STATUS) {
            byte[] data = super.read(SegmentHead.size());
            SegmentHead segmentHead = new SegmentHead();
            segmentHead.deserialize(ByteBuffer.wrap(data));
            fileInfo.setLength(segmentHead.getSegmentLength());
            // occupyLength = realFileLength + metaFileLength
            fileInfo.setOccupyLength(segmentHead.getSegmentLength() + fileInfo.getOccupyLength());
        } // else ... avoid to recover unlink or hide status, just ignore

        return fileInfo;
    }

    public long unlink(int action)
        throws TfsException {
        if (!isOpen) {
            throw new TfsException("file not opened.");
        }
        if ((mode & TfsConstant.UNLINK_MODE) == 0) {
            throw new TfsException("not open unlink mode: " + mode);
        }

        long retLength = -1;

        if (session.unlinkFile(fsName.getBlockId(), fsName.getFileId(),
                               action, 0, session.getUnlinkBlockInfo(fsName.getBlockId())) <= 0) {
            log.error("unlink meta file fail.");
        } else {
            // return length
            retLength = localKey.getSegmentLength() + fileInfo.getLength();

            TreeSet<SegmentInfo> segmentInfoSet = localKey.getSegmentInfos();
            int i = 0;
            for (SegmentInfo segmentInfo : segmentInfoSet) {
                if (log.isDebugEnabled()) {
                    log.debug("unlink ok " + segmentInfo + " in " + i++);
                }
                session.unlinkFile(segmentInfo.getBlockId(), segmentInfo.getFileId(),
                                   action, 0, session.getUnlinkBlockInfo(segmentInfo.getBlockId()));
            }
        }

        return retLength;
    }

    public String saveUnique(UniqueStore uniqueStore, String mainName,
                             String suffix, byte[] data, int offset, int length)
        throws TfsException {
        throw new TfsException("large file saveUnique not supported");
    }

    public long unlinkUnique(UniqueStore uniqueStore, String mainName, String suffix)
        throws TfsException {
        throw new TfsException("large file unlinkUnique not supported");
    }

}
