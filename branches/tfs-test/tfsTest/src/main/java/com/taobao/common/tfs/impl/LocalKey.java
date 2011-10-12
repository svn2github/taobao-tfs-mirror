/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.impl;

import java.lang.Math;
import java.util.List;
import java.util.ArrayList;
import java.util.TreeSet;
import java.util.Iterator;

import java.nio.ByteBuffer;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.common.tfs.TfsException;
import com.taobao.common.tfs.common.TfsUtil;
import com.taobao.common.tfs.common.TfsConstant;

public class LocalKey implements SegmentInfoContainer {
    private static final Log log = LogFactory.getLog(LocalKey.class);

    private String localKeyName = null;
    private RandomAccessFile fileOp = null;
    private ByteBuffer rawData = null;

    private GcFile gcFile = new GcFile();
    private SegmentHead segmentHead = new SegmentHead();
    private TreeSet<SegmentInfo> segmentInfoSet =
        new TreeSet<SegmentInfo>(new SegmentInfo.SegmentInfoComparator());

    private TfsSession session = null; // for stat
    public void setSession(TfsSession session) {
        this.session = session;
    }
    public TfsSession session() {
        return session;
    }

    public void loadFile(String fileName) throws TfsException, IOException {
        localKeyName = fileName;
        fileOp = new RandomAccessFile(fileName, "rw");
        load();
    }

    public TreeSet<SegmentInfo> getSegmentInfos() {
        return segmentInfoSet;
    }

    public int getSegmentCount() {
        return segmentHead.getSegmentCount();
    }

    public long getSegmentLength() {
        return segmentHead.getSegmentLength();
    }

    public void cleanUp() {
        if (fileOp == null) {
            return;
        }

        try {
            fileOp.close();
            fileOp = null;

            if (segmentHead.getSegmentCount() == 0) {
                (new File(localKeyName)).delete();
            }
        } catch (IOException e) {
            log.warn("cleanup localkey fail", e);
        }

        gcFile.cleanUp();
    }

    public void init(String key, long serverId)
        throws TfsException {
        TfsUtil.createDirectory(TfsConstant.TFS_TMP_PATH, TfsConstant.TFS_TMP_PATH_MODE);
        clear();

        try {
            localKeyName = convertName(key, serverId);

            fileOp = new RandomAccessFile(localKeyName, "rw");

            if (fileOp.length() == 0) {
                log.info("create new localkey file: " + localKeyName);
            } else if (fileOp.length() < SegmentHead.size()) {
                log.warn("localkey file is invaid, ignore: " + localKeyName);
            } else {
                log.info("localkey file exist, load: " + localKeyName);
                try {
                    load();
                } catch (Exception e) {
                    log.warn("load localkey fail, file is invalid, create new localkey file. " + localKeyName, e);
                    clear();
                }
            }
            // init gc file with localkey file name
            gcFile.init(localKeyName.substring(TfsConstant.TFS_TMP_PATH.length()));
        } catch (Exception e) {
            throw new TfsException("init local key fail: " + localKeyName, e);
        }
    }

    public void load(byte[] data) throws TfsException {
        rawData = ByteBuffer.wrap(data);
        loadHead();
        if (data.length  < segmentHead.getSegmentCount() * SegmentInfo.size() + SegmentHead.size()) {
            throw new TfsException("data length not enough to hold head recording segment count: " +
                                   data.length + " < " +
                                   segmentHead.getSegmentCount() * SegmentInfo.size() + SegmentHead.size());
        }

        loadSegment();
    }

    public byte [] dump() throws TfsException {
        rawData = ByteBuffer.allocate(SegmentHead.size() + segmentHead.getSegmentCount() * SegmentInfo.size());
        segmentHead.serialize(rawData);
        for (SegmentInfo segmentInfo : segmentInfoSet) {
            segmentInfo.serialize(rawData);
        }
        return rawData.array();
    }

    public void save() throws TfsException, IOException {
        fileOp.seek(0);
        fileOp.write(dump());
        gcFile.save();
    }

    public void addSegment(SegmentInfo segmentInfo) throws TfsException {
        if (segmentInfoSet.add(segmentInfo)) {
            if (log.isDebugEnabled()) {
                log.debug("add segment info. " + segmentInfo);
            }
            segmentHead.increment(segmentInfo.getLength());
        } else {
            throw new TfsException("add segment info fail. " + segmentInfo);
        }
    }

    public int getSegmentForWrite(List<SegmentData> segmentDataList, long offset,
                                  byte[] data, int start, int length) {
        long currentOffset = offset;
        int currentStart = start;
        int remainLength = length, writtenLength = 0,
            needWriteLength = 0, remainNwLength = 0, // remain_need_write_length
            totalLength = 0;
        List<SegmentInfo> gcSegmentList = new ArrayList<SegmentInfo>();
        SegmentInfo segmentInfo = new SegmentInfo();
        segmentDataList.clear();

        while (segmentDataList.size() < ClientConfig.BATCH_COUNT &&
               remainLength > 0) {
            writtenLength = needWriteLength = 0;

            remainNwLength =
                Math.min((ClientConfig.BATCH_COUNT - segmentDataList.size()) * ClientConfig.SEGMENT_LENGTH,
                         remainLength);

            segmentInfo.setOffset(currentOffset);
            TreeSet<SegmentInfo> tailInfoSet =
                (TreeSet<SegmentInfo>)segmentInfoSet.tailSet(segmentInfo);

            if (tailInfoSet.size() == 0) {
                needWriteLength = remainNwLength;
                checkOverlap(segmentInfo, gcSegmentList);
            } else {
                SegmentInfo firstInfo = tailInfoSet.first();
                if (firstInfo.getOffset() != currentOffset) {
                    needWriteLength = (int)(firstInfo.getOffset() - currentOffset);
                    checkOverlap(segmentInfo, gcSegmentList);
                }
                if (needWriteLength > remainNwLength) {
                    needWriteLength = remainNwLength;
                } else {
                    Iterator it = tailInfoSet.iterator();
                    remainNwLength -= needWriteLength;
                    while (remainNwLength > 0 && it.hasNext()) {
                        SegmentInfo curInfo = (SegmentInfo)it.next();
                        int tmpCrc = 0, curLength = curInfo.getLength();
                        if (remainNwLength < curLength) {
                            log.info("segment length conflict: " + curLength + " <> " + remainNwLength);
                            needWriteLength += remainNwLength;
                            remainNwLength = 0;
                        } else if ((tmpCrc = TfsUtil.crc32(0, data, currentStart + needWriteLength, curLength)) !=
                                   curInfo.getCrc()) {
                            log.info("segment crc conflict: " + curInfo.getCrc() + " <> " + tmpCrc);
                            needWriteLength += curLength;
                            remainNwLength -= curLength;
                        } else if (TfsFile.stat(this.session,
                                                     curInfo.getBlockId(), curInfo.getFileId()) == null) {
                            log.info("segment info is invalid: blockid: " + curInfo.getBlockId() +
                                     " fileid: " + curInfo.getFileId());
                            needWriteLength += curLength;
                            remainNwLength -= curLength;
                        } else {        // full segment crc is correct and status is normal, use it
                            log.debug("segment data written: " + curInfo);
                            writtenLength += curLength;
                            remainNwLength = 0;
                            break;
                        }
                        gcSegmentList.add(curInfo);
                    }
                    if (!it.hasNext()) {
                        needWriteLength += remainNwLength;
                    }
                }
            }

            getSegment(segmentDataList, currentOffset, data,
                       currentStart, needWriteLength);
            totalLength = needWriteLength + writtenLength;
            remainLength -= totalLength;
            currentStart += totalLength;
            currentOffset += totalLength;
            gcSegment(gcSegmentList);
        }
        return (length - remainLength);
    }

    public int getSegmentForRead(List<SegmentData> segmentDataList, long offset,
                                 byte[] data, int start, int length) {
        if (offset > segmentHead.getSegmentLength()) {
            log.error("read offset over file length: " + offset + " > " + segmentHead.getSegmentLength());
            return 0;
        }

        // To read, segment info SHOULD and MUST be adjacent and completed
        // but not check here ...
        SegmentInfo segmentInfo = new SegmentInfo();
        int checkLength = 0;
        int currentLength = 0;
        segmentDataList.clear();

        segmentInfo.setOffset(offset);

        TreeSet<SegmentInfo> tailInfoSet =
            (TreeSet<SegmentInfo>)segmentInfoSet.tailSet(segmentInfo);

        if (tailInfoSet.size() == 0 ||
            tailInfoSet.first().getOffset() != offset) {
            TreeSet<SegmentInfo> headInfoSet =
                (TreeSet<SegmentInfo>)segmentInfoSet.headSet(segmentInfo);
            // should NEVER happen: queried offset less than least offset(0) in stored segment info
            if (headInfoSet.size() == 0) {
                log.error("can not find segment for offset: " + offset);
                return TfsConstant.EXIT_GENERAL_ERROR;
            }

            SegmentInfo endInfo = headInfoSet.last();
            // actually SHOULD always occur, cause adjacent and completed read segment info
            if (endInfo.getOffset() + endInfo.getLength() > offset) {
                checkLength = (int)Math.min(length,
                                            endInfo.getOffset() + endInfo.getLength() - offset);
                SegmentData segmentData = new SegmentData(endInfo);
                segmentData.setInnerOffset((int)(offset - endInfo.getOffset()));
                segmentData.setData(data, start, checkLength);

                segmentDataList.add(segmentData);
            }
        }

        // get following adjacent segment info
        Iterator it = tailInfoSet.iterator();
        while (segmentDataList.size() < ClientConfig.BATCH_COUNT &&
               checkLength < length && it.hasNext()) {
            segmentInfo = (SegmentInfo)it.next();
            currentLength = Math.min(segmentInfo.getLength(), length - checkLength);

            SegmentData segmentData = new SegmentData(segmentInfo);
            segmentData.setData(data, start + checkLength, currentLength);

            segmentDataList.add(segmentData);
            checkLength += currentLength;
        }
        return checkLength;
    }

    public boolean validate(long length) {
        if (length != 0 && segmentHead.getSegmentLength() < length) {
            log.error("segment containing size less than required size: " +
                      segmentHead.getSegmentLength() + " < " + length);
            return false;
        }
        if (segmentHead.getSegmentCount() != segmentInfoSet.size()) {
            log.error("segment head recording count conflict with info count: " +
                      segmentHead.getSegmentCount() + " <> " + segmentInfoSet.size());
            return false;
        }
        if (segmentHead.getSegmentCount() == 0) {
            log.info("no segment info");
            return true;
        }
        if (segmentInfoSet.first().getOffset() != 0) {
            log.error("segment offset not start with 0: " + segmentInfoSet.first().getOffset());
            return false;
        }

        Iterator<SegmentInfo> it = segmentInfoSet.iterator();
        SegmentInfo currentInfo = it.next();
        SegmentInfo nextInfo;
        long totalLength = (length == 0) ? segmentHead.getSegmentLength() : length;
        long checkLength = currentInfo.getLength();

        while (checkLength < totalLength && it.hasNext()) {
            nextInfo = it.next();
            if (currentInfo.getOffset() + currentInfo.getLength() != nextInfo.getOffset()) {
                log.error("segment info conflict (offset + size != nextOffset): " +
                          currentInfo.getOffset() + " + " + currentInfo.getLength() +
                          " != " + nextInfo.getOffset());
                return false;
            }
            checkLength += nextInfo.getLength();
            currentInfo = nextInfo;
        }

        // no enough or overlap
        if (checkLength != totalLength) {
            log.error("segment info length conflict with required length: "
                      + checkLength + " != " + totalLength);
            return false;
        }

        // has remaining segment info, gc them
        if (length != 0 && it.hasNext()) {
            List<SegmentInfo> gcSegmentList = new ArrayList<SegmentInfo>();
            while (it.hasNext()) {
                gcSegmentList.add(it.next());
            }
            gcSegment(gcSegmentList);
        }
        return true;
    }

    public void clear() {
        segmentHead.clear();
        segmentInfoSet.clear();
    }

    public boolean remove() throws IOException {
        cleanUp();
        return (new File(localKeyName)).delete();
    }

    private String convertName(String key, long id) throws IOException {
        // "\\" is for transfer Windows' file separator
        return TfsConstant.TFS_TMP_PATH +
            (new File(key)).getAbsolutePath().replaceAll("[\\" + TfsConstant.SEPARATOR + ":]", "!") +
            '!' + id;
    }

    private void load() throws TfsException, IOException {
        fileOp.seek(0);
        rawData = ByteBuffer.allocate(SegmentHead.size());
        fileOp.readFully(rawData.array());
        loadHead();

        rawData = ByteBuffer.allocate(segmentHead.getSegmentCount() * SegmentInfo.size());
        fileOp.readFully(rawData.array());
        loadSegment();
    }

    private void loadHead() {
        segmentHead.deserialize(rawData);
    }

    private void loadSegment() throws TfsException {
        int count = segmentHead.getSegmentCount();
        SegmentInfo segmentInfo;

        segmentInfoSet.clear();
        for (int i = 0; i < count; i++) {
            segmentInfo = new SegmentInfo();
            segmentInfo.deserialize(rawData);
            if (segmentInfoSet.add(segmentInfo)) {
                if (log.isDebugEnabled()) {
                    log.debug("load segment success: " + segmentInfo);
                }
            } else {
                throw new TfsException("load segment fail: " + segmentInfo);
            }
        }
    }

    private void checkOverlap(SegmentInfo segmentInfo, List<SegmentInfo> gcSegmentList) {
        TreeSet<SegmentInfo> headInfoSet =
            (TreeSet<SegmentInfo>)(segmentInfoSet.headSet(segmentInfo));
        if (headInfoSet.size() != 0) {
            SegmentInfo endInfo = headInfoSet.last();
            // overlap, gc
            if (endInfo.getOffset() + endInfo.getLength() > segmentInfo.getOffset()) {
                gcSegmentList.add(endInfo);
            }
        }
    }

    private void getSegment(List<SegmentData> segmentDataList, long offset,
                            byte[] data, int dataOffset, int length) {
        if (length <= 0) {
            return;
        }
        int currentLength = 0;
        int checkLength = 0;

        while (checkLength < length) {
            currentLength = Math.min(ClientConfig.SEGMENT_LENGTH, length - checkLength);

            SegmentData segmentData = new SegmentData();
            segmentData.setOffset(offset + checkLength);
            segmentData.setLength(currentLength);
            segmentData.setData(data, dataOffset + checkLength, currentLength);
            segmentDataList.add(segmentData);

            checkLength += currentLength;
        }
    }

    private void gcSegment(List<SegmentInfo> segmentInfoList) {
        long totalLength = 0;
        for (SegmentInfo segmentInfo : segmentInfoList) {
            totalLength += segmentInfo.getLength();
            gcFile.addSegment(segmentInfo);
            segmentInfoSet.remove(segmentInfo);
        }
        segmentHead.decrement(segmentInfoList.size(), totalLength);
        segmentInfoList.clear();
    }

}
