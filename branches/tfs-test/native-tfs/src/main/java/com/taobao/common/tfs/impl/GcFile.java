package com.taobao.common.tfs.impl;

import java.util.Collection;
import java.util.List;
import java.util.ArrayList;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.common.tfs.common.TfsConstant;
import com.taobao.common.tfs.common.TfsUtil;
import com.taobao.common.tfs.TfsException;

public class GcFile implements SegmentInfoContainer {
    private static final Log log = LogFactory.getLog(GcFile.class);

    private static final int GC_BATCH_WRITE_COUNT = 10;

    private String gcFileName = null;
    private RandomAccessFile fileOp = null;

    private SegmentHead segmentHead = new SegmentHead();
    private List<SegmentInfo> segmentInfoList = new ArrayList<SegmentInfo>();
    private ByteBuffer rawData = null;

    private long filePos = SegmentHead.size();
    private boolean isLoad = false;

    public void loadFile(String fileName) throws TfsException, IOException {
        gcFileName = fileName;
        isLoad = true;
        fileOp = new RandomAccessFile(fileName, "rw");
        load();
    }

    public Collection<SegmentInfo> getSegmentInfos() {
        return segmentInfoList;
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
            if (!isLoad && segmentInfoList.size() != 0) {
                saveGc();
            }

            fileOp.close();
            fileOp = null;

            if (segmentHead.getSegmentCount() == 0) {
                (new File(gcFileName)).delete();
            }
        } catch (IOException e) {
            log.warn("clean up gcfile fail.", e);
        }
    }

    public void init(String fileName) throws TfsException {
        gcFileName = TfsConstant.GC_FILE_PATH + fileName;

        TfsUtil.createDirectory(TfsConstant.GC_FILE_PATH, TfsConstant.TFS_TMP_PATH_MODE);
        segmentInfoList.clear();

        try {
            fileOp = new RandomAccessFile(gcFileName, "rw");
            if (fileOp.length() == 0) {
                log.info("create new gc file: " + gcFileName);
            } else if (fileOp.length() < SegmentHead.size()) {
                log.warn("gc file is invaid, ignore: " + gcFileName);
            } else {
                log.info("gc file exist, load: " + gcFileName);
                loadHead();
            }
        } catch (Exception e) {
            throw new TfsException("init gcfile fail: " + fileName, e);
        }
    }

    public void addSegment(SegmentInfo segmentInfo) {
        segmentInfoList.add(segmentInfo);
        segmentHead.increment(segmentInfo.getLength());
    }

    public boolean remove() {
        cleanUp();
        return (new File(gcFileName)).delete();
    }

    private void load() throws IOException {
        loadHead();
        loadSegment();
    }

    private void loadHead() throws IOException {
        rawData = ByteBuffer.allocate(SegmentHead.size());
        fileOp.readFully(rawData.array());
        segmentHead.deserialize(rawData);
        log.debug("gc file load head: " + segmentHead);
    }

    private void loadSegment() throws IOException {
        int count = segmentHead.getSegmentCount();
        SegmentInfo segmentInfo;
        segmentInfoList.clear();

        rawData = ByteBuffer.allocate(count * SegmentInfo.size());
        fileOp.readFully(rawData.array());
        for (int i = 0; i < count; i++) {
            segmentInfo = new SegmentInfo();
            segmentInfo.deserialize(rawData);
            segmentInfoList.add(segmentInfo);
        }
    }

    public void save() {
        if (segmentInfoList.size() > GC_BATCH_WRITE_COUNT) {
            saveGc();
        }
    }

    private void saveGc() {
        byte[] data = dump();
        try {
            fileOp.seek(0);
            fileOp.write(data, 0, SegmentHead.size());
            fileOp.seek(filePos);
            fileOp.write(data, SegmentHead.size(), data.length - SegmentHead.size());
            filePos += segmentInfoList.size() * SegmentInfo.size();
            segmentInfoList.clear();
        } catch (IOException e) {
            log.error("save gc file fail.", e);
        }
    }

    private byte[] dump() {
        rawData = ByteBuffer.allocate(SegmentHead.size() + segmentInfoList.size() * SegmentInfo.size());
        segmentHead.serialize(rawData);
        for (SegmentInfo segmentInfo : segmentInfoList) {
            segmentInfo.serialize(rawData);
        }
        return rawData.array();
    }

}
