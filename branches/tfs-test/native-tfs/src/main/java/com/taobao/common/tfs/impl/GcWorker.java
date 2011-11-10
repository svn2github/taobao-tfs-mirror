/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.impl;

import java.util.TimerTask;
import java.util.Collection;

import java.io.IOException;
import java.io.File;
import java.io.FileFilter;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.common.tfs.TfsException;
import com.taobao.common.tfs.common.TfsConstant;

public class GcWorker extends TimerTask {
    private static final Log log = LogFactory.getLog(GcWorker.class);

    private static final int GC_GARBAGE_FILE = 1;
    private static final int GC_EXPIRED_LOCAL_KEY = 2;

    private File gcFilePath = new File(TfsConstant.GC_FILE_PATH);
    private File localKeyPath = new File(TfsConstant.TFS_TMP_PATH);

    private FileFilter expiredFileFilter = new FileFilter() {
            public boolean accept(File file) {
                return (file.isFile() &&
                        (System.currentTimeMillis() - file.lastModified() > ClientConfig.GC_EXPIRED_TIME)
                        );
            }
        };

    public void run() {
        try {
            runGc(GC_GARBAGE_FILE);
            runGc(GC_EXPIRED_LOCAL_KEY);
        } catch (TfsException e) {
            log.error("", e);
        }
    }

    private void runGc(int type) throws TfsException {
        File gcPath = (type == GC_GARBAGE_FILE) ?
            gcFilePath : localKeyPath;
        if (!gcPath.isDirectory()) {
            log.info("gc path directory not exist: " + gcPath.getAbsolutePath());
            return;
        }

        File[] fileList = getExpiredFile(gcPath);

        log.info("run gc, path: " + gcPath.getAbsolutePath() + ", type: " + type);
        if (fileList == null ||
            fileList.length == 0) {
            log.info("no expired file, no gc");
        } else if (type == GC_EXPIRED_LOCAL_KEY) {
            doGc((new LocalKey()), fileList);
        } else if (type == GC_GARBAGE_FILE) {
            doGc((new GcFile()), fileList);
        } else {
            throw new TfsException("invaid gc type");
        }
    }

    private void doGc(SegmentInfoContainer segmentInfoContainer, File[] fileList) {
        for (File file : fileList) {
            if (file.length() == 0) {
                log.info("expired gc file is empty, unlink. " + file.getAbsolutePath());
                file.delete();
                continue;
            }

            String fileName = file.getAbsolutePath();
            log.info("do gc filename " + fileName);

            int serverIdIndex = fileName.lastIndexOf('!');
            if (serverIdIndex == -1) {
                log.error("file name is invalid, no server id: " + fileName);
                // unlink ?
                continue;
            }

            FileLock fileLock = null;
            try {
                FileChannel fileChannel = (new RandomAccessFile(file, "rw")).getChannel();
                fileLock = fileChannel.tryLock();
                if (fileLock == null) {
                    log.warn("file: " +  fileName +
                             " is busy, maybe another gc worker is working over it");
                    continue;
                }

                long serverId = Long.parseLong(fileName.substring(serverIdIndex + 1));
                segmentInfoContainer.loadFile(fileName);
                Collection<SegmentInfo> segmentInfos = segmentInfoContainer.getSegmentInfos();

                for (SegmentInfo segmentInfo : segmentInfos) {
                    if (TfsManagerLite.unlinkFile(segmentInfo.getBlockId(),
                                              segmentInfo.getFileId(), serverId)) {
                        log.info("gc success. blockId: " + segmentInfo.getBlockId() +
                                 " fileId: " + segmentInfo.getFileId() +
                                 " serverId: " + serverId);
                    } else {
                        log.error("gc fail. blockId: " + segmentInfo.getFileId() +
                                  " fileId: " + segmentInfo.getFileId() +
                                  " serverId: " + serverId);
                    }
                }
            } catch (Exception e) {
                log.warn("", e);
            } finally {
                try {
                    if (fileLock != null) {
                        fileLock.release();
                    }
                    segmentInfoContainer.cleanUp();
                    // delete anyway
                    file.delete();
                } catch (Exception e) {
                    log.warn("filelock realse fail.", e);
                }
            }
        }
    }

    private File[] getExpiredFile(File path) {
        return path.listFiles(expiredFileFilter);
    }
}
