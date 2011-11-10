/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.impl;

import com.taobao.common.tfs.impl.SegmentData.SegmentStatus;

public enum FilePhase {
    FILE_PHASE_OPEN_FILE(SegmentStatus.SEG_STATUS_NOT_INIT,
                         SegmentStatus.SEG_STATUS_OPEN_OVER),
    FILE_PHASE_CREATE_FILE(SegmentStatus.SEG_STATUS_OPEN_OVER,
                           SegmentStatus.SEG_STATUS_CREATE_OVER),
    FILE_PHASE_WRITE_FILE(SegmentStatus.SEG_STATUS_CREATE_OVER,
                          SegmentStatus.SEG_STATUS_BEFORE_CLOSE_OVER),
    FILE_PHASE_CLOSE_FILE(SegmentStatus.SEG_STATUS_BEFORE_CLOSE_OVER,
                          SegmentStatus.SEG_STATUS_ALL_OVER),
    FILE_PHASE_READ_FILE(SegmentStatus.SEG_STATUS_OPEN_OVER,
                         SegmentStatus.SEG_STATUS_ALL_OVER),
    FILE_PHASE_STAT_FILE(SegmentStatus.SEG_STATUS_OPEN_OVER,
                         SegmentStatus.SEG_STATUS_ALL_OVER),
    FILE_PHASE_UNLINK_FILE(SegmentStatus.SEG_STATUS_OPEN_OVER,
                           SegmentStatus.SEG_STATUS_ALL_OVER);

    private SegmentStatus previouStatus;
    private SegmentStatus currentStatus;

    FilePhase(SegmentStatus previouStatus, SegmentStatus currentStatus) {
        this.previouStatus = previouStatus;
        this.currentStatus = currentStatus;
    }

    public SegmentStatus getPreviousStatus() {
        return previouStatus;
    }

    public SegmentStatus getCurrentStatus() {
        return currentStatus;
    }
}
