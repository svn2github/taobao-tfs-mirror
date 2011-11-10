/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.impl;

import java.util.Collection;

import java.io.IOException;
import com.taobao.common.tfs.TfsException;
import com.taobao.common.tfs.impl.SegmentInfo;

public interface SegmentInfoContainer {

    public void loadFile(String fileName) throws TfsException, IOException;

    public Collection<SegmentInfo> getSegmentInfos();

    public int getSegmentCount();

    public long getSegmentLength();

    public void cleanUp();
}

