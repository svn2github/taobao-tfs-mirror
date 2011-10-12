/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.namemeta;

import java.nio.ByteBuffer;

import com.taobao.common.tfs.common.TfsConstant;

public class NameMetaUserInfo {
    private long appId;
    private long userId;

    public NameMetaUserInfo() {
    }

    public NameMetaUserInfo(long appId, long userId) {
        this.appId = appId;
        this.userId = userId;
    }

    public void setAppId(long appId) {
        this.appId = appId;
    }

    public long getAppId() {
        return appId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public long getUserId() {
        return userId;
    }

    public static int length() {
        return TfsConstant.LONG_SIZE * 2;        
    }

    public void serialize(ByteBuffer byteBuffer) {
        byteBuffer.putLong(appId);
        byteBuffer.putLong(userId);
    }

    public String toString() {
        return "appId: " + appId + " userId: " + userId;
    }
}
