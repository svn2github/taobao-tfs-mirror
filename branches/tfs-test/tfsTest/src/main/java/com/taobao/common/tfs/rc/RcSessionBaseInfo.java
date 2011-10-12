/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.rc;

import java.nio.ByteBuffer;

import com.taobao.common.tfs.common.TfsConstant;
import com.taobao.common.tfs.common.TfsUtil;
import com.taobao.common.tfs.common.StreamTranscoderUtil;

public class RcSessionBaseInfo {
    private String sessionId;
    private String clientVersion;
    private long cacheSize = 0;
    private long cacheTime = 0;
    private long modifyTime = 0;
    private boolean isLogout = false;

    public String getSessionId() {
        return this.sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getClientVersion() {
        return this.clientVersion;
    }

    public void setClientVersion(String clientVersion) {
        this.clientVersion = clientVersion;
    }

    public long getCacheSize() {
        return this.cacheSize;
    }

    public void setCacheSize(long cacheSize) {
        this.cacheSize = cacheSize;
    }

    public long getCacheTime() {
        return this.cacheTime;
    }

    public void setCacheTime(long cacheTime) {
        this.cacheTime = cacheTime;
    }

    public long getModifyTime() {
        return this.modifyTime;
    }

    public void setModifyTime(long modifyTime) {
        this.modifyTime = modifyTime;
    }

    public boolean isIsLogout() {
        return this.isLogout;
    }

    public void setIsLogout(boolean isLogout) {
        this.isLogout = isLogout;
    }

    public int length() {
        return StreamTranscoderUtil.getStringLength(sessionId) +
            StreamTranscoderUtil.getStringLength(clientVersion) +
            TfsConstant.LONG_SIZE * 3 +
            TfsConstant.BYTE_SIZE;
    }

    public boolean encode(ByteBuffer byteBuffer) {
        StreamTranscoderUtil.putString(byteBuffer, sessionId);
        StreamTranscoderUtil.putString(byteBuffer, clientVersion);
        byteBuffer.putLong(cacheSize);
        byteBuffer.putLong(cacheTime);
        byteBuffer.putLong(modifyTime);
        byteBuffer.put(isLogout ? (byte)0x01 : (byte)0x00);
        return true;
    }

    public String toString() {
        return "sessionId: " + sessionId + " clientVersion: " + clientVersion +
            " cacheSize: " + cacheSize + " cacheTime: " + cacheTime +
            " modifyTime: " + TfsUtil.microsTimeToString(modifyTime) +
            " isLogout: " + isLogout;            
    }
}
