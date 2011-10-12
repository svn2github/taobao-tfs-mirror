package com.taobao.common.tfs.net;

import com.taobao.common.tfs.packet.BasePacket;

public class RequestCommand {
    private long addr = 0;
    private BasePacket request = null;
    private int seqId = 0;

    public RequestCommand(long addr, BasePacket request, int seqId) {
        this.addr = addr;
        this.request = request;
        this.seqId = seqId;
    }

    public void setAddr(long addr) {
        this.addr = addr;
    }

    public long getAddr() {
        return this.addr;
    }

    public void setRequest(BasePacket request) {
        this.request = request;
    }

    public BasePacket getRequest() {
        return this.request;
    }

    public void setSeqId(int seqId) {
        this.seqId = seqId;
    }

    public int getSeqId() {
        return this.seqId;
    }
}
