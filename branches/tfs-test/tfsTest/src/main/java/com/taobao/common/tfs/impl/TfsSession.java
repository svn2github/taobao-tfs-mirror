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
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.common.tfs.common.TfsConstant;
import com.taobao.common.tfs.common.TfsUtil;
import com.taobao.common.tfs.net.ClientManager;
import com.taobao.common.tfs.impl.SegmentData;
import com.taobao.common.tfs.impl.SegmentData.SegmentStatus;
import com.taobao.common.tfs.TfsException;
import com.taobao.common.tfs.exception.ConnectionException;
import com.taobao.common.tfs.exception.ErrorStatusException;
import com.taobao.common.tfs.exception.UnexpectMessageException;
import com.taobao.common.tfs.packet.BasePacket;
import com.taobao.common.tfs.packet.ClientCmdMessage;
import com.taobao.common.tfs.packet.CloseFileInfo;
import com.taobao.common.tfs.packet.CloseFileMessage;
import com.taobao.common.tfs.packet.CreateFilenameMessage;
import com.taobao.common.tfs.packet.DsListWrapper;
import com.taobao.common.tfs.packet.FileInfo;
import com.taobao.common.tfs.packet.FileInfoMessage;
import com.taobao.common.tfs.packet.GetBlockInfoMessage;
import com.taobao.common.tfs.packet.BatchGetBlockInfoMessage;
import com.taobao.common.tfs.packet.ReadDataMessage;
import com.taobao.common.tfs.packet.ReadDataMessageV2;
import com.taobao.common.tfs.packet.RenameFileMessage;
import com.taobao.common.tfs.packet.RespCreateFilenameMessage;
import com.taobao.common.tfs.packet.RespFileInfoMessage;
import com.taobao.common.tfs.packet.RespReadDataMessage;
import com.taobao.common.tfs.packet.RespReadDataMessageV2;
import com.taobao.common.tfs.packet.SetBlockInfoMessage;
import com.taobao.common.tfs.packet.BatchSetBlockInfoMessage;
import com.taobao.common.tfs.packet.StatusMessage;
import com.taobao.common.tfs.packet.UnlinkFileMessage;
import com.taobao.common.tfs.packet.WriteDataInfo;
import com.taobao.common.tfs.packet.WriteDataMessage;


public class TfsSession {
    private static final Log log = LogFactory.getLog(TfsSession.class);
    private static final boolean isDebugEnabled = log.isDebugEnabled();

    public static final int CLIENT_CMD_SET_PARAM = 0x6;

    private long nameServerId;
    private char tfsClusterIndex;

    private ClientManager clientManager = new ClientManager();

    private ClientCache clientCache = new ClientCache();

    public TfsSession() {
        tfsClusterIndex = '0';
    }

    /**
     * initialize client cache and client manager,
     * get tfs cluster index from nameserver, use configured value if fail to get
     *
     */
    public boolean init() {
        clientCache.clear();
        clientManager.init();
        boolean ret = false;;
        int retry = 3;
        do {
            ret = getTfsClusterIndexFromNs();
        } while (!ret && --retry > 0);

        return ret;
    }

    public void destroy() {
        clientManager.destroy();
    }

    public void setClientManager(ClientManager clientManager) {
        this.clientManager = clientManager;
    }

    public ClientManager getClientManager() {
        return clientManager;
    }

    public void setNameServerIp(String ip) {
        setNameServerId(TfsUtil.hostToLong(ip));
    }

    public String getNameServerIp() {
        return TfsUtil.longToHost(getNameServerId());
    }

    public void setNameServerId(long nameServerId) {
        this.nameServerId = nameServerId;
    }

    public long getNameServerId() {
        return nameServerId;
    }

    public void setTfsClusterIndex(char tfsClusterIndex) {
        if (Character.isLetterOrDigit(tfsClusterIndex)) {
            this.tfsClusterIndex = tfsClusterIndex;
        }
    }

    public char getTfsClusterIndex() {
        return tfsClusterIndex;
    }

    public int getMaxCacheItemCount() {
        return clientCache.getMaxCacheItemCount();
    }

    public void setMaxCacheItemCount(int maxCacheItemCount) {
        clientCache.setMaxCacheItemCount(maxCacheItemCount);
    }

    public void setMaxCacheTime(int maxCacheTime) {
        clientCache.setMaxCacheTime(maxCacheTime);
    }

    public int getMaxCacheTime() {
        return clientCache.getMaxCacheTime();
    }

    public void setTimeout(int timeout) {
        clientManager.setTimeout(timeout);
    }

    public int getTimeout() {
        return clientManager.getTimeout();
    }

    /**
     * get tfs cluster index from nameserver
     *
     * @return
     */
    private boolean getTfsClusterIndexFromNs() {
        ClientCmdMessage cmd = new ClientCmdMessage(clientManager.getTranscoder());
        cmd.setType(CLIENT_CMD_SET_PARAM); // CLIENT_CMD_SET_PARAM
        cmd.setBlockId(20); // TfsClusterIndex param
        cmd.setServerId(0); // no meaning
        cmd.setVersion(0);
        cmd.setFromServerId(0);

        try {
            BasePacket retPacket = clientManager.sendPacket(this.nameServerId, cmd);
            if (retPacket != null && retPacket.getPcode() == TfsConstant.STATUS_MESSAGE) {
                StatusMessage status = (StatusMessage)retPacket;
                if (status.getStatus() == StatusMessage.STATUS_MESSAGE_OK) {
                    String value = status.getError();
                    if (value.length() > 0) {
                        int index = Integer.parseInt(value);
                        if (Character.isLetterOrDigit(index)) {
                            this.tfsClusterIndex = (char)index;
                            return true;
                        }
                    }
                }
            }
        } catch (ConnectionException e) {
            log.error("get tfs cluster index error:", e);
        } catch (NumberFormatException e) {
            log.error("get tfs cluster index error:", e);
        }
        return false;
    }

    public void removeBlockCache(int blockId) {
        clientCache.remove(blockId);
    }

    public DsListWrapper getReadBlockInfo(int blockId)
        throws ConnectionException {
        // fetch from client cache.
        List<Long> dsList = clientCache.get(blockId);
        // not found from cache, then retrieve from nameserver
        DsListWrapper dsListWrapper;
        if (dsList == null) {
            dsListWrapper = getBlockInfoEx(blockId, TfsConstant.READ_MODE).getDsListWrapper();
            // put into cache
            clientCache.put(blockId, dsListWrapper.getDsList());
        } else {
            dsListWrapper = new DsListWrapper();
            dsListWrapper.setDsList(dsList);
        }
        return dsListWrapper;
    }

    public void getReadBlockInfo(List<SegmentData> segmentList)
        throws TfsException {
        List<Integer> needBlockList = new ArrayList<Integer>();
        for (SegmentData segmentData : segmentList) {
            int blockId = segmentData.getBlockId();
            List<Long> dsList = clientCache.get(blockId);
            if (dsList == null) {
                needBlockList.add(blockId);
            } else {
                segmentData.setDsListWrapper(new DsListWrapper(dsList));
                segmentData.setSegmentStatus(SegmentStatus.SEG_STATUS_OPEN_OVER);
                segmentData.setReadPrimaryDsIndex();
            }
        }
        if (needBlockList.size() == 0) {
            log.debug("all block id ds list cached, count: " + segmentList.size());
        } else {
            Map<Integer, DsListWrapper> dsListMap =
                getBlockInfoEx(needBlockList, TfsConstant.READ_MODE, 0).getBlockDsMap();
            int blockId;
            DsListWrapper dsListWrapper;
            for (SegmentData segmentData : segmentList) {
                if (segmentData.getDsListWrapper() == null) {
                    blockId = segmentData.getBlockId();
                    dsListWrapper = dsListMap.get(blockId);
                    if (dsListWrapper == null ||
                        dsListWrapper.getDsList().size() == 0) {
                        throw new TfsException("get block info fail " + blockId);
                    } else {
                        segmentData.setDsListWrapper(dsListWrapper);
                        segmentData.setReadPrimaryDsIndex();
                        clientCache.put(blockId, dsListWrapper.getDsList());
                    }
                    segmentData.setSegmentStatus(SegmentStatus.SEG_STATUS_OPEN_OVER);
                }
            }
        }
    }

    public SetBlockInfoMessage getWriteBlockInfo(int blockId, int mode)
        throws ConnectionException {
        return getBlockInfoEx(blockId, mode | TfsConstant.CREATEBLK_MODE);
    }

    public void getWriteBlockInfo(List<SegmentData> segmentList)
        throws TfsException {
        Map<Integer, DsListWrapper> dsListMap =
            getBlockInfoEx(null, TfsConstant.WRITE_MODE | TfsConstant.CREATEBLK_MODE,
                           segmentList.size()).getBlockDsMap();

        if (segmentList.size() != dsListMap.size()) {
            throw new TfsException("get write block fail, request count conflict response: " +
                                   segmentList.size() + " <> " + dsListMap.size());
        }
        int i = 0;
        for (Entry<Integer, DsListWrapper> entry : dsListMap.entrySet()) {
            SegmentData segmentData = segmentList.get(i++);
            segmentData.setBlockId(entry.getKey());
            segmentData.setDsListWrapper(entry.getValue());
            segmentData.setSegmentStatus(SegmentStatus.SEG_STATUS_OPEN_OVER);
        }
    }

    public DsListWrapper getUnlinkBlockInfo(int blockId)
        throws ConnectionException {
        return getBlockInfoEx(blockId, TfsConstant.WRITE_MODE).getDsListWrapper();
    }

    /**
     * get block information message
     *
     * @param blockId
     * @param mode
     * @return
     * @exception
     */
    private SetBlockInfoMessage getBlockInfoEx(int blockId, int mode)
        throws ConnectionException {
        GetBlockInfoMessage sm =
            new GetBlockInfoMessage(clientManager.getTranscoder());
        sm.setBlockId(blockId);
        sm.setMode(mode);

        BasePacket retPacket = clientManager.sendPacket(nameServerId, sm);
        if (retPacket.getPcode() == TfsConstant.SET_BLOCK_INFO_MESSAGE) {
            return (SetBlockInfoMessage) retPacket;
        }
        if (retPacket.getPcode() == TfsConstant.STATUS_MESSAGE) {
            log.error("call GetBlockInfoMessage failed:"
                      + ((StatusMessage) retPacket).getStatus() + ", error:"
                      + ((StatusMessage) retPacket).getError());
            throw new ErrorStatusException(this.nameServerId,
                                           (StatusMessage) retPacket);
        }
        throw new UnexpectMessageException(nameServerId, retPacket);
    }

    /**
     * batch get block information message
     *
     * @param blockIdList
     * @param mode
     * @return
     * @exception
     */
    private BatchSetBlockInfoMessage getBlockInfoEx(List<Integer> blockIdList, int mode, int blockCount)
        throws TfsException {
        BatchGetBlockInfoMessage bsm =
            new BatchGetBlockInfoMessage(clientManager.getTranscoder());
        if ((mode & TfsConstant.READ_MODE) != 0 && blockIdList != null) {
            bsm.setBlockId(blockIdList);
        } else if ((mode & TfsConstant.WRITE_MODE) != 0 && blockCount > 0) {
            bsm.setBlockCount(blockCount);
        } else {
            throw new TfsException("invalid paramter. mode: " + mode +
                                   " blockIdList: " + blockIdList +
                                   "blockcount: " + blockCount);
        }

        bsm.setMode(mode);
        BasePacket retPacket = clientManager.sendPacket(nameServerId, bsm);
        if (retPacket.getPcode() == TfsConstant.BATCH_SET_BLOCK_INFO_MESSAGE) {
            return (BatchSetBlockInfoMessage) retPacket;
        }
        if (retPacket.getPcode() == TfsConstant.STATUS_MESSAGE) {
            log.error("call BatchGetBlockInfoMessage failed:"
                      + ((StatusMessage) retPacket).getStatus() + ", error:"
                      + ((StatusMessage) retPacket).getError());
            throw new ErrorStatusException(this.nameServerId,
                                           (StatusMessage) retPacket);
        }
        throw new UnexpectMessageException(nameServerId, retPacket);
    }

    public RespCreateFilenameMessage createFileName(int blockId, long fileId,
                                                    long primaryDs) throws ConnectionException {
        CreateFilenameMessage sm =
            new CreateFilenameMessage(clientManager.getTranscoder(), blockId, fileId);
        BasePacket retPacket = clientManager.sendPacket(primaryDs, sm);
        if (retPacket.getPcode() == TfsConstant.RESP_CREATE_FILENAME_MESSAGE) {
            return (RespCreateFilenameMessage) retPacket;
        }

        if (retPacket.getPcode() == TfsConstant.STATUS_MESSAGE) {
            log.error("call CreateFilenameMessage failed:"
                      + ((StatusMessage) retPacket).getStatus() + ", error:"
                      + ((StatusMessage) retPacket).getError());

            if (((StatusMessage)retPacket).getStatus() == TfsConstant.EXIT_NO_LOGICBLOCK_ERROR) {
                removeBlockCache(blockId);
            }

            throw new ErrorStatusException(primaryDs, (StatusMessage) retPacket);
        }

        throw new UnexpectMessageException(primaryDs, retPacket);
    }

    public int writeData(WriteDataInfo writeDataInfo,
                         DsListWrapper dsListWrapper, byte[] writeData, int offset)
        throws ConnectionException {
        WriteDataMessage wdm =
            new WriteDataMessage(clientManager.getTranscoder());
        wdm.setWriteDataInfo(writeDataInfo);
        wdm.setWriteData(writeData);
        wdm.setSourceOffset(offset);
        wdm.setDsListWrapper(dsListWrapper);
        int ret = TfsConstant.TFS_SUCCESS;

        try {
            ret = clientManager.sendPacketNoReturn(getPrimaryDs(dsListWrapper), wdm);
        } catch (ErrorStatusException e) {
            if (e.getStatus() == TfsConstant.EXIT_NO_LOGICBLOCK_ERROR) {
                removeBlockCache(writeDataInfo.getBlockId());
            }
            throw e;
        } catch (ConnectionException e) {
            removeBlockCache(writeDataInfo.getBlockId());
            throw e;
        }

        return ret;
    }

    public int closeFile(CloseFileInfo closeFileInfo,
                         DsListWrapper dsListWrapper, int optionFlag)
        throws ConnectionException {
        CloseFileMessage cfm =
            new CloseFileMessage(clientManager.getTranscoder());
        cfm.setCloseFileInfo(closeFileInfo);
        cfm.setDsListWrapper(dsListWrapper);
        cfm.setOptionFlag(optionFlag);
        int ret = TfsConstant.TFS_SUCCESS;

        try {
            ret = clientManager.sendPacketNoReturn(getPrimaryDs(dsListWrapper), cfm);
        } catch (ErrorStatusException e) {
            if (e.getStatus() == TfsConstant.EXIT_NO_LOGICBLOCK_ERROR) {
                removeBlockCache(closeFileInfo.getBlockId());
            }
            throw e;
        } catch (ConnectionException e) {
            removeBlockCache(closeFileInfo.getBlockId());
            throw e;
        }

        return ret;
    }

    public FileInfo getFileInfo(int blockId, long fileId, int mode, long primaryDs)
        throws ConnectionException {
        FileInfoMessage fm = new FileInfoMessage(clientManager.getTranscoder());
        fm.setBlockId(blockId);
        fm.setFileId(fileId);
        fm.setMode(mode);

        BasePacket retPacket;
        try {
            retPacket = clientManager.sendPacket(primaryDs, fm);
        } catch (ConnectionException e) {
            removeBlockCache(blockId);
            throw e;
        }
        if (retPacket.getPcode() == TfsConstant.RESP_FILE_INFO_MESSAGE) {
            return ((RespFileInfoMessage) retPacket).getFileInfo();
        }
        if (retPacket.getPcode() == TfsConstant.STATUS_MESSAGE) {
            log.error("call FileInfoMessage failed:"
                      + ((StatusMessage) retPacket).getStatus() + ", error:"
                      + ((StatusMessage) retPacket).getError());

            if (((StatusMessage)retPacket).getStatus() == TfsConstant.EXIT_NO_LOGICBLOCK_ERROR) {
                removeBlockCache(blockId);
            }

            throw new ErrorStatusException(primaryDs, (StatusMessage) retPacket);
        }
        throw new UnexpectMessageException(primaryDs, retPacket);
    }

    public byte[] readData(int blockId, long fileId, int offset, int length, long primaryDs, int readFlag)
        throws TfsException {
        ReadDataMessage rm = new ReadDataMessage(clientManager.getTranscoder());
        rm.setBlockId(blockId);
        rm.setFileId(fileId);
        rm.setOffset(offset);
        rm.setLength(length);
        rm.setFlag((byte)readFlag);

        BasePacket retPacket;
        try {
            retPacket = clientManager.sendPacket(primaryDs, rm);
        } catch (ConnectionException e) {
            removeBlockCache(blockId);
            throw e;
        }
        if (retPacket.getPcode() == TfsConstant.RESP_READ_DATA_MESSAGE) {
            int retLength = ((RespReadDataMessage)retPacket).getLength();
            if (retLength < 0) {
                if (retLength == TfsConstant.EXIT_NO_LOGICBLOCK_ERROR) {
                    removeBlockCache(blockId);
                }
                throw new TfsException("readDataMessage fail. server: " +
                                       TfsUtil.longToHost(primaryDs) +
                                       " ret: " + retLength);
            }
            return ((RespReadDataMessage) retPacket).getData();
        }

        // actually no return StatusMessage
        if (retPacket.getPcode() == TfsConstant.STATUS_MESSAGE) {
            log.error("call ReadDataMessage failed. ret: "
                      + ((StatusMessage) retPacket).getStatus() + " error:"
                      + ((StatusMessage) retPacket).getError());

            if (((StatusMessage)retPacket).getStatus() == TfsConstant.EXIT_NO_LOGICBLOCK_ERROR) {
                removeBlockCache(blockId);
            }

            throw new ErrorStatusException(primaryDs, (StatusMessage) retPacket);
        }
        throw new UnexpectMessageException(primaryDs, retPacket);
    }

    public RespReadDataMessageV2 readDataV2(int blockId, long fileId,
                                            int offset, int length, long primaryDs, int readFlag)
        throws TfsException {
        ReadDataMessageV2 rm =
            new ReadDataMessageV2(clientManager.getTranscoder());
        rm.setBlockId(blockId);
        rm.setFileId(fileId);
        rm.setOffset(offset);
        rm.setLength(length);
        rm.setFlag((byte)readFlag);

        BasePacket retPacket;
        try {
            retPacket = clientManager.sendPacket(primaryDs, rm);
        } catch (ConnectionException e) {
            removeBlockCache(blockId);
            throw e;
        }
        if (retPacket.getPcode() == TfsConstant.RESP_READ_DATA_MESSAGE_V2) {
            int retLength = ((RespReadDataMessage)retPacket).getLength();
            if (retLength < 0) {
                if (retLength == TfsConstant.EXIT_NO_LOGICBLOCK_ERROR) {
                    removeBlockCache(blockId);
                }
                throw new TfsException("readDataMessageV2 fail. server: " +
                                       TfsUtil.longToHost(primaryDs) +
                                       " ret: " + retLength);
            }
            if (offset == 0 && ((RespReadDataMessageV2)retPacket).getFileInfo() == null) {
                throw new TfsException("readDataMessageV2 fail. server: " +
                                       TfsUtil.longToHost(primaryDs) +
                                       " first read cannot get fileinfo, ret:" +
                                       retLength);
            }
            return ((RespReadDataMessageV2) retPacket);
        }

        // actually no return StatusMessage
        if (retPacket.getPcode() == TfsConstant.STATUS_MESSAGE) {
            log.error("call ReadDataMessageV2 failed:"
                      + ((StatusMessage) retPacket).getStatus() + ", error:"
                      + ((StatusMessage) retPacket).getError());

            if (((StatusMessage)retPacket).getStatus() == TfsConstant.EXIT_NO_LOGICBLOCK_ERROR) {
                removeBlockCache(blockId);
            }

            throw new ErrorStatusException(primaryDs, (StatusMessage) retPacket);
        }
        throw new UnexpectMessageException(primaryDs, retPacket);
    }

    public long unlinkFile(int blockId, long fileId, int action, int optionFlag,
                          DsListWrapper dsListWrapper) throws ConnectionException {
        UnlinkFileMessage ufm =
            new UnlinkFileMessage(clientManager.getTranscoder());
        ufm.setBlockId(blockId);
        ufm.setFileId(fileId);
        ufm.setDsListWrapper(dsListWrapper);
        ufm.setOptionFlag(optionFlag);
        ufm.setUnlinkType(action);

        BasePacket retPacket;
        try {
            retPacket = clientManager.sendPacket(dsListWrapper.getDsList().get(0), ufm);
        } catch (ConnectionException e) {
            removeBlockCache(blockId);
            throw e;
        }

        if (retPacket.getPcode() == TfsConstant.STATUS_MESSAGE) {
            if (((StatusMessage)retPacket).getStatus() != StatusMessage.STATUS_MESSAGE_OK) {
                throw new ErrorStatusException(dsListWrapper.getDsList().get(0), (StatusMessage)retPacket);
            }

            return Long.parseLong(((StatusMessage)retPacket).getError());
        }

        throw new UnexpectMessageException(dsListWrapper.getDsList().get(0), retPacket);
    }

    public int renameFile(int blockId, long fileId, long newFileId,
                          int optionFlag, DsListWrapper dsListWrapper)
        throws ConnectionException {
        RenameFileMessage rfm =
            new RenameFileMessage(clientManager.getTranscoder());
        rfm.setBlockId(blockId);
        rfm.setFileId(fileId);
        rfm.setNewFileId(newFileId);
        rfm.setOptionFlag(optionFlag);
        rfm.setDsListWrapper(dsListWrapper);

        int ret = TfsConstant.TFS_SUCCESS;
        try {
            ret = clientManager.sendPacketNoReturn(dsListWrapper.getDsList().get(0), rfm);
        } catch (ErrorStatusException e) {
            if (e.getStatus() == TfsConstant.EXIT_NO_LOGICBLOCK_ERROR) {
                removeBlockCache(blockId);
            }
            throw e;
        } catch (ConnectionException e) {
            removeBlockCache(blockId);
            throw e;
        }

        return ret;
    }

    // process
    public int process(List<SegmentData> segmentList, FilePhase phase) {
        int ret = TfsConstant.TFS_SUCCESS;
        int requestId = clientManager.getAsyncId();
        int requestCount = 0;

        if (isDebugEnabled) {
            log.debug("process phase " + phase + " count " + segmentList.size());
        }
        // just use list index as seqId
        int seqId = 0;
        for (SegmentData segmentData : segmentList) {
            if (segmentData.getSegmentStatus() == phase.getPreviousStatus()) {
                if (!doRequest(phase, segmentData, requestId, seqId)) {
                    removeBlockCache(segmentData.getBlockId());
                } else {
                    requestCount++;
                }
            }
            seqId++;
        }

        if (requestCount == 0) {
            log.error("process all request fail");
            ret = TfsConstant.EXIT_ALL_SEGMENT_FAIL_ERROR;
        } else {
            // wait not ture, then not all success
            if (!clientManager.await(requestId)) {
                List<Integer> failIdList = clientManager.getFailIdList(requestId);
                if (failIdList != null) {
                    for (Integer failId : failIdList) {
                        removeBlockCache(segmentList.get(failId).getBlockId());
                    }
                }
            }

            List<BasePacket> responseList = clientManager.getResponseList(requestId);
            if (responseList == null || responseList.isEmpty()) {
                log.error("get all response fail.");
                ret = TfsConstant.EXIT_ALL_SEGMENT_FAIL_ERROR;
            } else {
                int responseCount = 0;
                for (BasePacket response : responseList) {
                    if (doResponse(phase, segmentList, response)
                        == TfsConstant.TFS_SUCCESS) {
                        responseCount++;
                    }
                }

                ret = responseCount == 0 ? TfsConstant.EXIT_ALL_SEGMENT_FAIL_ERROR :
                    responseCount != segmentList.size() ? TfsConstant.TFS_ERROR :
                    TfsConstant.TFS_SUCCESS;
            }
        }
        clientManager.destroyAsync(requestId);
        return ret;
    }

    private long getPrimaryDs(DsListWrapper dsListWrapper) {
        return dsListWrapper.getDsList().get(0);
    }

    private boolean doRequest(FilePhase phase, SegmentData segmentData, int requstId, int seqId) {
        boolean ret = false;

        switch (phase) {
        case FILE_PHASE_CREATE_FILE :
            ret = reqCreateFileName(segmentData, requstId, seqId);
            break;
        case FILE_PHASE_WRITE_FILE :
            ret = reqWriteData(segmentData, requstId, seqId);
            break;
        case FILE_PHASE_READ_FILE :
            ret = reqReadData(segmentData, requstId, seqId);
            break;
        case FILE_PHASE_CLOSE_FILE :
            ret = reqCloseFile(segmentData, requstId, seqId);
            break;
        }

        if (ret) {
            log.debug(phase + " seqId: " + seqId +  " request success: ");
        } else {
            log.error(phase + " seqId: " + seqId + " request fail: " + segmentData);
        }
        return ret;
    }

    private int doResponse(FilePhase phase, List<SegmentData> segList, BasePacket response) {
        // seqId valid
        int seqId = response.getSeqId();
        SegmentData segmentData = segList.get(seqId);
        int ret = TfsConstant.TFS_SUCCESS;

        switch (phase) {
        case FILE_PHASE_CREATE_FILE :
            ret = respCreateFileName(segmentData, response);
            break;
        case FILE_PHASE_WRITE_FILE :
            ret = respWriteData(segmentData, response);
            break;
        case FILE_PHASE_READ_FILE :
            ret = respReadData(segmentData, response);
            break;
        case FILE_PHASE_CLOSE_FILE :
            ret = respCloseFile(segmentData, response);
            break;
        }

        if (ret == TfsConstant.TFS_SUCCESS) {
            segmentData.setSegmentStatus(phase.getCurrentStatus());
            log.debug(phase + " seqId: " + seqId + " response success.");
        } else {
            log.error(phase + " seqId: " + seqId +  " response fail: " + segmentData);
        }

        return ret;
    }

    private boolean reqCreateFileName(SegmentData segmentData, int requestId, int seqId) {
        CreateFilenameMessage cfm =
            new CreateFilenameMessage(clientManager.getTranscoder(),
                                      segmentData.getBlockId(), segmentData.getFileId());
        return clientManager.postPacket(segmentData.getPrimaryDs(),
                                        cfm, requestId, seqId);
    }

    private boolean reqWriteData(SegmentData segmentData, int requestId, int seqId) {
        WriteDataInfo writeDataInfo = new WriteDataInfo();
        writeDataInfo.setBlockId(segmentData.getBlockId());
        writeDataInfo.setFileId(segmentData.getFileId());
        writeDataInfo.setFileNumber(segmentData.getFileNumber());
        writeDataInfo.setOffset(segmentData.getInnerOffset());
        writeDataInfo.setLength(segmentData.getDataLength());
        writeDataInfo.setIsServer(0);

        WriteDataMessage wdm =
            new WriteDataMessage(clientManager.getTranscoder());
        wdm.setWriteDataInfo(writeDataInfo);
        wdm.setWriteData(segmentData.getData());
        wdm.setSourceOffset(segmentData.getDataOffset());
        wdm.setDsListWrapper(segmentData.getDsListWrapper());

        return clientManager.postPacket(segmentData.getPrimaryDs(),
                                        wdm, requestId, seqId);
    }

    private boolean reqReadData(SegmentData segmentData, int requestId, int seqId, BasePacket rdm) {
        boolean removeCache = false;
        boolean ret = false;
        int retry = segmentData.getReadPrimaryDsIndex() - segmentData.getPrimaryDsIndex();
        retry = retry <= 0 ? segmentData.getDsListWrapper().getDsList().size() + retry : retry;

        // try until post success
        while (retry-- > 0 &&
               !(ret = clientManager.
                 postPacket(segmentData.getPrimaryDs(), rdm, requestId, seqId))) {
            log.warn("req read post fail to: " + segmentData.getPrimaryDs() + " retry: " + retry);
            removeCache = true;
            segmentData.readReset();
        }

        // outer loop may removeBlockCache again. no harm
        if (removeCache) {
            removeBlockCache(segmentData.getBlockId());
        }

        if (retry == 0) { // try the last ds
            segmentData.setPrimaryDsIndex(-1);
        } else {
            segmentData.readReset();
        }
        return ret;
    }

    private boolean reqReadData(SegmentData segmentData, int requestId, int seqId) {
        if (segmentData.getPrimaryDsIndex() < 0) {
            return false;
        }

        ReadDataMessage rdm = new ReadDataMessage(clientManager.getTranscoder());
        rdm.setBlockId(segmentData.getBlockId());
        rdm.setFileId(segmentData.getFileId());
        rdm.setOffset(segmentData.getInnerOffset());
        rdm.setLength(segmentData.getDataLength());
        return reqReadData(segmentData, requestId, seqId, rdm);
    }

    private boolean reqReadDataV2(SegmentData segmentData, int requestId, int seqId) {
        ReadDataMessageV2 rdmV2 = new ReadDataMessageV2(clientManager.getTranscoder());
        rdmV2.setBlockId(segmentData.getBlockId());
        rdmV2.setFileId(segmentData.getFileId());
        rdmV2.setOffset(segmentData.getInnerOffset());
        rdmV2.setLength(segmentData.getDataLength());

        return reqReadData(segmentData, requestId, seqId, rdmV2);
    }

    private boolean reqCloseFile(SegmentData segmentData, int requestId, int seqId) {
        CloseFileInfo closeFileInfo = new CloseFileInfo();
        closeFileInfo.setBlockId(segmentData.getBlockId());
        closeFileInfo.setFileId(segmentData.getFileId());
        closeFileInfo.setFileNumber(segmentData.getFileNumber());
        closeFileInfo.setCrc(segmentData.getCrc());
        closeFileInfo.setMode(0);

        CloseFileMessage cfm =
            new CloseFileMessage(clientManager.getTranscoder());
        cfm.setCloseFileInfo(closeFileInfo);
        cfm.setDsListWrapper(segmentData.getDsListWrapper());
        cfm.setOptionFlag(0);   // TODO option flag

        // write and close should be the same primaryDs,
        // Absolutely now
        return clientManager.postPacket(segmentData.getPrimaryDs(),
                                        cfm, requestId, seqId);
    }

    private int respCreateFileName(SegmentData segmentData, BasePacket response) {
        if (response.getPcode() == TfsConstant.RESP_CREATE_FILENAME_MESSAGE) {
            RespCreateFilenameMessage rcfn = (RespCreateFilenameMessage)response;
            if (rcfn.getFileId() == 0 || rcfn.getFileNumber() == 0) {
                log.error("resp create file name fail. fileid filenumber zero return." +
                          segmentData.getSegmentInfo());
                return TfsConstant.TFS_ERROR;
            }
            // no suffix to segment
            segmentData.setFileId(rcfn.getFileId());
            segmentData.setFileNumber(rcfn.getFileNumber());

            return TfsConstant.TFS_SUCCESS;
        } else if (response.getPcode() == TfsConstant.STATUS_MESSAGE) {
            StatusMessage sm = (StatusMessage)response;
            if (sm.getStatus() == TfsConstant.EXIT_NO_LOGICBLOCK_ERROR) {
                removeBlockCache(segmentData.getBlockId());
            }
            log.error("resp create name fail. ret: " + sm.getStatus() +
                      " error: " + sm.getError());
            return sm.getStatus();
        } else {
            log.error("resp create file name fail. invalid packet return: " + response.getPcode());
        }
        return TfsConstant.TFS_ERROR;
    }

    private int respWriteData(SegmentData segmentData, BasePacket response) {
        if (response.getPcode() == TfsConstant.STATUS_MESSAGE) {
            StatusMessage sm = (StatusMessage)response;
            if (sm.getStatus() == TfsConstant.TFS_SUCCESS) {
                segmentData.setCrc(TfsUtil.crc32(segmentData.getCrc(), segmentData.getData(),
                                                 segmentData.getDataOffset(), segmentData.getLength()));
            } else {
                if (sm.getStatus() == TfsConstant.EXIT_NO_LOGICBLOCK_ERROR) {
                    removeBlockCache(segmentData.getBlockId());
                }
                log.error("resp write data fail. ret: " + sm.getStatus() +
                          " error: " + sm.getError());
            }
            return sm.getStatus();
        } else {
            log.error("resp write data fail. invalid packet return: " + response.getPcode());
        }

        return TfsConstant.TFS_ERROR;
    }

    private  int respReadData(SegmentData segmentData, BasePacket response) {
        if (response.getPcode() == TfsConstant.RESP_READ_DATA_MESSAGE) {
            int retLength = ((RespReadDataMessage)response).getLength();
            if (retLength < 0) {
                if (retLength == TfsConstant.EXIT_NO_LOGICBLOCK_ERROR) {
                    removeBlockCache(segmentData.getBlockId());
                }
                log.error("resp readData fail. ret: " + retLength);
                return retLength;
            }

            // no check eof
            segmentData.putData(((RespReadDataMessage)response).getData());
            return TfsConstant.TFS_SUCCESS;
        }

        // read return status message
        if (response.getPcode() == TfsConstant.STATUS_MESSAGE) {
            log.error("resp readData fail. get statusmessage. ret: " +
                      ((StatusMessage)response).getStatus() +
                      " error: " + ((StatusMessage)response).getError());
        } else {
            log.error("resp readData fail. invalid packet return: " + response.getPcode());
        }

        return TfsConstant.TFS_ERROR;
    }

    private int respReadDataV2(SegmentData segmentData, BasePacket response) {
        if (response.getPcode() == TfsConstant.RESP_READ_DATA_MESSAGE_V2) {
            int retLength = ((RespReadDataMessageV2)response).getLength();
            if (retLength < 0) {
                if (retLength == TfsConstant.EXIT_NO_LOGICBLOCK_ERROR) {
                    removeBlockCache(segmentData.getBlockId());
                }
                log.error("resp readDataV2 fail. ret: " + retLength);
                return retLength;
            }

            if (segmentData.getInnerOffset() == 0 &&
                ((RespReadDataMessageV2)response).getFileInfo() == null) {
                log.error("resp readDataV2 can't get fileinfo");
                return TfsConstant.TFS_ERROR;
            }
            // no check eof
            segmentData.putData(((RespReadDataMessageV2)response).getData());
            return TfsConstant.TFS_SUCCESS;
        }

        // read should not return statusmessage
        if (response.getPcode() == TfsConstant.STATUS_MESSAGE) {
            log.error("resp readDataV2 fail. get statusmessage. ret: " +
                      ((StatusMessage)response).getStatus() +
                      " error: " + ((StatusMessage)response).getError());
        } else {
            log.error("resp readDataV2 fail. invalid packet return: " + response.getPcode());
        }

        return TfsConstant.TFS_ERROR;
    }

    private int respCloseFile(SegmentData segmentData, BasePacket response) {
        if (response.getPcode() == TfsConstant.STATUS_MESSAGE) {
            StatusMessage sm = (StatusMessage)response;
            if (sm.getStatus() != TfsConstant.TFS_SUCCESS) {
                if (sm.getStatus() == TfsConstant.EXIT_NO_LOGICBLOCK_ERROR) {
                    removeBlockCache(segmentData.getBlockId());
                }
                log.error("resp close fail, ret: " + sm.getStatus() +
                          " error:" + sm.getError());
            }
            return sm.getStatus();
        } else {
            log.error("resp close fail. invalid packet return: " + response.getPcode());
        }
        return TfsConstant.TFS_ERROR;
    }

}
