/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: message_factory.cpp 864 2011-09-29 01:54:18Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include "message_factory.h"

namespace tfs
{
  namespace message
  {
    tbnet::Packet* MessageFactory::createPacket(int pcode)
    {
      tbnet::Packet* packet = BasePacketFactory::createPacket(pcode);
      if (NULL == packet)
      {
        int32_t real_pcode = pcode & 0xFFFF;
        switch (real_pcode)
        {
          case common::GET_BLOCK_INFO_MESSAGE:
            packet = new (std::nothrow)GetBlockInfoMessage();
            break;
          case common::SET_BLOCK_INFO_MESSAGE:
            packet = new (std::nothrow) SetBlockInfoMessage();
            break;
          case common::BATCH_GET_BLOCK_INFO_MESSAGE:
            packet = new (std::nothrow) BatchGetBlockInfoMessage();
            break;
          case common::BATCH_SET_BLOCK_INFO_MESSAGE:
            packet = new (std::nothrow) BatchSetBlockInfoMessage();
            break;
          case common::CARRY_BLOCK_MESSAGE:
            packet = new (std::nothrow) CarryBlockMessage();
            break;
          case common::SET_DATASERVER_MESSAGE:
            packet = new (std::nothrow) SetDataserverMessage();
            break;
          case common::UPDATE_BLOCK_INFO_MESSAGE:
            packet = new (std::nothrow) UpdateBlockInfoMessage();
            break;
          case common::BLOCK_WRITE_COMPLETE_MESSAGE:
            packet = new (std::nothrow) BlockWriteCompleteMessage();
            break;
          case common::READ_DATA_MESSAGE:
            packet = new (std::nothrow) ReadDataMessage();
            break;
          case common::RESP_READ_DATA_MESSAGE:
            packet = new (std::nothrow) RespReadDataMessage();
            break;
          case common::FILE_INFO_MESSAGE:
            packet = new (std::nothrow) FileInfoMessage();
            break;
          case common::RESP_FILE_INFO_MESSAGE:
            packet = new (std::nothrow) RespFileInfoMessage();
            break;
          case common::WRITE_DATA_MESSAGE:
            packet = new (std::nothrow) WriteDataMessage();
            break;
          case common::CLOSE_FILE_MESSAGE:
            packet = new (std::nothrow) CloseFileMessage();
            break;
          case common::UNLINK_FILE_MESSAGE:
            packet = new (std::nothrow) UnlinkFileMessage();
            break;
          case common::REPLICATE_BLOCK_MESSAGE:
            packet = new (std::nothrow) ReplicateBlockMessage();
            break;
          case common::COMPACT_BLOCK_MESSAGE:
            packet = new (std::nothrow) NsRequestCompactBlockMessage();
            break;
          case common::GET_SERVER_STATUS_MESSAGE:
            packet = new (std::nothrow) GetServerStatusMessage();
            break;
          /*case common::SUSPECT_DATASERVER_MESSAGE:
            packet = new (std::nothrow) SuspectDataserverMessage();
            break;*/
          case common::RENAME_FILE_MESSAGE:
            //packet = new (std::nothrow) RenameFileMessage();
            break;
          case common::CLIENT_CMD_MESSAGE:
            packet = new (std::nothrow) ClientCmdMessage();
            break;
          case common::CREATE_FILENAME_MESSAGE:
            packet = new (std::nothrow) CreateFilenameMessage();
            break;
          case common::RESP_CREATE_FILENAME_MESSAGE:
            packet = new (std::nothrow) RespCreateFilenameMessage();
            break;
          case common::ROLLBACK_MESSAGE:
            packet = new (std::nothrow) RollbackMessage();
            break;
          case common::RESP_HEART_MESSAGE:
            packet = new (std::nothrow) RespHeartMessage();
            break;
          case common::RESET_BLOCK_VERSION_MESSAGE:
            packet = new (std::nothrow) ResetBlockVersionMessage();
            break;
          case common::BLOCK_FILE_INFO_MESSAGE:
            packet = new (std::nothrow) BlockFileInfoMessage();
            break;
          case common::NEW_BLOCK_MESSAGE:
            packet = new (std::nothrow) NewBlockMessage();
            break;
          case common::REMOVE_BLOCK_MESSAGE:
            packet = new (std::nothrow) RemoveBlockMessage();
            break;
          case common::LIST_BLOCK_MESSAGE:
            packet = new (std::nothrow) ListBlockMessage();
            break;
          case common::RESP_LIST_BLOCK_MESSAGE:
            packet = new (std::nothrow) RespListBlockMessage();
            break;
          case common::BLOCK_RAW_META_MESSAGE:
            //packet = new (std::nothrow) BlockRawMetaMessage();
            break;
          case common::WRITE_RAW_DATA_MESSAGE:
            //packet = new (std::nothrow) WriteRawDataMessage();
            break;
          case common::WRITE_INFO_BATCH_MESSAGE:
            //packet = new (std::nothrow) WriteInfoBatchMessage();
            break;
          case common::BLOCK_COMPACT_COMPLETE_MESSAGE:
            packet = new (std::nothrow) DsCommitCompactBlockCompleteToNsMessage();
            break;
          case common::READ_DATA_MESSAGE_V2:
            packet = new (std::nothrow) ReadDataMessageV2();
            break;
          case common::RESP_READ_DATA_MESSAGE_V2:
            packet = new (std::nothrow) RespReadDataMessageV2();
            break;
          case common::LIST_BITMAP_MESSAGE:
            packet = new (std::nothrow) ListBitMapMessage();
            break;
          case common::RESP_LIST_BITMAP_MESSAGE:
            packet = new (std::nothrow) RespListBitMapMessage();
            break;
          case common::RELOAD_CONFIG_MESSAGE:
            packet = new (std::nothrow) ReloadConfigMessage();
            break;
          case common::READ_RAW_DATA_MESSAGE:
            packet = new (std::nothrow) ReadRawDataMessage();
            break;
          case common::RESP_READ_RAW_DATA_MESSAGE:
            packet = new (std::nothrow) RespReadRawDataMessage();
            break;
          case common::ACCESS_STAT_INFO_MESSAGE:
            packet = new (std::nothrow) AccessStatInfoMessage();
            break;
          case common::READ_SCALE_IMAGE_MESSAGE:
            packet = new (std::nothrow) ReadScaleImageMessage();
            break;
          case common::CRC_ERROR_MESSAGE:
            packet = new (std::nothrow) CrcErrorMessage();
            break;
          case common::OPLOG_SYNC_MESSAGE:
            packet = new (std::nothrow) OpLogSyncMessage();
            break;
          case common::OPLOG_SYNC_RESPONSE_MESSAGE:
            packet = new (std::nothrow) OpLogSyncResponeMessage();
            break;
          case common::MASTER_AND_SLAVE_HEART_MESSAGE:
            packet = new (std::nothrow) MasterAndSlaveHeartMessage();
            break;
          case common::MASTER_AND_SLAVE_HEART_RESPONSE_MESSAGE:
            packet = new (std::nothrow) MasterAndSlaveHeartResponseMessage();
            break;
          case common::HEARTBEAT_AND_NS_HEART_MESSAGE:
            packet = new (std::nothrow) HeartBeatAndNSHeartMessage();
            break;
          case common::ADMIN_CMD_MESSAGE:
            packet = new (std::nothrow) AdminCmdMessage();
            break;
          case common::REMOVE_BLOCK_RESPONSE_MESSAGE:
            packet = new (std::nothrow) RemoveBlockResponseMessage();
            break;
          case common::DUMP_PLAN_MESSAGE:
            packet = new (std::nothrow) DumpPlanMessage();
            break;
          case common::DUMP_PLAN_RESPONSE_MESSAGE:
            packet = new (std::nothrow) DumpPlanResponseMessage();
            break;
          case common::SHOW_SERVER_INFORMATION_MESSAGE:
            packet = new (std::nothrow) ShowServerInformationMessage();
            break;
          case common::REQ_RC_LOGIN_MESSAGE:
            packet = new (std::nothrow)ReqRcLoginMessage();
            break;
          case common::RSP_RC_LOGIN_MESSAGE:
            packet = new (std::nothrow)RspRcLoginMessage();
            break;
          case common::REQ_RC_KEEPALIVE_MESSAGE:
            packet = new (std::nothrow)ReqRcKeepAliveMessage();
            break;
          case common::RSP_RC_KEEPALIVE_MESSAGE:
            packet = new (std::nothrow)RspRcKeepAliveMessage();
            break;
          case common::REQ_RC_LOGOUT_MESSAGE:
            packet = new (std::nothrow)ReqRcLogoutMessage();
            break;
          case common::REQ_RC_RELOAD_MESSAGE:
            packet = new (std::nothrow)ReqRcReloadMessage();
            break;
          case common::GET_DATASERVER_INFORMATION_MESSAGE:
            packet = new (std::nothrow)GetDataServerInformationMessage();
            break;
          case common::GET_DATASERVER_INFORMATION_RESPONSE_MESSAGE:
            packet = new (std::nothrow)GetDataServerInformationResponseMessage();
            break;
          case common::FILEPATH_ACTION_MESSAGE:
            packet = new (std::nothrow)FilepathActionMessage();
            break;
          case common::WRITE_FILEPATH_MESSAGE:
            packet = new (std::nothrow)WriteFilepathMessage();
            break;
          case common::READ_FILEPATH_MESSAGE:
            packet = new (std::nothrow)ReadFilepathMessage();
            break;
          case common::RESP_READ_FILEPATH_MESSAGE:
            packet = new (std::nothrow)RespReadFilepathMessage();
            break;
          case common::LS_FILEPATH_MESSAGE:
            packet = new (std::nothrow)LsFilepathMessage();
            break;
          case common::RESP_LS_FILEPATH_MESSAGE:
            packet = new (std::nothrow)RespLsFilepathMessage();
            break;
          case common::REQ_RT_MS_KEEPALIVE_MESSAGE:
            packet =  new (std::nothrow)RtsMsHeartMessage();
            break;
          case common::RSP_RT_MS_KEEPALIVE_MESSAGE:
            packet = new (std::nothrow)RtsMsHeartResponseMessage();
            break;
          case common::REQ_RT_RS_KEEPALIVE_MESSAGE:
            packet =  new (std::nothrow)RtsRsHeartMessage();
            break;
          case common::RSP_RT_RS_KEEPALIVE_MESSAGE:
            packet = new (std::nothrow)RtsRsHeartResponseMessage();
            break;
          case common::REQ_RT_GET_TABLE_MESSAGE:
            packet = new (std::nothrow)GetTableFromRtsMessage();
            break;
          case common::RSP_RT_GET_TABLE_MESSAGE:
            packet = new (std::nothrow)GetTableFromRtsResponseMessage();
            break;
          case common::REQ_RT_UPDATE_TABLE_MESSAGE:
            packet = new (std::nothrow)UpdateTableMessage();
            break;
          case common::RSP_RT_UPDATE_TABLE_MESSAGE:
            packet = new (std::nothrow)UpdateTableResponseMessage();
            break;
          case common::REQ_CALL_DS_REPORT_BLOCK_MESSAGE:
            packet = new (std::nothrow)CallDsReportBlockRequestMessage();
            break;
          case common::REQ_REPORT_BLOCKS_TO_NS_MESSAGE:
            packet = new (std::nothrow)ReportBlocksToNsRequestMessage();
            break;
          case common::RSP_REPORT_BLOCKS_TO_NS_MESSAGE:
            packet = new (std::nothrow)ReportBlocksToNsResponseMessage();
            break;
          case common::REQ_EC_MARSHALLING_MESSAGE:
            packet = new (std::nothrow)ECMarshallingMessage();
            break;
          case common::REQ_EC_MARSHALLING_COMMIT_MESSAGE:
            packet = new (std::nothrow)ECMarshallingCommitMessage();
            break;
          case common::REQ_EC_REINSTATE_MESSAGE:
            packet = new (std::nothrow)ECReinstateMessage();
            break;
          case common::REQ_EC_REINSTATE_COMMIT_MESSAGE:
            packet = new (std::nothrow)ECReinstateCommitMessage();
            break;
          case common::REQ_EC_DISSOLVE_MESSAGE:
            packet = new (std::nothrow)ECDissolveMessage();
            break;
          case common::REQ_EC_DISSOLVE_COMMIT_MESSAGE:
            packet = new (std::nothrow)ECDissolveCommitMessage();
            break;
          case common::READ_RAW_INDEX_MESSAGE:
            //packet = new (std::nothrow)ReadRawIndexMessage();
            break;
          case common::RSP_READ_RAW_INDEX_MESSAGE:
            //packet = new (std::nothrow)RespReadRawIndexMessage();
            break;
          case common::WRITE_RAW_INDEX_MESSAGE:
            //packet = new (std::nothrow)WriteRawIndexMessage();
            break;
          case common::DS_COMPACT_BLOCK_MESSAGE:
            packet = new (std::nothrow)DsCompactBlockMessage();
            break;
          case common::DS_REPLICATE_BLOCK_MESSAGE:
            packet = new (std::nothrow)DsReplicateBlockMessage();
            break;
          case common::RESP_DS_REPLICATE_BLOCK_MESSAGE:
            packet = new (std::nothrow)RespDsReplicateBlockMessage();
            break;
          case common::RESP_DS_COMPACT_BLOCK_MESSAGE:
            packet = new (std::nothrow)RespDsCompactBlockMessage();
            break;
          case common::REQ_CHECK_BLOCK_MESSAGE:
            packet = new (std::nothrow)CheckBlockRequestMessage();
            break;
          case common::RSP_CHECK_BLOCK_MESSAGE:
            packet = new (std::nothrow)CheckBlockResponseMessage();
            break;
          case common::RSP_WRITE_DATA_MESSAGE:
            packet = new (std::nothrow)WriteDataResponseMessage();
            break;
          case common::RSP_UNLINK_FILE_MESSAGE:
            packet = new (std::nothrow)UnlinkFileResponseMessage();
            break;
          case common::REQ_RESOLVE_BLOCK_VERSION_CONFLICT_MESSAGE:
            packet = new (std::nothrow)ResolveBlockVersionConflictMessage();
            break;
          case common::RSP_RESOLVE_BLOCK_VERSION_CONFLICT_MESSAGE:
            packet = new (std::nothrow)ResolveBlockVersionConflictResponseMessage();
            break;
          case common::REQ_GET_FAMILY_INFO_MESSAGE:
            packet = new (std::nothrow)GetFamilyInfoMessage();
            break;
          case common::RSP_GET_FAMILY_INFO_MESSAGE:
            packet = new (std::nothrow)GetFamilyInfoResponseMessage();
            break;
          case common::DEGRADE_READ_DATA_MESSAGE:
            packet = new (std::nothrow)DegradeReadDataMessage();
            break;
          case common::REQ_KVMETA_GET_OBJECT_MESSAGE:
            packet = new (std::nothrow)ReqKvMetaGetObjectMessage();
            break;
          case common::RSP_KVMETA_GET_OBJECT_MESSAGE:
            packet = new (std::nothrow)RspKvMetaGetObjectMessage();
            break;
          case common::REQ_KVMETA_PUT_OBJECT_MESSAGE:
            packet = new (std::nothrow)ReqKvMetaPutObjectMessage();
            break;
          case common::REQ_KVMETA_DEL_OBJECT_MESSAGE:
            packet = new (std::nothrow)ReqKvMetaDelObjectMessage();
            break;
          case common::RSP_KVMETA_DEL_OBJECT_MESSAGE:
            packet = new (std::nothrow)RspKvMetaDelObjectMessage();
            break;
          case common::REQ_KVMETA_HEAD_OBJECT_MESSAGE:
            packet = new (std::nothrow)ReqKvMetaHeadObjectMessage();
            break;
          case common::RSP_KVMETA_HEAD_OBJECT_MESSAGE:
            packet = new (std::nothrow)RspKvMetaHeadObjectMessage();
            break;
          case common::REQ_KVMETA_GET_BUCKET_MESSAGE:
            packet = new (std::nothrow)ReqKvMetaGetBucketMessage();
            break;
          case common::RSP_KVMETA_GET_BUCKET_MESSAGE:
            packet = new (std::nothrow)RspKvMetaGetBucketMessage();
            break;
          case common::REQ_KVMETA_PUT_BUCKET_MESSAGE:
            packet = new (std::nothrow)ReqKvMetaPutBucketMessage();
            break;
          case common::REQ_KVMETA_DEL_BUCKET_MESSAGE:
            packet = new (std::nothrow)ReqKvMetaDelBucketMessage();
            break;
          case common::REQ_KVMETA_HEAD_BUCKET_MESSAGE:
            packet = new (std::nothrow)ReqKvMetaHeadBucketMessage();
            break;
          case common::RSP_KVMETA_HEAD_BUCKET_MESSAGE:
            packet = new (std::nothrow)RspKvMetaHeadBucketMessage();
            break;
          case common::REQ_KV_RT_MS_KEEPALIVE_MESSAGE:
            packet = new (std::nothrow)KvRtsMsHeartMessage();
            break;
          case common::RSP_KV_RT_MS_KEEPALIVE_MESSAGE:
            packet = new (std::nothrow)KvRtsMsHeartResponseMessage();
            break;
          case common::REQ_KV_RT_GET_TABLE_MESSAGE:
            packet = new (std::nothrow)GetTableFromKvRtsMessage();
            break;
          case common::RSP_KV_RT_GET_TABLE_MESSAGE:
            packet = new (std::nothrow)GetTableFromKvRtsResponseMessage();
            break;
          case common::GET_BLOCK_INFO_MESSAGE_V2:
            packet = new (std::nothrow)GetBlockInfoMessageV2();
            break;
          case common::GET_BLOCK_INFO_RESP_MESSAGE_V2:
            packet = new (std::nothrow)GetBlockInfoRespMessageV2();
            break;
          case common::BATCH_GET_BLOCK_INFO_MESSAGE_V2:
            packet = new (std::nothrow)BatchGetBlockInfoMessageV2();
            break;
          case common::BATCH_GET_BLOCK_INFO_RESP_MESSAGE_V2:
            packet = new (std::nothrow)BatchGetBlockInfoRespMessageV2();
            break;
          case common::WRITE_FILE_MESSAGE_V2:
            packet = new (std::nothrow)WriteFileMessageV2();
            break;
          case common::WRITE_FILE_RESP_MESSAGE_V2:
            packet = new (std::nothrow)WriteFileRespMessageV2();
            break;
          case common::SLAVE_DS_RESP_MESSAGE:
            packet = new (std::nothrow)SlaveDsRespMessage();
            break;
          case common::CLOSE_FILE_MESSAGE_V2:
            packet = new (std::nothrow)CloseFileMessageV2();
            break;
          case common::UPDATE_BLOCK_INFO_MESSAGE_V2:
            packet = new (std::nothrow)UpdateBlockInfoMessageV2();
            break;
          case common::REPAIR_BLOCK_MESSAGE_V2:
            packet = new (std::nothrow)RepairBlockMessageV2();
            break;
          case common::STAT_FILE_MESSAGE_V2:
            packet = new (std::nothrow)StatFileMessageV2();
            break;
          case common::STAT_FILE_RESP_MESSAGE_V2:
            packet = new (std::nothrow)StatFileRespMessageV2();
            break;
          case common::READ_FILE_MESSAGE_V2:
            packet = new (std::nothrow)ReadFileMessageV2();
            break;
          case common::READ_FILE_RESP_MESSAGE_V2:
            packet = new (std::nothrow)ReadFileRespMessageV2();
            break;
          case common::UNLINK_FILE_MESSAGE_V2:
            packet = new (std::nothrow)UnlinkFileMessageV2();
            break;
          case common::NEW_BLOCK_MESSAGE_V2:
            packet = new (std::nothrow)NewBlockMessageV2();
            break;
          case common::REMOVE_BLOCK_MESSAGE_V2:
            packet = new (std::nothrow)RemoveBlockMessageV2();
            break;
          case common::READ_RAWDATA_MESSAGE_V2:
            packet = new (std::nothrow)ReadRawdataMessageV2();
            break;
          case common::READ_RAWDATA_RESP_MESSAGE_V2:
            packet = new (std::nothrow)ReadRawdataRespMessageV2();
            break;
          case common::WRITE_RAWDATA_MESSAGE_V2:
            packet = new (std::nothrow)WriteRawdataMessageV2();
            break;
          case common::READ_INDEX_MESSAGE_V2:
            packet = new (std::nothrow)ReadIndexMessageV2();
            break;
          case common::READ_INDEX_RESP_MESSAGE_V2:
            packet = new (std::nothrow)ReadIndexRespMessageV2();
            break;
          case common::WRITE_INDEX_MESSAGE_V2:
            packet = new (std::nothrow)WriteIndexMessageV2();
            break;
          case common::QUERY_EC_META_MESSAGE:
            packet = new (std::nothrow)QueryEcMetaMessage();
            break;
          case common::QUERY_EC_META_RESP_MESSAGE:
            packet = new (std::nothrow)QueryEcMetaRespMessage();
            break;
          case common::COMMIT_EC_META_MESSAGE:
            packet = new (std::nothrow)CommitEcMetaMessage();
            break;
          case common::BLOCK_FILE_INFO_MESSAGE_V2:
            packet = new (std::nothrow)BlockFileInfoMessageV2();
            break;
          case common::REPORT_CHECK_BLOCK_MESSAGE:
            packet = new (std::nothrow)ReportCheckBlockMessage();
            break;
          case common::DS_APPLY_LEASE_MESSAGE:
            packet = new (std::nothrow)DsApplyLeaseMessage();
            break;
          case common::DS_APPLY_LEASE_RESPONSE_MESSAGE:
            packet = new (std::nothrow)DsApplyLeaseResponseMessage();
            break;
          case common::DS_RENEW_LEASE_MESSAGE:
            packet = new (std::nothrow)DsRenewLeaseMessage();
            break;
          case common::DS_RENEW_LEASE_RESPONSE_MESSAGE:
            packet = new (std::nothrow)DsRenewLeaseResponseMessage();
            break;
          case common::DS_GIVEUP_LEASE_MESSAGE:
            packet = new (std::nothrow)DsGiveupLeaseMessage();
            break;
          case common::DS_APPLY_BLOCK_MESSAGE:
            packet = new (std::nothrow)DsApplyBlockMessage();
            break;
          case common::DS_APPLY_BLOCK_RESPONSE_MESSAGE:
            packet = new (std::nothrow)DsApplyBlockResponseMessage();
            break;
          case common::DS_APPLY_BLOCK_FOR_UPDATE_MESSAGE:
            packet = new (std::nothrow)DsApplyBlockForUpdateMessage();
            break;
          case common::DS_APPLY_BLOCK_FOR_UPDATE_RESPONSE_MESSAGE:
            packet = new (std::nothrow)DsApplyBlockForUpdateResponseMessage();
            break;
          case common::DS_GIVEUP_BLOCK_MESSAGE:
            packet = new (std::nothrow)DsGiveupBlockMessage();
            break;
          case common::DS_GIVEUP_BLOCK_RESPONSE_MESSAGE:
            packet = new (std::nothrow)DsGiveupBlockResponseMessage();
            break;
          case common::NS_REQ_RESOLVE_BLOCK_VERSION_CONFLICT_MESSAGE:
            packet = new (std::nothrow)NsReqResolveBlockVersionConflictMessage();
            break;
          case common::NS_RSP_RESOLVE_BLOCK_VERSION_CONFLICT_MESSAGE:
            packet = new (std::nothrow)NsReqResolveBlockVersionConflictResponseMessage();
            break;
          case common::GET_ALL_BLOCKS_HEADER_MESSAGE:
            packet = new (std::nothrow)GetAllBlocksHeaderMessage();
            break;
          case common::GET_ALL_BLOCKS_HEADER_RESP_MESSAGE:
            packet = new (std::nothrow)GetAllBlocksHeaderRespMessage();
            break;
          default:
            TBSYS_LOG(ERROR, "pcode: %d not found in message factory", pcode);
            break;
         }
      }
      return packet;
    }
  }/** message **/
}/** tfs **/
