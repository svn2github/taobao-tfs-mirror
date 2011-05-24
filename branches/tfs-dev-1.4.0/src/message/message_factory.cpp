/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
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
    using namespace tfs::common;

    int MessageFactory::initialize()
    {
      BasePacketFactory::initialize();
      packet_maps_[GET_BLOCK_INFO_MESSAGE] = GetBlockInfoMessage::create;
      packet_maps_[SET_BLOCK_INFO_MESSAGE] = SetBlockInfoMessage::create;
      packet_maps_[BATCH_GET_BLOCK_INFO_MESSAGE] = BatchGetBlockInfoMessage::create;
      packet_maps_[BATCH_SET_BLOCK_INFO_MESSAGE] = BatchSetBlockInfoMessage::create;
      packet_maps_[CARRY_BLOCK_MESSAGE] = CarryBlockMessage::create;
      packet_maps_[SET_DATASERVER_MESSAGE] = SetDataserverMessage::create;
      packet_maps_[UPDATE_BLOCK_INFO_MESSAGE] = UpdateBlockInfoMessage::create;
      packet_maps_[BLOCK_WRITE_COMPLETE_MESSAGE] = BlockWriteCompleteMessage::create;
      packet_maps_[READ_DATA_MESSAGE] = ReadDataMessage::create;
      packet_maps_[RESP_READ_DATA_MESSAGE] = RespReadDataMessage::create;
      packet_maps_[FILE_INFO_MESSAGE] = FileInfoMessage::create;
      packet_maps_[RESP_FILE_INFO_MESSAGE] = RespFileInfoMessage::create;
      packet_maps_[WRITE_DATA_MESSAGE] = WriteDataMessage::create;
      packet_maps_[CLOSE_FILE_MESSAGE] = CloseFileMessage::create;
      packet_maps_[UNLINK_FILE_MESSAGE] = UnlinkFileMessage::create;
      packet_maps_[REPLICATE_BLOCK_MESSAGE] = ReplicateBlockMessage::create;
      packet_maps_[COMPACT_BLOCK_MESSAGE] = CompactBlockMessage::create;
      packet_maps_[GET_SERVER_STATUS_MESSAGE] = GetServerStatusMessage::create;
      packet_maps_[SUSPECT_DATASERVER_MESSAGE] = SuspectDataserverMessage::create;
      packet_maps_[RENAME_FILE_MESSAGE] = RenameFileMessage::create;
      packet_maps_[CLIENT_CMD_MESSAGE] = ClientCmdMessage::create;
      packet_maps_[CREATE_FILENAME_MESSAGE] = CreateFilenameMessage::create;
      packet_maps_[RESP_CREATE_FILENAME_MESSAGE] = RespCreateFilenameMessage::create;
      packet_maps_[ROLLBACK_MESSAGE] = RollbackMessage::create;
      packet_maps_[RESP_HEART_MESSAGE] = RespHeartMessage::create;
      packet_maps_[RESET_BLOCK_VERSION_MESSAGE] = ResetBlockVersionMessage::create;
      packet_maps_[BLOCK_FILE_INFO_MESSAGE] = BlockFileInfoMessage::create;
      packet_maps_[NEW_BLOCK_MESSAGE] = NewBlockMessage::create;
      packet_maps_[REMOVE_BLOCK_MESSAGE] = RemoveBlockMessage::create;
      packet_maps_[LIST_BLOCK_MESSAGE] = ListBlockMessage::create;
      packet_maps_[RESP_LIST_BLOCK_MESSAGE] = RespListBlockMessage::create;
      packet_maps_[BLOCK_WRITE_COMPLETE_MESSAGE] = BlockWriteCompleteMessage::create;
      packet_maps_[BLOCK_RAW_META_MESSAGE] = BlockRawMetaMessage::create;
      packet_maps_[WRITE_RAW_DATA_MESSAGE] = WriteRawDataMessage::create;
      packet_maps_[WRITE_INFO_BATCH_MESSAGE] = WriteInfoBatchMessage::create;
      packet_maps_[BLOCK_COMPACT_COMPLETE_MESSAGE] = CompactBlockCompleteMessage::create;
      packet_maps_[READ_DATA_MESSAGE_V2] = ReadDataMessageV2::create;
      packet_maps_[RESP_READ_DATA_MESSAGE_V2] = RespReadDataMessageV2::create;
      packet_maps_[LIST_BITMAP_MESSAGE] = ListBitMapMessage::create;
      packet_maps_[RESP_LIST_BITMAP_MESSAGE] = RespListBitMapMessage::create;
      packet_maps_[RELOAD_CONFIG_MESSAGE] = ReloadConfigMessage::create;
      packet_maps_[READ_RAW_DATA_MESSAGE] = ReadRawDataMessage::create;
      packet_maps_[RESP_READ_RAW_DATA_MESSAGE] = RespReadRawDataMessage::create;
      packet_maps_[ACCESS_STAT_INFO_MESSAGE] = AccessStatInfoMessage::create;
      packet_maps_[READ_SCALE_IMAGE_MESSAGE] = ReadScaleImageMessage::create;
      packet_maps_[CRC_ERROR_MESSAGE] = CrcErrorMessage::create;

      packet_maps_[OPLOG_SYNC_MESSAGE] = OpLogSyncMessage::create;
      packet_maps_[OPLOG_SYNC_RESPONSE_MESSAGE] = OpLogSyncResponeMessage::create;
      packet_maps_[MASTER_AND_SLAVE_HEART_MESSAGE] = MasterAndSlaveHeartMessage::create;
      packet_maps_[MASTER_AND_SLAVE_HEART_RESPONSE_MESSAGE] = MasterAndSlaveHeartResponseMessage::create;
      packet_maps_[HEARTBEAT_AND_NS_HEART_MESSAGE] = HeartBeatAndNSHeartMessage::create;
      packet_maps_[ADMIN_CMD_MESSAGE] = AdminCmdMessage::create;
      packet_maps_[REMOVE_BLOCK_RESPONSE_MESSAGE] = RemoveBlockResponseMessage::create;
      packet_maps_[DUMP_PLAN_MESSAGE] = DumpPlanMessage::create;
      packet_maps_[DUMP_PLAN_RESPONSE_MESSAGE] = DumpPlanResponseMessage::create;
      packet_maps_[SHOW_SERVER_INFORMATION_MESSAGE] = ShowServerInformationMessage::create;
      return common::TFS_SUCCESS;
    }
  }
}
