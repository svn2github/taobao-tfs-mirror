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

    MessageFactory::MessageFactory()
    {
      create_message_map_[STATUS_MESSAGE] = StatusMessage::create;
      create_message_map_[GET_BLOCK_INFO_MESSAGE] = GetBlockInfoMessage::create;
      create_message_map_[SET_BLOCK_INFO_MESSAGE] = SetBlockInfoMessage::create;
      create_message_map_[BATCH_GET_BLOCK_INFO_MESSAGE] = BatchGetBlockInfoMessage::create;
      create_message_map_[BATCH_SET_BLOCK_INFO_MESSAGE] = BatchSetBlockInfoMessage::create;
      create_message_map_[CARRY_BLOCK_MESSAGE] = CarryBlockMessage::create;
      create_message_map_[SET_DATASERVER_MESSAGE] = SetDataserverMessage::create;
      create_message_map_[UPDATE_BLOCK_INFO_MESSAGE] = UpdateBlockInfoMessage::create;
      create_message_map_[BLOCK_WRITE_COMPLETE_MESSAGE] = BlockWriteCompleteMessage::create;
      create_message_map_[READ_DATA_MESSAGE] = ReadDataMessage::create;
      create_message_map_[RESP_READ_DATA_MESSAGE] = RespReadDataMessage::create;
      create_message_map_[FILE_INFO_MESSAGE] = FileInfoMessage::create;
      create_message_map_[RESP_FILE_INFO_MESSAGE] = RespFileInfoMessage::create;
      create_message_map_[WRITE_DATA_MESSAGE] = WriteDataMessage::create;
      create_message_map_[CLOSE_FILE_MESSAGE] = CloseFileMessage::create;
      create_message_map_[UNLINK_FILE_MESSAGE] = UnlinkFileMessage::create;
      create_message_map_[REPLICATE_BLOCK_MESSAGE] = ReplicateBlockMessage::create;
      create_message_map_[COMPACT_BLOCK_MESSAGE] = CompactBlockMessage::create;
      create_message_map_[GET_SERVER_STATUS_MESSAGE] = GetServerStatusMessage::create;
      //create_message_map_[SET_SERVER_STATUS_MESSAGE] = SetServerStatusMessage::create;
      create_message_map_[SUSPECT_DATASERVER_MESSAGE] = SuspectDataserverMessage::create;
      create_message_map_[RENAME_FILE_MESSAGE] = RenameFileMessage::create;
      create_message_map_[CLIENT_CMD_MESSAGE] = ClientCmdMessage::create;
      create_message_map_[CREATE_FILENAME_MESSAGE] = CreateFilenameMessage::create;
      create_message_map_[RESP_CREATE_FILENAME_MESSAGE] = RespCreateFilenameMessage::create;
      create_message_map_[ROLLBACK_MESSAGE] = RollbackMessage::create;
      create_message_map_[RESP_HEART_MESSAGE] = RespHeartMessage::create;
      create_message_map_[RESET_BLOCK_VERSION_MESSAGE] = ResetBlockVersionMessage::create;
      create_message_map_[BLOCK_FILE_INFO_MESSAGE] = BlockFileInfoMessage::create;
      create_message_map_[NEW_BLOCK_MESSAGE] = NewBlockMessage::create;
      create_message_map_[REMOVE_BLOCK_MESSAGE] = RemoveBlockMessage::create;
      create_message_map_[LIST_BLOCK_MESSAGE] = ListBlockMessage::create;
      create_message_map_[RESP_LIST_BLOCK_MESSAGE] = RespListBlockMessage::create;
      create_message_map_[BLOCK_WRITE_COMPLETE_MESSAGE] = BlockWriteCompleteMessage::create;
      create_message_map_[BLOCK_RAW_META_MESSAGE] = BlockRawMetaMessage::create;
      create_message_map_[WRITE_RAW_DATA_MESSAGE] = WriteRawDataMessage::create;
      create_message_map_[WRITE_INFO_BATCH_MESSAGE] = WriteInfoBatchMessage::create;
      create_message_map_[BLOCK_COMPACT_COMPLETE_MESSAGE] = CompactBlockCompleteMessage::create;
      create_message_map_[READ_DATA_MESSAGE_V2] = ReadDataMessageV2::create;
      create_message_map_[RESP_READ_DATA_MESSAGE_V2] = RespReadDataMessageV2::create;
      create_message_map_[LIST_BITMAP_MESSAGE] = ListBitMapMessage::create;
      create_message_map_[RESP_LIST_BITMAP_MESSAGE] = RespListBitMapMessage::create;
      create_message_map_[RELOAD_CONFIG_MESSAGE] = ReloadConfigMessage::create;
      create_message_map_[SERVER_META_INFO_MESSAGE] = ServerMetaInfoMessage::create;
      create_message_map_[RESP_SERVER_META_INFO_MESSAGE] = RespServerMetaInfoMessage::create;
      create_message_map_[READ_RAW_DATA_MESSAGE] = ReadRawDataMessage::create;
      create_message_map_[RESP_READ_RAW_DATA_MESSAGE] = RespReadRawDataMessage::create;
      create_message_map_[REPLICATE_INFO_MESSAGE] = ReplicateInfoMessage::create;
      create_message_map_[ACCESS_STAT_INFO_MESSAGE] = AccessStatInfoMessage::create;
      create_message_map_[READ_SCALE_IMAGE_MESSAGE] = ReadScaleImageMessage::create;
      create_message_map_[CRC_ERROR_MESSAGE] = CrcErrorMessage::create;

      create_message_map_[OPLOG_SYNC_MESSAGE] = OpLogSyncMessage::create;//add by duanbing 2010.03.01:15:31
      create_message_map_[OPLOG_SYNC_RESPONSE_MESSAGE] = OpLogSyncResponeMessage::create;
      create_message_map_[MASTER_AND_SLAVE_HEART_MESSAGE] = MasterAndSlaveHeartMessage::create;
      create_message_map_[MASTER_AND_SLAVE_HEART_RESPONSE_MESSAGE] = MasterAndSlaveHeartResponseMessage::create;
      create_message_map_[HEARTBEAT_AND_NS_HEART_MESSAGE] = HeartBeatAndNSHeartMessage::create;
      //create_message_map_[GET_BLOCK_LIST_MESSAGE] = GetBlockListMessage::create;
      create_message_map_[ADMIN_CMD_MESSAGE] = AdminCmdMessage::create;
      create_message_map_[REMOVE_BLOCK_RESPONSE_MESSAGE] = RemoveBlockResponseMessage::create;
      create_message_map_[DUMP_PLAN_MESSAGE] = DumpPlanMessage::create;
      create_message_map_[DUMP_PLAN_RESPONSE_MESSAGE] = DumpPlanResponseMessage::create;
      create_message_map_[SHOW_SERVER_INFORMATION_MESSAGE] = ShowServerInformationMessage::create;
    }

    MessageFactory::~MessageFactory()
    {
      create_message_map_.clear();
    }

    Message* MessageFactory::create_message(int32_t type)
    {
      type = (type & 0xFFFF);
      CREATE_MESSAGE_MAP_ITER it = create_message_map_.find(type);
      if (it == create_message_map_.end())
      {
        TBSYS_LOG(ERROR, "createMessage error: type=%d", type);
        return NULL;
      }
      return (it->second)(type);
    }

    tbnet::Packet* MessageFactory::createPacket(int32_t pcode)
    {
      return create_message(pcode);
    }

    Message* MessageFactory::clone_message(Message* message, int32_t version, bool deserialize)
    {
      Message* cloned = create_message(message->get_message_type());
      if (cloned)
      {
        if (!cloned->copy(message, version, deserialize))
        {
          TBSYS_LOG(ERROR, "copy messsge %x error\n", message);
          delete cloned;
          return NULL;
        }
      }
      return cloned;
    }

    int MessageFactory::send_error_message(Message* packet, int32_t level, char* file, int32_t line, const char* function,
        uint64_t server_id, char* fmt, ...)
    {
      char buffer[1024];
      char msgstr[512];

      va_list ap;
      va_start(ap, fmt);
      vsnprintf(msgstr, 512, fmt, ap);
      va_end(ap);
      TBSYS_LOGGER.logMessage(level, file, line, function, "%s", msgstr);

      snprintf(buffer, 1024, "%s %s", tbsys::CNetUtil::addrToString(server_id).c_str(), msgstr);
      StatusMessage* msg = new StatusMessage(STATUS_MESSAGE_ERROR, buffer);

      packet->reply_message(msg);

      return TFS_SUCCESS;
    }

    int MessageFactory::send_error_message(Message* packet, int32_t level, char* file, int32_t line, const char* function,
        int32_t err_code, uint64_t server_id, char* fmt, ...)
    {
      char buffer[1024];
      char msgstr[512];

      va_list ap;
      va_start(ap, fmt);
      vsnprintf(msgstr, 512, fmt, ap);
      va_end(ap);
      TBSYS_LOGGER.logMessage(level, file, line, function, "%s", msgstr);

      snprintf(buffer, 1024, "%s %s", tbsys::CNetUtil::addrToString(server_id).c_str(), msgstr);
      StatusMessage* msg = new StatusMessage(err_code, buffer);

      packet->reply_message(msg);

      return TFS_SUCCESS;
    }
  }
}
