/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: base_packet.h 213 2011-04-22 16:22:51Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_COMMON_BASE_PACKET_H_
#define TFS_COMMON_BASE_PACKET_H_

#include <tbsys.h>
#include <tbnet.h>
#include <Memory.hpp>
#include "internal.h"
#include "func.h"
#include "stream.h"
#include "serialization.h"

namespace tfs
{
  namespace common 
  {
    //old structure
    #pragma pack(4)
    struct TfsPacketNewHeaderV0
    {
      uint32_t flag_;
      int32_t length_;
      int16_t type_;
      int16_t check_;
      int serialize(char*data, const int64_t data_len, int64_t& pos)
      {
        int32_t iret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::set_int32(data, data_len, pos, flag_);
        }
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::set_int32(data, data_len, pos, length_);
        }
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::set_int16(data, data_len, pos, type_);
        }
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::set_int16(data, data_len, pos, check_);
        }
        return iret;
      }

      int deserialize(char*data, const int64_t data_len, int64_t& pos)
      {
        int32_t iret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&flag_));
        }
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::get_int32(data, data_len, pos, &length_);
        }
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::get_int16(data, data_len, pos, &type_);
        }
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::get_int16(data, data_len, pos, &check_);
        }
        return iret;
      }

      int64_t length() const
      {
        return sizeof(TfsPacketNewHeaderV0);
      }
    };

    //new structure
    struct TfsPacketNewHeaderV1
    {
      uint64_t id_;
      uint32_t flag_;
      uint32_t crc_;
      int32_t  length_;
      int16_t  type_;
      int16_t  version_;
      int serialize(char*data, const int64_t data_len, int64_t& pos)
      {
        int32_t iret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::set_int32(data, data_len, pos, flag_);
        }
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::set_int32(data, data_len, pos, length_);
        }
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::set_int16(data, data_len, pos, type_);
        }
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::set_int16(data, data_len, pos, version_);
        }
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::set_int64(data, data_len, pos, id_);
        }
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::set_int32(data, data_len, pos, crc_);
        }
        return iret;
      }

      int deserialize(const char*data, const int64_t data_len, int64_t& pos)
      {
        int32_t iret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&flag_));
        }
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::get_int32(data, data_len, pos, &length_);
        }
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::get_int16(data, data_len, pos, &type_);
        }
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::get_int16(data, data_len, pos, &version_);
        }
        /*if (TFS_SUCCESS == iret)
        {
          iret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&id_));
        }
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&crc_));
        }*/
        return iret;
      }

      int64_t length() const
      {
        return sizeof(TfsPacketNewHeaderV1);
      }
    };
  #pragma pack()

    enum DirectionStatus
    {
      DIRECTION_RECEIVE = 1,
      DIRECTION_SEND = 2,
      DIRECTION_MASTER_SLAVE_NS = 4
    };
    enum TfsPacketVersion
    {
      TFS_PACKET_VERSION_V0 = 0,
      TFS_PACKET_VERSION_V1,
      TFS_PACKET_VERSION_V2
    };

    enum MessageType
    {
      STATUS_MESSAGE = 1,
      GET_BLOCK_INFO_MESSAGE,
      SET_BLOCK_INFO_MESSAGE,
      CARRY_BLOCK_MESSAGE,
      SET_DATASERVER_MESSAGE,
      UPDATE_BLOCK_INFO_MESSAGE,
      READ_DATA_MESSAGE,
      RESP_READ_DATA_MESSAGE,
      WRITE_DATA_MESSAGE,
      CLOSE_FILE_MESSAGE,
      UNLINK_FILE_MESSAGE,
      REPLICATE_BLOCK_MESSAGE,
      COMPACT_BLOCK_MESSAGE,
      GET_SERVER_STATUS_MESSAGE,
      SHOW_SERVER_INFORMATION_MESSAGE,
      SUSPECT_DATASERVER_MESSAGE,
      FILE_INFO_MESSAGE,
      RESP_FILE_INFO_MESSAGE,
      RENAME_FILE_MESSAGE,
      CLIENT_CMD_MESSAGE,
      CREATE_FILENAME_MESSAGE,
      RESP_CREATE_FILENAME_MESSAGE,
      ROLLBACK_MESSAGE,
      RESP_HEART_MESSAGE,
      RESET_BLOCK_VERSION_MESSAGE,
      BLOCK_FILE_INFO_MESSAGE,
      LEGACY_UNIQUE_FILE_MESSAGE,
      LEGACY_RETRY_COMMAND_MESSAGE,
      NEW_BLOCK_MESSAGE,
      REMOVE_BLOCK_MESSAGE,
      LIST_BLOCK_MESSAGE,
      RESP_LIST_BLOCK_MESSAGE,
      BLOCK_WRITE_COMPLETE_MESSAGE,
      BLOCK_RAW_META_MESSAGE,
      WRITE_RAW_DATA_MESSAGE,
      WRITE_INFO_BATCH_MESSAGE,
      BLOCK_COMPACT_COMPLETE_MESSAGE,
      READ_DATA_MESSAGE_V2,
      RESP_READ_DATA_MESSAGE_V2,
      LIST_BITMAP_MESSAGE,
      RESP_LIST_BITMAP_MESSAGE,
      RELOAD_CONFIG_MESSAGE,
      SERVER_META_INFO_MESSAGE,
      RESP_SERVER_META_INFO_MESSAGE,
      READ_RAW_DATA_MESSAGE,
      RESP_READ_RAW_DATA_MESSAGE,
      REPLICATE_INFO_MESSAGE,
      ACCESS_STAT_INFO_MESSAGE,
      READ_SCALE_IMAGE_MESSAGE,
      OPLOG_SYNC_MESSAGE,
      OPLOG_SYNC_RESPONSE_MESSAGE,
      MASTER_AND_SLAVE_HEART_MESSAGE,
      MASTER_AND_SLAVE_HEART_RESPONSE_MESSAGE,
      HEARTBEAT_AND_NS_HEART_MESSAGE,
      OWNER_CHECK_MESSAGE,
      GET_BLOCK_LIST_MESSAGE,
      CRC_ERROR_MESSAGE,
      ADMIN_CMD_MESSAGE,
      BATCH_GET_BLOCK_INFO_MESSAGE,
      BATCH_SET_BLOCK_INFO_MESSAGE,
      REMOVE_BLOCK_RESPONSE_MESSAGE,
      DUMP_PLAN_MESSAGE,
      DUMP_PLAN_RESPONSE_MESSAGE,
      LOCAL_PACKET,
      STATUS_PACKET,
    };

    // StatusMessage status value
    enum StatusMessageStatus
    {
      STATUS_MESSAGE_OK = 0,
      STATUS_MESSAGE_ERROR,
      STATUS_NEED_SEND_BLOCK_INFO,
      STATUS_MESSAGE_PING,
      STATUS_MESSAGE_REMOVE,
      STATUS_MESSAGE_BLOCK_FULL,
      STATUS_MESSAGE_ACCESS_DENIED
    };

		static const uint32_t TFS_PACKET_FLAG_V0 = 0x4d534654;//TFSM
		static const uint32_t TFS_PACKET_FLAG_V1 = 0x4e534654;//TFSN(V1)
		static const int32_t TFS_PACKET_HEADER_V0_SIZE = sizeof(TfsPacketNewHeaderV0);
		static const int32_t TFS_PACKET_HEADER_V1_SIZE = sizeof(TfsPacketNewHeaderV1);
		static const int32_t TFS_PACKET_HEADER_DIFF_SIZE = TFS_PACKET_HEADER_V1_SIZE - TFS_PACKET_HEADER_V0_SIZE;

    class BasePacket: public tbnet::Packet
    {
    public:
      BasePacket();
      virtual ~BasePacket();
      virtual bool copy(BasePacket* src, const int32_t version, const bool deserialize);
      bool encode(tbnet::DataBuffer* output);
      bool decode(tbnet::DataBuffer* input, tbnet::PacketHeader* header);

      virtual int serialize(Stream& output) = 0;
      virtual int deserialize(Stream& input) = 0;
      virtual int64_t length() const = 0;
      virtual int reply(tbnet::Packet* packet);
      int reply_error_packet(const int32_t level, const char* file, const int32_t line,
               const char* function, const int32_t error_code, const char* fmt, ...);
      virtual void dump() const {}

      inline bool is_enable_dump() const { return dump_flag_;}
      inline void enable_dump() { dump_flag_ = true;}
      inline void disable_dump() { dump_flag_ = false;}
      inline void set_auto_free(const bool auto_free = true) { auto_free_ = auto_free;}
      inline bool get_auto_free() const { return auto_free_;}
      inline void free() { if (auto_free_) { delete this;}}
      inline void set_connection(tbnet::Connection* connection) { connection_ = connection;}
      inline tbnet::Connection* get_connection() const { return connection_;}
      inline void set_direction(const DirectionStatus direction) { direction_ = direction; }
      inline DirectionStatus get_direction() const { return direction_;}
      inline void set_version(const int32_t version) { version_ = version;}
      inline int32_t get_version() const { return version_;}
      inline void set_crc(const uint32_t crc) { crc_ = crc;}
      inline uint32_t get_crc() const { return crc_;}
      inline void set_id(const uint64_t id) { id_ = id;}
      inline uint64_t get_id() const { return id_;}

  #ifdef TFS_GTEST
    public:
  #else
    protected:
  #endif
      Stream stream_;
      tbnet::Connection* connection_;
      uint64_t id_;
      uint32_t crc_;
      DirectionStatus direction_;
      int32_t version_;
      static const int16_t MAX_ERROR_MSG_LENGTH = 512;
      bool auto_free_;
      bool dump_flag_;
    };
  }
}
#endif //TFS_COMMON_BASE_PACKET_H_
