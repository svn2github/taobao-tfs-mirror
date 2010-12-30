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
#ifndef TFS_MESSAGE_MESSAGE_H_
#define TFS_MESSAGE_MESSAGE_H_

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string>
#include <errno.h>
#include <assert.h>
#include <limits.h>
#include <tbsys.h>
#include <tbnet.h>
#include <Memory.hpp>
#include "common/interval.h"
#include "common/func.h"
#include <ext/hash_map>

namespace tfs
{
  namespace message
  {
    class Message;
    typedef Message* (*create_message_handler_t)(int32_t);
    typedef __gnu_cxx::hash_map<int32_t, create_message_handler_t> CREATE_MESSAGE_MAP;
    typedef CREATE_MESSAGE_MAP::iterator CREATE_MESSAGE_MAP_ITER;

#pragma pack(4)
    //old structure
    struct TfsPacketHeaderV0
    {
      uint32_t flag_;
      int32_t length_;
      short type_;
      short check_;
    };

    //new structure
    struct TfsPacketHeaderV1
    {
      uint32_t flag_;
      int32_t length_;
      short type_;
      short version_;
      uint64_t id_;
      uint32_t crc_;
    };
#pragma pack()

    //Define Message Type Start
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
      SET_SERVER_STATUS_MESSAGE,
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
      ADMIN_CMD_MESSAGE
    };
    //Define Message Type End

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

    // HeartMessage status value
    enum HeartMessageStatus
    {
      HEART_MESSAGE_OK = 0,
      HEART_NEED_SEND_BLOCK_INFO,
      HEART_EXP_BLOCK_ID
    };

    enum DirectionStatus
    {
      DIRECTION_RECEIVE = 1,
      DIRECTION_SEND = 2,
      DIRECTION_MASTER_SLAVE_NS = 4
    };

		static const uint32_t TFS_PACKET_FLAG_V0 = 0x4d534654;//TFSM
		static const uint32_t TFS_PACKET_FLAG_V1 = 0x4e534654;//TFSN(V1)
		static const uint32_t TFS_PACKET_HEADER_V0_SIZE = sizeof(TfsPacketHeaderV0);
		static const uint32_t TFS_PACKET_HEADER_V1_SIZE = sizeof(TfsPacketHeaderV1);
		static const uint32_t TFS_PACKET_HEADER_DIFF_SIZE = TFS_PACKET_HEADER_V1_SIZE - TFS_PACKET_HEADER_V0_SIZE;

    class Message: public tbnet::Packet
    {
      public:
        Message() :
          raw_data_(NULL), raw_size_(0), connection_(NULL), direction_(DIRECTION_SEND), id_(0), crc_(0), version_(2),
              auto_free_(true)
        {
        }

        inline bool copy(Message* src, const int32_t version, bool deserialize)
        {
          if (_packetHeader._pcode != src->get_message_type())
          {
            return false;
          }

          // base class
          _next = NULL;
          _channel = NULL;
          _expireTime = 0;
          memcpy(&_packetHeader, &src->_packetHeader, sizeof(tbnet::PacketHeader));

          // self members;
          id_ = src->id_;
          crc_ = src->crc_;
          if (version >= 0)
          {
            version_ = version;
          }
          else
          {
            version_ = src->version_;
          }

          auto_free_ = src->auto_free_;
          connection_ = src->connection_;
          direction_ = src->direction_;

					tbsys::gDeleteA(raw_data_);
          raw_size_ = src->message_length();
          raw_data_ = new char[raw_size_];
          if (src->build(raw_data_, raw_size_) != common::TFS_SUCCESS)
          {
            return false;
          }
          // recalculate crc
          if (version_ >= 1)
          {
            crc_ = common::Func::crc(TFS_PACKET_FLAG_V1, raw_data_, raw_size_);
          }
          if (deserialize)
          {
            return parse(raw_data_, raw_size_) == common::TFS_SUCCESS;
          }
          return true;
        }

        inline void set_connection(tbnet::Connection* connection)
        {
          connection_ = connection;
        }

        inline tbnet::Connection* get_connection() const
        {
          return connection_;
        }

        inline void set_direction(const int32_t direction)
        {
          direction_ = direction;
        }

        inline int32_t get_direction() const
        {
          return direction_;
        }

        inline void set_message_type(const int32_t type)
        {
          _packetHeader._pcode = type;
        }

        inline int32_t get_message_type() const
        {
          return _packetHeader._pcode;
        }

        inline void set_id(const uint64_t id)
        {
          id_ = id;
        }

        inline uint64_t get_id() const
        {
          return id_;
        }

        inline uint32_t get_crc() const
        {
          return crc_;
        }
        //encode to raw data
        inline bool encode(tbnet::DataBuffer* output)
        {
          if ((raw_size_ <= 0) || (raw_data_ == NULL))
          {
            return false;
          }
          output->writeBytes(raw_data_, raw_size_);
          return true;
        }
        // decode
        inline bool decode(tbnet::DataBuffer* input, tbnet::PacketHeader* header)
        {
          version_ = ((header->_pcode >> 16) & 0xFFFF);
          header->_pcode = (header->_pcode & 0xFFFF);
          int32_t size = header->_dataLen;
          if (version_ >= 1)
          {
            size -= TFS_PACKET_HEADER_DIFF_SIZE;
          }
          if (size <= 0)
          {
            return false;
          }
          // both v1 and v2 have id and crc
          if (version_ >= 1)
          {
            input->readBytes(&id_, sizeof(int64_t));
            input->readBytes(&crc_, sizeof(uint32_t));
            // check crc
            uint32_t crc = common::Func::crc(TFS_PACKET_FLAG_V1, input->getData(), size);
            if (crc != crc_)
            {
              TBSYS_LOG(ERROR, "decode packet crc check error, header crc(%u), calc crc(%u)\n", crc_, crc);
              return false;
            }
          }

					tbsys::gDeleteA(raw_data_);
          raw_size_ = size;
          raw_data_ = new char[size];
          char* data = input->getData();
          memcpy(raw_data_, data, size);
          input->drainData(size);

          data = raw_data_;
          if (parse(data, size) == common::TFS_SUCCESS)
          {
            return true;
          }
          else
          {
            return false;
          }
        }
        inline void reply_message(Message* message)
        {
          if (getChannelId() == 0)
          {
            TBSYS_LOG(ERROR, "message (%d) channel is null, reply message (%d)", getPCode(), message->getPCode());
            delete message;
            return;
          }
          if (((direction_ & DIRECTION_RECEIVE) && _expireTime > 0) && (tbsys::CTimeUtil::getTime() > _expireTime))
          {
            TBSYS_LOG(ERROR, "message (%d), timeout for response, reply message (%d)", getPCode(), message->getPCode());
            delete message;
            return;
          }

          message->setChannelId(getChannelId());
          message->set_id(id_ + 1);
          message->set_version(version_);
          // build message
          if (message->build_message() == false)
          {
            TBSYS_LOG(ERROR, "build message failure, pcode:%d", message->getPCode());
            delete message;
            return;
          }
          // post
          if (connection_->postPacket(message) == false)
          {
            TBSYS_LOG(ERROR, "post packet failure, server: %s, pcode:%d", tbsys::CNetUtil::addrToString(
                connection_->getServerId()).c_str(), message->getPCode());
            delete message;
            return;
          }
        }
        inline char* get_raw_data() const
        {
          return raw_data_;
        }
        inline int32_t get_raw_size() const
        {
          return raw_size_;
        }
        inline void set_version(const int32_t version)
        {
          version_ = version;
        }
        inline int32_t get_version() const
        {
          return version_;
        }

        // build the message to raw data
        inline bool build_message()
        {
					tbsys::gDeleteA(raw_data_);
          raw_size_ = message_length();
          raw_data_ = new char[raw_size_];
					raw_data_[0] = '\0';
          if (build(raw_data_, raw_size_) == common::TFS_SUCCESS)
          {
            if (version_ >= 1)
            {
              crc_ = common::Func::crc(TFS_PACKET_FLAG_V1, raw_data_, raw_size_);
            }
            return true;
          }
          else
          {
            return false;
          }
        }
        virtual ~Message()
        {
					tbsys::gDeleteA(raw_data_);
        }
        virtual int parse(char* data, int32_t len) = 0;
        virtual int build(char* data, int32_t len) = 0;
        virtual int message_length() = 0;
        virtual char* get_name() = 0;

        inline void free()
        {
          if (auto_free_ == true)
          {
            delete this;
          }
        }
        inline void set_auto_free(bool auto_free)
        {
          auto_free_ = auto_free;
        }

      private:
        char* raw_data_;
        int32_t raw_size_;

      protected:
        tbnet::Connection* connection_;
        int32_t direction_;
        uint64_t id_;
        uint32_t crc_;
        int32_t version_;
        bool auto_free_;

        inline int get_int32(char** data, int32_t* len, int32_t* value)
        {
          if ((*len) < common::INT_SIZE || (*len) > common::TFS_MALLOC_MAX_SIZE || data == NULL || (*data) == NULL)
          {
            return common::TFS_ERROR;
          }
          (*value) = *(reinterpret_cast<int32_t*> (*data));
          (*len) -= common::INT_SIZE;
          (*data) += common::INT_SIZE;
          assert(*len >= 0);
          return common::TFS_SUCCESS;
        }
        inline int get_uint32(char** data, int32_t* len, uint32_t* value)
        {
          if ((*len) < common::INT_SIZE || (*len) > common::TFS_MALLOC_MAX_SIZE || data == NULL || (*data) == NULL)
          {
            return common::TFS_ERROR;
          }
          (*value) = *(reinterpret_cast<uint32_t*> (*data));
          (*len) -= common::INT_SIZE;
          (*data) += common::INT_SIZE;
          assert(*len >= 0);
          return common::TFS_SUCCESS;
        }

        inline int get_int64(char** data, int32_t* len, int64_t* value)
        {
          if ((*len) < common::INT64_SIZE || (*len) > common::TFS_MALLOC_MAX_SIZE || data == NULL || (*data) == NULL)
          {
            return common::TFS_ERROR;
          }
          (*value) = *(reinterpret_cast<int64_t*> (*data));
          (*len) -= common::INT64_SIZE;
          (*data) += common::INT64_SIZE;
          assert(*len >= 0);
          return common::TFS_SUCCESS;
        }

        inline int get_string(char** data, int32_t* len, char** value)
        {
          if ((*len) < common::INT_SIZE || (*len) > common::TFS_MALLOC_MAX_SIZE || data == NULL || (*data) == NULL)
          {
            return common::TFS_ERROR;
          }
          int32_t string_size = *(reinterpret_cast<int32_t*> (*data));
          if ((*len) < common::INT_SIZE + string_size)
          {
            return common::TFS_ERROR;
          }
          if (string_size > 0)
          {
            (*value) = (*data) + common::INT_SIZE;
          }
          else
          {
            (*value) = NULL;
          }
          (*len) -= (common::INT_SIZE + string_size);
          (*data) += (common::INT_SIZE + string_size);
          assert(*len >= 0);
          return common::TFS_SUCCESS;
        }
        // get fixed length object, such as structure
        inline int get_object(char** data, int32_t* len, void** object, int32_t obj_len)
        {
          if ((*len) < obj_len || (*len) > common::TFS_MALLOC_MAX_SIZE || data == NULL || (*data) == NULL)
          {
            return common::TFS_ERROR;
          }
          (*object) = (*data);
          (*len) -= obj_len;
          (*data) += obj_len;
          assert(*len >= 0);
          return common::TFS_SUCCESS;
        }
        // get fixed length object and copy the object
        inline int get_object_copy(char** data, int32_t* len, void* object, int32_t obj_len)
        {
          if ((*len) < obj_len || (*len) > common::TFS_MALLOC_MAX_SIZE || data == NULL || (*data) == NULL)
          {
            return common::TFS_ERROR;
          }
          memcpy(object, *data, obj_len);
          (*len) -= obj_len;
          (*data) += obj_len;
          assert(*len >= 0);
          return common::TFS_SUCCESS;
        }

        inline int get_vint32(char** data, int32_t* len, common::VUINT32& value)
        {
          if ((*len) < common::INT_SIZE || (*len) > common::TFS_MALLOC_MAX_SIZE || data == NULL || (*data) == NULL)
          {
            return common::TFS_ERROR;
          }
          int32_t i = 0;
          int32_t count = *(reinterpret_cast<int32_t*> (*data));
          int32_t tlen = common::INT_SIZE + count * common::INT_SIZE;
          if ((*len) < tlen)
          {
            return common::TFS_ERROR;
          }
          uint32_t* int_array = reinterpret_cast<uint32_t*> ((*data) + common::INT_SIZE);
          for (i = 0; i < count; i++)
          {
            value.push_back(int_array[i]);
          }
          (*len) -= tlen;
          (*data) += tlen;
          assert(*len >= 0);
          return common::TFS_SUCCESS;
        }

        inline int get_vint64(char** data, int32_t* len, common::VUINT64& value)
        {
          if ((*len) < common::INT_SIZE || (*len) > common::TFS_MALLOC_MAX_SIZE || data == NULL || (*data) == NULL)
          {
            return common::TFS_ERROR;
          }
          int32_t i = 0;
          int32_t count = *(reinterpret_cast<int32_t*> (*data));
          int32_t tlen = common::INT_SIZE + count * common::INT64_SIZE;
          if ((*len) < tlen)
          {
            return common::TFS_ERROR;
          }
          uint64_t* int_array = reinterpret_cast<uint64_t*>((*data) + common::INT_SIZE);
          for (i = 0; i < count; i++)
          {
            value.push_back(int_array[i]);
          }
          (*len) -= tlen;
          (*data) += tlen;
          assert(*len >= 0);
          return common::TFS_SUCCESS;
        }

        inline int get_string_len(const char* value) const
        {
          if (value == NULL)
          {
            return common::INT_SIZE;
          }
          else
          {
            return strlen(value) + common::INT_SIZE + 1;
          }
        }

        inline int get_vint_len(const common::VUINT32& value) const
        {
          return value.size() * common::INT_SIZE + common::INT_SIZE;
        }

        inline int get_vint64_len(common::VUINT64& value) const
        {
          return value.size() * common::INT64_SIZE + common::INT_SIZE;
        }

        inline int set_int32(char** data, int32_t* len, int32_t status_)
        {
          if ((*len) < common::INT_SIZE)
          {
            return common::TFS_ERROR;
          }
          memcpy(*data, &status_, common::INT_SIZE);
          (*data) += common::INT_SIZE;
          (*len) -= common::INT_SIZE;
          assert(*len >= 0);
          return common::TFS_SUCCESS;
        }

        inline int set_int64(char** data, int32_t* len, int64_t value)
        {
          if ((*len) < common::INT64_SIZE)
          {
            return common::TFS_ERROR;
          }
          memcpy(*data, &value, common::INT64_SIZE);
          (*data) += common::INT64_SIZE;
          (*len) -= common::INT64_SIZE;
          assert(*len >= 0);
          return common::TFS_SUCCESS;
        }

        inline int set_string(char** data, int32_t* len, char* str_)
        {
          int32_t string_size = 0;
          if (str_ != NULL)
          {
            string_size = strlen(str_) + 1;
          }
          if ((*len) < (string_size + common::INT_SIZE))
          {
            return common::TFS_ERROR;
          }
          memcpy(*data, &string_size, common::INT_SIZE);
          if (string_size > 0)
          {
            memcpy((*data) + common::INT_SIZE, str_, string_size);
          }
          (*data) += (string_size + common::INT_SIZE);
          (*len) -= (string_size + common::INT_SIZE);
          assert(*len >= 0);
          return common::TFS_SUCCESS;
        }

        inline int set_object(char** data, int32_t* len, void* value, int32_t length)
        {
          if ((*len) < length)
          {
            return common::TFS_ERROR;
          }
          memcpy(*data, value, length);
          (*data) += length;
          (*len) -= length;
          assert(*len >= 0);
          return common::TFS_SUCCESS;
        }

        inline int32_t set_vint32(char** data, int32_t* len, common::VUINT32& value)
        {
          int32_t i = 0;
          int32_t count = value.size();
          if ((*len) < common::INT_SIZE + common::INT_SIZE * count)
          {
            return common::TFS_ERROR;
          }
          memcpy(*data, &count, common::INT_SIZE);
          (*data) += common::INT_SIZE;
          for (i = 0; i < count; i++)
          {
            memcpy(*data, &(value[i]), common::INT_SIZE);
            (*data) += common::INT_SIZE;
          }
          (*len) -= (common::INT_SIZE + common::INT_SIZE * count);
          assert(*len >= 0);
          return common::TFS_SUCCESS;
        }

        inline int set_vint64(char** data, int32_t* len, common::VUINT64& value)
        {
          int32_t i = 0;
          int32_t count = value.size();
          if ((*len) < common::INT_SIZE + common::INT64_SIZE * count)
          {
            return common::TFS_ERROR;
          }
          memcpy(*data, &count, common::INT_SIZE);
          (*data) += common::INT_SIZE;
          for (i = 0; i < count; i++)
          {
            memcpy(*data, &(value[i]), common::INT64_SIZE);
            (*data) += common::INT64_SIZE;
          }
          (*len) -= (common::INT_SIZE + common::INT64_SIZE * count);
          assert(*len >= 0);
          return common::TFS_SUCCESS;
        }

        // parse for version & lease
        // ds_: ds1 ds2 ds3 [flag=-1] [version] [leaseid]
        static bool parse_special_ds(common::VUINT64& ds, int32_t& version, int32_t& lease)
        {
          common::VUINT64::iterator it = find(ds.begin(), ds.end(), static_cast<uint64_t> (ULONG_LONG_MAX));
          common::VUINT64::iterator start = it;
          if (it != ds.end() && ((it + 2) < ds.end()))
          {
            version = static_cast<int32_t> (*(it + 1));
            lease = static_cast<int32_t> (*(it + 2));
            ds.erase(start, ds.end());
            return true;
          }
          return false;
        }
    };
  }
}
#endif //TFS_MESSAGE_MESSAGE_H_
