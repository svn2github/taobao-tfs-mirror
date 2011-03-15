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
#include "write_data_message.h"

using namespace tfs::common;

namespace tfs
{
  namespace message
  {
    WriteDataMessage::WriteDataMessage() :
      data_(NULL), version_(0), lease_(0), has_lease_(false)
    {
      _packetHeader._pcode = WRITE_DATA_MESSAGE;
      memset(&write_data_info_, 0, sizeof(WriteDataInfo));
      ds_.clear();
    }

    WriteDataMessage::~WriteDataMessage()
    {
    }

    int WriteDataMessage::parse(char* data, int32_t len)
    {
      if (get_object_copy(&data, &len, reinterpret_cast<void*>(&write_data_info_), sizeof(WriteDataInfo)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_vint64(&data, &len, ds_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (write_data_info_.length_ > 0)
      {
        if (get_object(&data, &len, reinterpret_cast<void**>(&data_), write_data_info_.length_) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }

      has_lease_ = parse_special_ds(ds_, version_, lease_);

      return TFS_SUCCESS;
    }

    int32_t WriteDataMessage::message_length()
    {
      int32_t len = sizeof(WriteDataInfo) + get_vint64_len(ds_);
      if (write_data_info_.length_ > 0)
      {
        len += write_data_info_.length_;
      }
      if (has_lease_ == true)
      {
        len += INT64_SIZE * 3;
      }
      return len;
    }

    int WriteDataMessage::build(char* data, int32_t len)
    {
      if (has_lease_ == true)
      {
        ds_.push_back(ULONG_LONG_MAX);
        ds_.push_back(static_cast<uint64_t>(version_));
        ds_.push_back(static_cast<uint64_t>(lease_));
      }

      if (set_object(&data, &len, &write_data_info_, sizeof(WriteDataInfo)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_vint64(&data, &len, ds_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (write_data_info_.length_ > 0)
      {
        if (set_object(&data, &len, data_, write_data_info_.length_) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }

      // reparse, avoid push verion&lease again when clone twice;
      has_lease_ = parse_special_ds(ds_, version_, lease_);
      return TFS_SUCCESS;
    }

    char* WriteDataMessage::get_name()
    {
      return "writedatamessage";
    }

    Message* WriteDataMessage::create(const int32_t type)
    {
      WriteDataMessage* req_wd_msg = new WriteDataMessage();
      req_wd_msg->set_message_type(type);
      return req_wd_msg;
    }

#ifdef _DEL_001_
    RespWriteDataMessage::RespWriteDataMessage():
      length_(0)
    {
      _packetHeader._pcode = RESP_WRITE_DATA_MESSAGE;
    }

    RespWriteDataMessage::~RespWriteDataMessage()
    {
    }

    int RespWriteDataMessage::parse(char* data, int32_t len)
    {
      if (get_int32(&data, &len, &length_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      return TFS_SUCCESS;
    }

    int32_t RespWriteDataMessage::message_length()
    {
      int32_t len = INT_SIZE;
      return len;
    }

    int RespWriteDataMessage::build(char* data, int32_t len)
    {
      if (set_int32(&data, &len, length_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      return TFS_SUCCESS;
    }

    char* RespWriteDataMessage::get_name()
    {
      return "respwritedatamessage";
    }

    Message* RespWriteDataMessage::create(const int32_t type)
    {
      RespWriteDataMessage* resp_wd_msg = new RespWriteDataMessage();
      resp_wd_msg->set_message_type(type);
      return resp_wd_msg;
    }
#endif

    WriteRawDataMessage::WriteRawDataMessage() :
      data_(NULL), flag_(0)
    {
      _packetHeader._pcode = WRITE_RAW_DATA_MESSAGE;
      memset(&write_data_info_, 0, sizeof(WriteDataInfo));
    }

    WriteRawDataMessage::~WriteRawDataMessage()
    {
    }

    int WriteRawDataMessage::parse(char* data, int32_t len)
    {
      if (get_object_copy(&data, &len, reinterpret_cast<void*>(&write_data_info_), sizeof(WriteDataInfo)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      if (write_data_info_.length_ > 0)
      {
        if (get_object(&data, &len, reinterpret_cast<void**>(&data_), write_data_info_.length_) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }

      if (get_int32(&data, &len, &flag_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      return TFS_SUCCESS;
    }

    int32_t WriteRawDataMessage::message_length()
    {
      int32_t len = sizeof(WriteDataInfo) + sizeof(int32_t);
      if (write_data_info_.length_ > 0)
      {
        len += write_data_info_.length_;
      }
      return len;
    }

    int WriteRawDataMessage::build(char* data, int32_t len)
    {
      if (set_object(&data, &len, &write_data_info_, sizeof(WriteDataInfo)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      if (write_data_info_.length_ > 0)
      {
        if (set_object(&data, &len, data_, write_data_info_.length_) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }

      if (set_int32(&data, &len, flag_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      return TFS_SUCCESS;
    }

    char* WriteRawDataMessage::get_name()
    {
      return "writedatabatchmessage";
    }

    Message* WriteRawDataMessage::create(const int32_t type)
    {
      WriteRawDataMessage* req_wrd_msg = new WriteRawDataMessage();
      req_wrd_msg->set_message_type(type);
      return req_wrd_msg;
    }

    WriteInfoBatchMessage::WriteInfoBatchMessage() :
      block_info_(NULL), cluster_(0)
    {
      _packetHeader._pcode = WRITE_INFO_BATCH_MESSAGE;
      memset(&write_data_info_, 0, sizeof(WriteDataInfo));
      meta_list_.clear();
    }

    WriteInfoBatchMessage::~WriteInfoBatchMessage()
    {
    }

    int WriteInfoBatchMessage::parse(char* data, int32_t len)
    {
      if (get_object_copy(&data, &len, reinterpret_cast<void*>(&write_data_info_), sizeof(WriteDataInfo)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      int32_t have_block = 0;
      if (get_int32(&data, &len, &have_block) == TFS_ERROR)
      {
        have_block = 0;
      }

      if (have_block > 0)
      {
        if (get_object(&data, &len, reinterpret_cast<void**>(&block_info_), BLOCKINFO_SIZE) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }

      int32_t size = 0;
      if (get_int32(&data, &len, &size) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      int32_t i = 0;
      for (i = 0; i < size; i++)
      {
        RawMeta raw_meta;
        if (get_object_copy(&data, &len, reinterpret_cast<void*> (&raw_meta), RAW_META_SIZE) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
        meta_list_.push_back(raw_meta);
      }

      if (get_int32(&data, &len, &cluster_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      return TFS_SUCCESS;
    }

    int32_t WriteInfoBatchMessage::message_length()
    {
      int32_t len = sizeof(WriteDataInfo) + 2 * INT_SIZE;

      if (block_info_ != NULL)
      {
        len += BLOCKINFO_SIZE;
      }

      len += INT_SIZE;
      len += meta_list_.size() * RAW_META_SIZE;
      return len;
    }

    int WriteInfoBatchMessage::build(char* data, int32_t len)
    {
      if (set_object(&data, &len, &write_data_info_, sizeof(WriteDataInfo)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      int32_t have_block = 0;
      if (block_info_ != NULL)
      {
        have_block = 1;
      }

      if (set_int32(&data, &len, have_block) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (block_info_ != NULL)
      {
        if (set_object(&data, &len, block_info_, BLOCKINFO_SIZE) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }

      int32_t size = meta_list_.size();
      if (set_int32(&data, &len, size) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      int32_t i = 0;
      for (i = 0; i < size; i++)
      {
        if (set_object(&data, &len, &(meta_list_.at(i)), RAW_META_SIZE) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }

      if (set_int32(&data, &len, cluster_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      return TFS_SUCCESS;
    }

    char* WriteInfoBatchMessage::get_name()
    {
      return "writeinfobatchmessage";
    }

    Message* WriteInfoBatchMessage::create(const int32_t type)
    {
      WriteInfoBatchMessage* req_wib_msg = new WriteInfoBatchMessage();
      req_wib_msg->set_message_type(type);
      return req_wib_msg;
    }
  }
}
