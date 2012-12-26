/*
* (C) 2007-2011 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
*
* Version: $Id
*
* Authors:
*   xueya.yy <xueya.yy@taobao.com>
*      - initial release
*
*/

#include "kv_meta_message.h"
#include "common/stream.h"

namespace tfs
{
  namespace message
  {
    // req_put_meta_msg
    KvReqPutMetaMessage::KvReqPutMetaMessage()
    {
      _packetHeader._pcode = common::KV_REQ_PUT_META_MESSAGE;
    }

    KvReqPutMetaMessage::~KvReqPutMetaMessage(){}

    int KvReqPutMetaMessage::serialize(common::Stream& output) const
    {
      int32_t iret = output.set_string(bucket_name_);

      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_string(file_name_);
      }

      if (common::TFS_SUCCESS == iret)
      {
        int64_t pos = 0;
        /*if (output.get_free_length() < tfs_file_info_.length())
        {
          output.expand(tfs_file_info_.length());
        }*/
        iret = tfs_file_info_.serialize(output.get_free(), output.get_free_length(), pos);
        if (common::TFS_SUCCESS == iret)
        {
          output.pour(tfs_file_info_.length());
        }

      }
      return iret;
    }

    int KvReqPutMetaMessage::deserialize(common::Stream& input)
    {
      int32_t iret = input.get_string(bucket_name_);

      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_string(file_name_);
      }

      if (common::TFS_SUCCESS == iret)
      {
        int64_t pos = 0;
        iret = tfs_file_info_.deserialize(input.get_data(), input.get_data_length(), pos);
        if (common::TFS_SUCCESS == iret)
        {
          input.drain(tfs_file_info_.length());
        }
      }

      return iret;
    }

    int64_t KvReqPutMetaMessage::length() const
    {
      return common::Serialization::get_string_length(bucket_name_) + common::Serialization::get_string_length(file_name_) + tfs_file_info_.length();
    }

    //req_get_meta_msg
    KvReqGetMetaMessage::KvReqGetMetaMessage()
    {
      _packetHeader._pcode = common::KV_REQ_GET_META_MESSAGE;
    }
    KvReqGetMetaMessage::~KvReqGetMetaMessage(){}

    int KvReqGetMetaMessage::serialize(common::Stream& output) const
    {
      int32_t iret = output.set_string(bucket_name_);

      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_string(file_name_);
      }

      return iret;
    }

    int KvReqGetMetaMessage::deserialize(common::Stream& input)
    {
      int32_t iret = input.get_string(bucket_name_);

      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_string(file_name_);
      }

      return iret;
    }

    int64_t KvReqGetMetaMessage::length() const
    {
      return common::Serialization::get_string_length(bucket_name_) + common::Serialization::get_string_length(file_name_);
    }

    // rsp_get_meta_msg
    KvRspGetMetaMessage::KvRspGetMetaMessage()
    {
      _packetHeader._pcode = common::KV_RSP_GET_META_MESSAGE;
    }
    KvRspGetMetaMessage::~KvRspGetMetaMessage(){}

    int KvRspGetMetaMessage::serialize(common::Stream& output) const
    {
      int64_t pos = 0;
      /*if (output.get_free_length() < tfs_file_info_.length())
      {
        output.expand(tfs_file_info_.length());
      }*/
      int32_t iret = tfs_file_info_.serialize(output.get_free(), output.get_free_length(), pos);
      if (common::TFS_SUCCESS == iret)
      {
        output.pour(tfs_file_info_.length());
      }

      return iret;
    }

    int KvRspGetMetaMessage::deserialize(common::Stream& input)
    {
      int64_t pos = 0;
      int32_t iret = tfs_file_info_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (common::TFS_SUCCESS == iret)
      {
        input.drain(tfs_file_info_.length());
      }

      return iret;
    }

    int64_t KvRspGetMetaMessage::length() const
    {
      return tfs_file_info_.length();
    }

    //req_put_bucket_msg
    KvReqPutBucketMessage::KvReqPutBucketMessage()
    {
      _packetHeader._pcode = common::KV_REQ_PUT_BUCKET_MESSAGE;
    }

    KvReqPutBucketMessage::~KvReqPutBucketMessage(){}

    int KvReqPutBucketMessage::serialize(common::Stream& output) const
    {
      int32_t iret = output.set_string(bucket_name_);

      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int64(create_time_);
      }

      return iret;
    }

    int KvReqPutBucketMessage::deserialize(common::Stream& input)
    {
      int32_t iret = input.get_string(bucket_name_);

      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int64(&create_time_);
      }

      return iret;
    }

    int64_t KvReqPutBucketMessage::length() const
    {
      return common::Serialization::get_string_length(bucket_name_) + common::INT64_SIZE;
    }

  }
}
