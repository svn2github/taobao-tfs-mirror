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
    ReqKvMetaPutObjectMessage::ReqKvMetaPutObjectMessage()
    {
      _packetHeader._pcode = common::REQ_KVMETA_PUT_OBJECT_MESSAGE;
    }

    ReqKvMetaPutObjectMessage::~ReqKvMetaPutObjectMessage(){}

    int ReqKvMetaPutObjectMessage::serialize(common::Stream& output) const
    {
      int32_t iret = output.set_string(bucket_name_);

      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_string(file_name_);
      }

      if (common::TFS_SUCCESS == iret)
      {
        int64_t pos = 0;
        iret = tfs_file_info_.serialize(output.get_free(), output.get_free_length(), pos);
        if (common::TFS_SUCCESS == iret)
        {
          output.pour(tfs_file_info_.length());
        }

      }
      return iret;
    }

    int ReqKvMetaPutObjectMessage::deserialize(common::Stream& input)
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

    int64_t ReqKvMetaPutObjectMessage::length() const
    {
      return common::Serialization::get_string_length(bucket_name_) + common::Serialization::get_string_length(file_name_) + tfs_file_info_.length();
    }

    //req_get_meta_msg
    ReqKvMetaGetObjectMessage::ReqKvMetaGetObjectMessage()
    {
      _packetHeader._pcode = common::REQ_KVMETA_GET_OBJECT_MESSAGE;
    }
    ReqKvMetaGetObjectMessage::~ReqKvMetaGetObjectMessage(){}

    int ReqKvMetaGetObjectMessage::serialize(common::Stream& output) const
    {
      int32_t iret = output.set_string(bucket_name_);

      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_string(file_name_);
      }

      return iret;
    }

    int ReqKvMetaGetObjectMessage::deserialize(common::Stream& input)
    {
      int32_t iret = input.get_string(bucket_name_);

      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_string(file_name_);
      }

      return iret;
    }

    int64_t ReqKvMetaGetObjectMessage::length() const
    {
      return common::Serialization::get_string_length(bucket_name_) + common::Serialization::get_string_length(file_name_);
    }

    // rsp_get_meta_msg
    RspKvMetaGetObjectMessage::RspKvMetaGetObjectMessage()
    {
      _packetHeader._pcode = common::RSP_KVMETA_GET_OBJECT_MESSAGE;
    }
    RspKvMetaGetObjectMessage::~RspKvMetaGetObjectMessage(){}

    int RspKvMetaGetObjectMessage::serialize(common::Stream& output) const
    {
      int64_t pos = 0;
      int32_t iret = tfs_file_info_.serialize(output.get_free(), output.get_free_length(), pos);
      if (common::TFS_SUCCESS == iret)
      {
        output.pour(tfs_file_info_.length());
      }

      return iret;
    }

    int RspKvMetaGetObjectMessage::deserialize(common::Stream& input)
    {
      int64_t pos = 0;
      int32_t iret = tfs_file_info_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (common::TFS_SUCCESS == iret)
      {
        input.drain(tfs_file_info_.length());
      }

      return iret;
    }

    int64_t RspKvMetaGetObjectMessage::length() const
    {
      return tfs_file_info_.length();
    }

  }
}
