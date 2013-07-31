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
    using namespace tfs::common;

    // req_put_object_msg
    ReqKvMetaPutObjectMessage::ReqKvMetaPutObjectMessage()
    {
      _packetHeader._pcode = REQ_KVMETA_PUT_OBJECT_MESSAGE;
    }

    ReqKvMetaPutObjectMessage::~ReqKvMetaPutObjectMessage(){}

    int ReqKvMetaPutObjectMessage::serialize(Stream& output) const
    {
      int32_t iret = output.set_string(bucket_name_);

      if (TFS_SUCCESS == iret)
      {
        iret = output.set_string(file_name_);
      }

      if (TFS_SUCCESS == iret)
      {
        int64_t pos = 0;

        iret = object_info_.serialize(output.get_free(), output.get_free_length(), pos);
        if (TFS_SUCCESS == iret)
        {
          output.pour(object_info_.length());
        }
      }

      if (TFS_SUCCESS == iret)
      {
        int64_t pos = 0;

        iret = user_info_.serialize(output.get_free(), output.get_free_length(), pos);
        if (TFS_SUCCESS == iret)
        {
          output.pour(user_info_.length());
        }
      }

      return iret;
    }

    int ReqKvMetaPutObjectMessage::deserialize(Stream& input)
    {
      int32_t iret = input.get_string(bucket_name_);

      if (TFS_SUCCESS == iret)
      {
        iret = input.get_string(file_name_);
      }

      if (TFS_SUCCESS == iret)
      {
        int64_t pos = 0;
        iret = object_info_.deserialize(input.get_data(), input.get_data_length(), pos);
        if (TFS_SUCCESS == iret)
        {
          input.drain(object_info_.length());
        }
      }

      if (TFS_SUCCESS == iret)
      {
        int64_t pos = 0;
        iret = user_info_.deserialize(input.get_data(), input.get_data_length(), pos);
        if (TFS_SUCCESS == iret)
        {
          input.drain(user_info_.length());
        }
      }

      return iret;
    }

    int64_t ReqKvMetaPutObjectMessage::length() const
    {
      return Serialization::get_string_length(bucket_name_) +
        Serialization::get_string_length(file_name_) +
        object_info_.length() + user_info_.length();
    }

    //req_get_object_msg
    ReqKvMetaGetObjectMessage::ReqKvMetaGetObjectMessage()
    {
      _packetHeader._pcode = REQ_KVMETA_GET_OBJECT_MESSAGE;
    }
    ReqKvMetaGetObjectMessage::~ReqKvMetaGetObjectMessage(){}

    int ReqKvMetaGetObjectMessage::serialize(Stream& output) const
    {
      int32_t iret = output.set_string(bucket_name_);

      if (TFS_SUCCESS == iret)
      {
        iret = output.set_string(file_name_);
      }

      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int64(offset_);
      }

      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int64(length_);
      }

      if (TFS_SUCCESS == iret)
      {
        int64_t pos = 0;

        iret = user_info_.serialize(output.get_free(), output.get_free_length(), pos);
        if (TFS_SUCCESS == iret)
        {
          output.pour(user_info_.length());
        }
      }
      return iret;
    }

    int ReqKvMetaGetObjectMessage::deserialize(Stream& input)
    {
      int32_t iret = input.get_string(bucket_name_);

      if (TFS_SUCCESS == iret)
      {
        iret = input.get_string(file_name_);
      }

      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int64(&offset_);
      }

      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int64(&length_);
      }

      if (TFS_SUCCESS == iret)
      {
        int64_t pos = 0;
        iret = user_info_.deserialize(input.get_data(), input.get_data_length(), pos);
        if (TFS_SUCCESS == iret)
        {
          input.drain(user_info_.length());
        }
      }
      return iret;
    }

    int64_t ReqKvMetaGetObjectMessage::length() const
    {
      return Serialization::get_string_length(bucket_name_) + Serialization::get_string_length(file_name_)
                             + INT64_SIZE * 2 + user_info_.length();
    }

    // rsp_get_object_msg
    RspKvMetaGetObjectMessage::RspKvMetaGetObjectMessage()
    : still_have_(false)
    {
      _packetHeader._pcode = RSP_KVMETA_GET_OBJECT_MESSAGE;
    }
    RspKvMetaGetObjectMessage::~RspKvMetaGetObjectMessage(){}

    int RspKvMetaGetObjectMessage::serialize(Stream& output) const
    {
      int ret = TFS_ERROR;
      ret = output.set_int8(still_have_);
      if (TFS_SUCCESS == ret)
      {
        int64_t pos = 0;
        ret = object_info_.serialize(output.get_free(), output.get_free_length(), pos);
        if (TFS_SUCCESS == ret)
        {
          output.pour(object_info_.length());
        }
      }

      return ret;
    }

    int RspKvMetaGetObjectMessage::deserialize(Stream& input)
    {
      int ret = TFS_ERROR;
      ret = input.get_int8(reinterpret_cast<int8_t*>(&still_have_));

      if (TFS_SUCCESS == ret)
      {
        int64_t pos = 0;
        ret = object_info_.deserialize(input.get_data(), input.get_data_length(), pos);
        if (TFS_SUCCESS == ret)
        {
          input.drain(object_info_.length());
        }
      }

      return ret;
    }
    int64_t RspKvMetaGetObjectMessage::length() const
    {
      return INT8_SIZE + object_info_.length();
    }

    //req_del_object_msg
    ReqKvMetaDelObjectMessage::ReqKvMetaDelObjectMessage()
    {
      _packetHeader._pcode = REQ_KVMETA_DEL_OBJECT_MESSAGE;
    }
    ReqKvMetaDelObjectMessage::~ReqKvMetaDelObjectMessage(){}

    int ReqKvMetaDelObjectMessage::serialize(Stream& output) const
    {
      int32_t iret = output.set_string(bucket_name_);

      if (TFS_SUCCESS == iret)
      {
        iret = output.set_string(file_name_);
      }

      if (TFS_SUCCESS == iret)
      {
        int64_t pos = 0;

        iret = user_info_.serialize(output.get_free(), output.get_free_length(), pos);
        if (TFS_SUCCESS == iret)
        {
          output.pour(user_info_.length());
        }
      }

      return iret;
    }

    int ReqKvMetaDelObjectMessage::deserialize(Stream& input)
    {
      int32_t iret = input.get_string(bucket_name_);

      if (TFS_SUCCESS == iret)
      {
        iret = input.get_string(file_name_);
      }

      if (TFS_SUCCESS == iret)
      {
        int64_t pos = 0;
        iret = user_info_.deserialize(input.get_data(), input.get_data_length(), pos);
        if (TFS_SUCCESS == iret)
        {
          input.drain(user_info_.length());
        }
      }

      return iret;
    }

    int64_t ReqKvMetaDelObjectMessage::length() const
    {
      return Serialization::get_string_length(bucket_name_) + Serialization::get_string_length(file_name_) + user_info_.length();
    }

    // rsp_del_object_msg
    RspKvMetaDelObjectMessage::RspKvMetaDelObjectMessage()
    : still_have_(false)
    {
      _packetHeader._pcode = RSP_KVMETA_DEL_OBJECT_MESSAGE;
    }
    RspKvMetaDelObjectMessage::~RspKvMetaDelObjectMessage(){}

    int RspKvMetaDelObjectMessage::serialize(Stream& output) const
    {
      int ret = TFS_ERROR;
      ret = output.set_int8(still_have_);
      if (TFS_SUCCESS == ret)
      {
        int64_t pos = 0;
        ret = object_info_.serialize(output.get_free(), output.get_free_length(), pos);
        if (TFS_SUCCESS == ret)
        {
          output.pour(object_info_.length());
        }
      }

      return ret;
    }

    int RspKvMetaDelObjectMessage::deserialize(Stream& input)
    {
      int ret = TFS_ERROR;
      ret = input.get_int8(reinterpret_cast<int8_t*>(&still_have_));

      if (TFS_SUCCESS == ret)
      {
        int64_t pos = 0;
        ret = object_info_.deserialize(input.get_data(), input.get_data_length(), pos);
        if (TFS_SUCCESS == ret)
        {
          input.drain(object_info_.length());
        }
      }

      return ret;
    }
    int64_t RspKvMetaDelObjectMessage::length() const
    {
      return INT8_SIZE + object_info_.length();
    }

    //req_head_object_msg
    ReqKvMetaHeadObjectMessage::ReqKvMetaHeadObjectMessage()
    {
      _packetHeader._pcode = REQ_KVMETA_HEAD_OBJECT_MESSAGE;
    }
    ReqKvMetaHeadObjectMessage::~ReqKvMetaHeadObjectMessage(){}

    int ReqKvMetaHeadObjectMessage::serialize(Stream& output) const
    {
      int32_t iret = output.set_string(bucket_name_);

      if (TFS_SUCCESS == iret)
      {
        iret = output.set_string(file_name_);
      }

      if (TFS_SUCCESS == iret)
      {
        int64_t pos = 0;

        iret = user_info_.serialize(output.get_free(), output.get_free_length(), pos);
        if (TFS_SUCCESS == iret)
        {
          output.pour(user_info_.length());
        }
      }

      return iret;
    }

    int ReqKvMetaHeadObjectMessage::deserialize(Stream& input)
    {
      int32_t iret = input.get_string(bucket_name_);

      if (TFS_SUCCESS == iret)
      {
        iret = input.get_string(file_name_);
      }

      if (TFS_SUCCESS == iret)
      {
        int64_t pos = 0;
        iret = user_info_.deserialize(input.get_data(), input.get_data_length(), pos);
        if (TFS_SUCCESS == iret)
        {
          input.drain(user_info_.length());
        }
      }
      return iret;
    }

    int64_t ReqKvMetaHeadObjectMessage::length() const
    {
      return Serialization::get_string_length(bucket_name_) + Serialization::get_string_length(file_name_) + user_info_.length();
    }

     //rsp_head_object_msg
    RspKvMetaHeadObjectMessage::RspKvMetaHeadObjectMessage()
    {
      _packetHeader._pcode = RSP_KVMETA_HEAD_OBJECT_MESSAGE;
    }
    RspKvMetaHeadObjectMessage::~RspKvMetaHeadObjectMessage(){}

    int RspKvMetaHeadObjectMessage::serialize(Stream& output) const
    {
      int32_t iret = output.set_string(bucket_name_);

      if (TFS_SUCCESS == iret)
      {
        iret = output.set_string(file_name_);
      }

      if (TFS_SUCCESS == iret)
      {
        int64_t pos = 0;
        iret = object_info_.serialize(output.get_free(), output.get_free_length(), pos);
        if (common::TFS_SUCCESS == iret)
        {
          output.pour(object_info_.length());
        }
      }

      return iret;
    }

    int RspKvMetaHeadObjectMessage::deserialize(Stream& input)
    {
      int32_t iret = input.get_string(bucket_name_);

      if (TFS_SUCCESS == iret)
      {
        iret = input.get_string(file_name_);
      }

      if (TFS_SUCCESS == iret)
      {
        int64_t pos = 0;
        iret = object_info_.deserialize(input.get_data(), input.get_data_length(), pos);
        if (common::TFS_SUCCESS == iret)
        {
          input.drain(object_info_.length());
        }
      }

      return iret;
    }

    int64_t RspKvMetaHeadObjectMessage::length() const
    {
      return Serialization::get_string_length(bucket_name_)
        + Serialization::get_string_length(file_name_)
        + object_info_.length();
    }

//-------------------------------------bucket part ---------------------------------------------

    //req_put_bucket_msg
    ReqKvMetaPutBucketMessage::ReqKvMetaPutBucketMessage()
    {
      _packetHeader._pcode = REQ_KVMETA_PUT_BUCKET_MESSAGE;
    }

    ReqKvMetaPutBucketMessage::~ReqKvMetaPutBucketMessage(){}

    int ReqKvMetaPutBucketMessage::serialize(Stream& output) const
    {
      int32_t iret = output.set_string(bucket_name_);

      if (common::TFS_SUCCESS == iret)
      {
        int64_t pos = 0;
        iret = bucket_meta_info_.serialize(output.get_free(), output.get_free_length(), pos);
        if (common::TFS_SUCCESS == iret)
        {
          output.pour(bucket_meta_info_.length());
        }
      }

      if (TFS_SUCCESS == iret)
      {
        int64_t pos = 0;

        iret = user_info_.serialize(output.get_free(), output.get_free_length(), pos);
        if (TFS_SUCCESS == iret)
        {
          output.pour(user_info_.length());
        }
      }

      return iret;
    }

    int ReqKvMetaPutBucketMessage::deserialize(Stream& input)
    {
      int32_t iret = input.get_string(bucket_name_);

      if (common::TFS_SUCCESS == iret)
      {
        int64_t pos = 0;
        iret = bucket_meta_info_.deserialize(input.get_data(), input.get_data_length(), pos);
        if (common::TFS_SUCCESS == iret)
        {
          input.drain(bucket_meta_info_.length());
        }
      }

      if (TFS_SUCCESS == iret)
      {
        int64_t pos = 0;
        iret = user_info_.deserialize(input.get_data(), input.get_data_length(), pos);
        if (TFS_SUCCESS == iret)
        {
          input.drain(user_info_.length());
        }
      }

      return iret;
    }

    int64_t ReqKvMetaPutBucketMessage::length() const
    {
      return common::Serialization::get_string_length(bucket_name_)
           + bucket_meta_info_.length() + user_info_.length();
    }

    //req_get_bucket_msg
    ReqKvMetaGetBucketMessage::ReqKvMetaGetBucketMessage()
    {
      _packetHeader._pcode = REQ_KVMETA_GET_BUCKET_MESSAGE;
    }

    ReqKvMetaGetBucketMessage::~ReqKvMetaGetBucketMessage(){}

    int ReqKvMetaGetBucketMessage::serialize(Stream& output) const
    {
      int32_t iret = output.set_string(bucket_name_);

      if (TFS_SUCCESS == iret)
      {
        iret = output.set_string(prefix_);
      }

      if (TFS_SUCCESS == iret)
      {
        iret = output.set_string(start_key_);
      }

      if (TFS_SUCCESS == iret)
      {
        iret = output.set_int32(limit_);
      }

      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int8(delimiter_);
      }

      if (TFS_SUCCESS == iret)
      {
        int64_t pos = 0;

        iret = user_info_.serialize(output.get_free(), output.get_free_length(), pos);
        if (TFS_SUCCESS == iret)
        {
          output.pour(user_info_.length());
        }
      }

      return iret;
    }

    int ReqKvMetaGetBucketMessage::deserialize(Stream& input)
    {
      int32_t iret = input.get_string(bucket_name_);

      if (TFS_SUCCESS == iret)
      {
        iret = input.get_string(prefix_);
      }

      if (TFS_SUCCESS == iret)
      {
        iret = input.get_string(start_key_);
      }

      if (TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&limit_);
      }

      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int8(reinterpret_cast<int8_t*>(&delimiter_));
      }

      if (TFS_SUCCESS == iret)
      {
        int64_t pos = 0;
        iret = user_info_.deserialize(input.get_data(), input.get_data_length(), pos);
        if (TFS_SUCCESS == iret)
        {
          input.drain(user_info_.length());
        }
      }
      return iret;
    }

    int64_t ReqKvMetaGetBucketMessage::length() const
    {
      return common::Serialization::get_string_length(bucket_name_)
        + common::Serialization::get_string_length(prefix_)
        + common::Serialization::get_string_length(start_key_)
        + common::INT_SIZE + common::INT8_SIZE + user_info_.length();
    }

    //rsp_get_bucket_msg
    RspKvMetaGetBucketMessage::RspKvMetaGetBucketMessage()
    {
      _packetHeader._pcode = RSP_KVMETA_GET_BUCKET_MESSAGE;
    }

    RspKvMetaGetBucketMessage::~RspKvMetaGetBucketMessage(){}

    int RspKvMetaGetBucketMessage::serialize(Stream& output) const
    {
      int32_t iret = output.set_string(bucket_name_);

      if (TFS_SUCCESS == iret)
      {
        iret = output.set_string(prefix_);
      }

      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_string(start_key_);
      }

      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_sstring(s_common_prefix_);
      }

      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_vstring(v_object_name_);
      }

      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(v_object_meta_info_.size());
      }

      if (common::TFS_SUCCESS == iret)
      {
        for (size_t i = 0; common::TFS_SUCCESS == iret && i < v_object_meta_info_.size(); i++)
        {
          int64_t pos = 0;
          iret = v_object_meta_info_[i].serialize(output.get_free(), output.get_free_length(), pos);
          if (common::TFS_SUCCESS == iret)
          {
            output.pour(v_object_meta_info_[i].length());
          }
        }
      }

      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(limit_);
      }

      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int8(is_truncated);
      }

      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int8(delimiter_);
      }

      return iret;
    }

    int RspKvMetaGetBucketMessage::deserialize(Stream& input)
    {
      int32_t iret = input.get_string(bucket_name_);

      if (TFS_SUCCESS == iret)
      {
        iret = input.get_string(prefix_);
      }

      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_string(start_key_);
      }

      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_sstring(s_common_prefix_);
      }

      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_vstring(v_object_name_);
      }

      int32_t obj_size = -1;
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&obj_size);
      }

      if (common::TFS_SUCCESS == iret)
      {
        ObjectMetaInfo object_meta_info;
        for (int i = 0; common::TFS_SUCCESS == iret && i < obj_size; i++)
        {
          int64_t pos = 0;
          iret = object_meta_info.deserialize(input.get_data(), input.get_data_length(), pos);
          if (common::TFS_SUCCESS == iret)
          {
            v_object_meta_info_.push_back(object_meta_info);
            input.drain(object_meta_info.length());
          }
        }
      }

      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&limit_);
      }

      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int8(&is_truncated);
      }

      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int8(reinterpret_cast<int8_t*>(&delimiter_));
      }

      return iret;
    }

    int64_t RspKvMetaGetBucketMessage::length() const
    {
      int64_t len = 0;
      len = common::Serialization::get_string_length(bucket_name_)
        + common::Serialization::get_string_length(prefix_)
        + common::Serialization::get_string_length(start_key_)
        + common::Serialization::get_sstring_length(s_common_prefix_)
        + common::Serialization::get_vstring_length(v_object_name_)
        + common::INT_SIZE + common::INT8_SIZE + common::INT8_SIZE
        + common::INT_SIZE;

      for (size_t i = 0; i < v_object_meta_info_.size(); i++)
      {
        len +=  v_object_meta_info_[i].length();
      }

      return len;
    }

    //req_del_bucket_msg
    ReqKvMetaDelBucketMessage::ReqKvMetaDelBucketMessage()
    {
      _packetHeader._pcode = REQ_KVMETA_DEL_BUCKET_MESSAGE;
    }

    ReqKvMetaDelBucketMessage::~ReqKvMetaDelBucketMessage(){}

    int ReqKvMetaDelBucketMessage::serialize(Stream& output) const
    {
      int32_t iret = output.set_string(bucket_name_);
      if (TFS_SUCCESS == iret)
      {
        int64_t pos = 0;

        iret = user_info_.serialize(output.get_free(), output.get_free_length(), pos);
        if (TFS_SUCCESS == iret)
        {
          output.pour(user_info_.length());
        }
      }
      return iret;
    }

    int ReqKvMetaDelBucketMessage::deserialize(Stream& input)
    {
      int32_t iret = input.get_string(bucket_name_);
      if (TFS_SUCCESS == iret)
      {
        int64_t pos = 0;
        iret = user_info_.deserialize(input.get_data(), input.get_data_length(), pos);
        if (TFS_SUCCESS == iret)
        {
          input.drain(user_info_.length());
        }
      }
      return iret;
    }

    int64_t ReqKvMetaDelBucketMessage::length() const
    {
      return Serialization::get_string_length(bucket_name_) + user_info_.length();
    }

    //req_head_bucket_msg
    ReqKvMetaHeadBucketMessage::ReqKvMetaHeadBucketMessage()
    {
      _packetHeader._pcode = REQ_KVMETA_HEAD_BUCKET_MESSAGE;
    }

    ReqKvMetaHeadBucketMessage::~ReqKvMetaHeadBucketMessage(){}

    int ReqKvMetaHeadBucketMessage::serialize(Stream& output) const
    {
      int32_t iret = output.set_string(bucket_name_);
      if (TFS_SUCCESS == iret)
      {
        int64_t pos = 0;

        iret = user_info_.serialize(output.get_free(), output.get_free_length(), pos);
        if (TFS_SUCCESS == iret)
        {
          output.pour(user_info_.length());
        }
      }

      return iret;
    }

    int ReqKvMetaHeadBucketMessage::deserialize(Stream& input)
    {
      int32_t iret = input.get_string(bucket_name_);
      if (TFS_SUCCESS == iret)
      {
        int64_t pos = 0;
        iret = user_info_.deserialize(input.get_data(), input.get_data_length(), pos);
        if (TFS_SUCCESS == iret)
        {
          input.drain(user_info_.length());
        }
      }
      return iret;
    }

    int64_t ReqKvMetaHeadBucketMessage::length() const
    {
      return Serialization::get_string_length(bucket_name_) + user_info_.length();
    }

    //rsp_head_bucket_msg
    RspKvMetaHeadBucketMessage::RspKvMetaHeadBucketMessage()
    {
      _packetHeader._pcode = RSP_KVMETA_HEAD_BUCKET_MESSAGE;
    }

    RspKvMetaHeadBucketMessage::~RspKvMetaHeadBucketMessage(){}

    int RspKvMetaHeadBucketMessage::serialize(Stream& output) const
    {
      int32_t iret = output.set_string(bucket_name_);

      if (TFS_SUCCESS == iret)
      {
        int64_t pos = 0;
        iret = bucket_meta_info_.serialize(output.get_free(), output.get_free_length(), pos);
        if (common::TFS_SUCCESS == iret)
        {
          output.pour(bucket_meta_info_.length());
        }
      }
      return iret;
    }

    int RspKvMetaHeadBucketMessage::deserialize(Stream& input)
    {
      int32_t iret = input.get_string(bucket_name_);

      if (TFS_SUCCESS == iret)
      {
        int64_t pos = 0;
        iret = bucket_meta_info_.deserialize(input.get_data(), input.get_data_length(), pos);
        if (common::TFS_SUCCESS == iret)
        {
          input.drain(bucket_meta_info_.length());
        }
      }
      return iret;
    }

    int64_t RspKvMetaHeadBucketMessage::length() const
    {
      return Serialization::get_string_length(bucket_name_) + bucket_meta_info_.length();
    }

  }
}
