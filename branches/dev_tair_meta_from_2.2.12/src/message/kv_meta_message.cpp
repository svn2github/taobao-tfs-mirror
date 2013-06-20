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

    //req_get_service_msg
    ReqKvMetaGetServiceMessage::ReqKvMetaGetServiceMessage()
    {
      _packetHeader._pcode = REQ_KVMETA_GET_SERVICE_MESSAGE;
    }

    ReqKvMetaGetServiceMessage::~ReqKvMetaGetServiceMessage(){}

    int ReqKvMetaGetServiceMessage::serialize(Stream& output) const
    {
      int32_t iret = TFS_SUCCESS;

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

    int ReqKvMetaGetServiceMessage::deserialize(Stream& input)
    {
      int32_t iret = TFS_SUCCESS;

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

    int64_t ReqKvMetaGetServiceMessage::length() const
    {
      return user_info_.length();
    }

    //rsp_get_service_msg
    RspKvMetaGetServiceMessage::RspKvMetaGetServiceMessage()
    {
      _packetHeader._pcode = RSP_KVMETA_GET_SERVICE_MESSAGE;
    }

    RspKvMetaGetServiceMessage::~RspKvMetaGetServiceMessage(){}

    int RspKvMetaGetServiceMessage::serialize(Stream& output) const
    {
      int32_t iret = TFS_SUCCESS;

      if (TFS_SUCCESS == iret)
      {
        int64_t pos = 0;

        iret = buckets_result_.serialize(output.get_free(), output.get_free_length(), pos);
        if (TFS_SUCCESS == iret)
        {
          output.pour(buckets_result_.length());
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

    int RspKvMetaGetServiceMessage::deserialize(Stream& input)
    {
      int32_t iret = TFS_SUCCESS;

      if (TFS_SUCCESS == iret)
      {
        int64_t pos = 0;
        iret = buckets_result_.deserialize(input.get_data(), input.get_data_length(), pos);
        if (TFS_SUCCESS == iret)
        {
          input.drain(buckets_result_.length());
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

    int64_t RspKvMetaGetServiceMessage::length() const
    {
      return buckets_result_.length() + user_info_.length();
    }

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

    //req_list_multipart_object
    ReqKvMetaListMultipartObjectMessage::ReqKvMetaListMultipartObjectMessage()
    {
      _packetHeader._pcode = REQ_KVMETA_LIST_MULTIPART_OBJECT_MESSAGE;
    }

    ReqKvMetaListMultipartObjectMessage::~ReqKvMetaListMultipartObjectMessage(){}

    int ReqKvMetaListMultipartObjectMessage::serialize(Stream& output) const
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
        iret = output.set_string(start_id_);
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

    int ReqKvMetaListMultipartObjectMessage::deserialize(Stream& input)
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
        iret = input.get_string(start_id_);
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

    int64_t ReqKvMetaListMultipartObjectMessage::length() const
    {
      return common::Serialization::get_string_length(bucket_name_)
        + common::Serialization::get_string_length(prefix_)
        + common::Serialization::get_string_length(start_key_)
        + common::Serialization::get_string_length(start_id_)
        + common::INT_SIZE + common::INT8_SIZE + user_info_.length();
    }

    RspKvMetaListMultipartObjectMessage::RspKvMetaListMultipartObjectMessage()
    {
      _packetHeader._pcode = RSP_KVMETA_LIST_MULTIPART_OBJECT_MESSAGE;
    }

    RspKvMetaListMultipartObjectMessage::~RspKvMetaListMultipartObjectMessage(){}

    int RspKvMetaListMultipartObjectMessage::serialize(Stream& output) const
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
        iret = output.set_string(start_id_);
      }

      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_string(next_start_key_);
      }

      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_string(next_start_id_);
      }

      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_sstring(s_common_prefix_);
      }

      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(v_object_upload_info_.size());
      }

      if (common::TFS_SUCCESS == iret)
      {
        for (size_t i = 0; common::TFS_SUCCESS == iret && i < v_object_upload_info_.size(); i++)
        {
          int64_t pos = 0;
          iret = v_object_upload_info_[i].serialize(output.get_free(), output.get_free_length(), pos);
          if (common::TFS_SUCCESS == iret)
          {
            output.pour(v_object_upload_info_[i].length());
          }
        }
      }

      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(limit_);
      }

      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int8(is_truncated_);
      }

      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int8(delimiter_);
      }

      return iret;
    }

    int RspKvMetaListMultipartObjectMessage::deserialize(Stream& input)
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
        iret = input.get_string(start_id_);
      }

      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_string(next_start_key_);
      }

      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_string(next_start_id_);
      }

      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_sstring(s_common_prefix_);
      }

      int32_t obj_upload_size = -1;
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&obj_upload_size);
      }

      if (common::TFS_SUCCESS == iret)
      {
        ObjectUploadInfo object_upload_info;
        for (int i = 0; common::TFS_SUCCESS == iret && i < obj_upload_size; i++)
        {
          int64_t pos = 0;
          iret = object_upload_info.deserialize(input.get_data(), input.get_data_length(), pos);
          if (common::TFS_SUCCESS == iret)
          {
            v_object_upload_info_.push_back(object_upload_info);
            input.drain(object_upload_info.length());
          }
        }
      }

      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&limit_);
      }

      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int8(reinterpret_cast<int8_t*>(&is_truncated_));
      }

      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int8(reinterpret_cast<int8_t*>(&delimiter_));
      }

      return iret;
    }

    int64_t RspKvMetaListMultipartObjectMessage::length() const
    {
      int64_t len = 0;
      len = common::Serialization::get_string_length(bucket_name_)
        + common::Serialization::get_string_length(prefix_)
        + common::Serialization::get_string_length(start_key_)
        + common::Serialization::get_string_length(start_id_)
        + common::Serialization::get_string_length(next_start_key_)
        + common::Serialization::get_string_length(next_start_id_)
        + common::Serialization::get_sstring_length(s_common_prefix_)
        + common::INT_SIZE + common::INT8_SIZE + common::INT8_SIZE
        + common::INT_SIZE;

      for (size_t i = 0; i < v_object_upload_info_.size(); i++)
      {
        len +=  v_object_upload_info_[i].length();
      }

      return len;
    }

    //about tag
    //put bucket tag
    ReqKvMetaPutBucketTagMessage::ReqKvMetaPutBucketTagMessage()
    {
      _packetHeader._pcode = REQ_KVMETA_PUT_BUCKET_TAG_MESSAGE;
    }

    ReqKvMetaPutBucketTagMessage::~ReqKvMetaPutBucketTagMessage(){}

    int ReqKvMetaPutBucketTagMessage::serialize(Stream& output) const
    {
      int32_t iret = output.set_string(bucket_name_);

      if (common::TFS_SUCCESS == iret)
      {
        int32_t size = bucket_tag_map_.size();
        iret = output.set_int32(size);
      }

      if (common::TFS_SUCCESS == iret)
      {
        MAP_STRING_ITER iter = bucket_tag_map_.begin();
        for (; iter != bucket_tag_map_.end() && common::TFS_SUCCESS == iret; iter++)
        {
          iret = output.set_string(iter->first);
          if (common::TFS_SUCCESS == iret)
          {
            iret = output.set_string(iter->second);
          }
        }
      }

      return iret;
    }

    int ReqKvMetaPutBucketTagMessage::deserialize(Stream& input)
    {
      int32_t iret = input.get_string(bucket_name_);

      int32_t size = -1;

      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&size);
      }

      if (common::TFS_SUCCESS == iret)
      {
        std::string key;
        std::string value;
        for (int32_t i = 0; i < size && common::TFS_SUCCESS == iret; i++)
        {
          iret = input.get_string(key);
          if (common::TFS_SUCCESS == iret)
          {
            iret = input.get_string(value);
          }

          if (common::TFS_SUCCESS == iret)
          {
            bucket_tag_map_.insert(std::make_pair(key, value));
          }
        }
      }

      return iret;
    }

    int64_t ReqKvMetaPutBucketTagMessage::length() const
    {
      int64_t len = common::Serialization::get_string_length(bucket_name_);
      len += INT_SIZE;

      MAP_STRING_ITER iter = bucket_tag_map_.begin();
      for (; iter != bucket_tag_map_.end(); iter++)
      {
        len += common::Serialization::get_string_length(iter->first);
        len += common::Serialization::get_string_length(iter->second);
      }
      return len;
    }

    //get bucket tag
    ReqKvMetaGetBucketTagMessage::ReqKvMetaGetBucketTagMessage()
    {
      _packetHeader._pcode = REQ_KVMETA_GET_BUCKET_TAG_MESSAGE;
    }

    ReqKvMetaGetBucketTagMessage::~ReqKvMetaGetBucketTagMessage(){}

    int ReqKvMetaGetBucketTagMessage::serialize(Stream& output) const
    {
      int32_t iret = output.set_string(bucket_name_);

      return iret;
    }

    int ReqKvMetaGetBucketTagMessage::deserialize(Stream& input)
    {
      int32_t iret = input.get_string(bucket_name_);

      return iret;
    }

    int64_t ReqKvMetaGetBucketTagMessage::length() const
    {
      return common::Serialization::get_string_length(bucket_name_);
    }

    RspKvMetaGetBucketTagMessage::RspKvMetaGetBucketTagMessage()
    {
      _packetHeader._pcode = RSP_KVMETA_GET_BUCKET_TAG_MESSAGE;
    }

    RspKvMetaGetBucketTagMessage::~RspKvMetaGetBucketTagMessage(){}

    int RspKvMetaGetBucketTagMessage::serialize(Stream& output) const
    {
      int32_t iret = output.set_string(bucket_name_);

      if (common::TFS_SUCCESS == iret)
      {
        int32_t size = bucket_tag_map_.size();
        iret = output.set_int32(size);
      }

      if (common::TFS_SUCCESS == iret)
      {
        MAP_STRING_ITER iter = bucket_tag_map_.begin();
        for (; iter != bucket_tag_map_.end() && common::TFS_SUCCESS == iret; iter++)
        {
          iret = output.set_string(iter->first);
          if (common::TFS_SUCCESS == iret)
          {
            iret = output.set_string(iter->second);
          }
        }
      }

      return iret;
    }

    int RspKvMetaGetBucketTagMessage::deserialize(Stream& input)
    {
      int32_t iret = input.get_string(bucket_name_);

      int32_t size = -1;

      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&size);
      }

      if (common::TFS_SUCCESS == iret)
      {
        std::string key;
        std::string value;
        for (int32_t i = 0; i < size && common::TFS_SUCCESS == iret; i++)
        {
          iret = input.get_string(key);
          if (common::TFS_SUCCESS == iret)
          {
            iret = input.get_string(value);
          }

          if (common::TFS_SUCCESS == iret)
          {
            bucket_tag_map_.insert(std::make_pair(key, value));
          }
        }
      }

      return iret;
    }

    int64_t RspKvMetaGetBucketTagMessage::length() const
    {
      int64_t len = common::Serialization::get_string_length(bucket_name_);
      len += INT_SIZE;

      MAP_STRING_ITER iter = bucket_tag_map_.begin();
      for (; iter != bucket_tag_map_.end(); iter++)
      {
        len += common::Serialization::get_string_length(iter->first);
        len += common::Serialization::get_string_length(iter->second);
      }
      return len;
    }

    //del bucket tag
    ReqKvMetaDelBucketTagMessage::ReqKvMetaDelBucketTagMessage()
    {
      _packetHeader._pcode = REQ_KVMETA_DEL_BUCKET_TAG_MESSAGE;
    }

    ReqKvMetaDelBucketTagMessage::~ReqKvMetaDelBucketTagMessage(){}

    int ReqKvMetaDelBucketTagMessage::serialize(Stream& output) const
    {
      int32_t iret = output.set_string(bucket_name_);

      return iret;
    }

    int ReqKvMetaDelBucketTagMessage::deserialize(Stream& input)
    {
      int32_t iret = input.get_string(bucket_name_);

      return iret;
    }

    int64_t ReqKvMetaDelBucketTagMessage::length() const
    {
      return common::Serialization::get_string_length(bucket_name_);
    }

    //ReqApplyAuthorize
    ReqApplyAuthorizeMessage::ReqApplyAuthorizeMessage()
    {
      _packetHeader._pcode = REQ_KV_APPLY_AUTHORIZE_MESSAGE;
    }

    ReqApplyAuthorizeMessage::~ReqApplyAuthorizeMessage(){}

    int ReqApplyAuthorizeMessage::serialize(Stream& output) const
    {
      int32_t iret = output.set_string(user_name_);
      return iret;
    }

    int ReqApplyAuthorizeMessage::deserialize(Stream& input)
    {
      int32_t iret = input.get_string(user_name_);
      return iret;
    }

    int64_t ReqApplyAuthorizeMessage::length() const
    {
      return Serialization::get_string_length(user_name_);
    }

    RspApplyAuthorizeMessage::RspApplyAuthorizeMessage()
    {
      _packetHeader._pcode = RSP_KV_APPLY_AUTHORIZE_MESSAGE;
    }

    RspApplyAuthorizeMessage::~RspApplyAuthorizeMessage(){}

    int RspApplyAuthorizeMessage::serialize(Stream& output) const
    {
      int32_t iret = output.set_string(access_key_id_);
      if (TFS_SUCCESS == iret)
      {
        iret = output.set_string(access_secret_key_);
      }
      return iret;
    }

    int RspApplyAuthorizeMessage::deserialize(Stream& input)
    {
      int32_t iret = input.get_string(access_key_id_);
      if (TFS_SUCCESS == iret)
      {
        iret = input.get_string(access_secret_key_);
      }
      return iret;
    }

    int64_t RspApplyAuthorizeMessage::length() const
    {
      return Serialization::get_string_length(access_key_id_)
      + Serialization::get_string_length(access_secret_key_);
    }

    //ReqGETAuthorize
    ReqGetAuthorizeMessage::ReqGetAuthorizeMessage()
    {
      _packetHeader._pcode = REQ_KV_GET_AUTHORIZE_MESSAGE;
    }

    ReqGetAuthorizeMessage::~ReqGetAuthorizeMessage(){}

    int ReqGetAuthorizeMessage::serialize(Stream& output) const
    {
      int32_t iret = output.set_string(access_key_id_);
      return iret;
    }

    int ReqGetAuthorizeMessage::deserialize(Stream& input)
    {
      int32_t iret = input.get_string(access_key_id_);
      return iret;
    }

    int64_t ReqGetAuthorizeMessage::length() const
    {
      return Serialization::get_string_length(access_key_id_);
    }

    RspGetAuthorizeMessage::RspGetAuthorizeMessage()
    {
      _packetHeader._pcode = RSP_KV_GET_AUTHORIZE_MESSAGE;
    }

    RspGetAuthorizeMessage::~RspGetAuthorizeMessage(){}

    int RspGetAuthorizeMessage::serialize(Stream& output) const
    {
      int32_t iret = TFS_SUCCESS;
      int64_t pos = 0;

      iret = authorize_info_.serialize(output.get_free(), output.get_free_length(), pos);
      if (TFS_SUCCESS == iret)
      {
        output.pour(authorize_info_.length());
      }
      return iret;
    }

    int RspGetAuthorizeMessage::deserialize(Stream& input)
    {
      int32_t iret = TFS_SUCCESS;
      int64_t pos = 0;
      iret = authorize_info_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (TFS_SUCCESS == iret)
      {
        input.drain(authorize_info_.length());
      }
      return iret;
    }

    int64_t RspGetAuthorizeMessage::length() const
    {
      return authorize_info_.length();
    }

    //about bucket acl
    //put bucket acl
    ReqKvMetaPutBucketAclMessage::ReqKvMetaPutBucketAclMessage()
    {
      _packetHeader._pcode = REQ_KVMETA_PUT_BUCKET_ACL_MESSAGE;
    }

    ReqKvMetaPutBucketAclMessage::~ReqKvMetaPutBucketAclMessage(){}

    int ReqKvMetaPutBucketAclMessage::serialize(Stream& output) const
    {
      int32_t iret = output.set_string(bucket_name_);

      if (common::TFS_SUCCESS == iret)
      {
        int32_t size = bucket_acl_map_.size();
        iret = output.set_int32(size);
      }

      if (common::TFS_SUCCESS == iret)
      {
        MAP_INT64_INT_ITER iter = bucket_acl_map_.begin();
        for (; iter != bucket_acl_map_.end() && common::TFS_SUCCESS == iret; iter++)
        {
          iret = output.set_int64(iter->first);
          if (common::TFS_SUCCESS == iret)
          {
            iret = output.set_int32(iter->second);
          }
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

      if (TFS_SUCCESS == iret)
      {
        iret = output.set_int32(static_cast<int32_t>(acl_));
      }

      return iret;
    }

    int ReqKvMetaPutBucketAclMessage::deserialize(Stream& input)
    {
      int32_t iret = input.get_string(bucket_name_);

      int32_t size = -1;

      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&size);
      }

      if (common::TFS_SUCCESS == iret)
      {
        int64_t key;
        int32_t value;
        for (int32_t i = 0; i < size && common::TFS_SUCCESS == iret; i++)
        {
          iret = input.get_int64(&key);
          if (common::TFS_SUCCESS == iret)
          {
            iret = input.get_int32(&value);
          }

          if (common::TFS_SUCCESS == iret)
          {
            bucket_acl_map_.insert(std::make_pair(key, value));
          }
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

      if (TFS_SUCCESS == iret)
      {
        iret = input.get_int32(reinterpret_cast<int32_t*>(&acl_));
      }

      return iret;
    }

    int64_t ReqKvMetaPutBucketAclMessage::length() const
    {
      int64_t len = common::Serialization::get_string_length(bucket_name_);
      len += INT_SIZE;

      MAP_INT64_INT_ITER iter = bucket_acl_map_.begin();
      for (; iter != bucket_acl_map_.end(); iter++)
      {
        len += INT64_SIZE;
        len += INT_SIZE;
      }

      len += user_info_.length();

      len += INT_SIZE;
      return len;
    }

    //req get bucket acl
    ReqKvMetaGetBucketAclMessage::ReqKvMetaGetBucketAclMessage()
    {
      _packetHeader._pcode = REQ_KVMETA_GET_BUCKET_ACL_MESSAGE;
    }

    ReqKvMetaGetBucketAclMessage::~ReqKvMetaGetBucketAclMessage(){}

    int ReqKvMetaGetBucketAclMessage::serialize(Stream& output) const
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

    int ReqKvMetaGetBucketAclMessage::deserialize(Stream& input)
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

    int64_t ReqKvMetaGetBucketAclMessage::length() const
    {
      return common::Serialization::get_string_length(bucket_name_) + user_info_.length();
    }

    //rsp get bucket acl
    RspKvMetaGetBucketAclMessage::RspKvMetaGetBucketAclMessage()
    {
      _packetHeader._pcode = RSP_KVMETA_GET_BUCKET_ACL_MESSAGE;
    }

    RspKvMetaGetBucketAclMessage::~RspKvMetaGetBucketAclMessage(){}

    int RspKvMetaGetBucketAclMessage::serialize(Stream& output) const
    {
      int32_t iret = output.set_string(bucket_name_);

      if (common::TFS_SUCCESS == iret)
      {
        int32_t size = bucket_acl_map_.size();
        iret = output.set_int32(size);
      }

      if (common::TFS_SUCCESS == iret)
      {
        MAP_INT64_INT_ITER iter = bucket_acl_map_.begin();
        for (; iter != bucket_acl_map_.end() && common::TFS_SUCCESS == iret; iter++)
        {
          iret = output.set_int64(iter->first);
          if (common::TFS_SUCCESS == iret)
          {
            iret = output.set_int32(iter->second);
          }
        }
      }

      return iret;
    }

    int RspKvMetaGetBucketAclMessage::deserialize(Stream& input)
    {
      int32_t iret = input.get_string(bucket_name_);

      int32_t size = -1;

      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&size);
      }

      if (common::TFS_SUCCESS == iret)
      {
        int64_t key;
        int32_t value;
        for (int32_t i = 0; i < size && common::TFS_SUCCESS == iret; i++)
        {
          iret = input.get_int64(&key);
          if (common::TFS_SUCCESS == iret)
          {
            iret = input.get_int32(&value);
          }

          if (common::TFS_SUCCESS == iret)
          {
            bucket_acl_map_.insert(std::make_pair(key, value));
          }
        }
      }

      return iret;
    }

    int64_t RspKvMetaGetBucketAclMessage::length() const
    {
      int64_t len = common::Serialization::get_string_length(bucket_name_);
      len += INT_SIZE;

      MAP_INT64_INT_ITER iter = bucket_acl_map_.begin();
      for (; iter != bucket_acl_map_.end(); iter++)
      {
        len += INT64_SIZE;
        len += INT_SIZE;
      }
      return len;
    }

    //init multipart
    ReqKvMetaInitMulitpartMessage::ReqKvMetaInitMulitpartMessage()
    {
      _packetHeader._pcode = REQ_KVMETA_INIT_MULTIPART_MESSAGE;
    }

    ReqKvMetaInitMulitpartMessage::~ReqKvMetaInitMulitpartMessage(){}

    int ReqKvMetaInitMulitpartMessage::serialize(Stream& output) const
    {
      int32_t iret = output.set_string(bucket_name_);

      if (TFS_SUCCESS == iret)
      {
        iret = output.set_string(file_name_);
      }
      return iret;
    }

    int ReqKvMetaInitMulitpartMessage::deserialize(Stream& input)
    {
      int32_t iret = input.get_string(bucket_name_);

      if (TFS_SUCCESS == iret)
      {
        iret = input.get_string(file_name_);
      }
      return iret;
    }

    int64_t ReqKvMetaInitMulitpartMessage::length() const
    {
      return Serialization::get_string_length(bucket_name_) +
        Serialization::get_string_length(file_name_);
    }

    RspKvMetaInitMulitpartMessage::RspKvMetaInitMulitpartMessage()
    {
      _packetHeader._pcode = RSP_KVMETA_INIT_MULTIPART_MESSAGE;
    }

    RspKvMetaInitMulitpartMessage::~RspKvMetaInitMulitpartMessage(){}

    int RspKvMetaInitMulitpartMessage::serialize(Stream& output) const
    {
      int32_t iret = output.set_string(upload_id_);
      return iret;
    }

    int RspKvMetaInitMulitpartMessage::deserialize(Stream& input)
    {
      int32_t iret = input.get_string(upload_id_);
      return iret;
    }

    int64_t RspKvMetaInitMulitpartMessage::length() const
    {
      return Serialization::get_string_length(upload_id_);
    }

    //upload mulitpart
    ReqKvMetaUploadMulitpartMessage::ReqKvMetaUploadMulitpartMessage()
    {
      _packetHeader._pcode = REQ_KVMETA_UPLOAD_MULTIPART_MESSAGE;
    }

    ReqKvMetaUploadMulitpartMessage::~ReqKvMetaUploadMulitpartMessage(){}

    int ReqKvMetaUploadMulitpartMessage::serialize(Stream& output) const
    {
      int32_t iret = output.set_string(bucket_name_);

      if (TFS_SUCCESS == iret)
      {
        iret = output.set_string(file_name_);
      }

      if (TFS_SUCCESS == iret)
      {
        iret = output.set_string(upload_id_);
      }

      if (TFS_SUCCESS == iret)
      {
        iret = output.set_int32(part_num_);
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

    int ReqKvMetaUploadMulitpartMessage::deserialize(Stream& input)
    {
      int32_t iret = input.get_string(bucket_name_);

      if (TFS_SUCCESS == iret)
      {
        iret = input.get_string(file_name_);
      }

      if (TFS_SUCCESS == iret)
      {
        iret = input.get_string(upload_id_);
      }

      if (TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&part_num_);
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

    int64_t ReqKvMetaUploadMulitpartMessage::length() const
    {
      return Serialization::get_string_length(bucket_name_) +
        Serialization::get_string_length(file_name_) +
        Serialization::get_string_length(upload_id_) +
        INT_SIZE +
        object_info_.length() + user_info_.length();
    }

    //complete mulitpart
    ReqKvMetaCompleteMulitpartMessage::ReqKvMetaCompleteMulitpartMessage()
    {
      _packetHeader._pcode = REQ_KVMETA_COMPLETE_MULTIPART_MESSAGE;
    }

    ReqKvMetaCompleteMulitpartMessage::~ReqKvMetaCompleteMulitpartMessage(){}

    int ReqKvMetaCompleteMulitpartMessage::serialize(Stream& output) const
    {
      int32_t iret = output.set_string(bucket_name_);

      if (TFS_SUCCESS == iret)
      {
        iret = output.set_string(file_name_);
      }

      if (TFS_SUCCESS == iret)
      {
        iret = output.set_string(upload_id_);
      }

      if (TFS_SUCCESS == iret)
      {
        iret = output.set_int32(v_part_num_.size());
      }

      if (TFS_SUCCESS == iret)
      {
        for (size_t i = 0; TFS_SUCCESS == iret && i < v_part_num_.size(); i++)
        {
          iret = output.set_int32(v_part_num_[i]);
        }
      }
      return iret;
    }

    int ReqKvMetaCompleteMulitpartMessage::deserialize(Stream& input)
    {
      int32_t iret = input.get_string(bucket_name_);

      if (TFS_SUCCESS == iret)
      {
        iret = input.get_string(file_name_);
      }

      if (TFS_SUCCESS == iret)
      {
        iret = input.get_string(upload_id_);
      }

      int32_t v_size = -1;
      if (TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&v_size);
      }

      if (TFS_SUCCESS == iret)
      {
        int32_t part_num = -1;
        for (int32_t i = 0; TFS_SUCCESS == iret && i < v_size; i++)
        {
          iret = input.get_int32(&part_num);
          if (common::TFS_SUCCESS == iret)
          {
            v_part_num_.push_back(part_num);
          }
        }
      }

      return iret;
    }

    int64_t ReqKvMetaCompleteMulitpartMessage::length() const
    {
      return Serialization::get_string_length(bucket_name_) +
        Serialization::get_string_length(file_name_) +
        Serialization::get_string_length(upload_id_) +
        INT_SIZE +
        INT_SIZE * v_part_num_.size();
    }

    //list mulitpart
    ReqKvMetaListMulitpartMessage::ReqKvMetaListMulitpartMessage()
    {
      _packetHeader._pcode = REQ_KVMETA_LIST_MULTIPART_MESSAGE;
    }

    ReqKvMetaListMulitpartMessage::~ReqKvMetaListMulitpartMessage(){}

    int ReqKvMetaListMulitpartMessage::serialize(Stream& output) const
    {
      int32_t iret = output.set_string(bucket_name_);

      if (TFS_SUCCESS == iret)
      {
        iret = output.set_string(file_name_);
      }

      if (TFS_SUCCESS == iret)
      {
        iret = output.set_string(upload_id_);
      }

      return iret;
    }

    int ReqKvMetaListMulitpartMessage::deserialize(Stream& input)
    {
      int32_t iret = input.get_string(bucket_name_);

      if (TFS_SUCCESS == iret)
      {
        iret = input.get_string(file_name_);
      }

      if (TFS_SUCCESS == iret)
      {
        iret = input.get_string(upload_id_);
      }

      return iret;
    }

    int64_t ReqKvMetaListMulitpartMessage::length() const
    {
      return Serialization::get_string_length(bucket_name_) +
        Serialization::get_string_length(file_name_) +
        Serialization::get_string_length(upload_id_);
    }

    RspKvMetaListMulitpartMessage::RspKvMetaListMulitpartMessage()
    {
      _packetHeader._pcode = RSP_KVMETA_LIST_MULTIPART_MESSAGE;
    }

    RspKvMetaListMulitpartMessage::~RspKvMetaListMulitpartMessage(){}

    int RspKvMetaListMulitpartMessage::serialize(Stream& output) const
    {
      int32_t iret = TFS_SUCCESS;
      if (TFS_SUCCESS == iret)
      {
        iret = output.set_int32(v_part_num_.size());
      }

      if (TFS_SUCCESS == iret)
      {
        for (size_t i = 0; TFS_SUCCESS == iret && i < v_part_num_.size(); i++)
        {
          iret = output.set_int32(v_part_num_[i]);
        }
      }
      return iret;
    }

    int RspKvMetaListMulitpartMessage::deserialize(Stream& input)
    {
      int32_t iret = TFS_SUCCESS;

      int32_t v_size = -1;
      if (TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&v_size);
      }

      if (TFS_SUCCESS == iret)
      {
        int32_t part_num = -1;
        for (int32_t i = 0; TFS_SUCCESS == iret && i < v_size; i++)
        {
          iret = input.get_int32(&part_num);
          if (common::TFS_SUCCESS == iret)
          {
            v_part_num_.push_back(part_num);
          }
        }
      }

      return iret;
    }

    int64_t RspKvMetaListMulitpartMessage::length() const
    {
      return INT_SIZE + INT_SIZE * v_part_num_.size();
    }

    ReqKvMetaAbortMulitpartMessage::ReqKvMetaAbortMulitpartMessage()
    {
      _packetHeader._pcode = REQ_KVMETA_ABORT_MULTIPART_MESSAGE;
    }

    ReqKvMetaAbortMulitpartMessage::~ReqKvMetaAbortMulitpartMessage(){}

    int ReqKvMetaAbortMulitpartMessage::serialize(Stream& output) const
    {
      int32_t iret = output.set_string(bucket_name_);

      if (TFS_SUCCESS == iret)
      {
        iret = output.set_string(file_name_);
      }

      if (TFS_SUCCESS == iret)
      {
        iret = output.set_string(upload_id_);
      }

      return iret;
    }

    int ReqKvMetaAbortMulitpartMessage::deserialize(Stream& input)
    {
      int32_t iret = input.get_string(bucket_name_);

      if (TFS_SUCCESS == iret)
      {
        iret = input.get_string(file_name_);
      }

      if (TFS_SUCCESS == iret)
      {
        iret = input.get_string(upload_id_);
      }

      return iret;
    }

    int64_t ReqKvMetaAbortMulitpartMessage::length() const
    {
      return Serialization::get_string_length(bucket_name_) +
        Serialization::get_string_length(file_name_) +
        Serialization::get_string_length(upload_id_);
    }


    RspKvMetaAbortMulitpartMessage::RspKvMetaAbortMulitpartMessage()
    : still_have_(false)
    {
      _packetHeader._pcode = RSP_KVMETA_ABORT_MULTIPART_MESSAGE;
    }
    RspKvMetaAbortMulitpartMessage::~RspKvMetaAbortMulitpartMessage(){}

    int RspKvMetaAbortMulitpartMessage::serialize(Stream& output) const
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

    int RspKvMetaAbortMulitpartMessage::deserialize(Stream& input)
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
    int64_t RspKvMetaAbortMulitpartMessage::length() const
    {
      return INT8_SIZE + object_info_.length();
    }

  }
}
