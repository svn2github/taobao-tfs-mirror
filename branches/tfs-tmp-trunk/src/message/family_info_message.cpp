/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: family_info_message.cpp 746 2012-09-03 14:27:59Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */
#include "family_info_message.h"

namespace tfs
{
  namespace message
  {
    GetFamilyInfoMessage::GetFamilyInfoMessage(const int32_t mode) :
      family_id_(common::INVALID_FAMILY_ID), mode_(mode)
    {
      _packetHeader._pcode = common::REQ_GET_FAMILY_INFO_MESSAGE;
    }

    GetFamilyInfoMessage::~GetFamilyInfoMessage()
    {
    }

    int GetFamilyInfoMessage::deserialize(common::Stream& input)
    {
      int32_t ret = input.get_int32(&mode_);
      if (common::TFS_SUCCESS == ret)
      {
        ret = input.get_int64(&family_id_);
      }
      return ret;
    }

    int64_t GetFamilyInfoMessage::length() const
    {
      return common::INT_SIZE + common::INT64_SIZE;
    }

    int GetFamilyInfoMessage::serialize(common::Stream& output)  const
    {
      int32_t ret = output.set_int32(mode_);
      if (common::TFS_SUCCESS == ret)
      {
        ret = output.set_int64(family_id_);
      }
      return ret;
    }

    GetFamilyInfoResponseMessage::GetFamilyInfoResponseMessage() :
      family_id_(common::INVALID_FAMILY_ID),
      family_aid_info_(0)
    {
      _packetHeader._pcode = common::RSP_GET_FAMILY_INFO_MESSAGE;
      members_.clear();
    }

    GetFamilyInfoResponseMessage::~GetFamilyInfoResponseMessage()
    {
    }

    int GetFamilyInfoResponseMessage::deserialize(common::Stream& input)
    {
      int32_t ret = input.get_int64(&family_id_);
      if (common::TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&family_aid_info_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        const uint32_t MEMBER_NUM = GET_DATA_MEMBER_NUM(family_aid_info_) + GET_CHECK_MEMBER_NUM(family_aid_info_);
        for (uint32_t index = 0; index < MEMBER_NUM && common::TFS_SUCCESS == ret; ++index)
        {
          std::pair<uint32_t, uint64_t> item;
          ret = input.get_int32(reinterpret_cast<int32_t*>(&item.first));
          if (common::TFS_SUCCESS == ret)
            ret = input.get_int64(reinterpret_cast<int64_t*>(&item.second));
          if (common::TFS_SUCCESS == ret)
            members_.push_back(item);
        }
        if (common::TFS_SUCCESS == ret)
          ret = members_.size() == MEMBER_NUM ? common::TFS_SUCCESS : common::EXIT_DESERIALIZE_ERROR;
      }
      return ret;
    }

    int64_t GetFamilyInfoResponseMessage::length() const
    {
      const uint32_t MEMBER_NUM = GET_DATA_MEMBER_NUM(family_aid_info_) + GET_CHECK_MEMBER_NUM(family_aid_info_);
      return common::INT64_SIZE + common::INT_SIZE + MEMBER_NUM * (common::INT64_SIZE + common::INT_SIZE);
    }

    int GetFamilyInfoResponseMessage::serialize(common::Stream& output)  const
    {
      int32_t ret = output.set_int64(family_id_);
      if (common::TFS_SUCCESS == ret)
      {
        ret = output.set_int32(family_aid_info_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        const uint32_t MEMBER_NUM = GET_DATA_MEMBER_NUM(family_aid_info_) + GET_CHECK_MEMBER_NUM(family_aid_info_);
        ret = members_.size() == MEMBER_NUM ? common::TFS_SUCCESS : common::EXIT_SERIALIZE_ERROR;
        if (common::TFS_SUCCESS == ret)
        {
          std::vector<std::pair<uint32_t, uint64_t> >::const_iterator iter = members_.begin();
          for (; iter != members_.end() && common::TFS_SUCCESS == ret; ++iter)
          {
            ret = output.set_int32(iter->first);
            if (common::TFS_SUCCESS == ret)
              ret = output.set_int64(iter->second);
          }
        }
      }
      return ret;
    }
  }/** end namespace message **/
}/** end namespace tfs **/
