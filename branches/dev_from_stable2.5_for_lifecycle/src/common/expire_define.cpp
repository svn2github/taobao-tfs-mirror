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
 *   qixiao.zs <qixiao.zs@alibaba-inc.com>
 *      - initial release
 *
 */

#include "expire_define.h"
#include "serialization.h"
#include "stream.h"

namespace tfs
{
  namespace common
  {
    const char DELIMITER_EXPIRE = 10;
    const char KEY_TYPE_ORI = 11;
    const char KEY_TYPE_S3 = 12;
    const int32_t HASH_BUCKET_NUM = 10243;

    //OriInvalidTimeValueInfo
    OriInvalidTimeValueInfo::OriInvalidTimeValueInfo()
    {}
    int64_t OriInvalidTimeValueInfo::length() const
    {
      return Serialization::get_string_length(appkey_);
    }

    int OriInvalidTimeValueInfo::serialize(char *data, const int64_t data_len, int64_t &pos) const
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_string(data, data_len, pos, appkey_);
      }
      return ret;
    }

    int OriInvalidTimeValueInfo::deserialize(const char *data, const int64_t data_len, int64_t &pos)
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_string(data, data_len, pos, appkey_);
      }
      return ret;
    }

    int ExpServerBaseInformation::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t iret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        iret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&id_));
      }
      if (TFS_SUCCESS == iret)
      {
        iret = Serialization::get_int64(data, data_len, pos, &start_time_);
      }
      if (TFS_SUCCESS == iret)
      {
        iret = Serialization::get_int64(data, data_len, pos, &last_update_time_);
      }
      return iret;
    }

    int ExpServerBaseInformation::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t iret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        iret = Serialization::set_int64(data, data_len, pos, id_);
      }
      if (TFS_SUCCESS == iret)
      {
        iret = Serialization::set_int64(data, data_len, pos, start_time_);
      }
      if (TFS_SUCCESS == iret)
      {
        iret = Serialization::set_int64(data, data_len, pos, last_update_time_);
      }
      return iret;
    }

    int64_t ExpServerBaseInformation::length() const
    {
      return INT64_SIZE * 3 ;
    }

  }//common end
}// tfs end

