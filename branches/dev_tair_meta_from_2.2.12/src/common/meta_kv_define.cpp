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
 *   daoan <daoan@taobao.com>
 *      - initial release
 *
 */
#include "meta_kv_define.h"
#include "serialization.h"

namespace tfs
{
  namespace common
  {
    TfsFileInfo::TfsFileInfo()
      :block_id(0), file_id(0)
    { }

    int64_t TfsFileInfo::length() const
    {
      return INT64_SIZE + INT64_SIZE + INT_SIZE;
    }
    int TfsFileInfo::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, block_id);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, file_id);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, cluster_id);
      }
      return ret;
    }

    int TfsFileInfo::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &block_id);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &file_id);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &cluster_id);
      }
      return ret;
    }

  }
}

