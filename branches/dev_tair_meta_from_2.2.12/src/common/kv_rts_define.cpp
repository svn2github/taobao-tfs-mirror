/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id:  $
 *
 * Authors:
 *   qixiao.zs <qixiao.zs@alibaba-inc.com>
 *      - initial release
 *
 */

#include "internal.h"
#include "kv_rts_define.h"
#include "parameter.h"
#include "serialization.h"

namespace tfs
{
  namespace common
  {
    bool KVRSLease::has_valid_lease(const int64_t now)
    {
      return ((lease_id_ != INVALID_LEASE_ID) && lease_expired_time_ >= now);
    }

    bool KVRSLease::renew(const int32_t step, const int64_t now)
    {
      bool bret = has_valid_lease(now);
      if (bret)
      {
        lease_expired_time_ = now + step;
      }
      return bret;
    }

    int KVRSLease::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t iret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        iret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&lease_id_));
      }
      if (TFS_SUCCESS == iret)
      {
        iret = Serialization::get_int64(data, data_len, pos, &lease_expired_time_);
      }
      return iret;
    }

    int KVRSLease::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t iret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        iret = Serialization::set_int64(data, data_len, pos, lease_id_);
      }
      if (TFS_SUCCESS == iret)
      {
        iret = Serialization::set_int64(data, data_len, pos, lease_expired_time_);
      }
      return iret;
    }

    int64_t KVRSLease::length() const
    {
      return INT64_SIZE + INT64_SIZE;
    }


    int KvMetaServerBaseInformation::deserialize(const char* data, const int64_t data_len, int64_t& pos)
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

    int KvMetaServerBaseInformation::serialize(char* data, const int64_t data_len, int64_t& pos) const
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
    int64_t KvMetaServerBaseInformation::length() const
    {
      return INT64_SIZE * 3 ;
    }

    int KvMetaServer::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t iret = lease_.deserialize(data, data_len, pos);

      if (TFS_SUCCESS == iret)
      {
        iret = base_info_.deserialize(data, data_len, pos);
      }
      return iret;
    }

    int KvMetaServer::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t iret = lease_.serialize(data, data_len, pos);

      if (TFS_SUCCESS == iret)
      {
        iret = base_info_.serialize(data, data_len, pos);
      }
      return iret;
    }

    int64_t KvMetaServer::length() const
    {
      return lease_.length() + base_info_.length();
    }

  } /** nameserver **/
}/** tfs **/
