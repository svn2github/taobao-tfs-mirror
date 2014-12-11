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

    // KvMetaTable
    KvMetaTable::KvMetaTable()
    {
    }
    int64_t KvMetaTable::length() const
    {
      return INT_SIZE + INT64_SIZE * v_meta_table_.size() + INT_SIZE * 2;
    }

    int KvMetaTable::serialize(char *data, const int64_t data_len, int64_t &pos) const
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, KV_META_TABLE_V_META_TABLE_TAG);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_vint64(data, data_len, pos, v_meta_table_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, END_TAG);
      }
      return ret;
    }

    int KvMetaTable::deserialize(const char *data, const int64_t data_len, int64_t &pos)
    {
      int ret = NULL != data/* && data_len - pos >= length()*/ ? TFS_SUCCESS : TFS_ERROR;

      while (TFS_SUCCESS == ret)
      {
        int32_t type_tag = 0;
        ret = Serialization::get_int32(data, data_len, pos, &type_tag);

        if (TFS_SUCCESS == ret)
        {
          switch (type_tag)
          {
            case KV_META_TABLE_V_META_TABLE_TAG:
              ret = Serialization::get_vint64(data, data_len, pos, v_meta_table_);
              break;
            case END_TAG:
              ;
              break;
            default:
              TBSYS_LOG(ERROR, "kv meta table: %d can't self-interpret", type_tag);
              ret = TFS_ERROR;
              break;
          }
        }

        if (END_TAG == type_tag)
        {
          break;
        }
      }

      return ret;
    }

    void KvMetaTable::dump()
    {
      VUINT64::iterator iter = v_meta_table_.begin();
      for(; iter != v_meta_table_.end(); iter++)
      {
        TBSYS_LOG(DEBUG, "kv meta server addr: %s", tbsys::CNetUtil::addrToString(*iter).c_str());
      }
    }

  } /** nameserver **/
}/** tfs **/
