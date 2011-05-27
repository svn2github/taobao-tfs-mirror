#include "serialization.h"
#include "internal.h"
#include "rc_define.h"
#include <tbsys.h>

namespace tfs
{
  namespace common
  {
    using namespace std;
    int ClusterData::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, cluster_stat_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, access_type_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_string(data, data_len, pos, cluster_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_string(data, data_len, pos, ns_vip_);
      }
      return ret;
    }

    int ClusterData::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &cluster_stat_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &access_type_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_string(data, data_len, pos, cluster_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_string(data, data_len, pos, ns_vip_);
      }
      return ret;
    }

    int64_t ClusterData::length() const
    {
      return INT_SIZE * 2 + Serialization::get_string_length(cluster_id_) + Serialization::get_string_length(ns_vip_);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    int ClusterRackData::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int8(data, data_len, pos, need_duplicate_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_string(data, data_len, pos, dupliate_server_addr_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::serialize_list(data, data_len, pos, cluster_data_);
      }
      return ret;
    }

    int ClusterRackData::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int8(data, data_len, pos, reinterpret_cast<int8_t*>(&need_duplicate_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_string(data, data_len, pos, dupliate_server_addr_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::deserialize_list(data, data_len, pos, cluster_data_);
      }
      return ret;
    }

    int64_t ClusterRackData::length() const
    {
      return INT8_SIZE + Serialization::get_string_length(dupliate_server_addr_) + Serialization::get_list_length(cluster_data_);
    }
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    int BaseInfo::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_vint64(data, data_len, pos, rc_server_infos_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::serialize_list(data, data_len, pos, cluster_infos_);
      }
      return ret;
    }

    int BaseInfo::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_vint64(data, data_len, pos, rc_server_infos_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::deserialize_list(data, data_len, pos, cluster_infos_);
      }
      return ret;
    }

    int64_t BaseInfo::length() const
    {
      return Serialization::get_vint64_length(rc_server_infos_) + Serialization::get_list_length(cluster_infos_);
    }
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    int SessionBaseInfo::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_string(data, data_len, pos, session_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_string(data, data_len, pos, client_version_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, cache_size_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, cache_time_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, modify_time_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int8(data, data_len, pos, is_logout_);
      }
      return ret;
    }

    int SessionBaseInfo::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_string(data, data_len, pos, session_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_string(data, data_len, pos, client_version_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &cache_size_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &cache_time_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &modify_time_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int8(data, data_len, pos, reinterpret_cast<int8_t*>(&is_logout_));
      }
      return ret;
    }

    int64_t SessionBaseInfo::length() const
    {
      return Serialization::get_string_length(session_id_) + Serialization::get_string_length(client_version_) +
        INT64_SIZE * 3 + INT8_SIZE;
    }
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    int AppOperInfo::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, oper_type_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, oper_times_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, oper_size_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, oper_rt_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, oper_succ_);
      }
      return ret;
    }

    int AppOperInfo::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&oper_type_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &oper_times_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &oper_size_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &oper_rt_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &oper_succ_);
      }
      return ret;
    }

    int64_t AppOperInfo::length() const
    {
      return INT_SIZE + INT64_SIZE * 4;
    }
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    int SessionStat::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        // set map size
        ret = Serialization::set_int32(data, data_len, pos, app_oper_info_.size());
      }

      if (TFS_SUCCESS == ret)
      {
        std::map<OperType, AppOperInfo>::const_iterator mit = app_oper_info_.begin();
        for ( ; mit != app_oper_info_.end(); ++mit)
        {
          ret = Serialization::set_int32(data, data_len, pos, mit->first);
          if (TFS_SUCCESS == ret)
          {
            ret = mit->second.serialize(data, data_len, pos);
          }

          if (TFS_SUCCESS != ret)
          {
            break;
          }
        }
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, cache_hit_ratio_);
      }
      return ret;
    }

    int SessionStat::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      int32_t len = 0;
      if (TFS_SUCCESS == ret)
      {
        // get map size
        ret = Serialization::get_int32(data, data_len, pos, &len);
      }

      if (TFS_SUCCESS == ret)
      {
        for (int32_t i = 0; i < len; ++i)
        {
          OperType oper_type;
          AppOperInfo app_oper;
          ret = Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&oper_type));
          if (TFS_SUCCESS == ret)
          {
            ret = app_oper.deserialize(data, len, pos);
          }
          if (TFS_SUCCESS == ret)
          {
            if (!app_oper_info_.insert(pair<OperType, AppOperInfo>(oper_type, app_oper)).second);
            {
              TBSYS_LOG(ERROR, "insert the same type: %d, size: %d", oper_type, len);
              ret = TFS_ERROR;
            }
          }
        }
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &cache_hit_ratio_);
      }
      return ret;
    }

    int64_t SessionStat::length() const
    {
      int64_t len = INT_SIZE + INT64_SIZE;
      std::map<OperType, AppOperInfo>::const_iterator mit = app_oper_info_.begin();
      for ( ; mit != app_oper_info_.end(); ++mit)
      {
        len += INT_SIZE;
        len += mit->second.length();
      }
      return len;
    }
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    int KeepAliveInfo::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = s_base_info_.serialize(data, data_len, pos);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = s_stat_.serialize(data, data_len, pos);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, last_report_time_);
      }
      return ret;
    }

    int KeepAliveInfo::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = s_base_info_.deserialize(data, data_len, pos);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = s_stat_.deserialize(data, data_len, pos);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &last_report_time_);
      }
      return ret;
    }

    int64_t KeepAliveInfo::length() const
    {
      return s_base_info_.length() + s_stat_.length() + INT64_SIZE;
    }
  }
}
