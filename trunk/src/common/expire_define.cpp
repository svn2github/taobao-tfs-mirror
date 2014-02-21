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
#include "kvengine_helper.h"

namespace tfs
{
  namespace common
  {
    const char ExpireDefine::DELIMITER_EXPIRE = 10;
    const int32_t ExpireDefine::HASH_BUCKET_NUM = 10243;
    const int32_t ExpireDefine::FILE_TYPE_RAW_TFS = 1;
    const int32_t ExpireDefine::FILE_TYPE_CUSTOM_TFS = 2;

    int ExpireDefine::serialize_name_expiretime_key(const int32_t file_type,
        const std::string& file_name,
        common::KvKey *key, char *data, const int32_t size)
    {
      int ret = (NULL == key || NULL == data) ? TFS_ERROR : TFS_SUCCESS;
      int64_t pos = 0;
      if(TFS_SUCCESS == ret)
      {
        data[pos] = KvKey::KEY_TYPE_NAME_EXPTIME;
        pos += 1;
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::int32_to_char(data + pos, size - pos, file_type);
        pos += 4;
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_string(data, size, pos, file_name);
      }

      if (TFS_SUCCESS == ret)
      {
        key->key_ = data;
        key->key_size_ = pos;
        key->key_type_ = KvKey::KEY_TYPE_NAME_EXPTIME;
      }
      return ret;
    }

    int ExpireDefine::deserialize_name_expiretime_key(const char *data, const int32_t size,
        int8_t* key_type, int32_t* file_type,
        std::string* file_name)
    {
      int ret = (NULL == data || NULL == key_type || NULL == file_type
          || NULL == file_name) ? TFS_ERROR : TFS_SUCCESS;
      int64_t pos = 0;
      if(TFS_SUCCESS == ret)
      {
        *key_type = data[pos];
        pos += 1;
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::char_to_int32(data + pos, size - pos, *file_type);
        pos += 4;
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_string(data, size, pos, *file_name);
      }

      return ret;
    }

    int ExpireDefine::serialize_exptime_app_key(const int32_t days_secs, const int32_t hours_secs,
        const int32_t hash_mod, const int32_t file_type, const std::string &file_name,
        KvKey *key, char *data, const int32_t size)
    {
      int ret = (hash_mod >= 0 && hash_mod < ExpireDefine::HASH_BUCKET_NUM && days_secs >= 0 && hours_secs >= 0
          && key != NULL &&  data != NULL ) ? TFS_SUCCESS : TFS_ERROR;
      int64_t pos = 0;
      if(TFS_SUCCESS == ret)
      {
        //key type ori tfs key
        data[pos] = KvKey::KEY_TYPE_EXPTIME_APPKEY;
        pos += 1;
      }

      //days_secs
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::int32_to_char(data + pos, size - pos, days_secs);
        pos = pos + 4;
      }

      //hash_mod
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::int32_to_char(data + pos, size - pos, hash_mod);
        pos = pos + 4;
      }
      //DELIMITER split pk . sk
      if (TFS_SUCCESS == ret && (pos + 1) < size)
      {
        data[pos] = KvKey::DELIMITER;
        pos ++;
      }
      else
      {
        ret = TFS_ERROR;
      }

      //hours_secs
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::int32_to_char(data + pos, size - pos, hours_secs);
        pos = pos + 4;
      }

      //custom file is 2; usual file is 1;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::int32_to_char(data + pos, size - pos, file_type);
        pos = pos + 4;
      }

      //file name
      if (TFS_SUCCESS == ret)
      {
        if (file_type == FILE_TYPE_RAW_TFS|| file_type == FILE_TYPE_CUSTOM_TFS)
        {
          ret = Serialization::set_string(data, size, pos, file_name);
        }
      }

      if (TFS_SUCCESS == ret)
      {
        key->key_ = data;
        key->key_size_ = pos;
        key->key_type_ = KvKey::KEY_TYPE_EXPTIME_APPKEY;
      }
      return ret;
    }

    int ExpireDefine::deserialize_exptime_app_key(const char *data, const int32_t size,
        int32_t* p_days_secs, int32_t* p_hours_secs,
        int32_t* p_hash_mod, int32_t *p_file_type, std::string *file_name)
    {
      int ret = (data != NULL && size > 0 && p_days_secs != NULL && p_hours_secs != NULL &&
          p_hash_mod != NULL && p_file_type != NULL && NULL != file_name) ? TFS_SUCCESS : TFS_ERROR;

      if (TFS_SUCCESS == ret)
      {
        int64_t pos = 0;
        //key type ori tfs key
        if (pos + 1 <= size)
        {
          pos++;
        }
        else
        {
          ret = TFS_ERROR;
        }

        //days_secs
        if (TFS_SUCCESS == ret)
        {
          ret = Serialization::char_to_int32(data + pos, size - pos, *p_days_secs);
          pos = pos + 4;
        }

        //hash_mod
        if (TFS_SUCCESS == ret)
        {
          ret = Serialization::char_to_int32(data + pos, size - pos, *p_hash_mod);
          pos = pos + 4;
        }

        //KvKey::DELIMITER
        if (pos + 1 <= size)
        {
          pos++;
        }
        else
        {
          ret = TFS_ERROR;
        }

        //days_secs
        if (TFS_SUCCESS == ret)
        {
          ret = Serialization::char_to_int32(data + pos, size - pos, *p_hours_secs);
          pos = pos + 4;
        }

        //file_type
        if (TFS_SUCCESS == ret)
        {
          ret = Serialization::char_to_int32(data + pos, size - pos, *p_file_type);
          pos = pos + 4;
        }

        //file_name
        if (TFS_SUCCESS == ret)
        {
          ret = Serialization::get_string(data, size, pos, *file_name);
        }
      }
      return ret;
    }

    int ExpireDefine::serialize_name_expiretime_value(const int32_t invalid_time,
          common::KvMemValue *value, char *data, const int32_t size)
    {
      int ret = (value != NULL &&  data != NULL ) ? TFS_SUCCESS : TFS_ERROR;
      int64_t pos = 0;

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::int32_to_char(data + pos, size - pos, invalid_time);
        pos = pos + 4;
      }

      if (TFS_SUCCESS == ret)
      {
        value->set_data(data, pos);
      }
      return ret;
    }

    int ExpireDefine::deserialize_name_expiretime_value(const char *data, const int32_t size,
          int32_t* invalid_time)
    {
      int ret = (data != NULL && size > 0 && invalid_time != NULL) ? TFS_SUCCESS : TFS_ERROR;
      int pos = 0;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::char_to_int32(data + pos, size - pos, *invalid_time);
        pos = pos + 4;
      }

      return ret;
    }

    int ExpireDefine::serialize_es_stat_key(const uint64_t local_ipport, const int32_t num_es,
        const int32_t task_time, const int32_t hash_bucket_num, const int64_t sum_file_num,
        KvKey *key, char *data, const int32_t size)
    {
      int ret = (local_ipport > 0 && hash_bucket_num < ExpireDefine::HASH_BUCKET_NUM
          && key != NULL &&  data != NULL ) ? TFS_SUCCESS : TFS_ERROR;
      int64_t pos = 0;
      //key type ori tfs note key
      if (TFS_SUCCESS == ret && (pos + 1) < size)
      {
        data[pos++] = KvKey::KEY_TYPE_ES_STAT;
      }
      else
      {
        ret = TFS_ERROR;
      }

      //local_ipport
      if (TFS_SUCCESS == ret && (pos + 8) < size)
      {
        ret = Serialization::int64_to_char(data + pos, size - pos, local_ipport);
        pos = pos + 8;
      }

      //num_es
      if (TFS_SUCCESS == ret && (pos + 4) < size)
      {
        ret = Serialization::int32_to_char(data + pos, size - pos, num_es);
        pos = pos + 4;
      }

      //task_time
      if (TFS_SUCCESS == ret && (pos + 4) < size)
      {
        ret = Serialization::int32_to_char(data + pos, size - pos, task_time);
        pos = pos + 4;
      }

      //DELIMITER split pk . sk
      if (TFS_SUCCESS == ret && (pos + 1) < size)
      {
        data[pos++] = KvKey::DELIMITER;
      }
      else
      {
        ret = TFS_ERROR;
      }

      //hash_bucket_num
      if (TFS_SUCCESS == ret && (pos + 4) < size)
      {
        ret = Serialization::int32_to_char(data + pos, size - pos, hash_bucket_num);
        pos = pos + 4;
      }

      //sum_file_num
      if (TFS_SUCCESS == ret && (pos + 8) < size)
      {
        ret = Serialization::int64_to_char(data + pos, size - pos, sum_file_num);
        pos = pos + 8;
      }


      if (TFS_SUCCESS == ret)
      {
        key->key_ = data;
        key->key_size_ = pos;
        key->key_type_ = KvKey::KEY_TYPE_ES_STAT;
      }
      return ret;
    }

    int ExpireDefine::deserialize_es_stat_key(const char *data, const int32_t size,
        uint64_t *es_id, int32_t* es_num, int32_t* task_time, int32_t *hash_bucket_num, int64_t *sum_file_num)
    {
      int ret = (data != NULL && size > 0 && es_id != NULL && es_num != NULL &&
          task_time != NULL && hash_bucket_num != NULL && sum_file_num != NULL) ? TFS_SUCCESS : TFS_ERROR;

      if (TFS_SUCCESS == ret)
      {
        int64_t pos = 0;
        //key type ori tfs key
        if (pos + 1 <= size)
        {
          pos++;
        }
        else
        {
          ret = TFS_ERROR;
        }

        //es_id
        if (TFS_SUCCESS == ret)
        {
          ret = Serialization::char_to_int64(data + pos, size - pos, *(reinterpret_cast<int64_t*>(es_id)));
          pos = pos + 8;
        }

        //es_num
        if (TFS_SUCCESS == ret)
        {
          ret = Serialization::char_to_int32(data + pos, size - pos, *es_num);
          pos = pos + 4;
        }

        //task_time
        if (TFS_SUCCESS == ret)
        {
          ret = Serialization::char_to_int32(data + pos, size - pos, *task_time);
          pos = pos + 4;
        }

        //KvKey::DELIMITER
        if (pos + 1 <= size)
        {
          pos++;
        }
        else
        {
          ret = TFS_ERROR;
        }

        //hash_bucket_num
        if (TFS_SUCCESS == ret)
        {
          ret = Serialization::char_to_int32(data + pos, size - pos, *hash_bucket_num);
          pos = pos + 4;
        }

        //sum_file_num
        if (TFS_SUCCESS == ret)
        {
          ret = Serialization::char_to_int64(data + pos, size - pos, *sum_file_num);
          pos = pos + 8;
        }
      }
      return ret;
    }

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
      if (TFS_SUCCESS == iret)
      {
        iret = Serialization::get_int32(data, data_len, pos, &task_status_);
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
      if (TFS_SUCCESS == iret)
      {
        iret = Serialization::set_int32(data, data_len, pos, task_status_);
      }
      return iret;
    }

    int64_t ExpServerBaseInformation::length() const
    {
      return INT64_SIZE * 3 + INT_SIZE;
    }

    //ExpRootServerLease
    bool ExpRootServerLease::has_valid_lease(const int64_t now)
    {
      return ((lease_id_ != INVALID_LEASE_ID) && lease_expired_time_ >= now);
    }

    bool ExpRootServerLease::renew(const int32_t step, const int64_t now)
    {
      bool bret = has_valid_lease(now);
      if (bret)
      {
        lease_expired_time_ = now + step;
      }
      return bret;
    }

    int ExpRootServerLease::deserialize(const char* data, const int64_t data_len, int64_t& pos)
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

    int ExpRootServerLease::serialize(char* data, const int64_t data_len, int64_t& pos) const
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

    int64_t ExpRootServerLease::length() const
    {
      return INT64_SIZE + INT64_SIZE;
    }

    //ExpServer
    int ExpServer::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t iret = lease_.deserialize(data, data_len, pos);

      if (TFS_SUCCESS == iret)
      {
        iret = base_info_.deserialize(data, data_len, pos);
      }
      return iret;
    }

    int ExpServer::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t iret = lease_.serialize(data, data_len, pos);

      if (TFS_SUCCESS == iret)
      {
        iret = base_info_.serialize(data, data_len, pos);
      }
      return iret;
    }

    int64_t ExpServer::length() const
    {
      return lease_.length() + base_info_.length();
    }

    // ExpTask

    ExpireTaskInfo::ExpireTaskInfo()
    {}

    ExpireTaskInfo::ExpireTaskInfo(int32_t alive_total, int32_t assign_no,
          int32_t spec_time, int32_t status, int32_t note_interval, ExpireTaskType type)
      :alive_total_(alive_total), assign_no_(assign_no), spec_time_(spec_time),
      status_(status), note_interval_(note_interval), type_(type){}

    int64_t ExpireTaskInfo::length() const
    {
      int64_t len = INT_SIZE * 6;
      return len;
    }

    int ExpireTaskInfo::serialize(char *data, const int64_t data_len, int64_t &pos) const
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, alive_total_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, assign_no_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, spec_time_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, status_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, note_interval_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, type_);
      }
      return ret;
    }

    int ExpireTaskInfo::deserialize(const char *data, const int64_t data_len, int64_t &pos)
    {
      int32_t iret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        iret = Serialization::get_int32(data, data_len, pos, &alive_total_);
      }
      if (TFS_SUCCESS == iret)
      {
        iret = Serialization::get_int32(data, data_len, pos, &assign_no_);
      }
      if (TFS_SUCCESS == iret)
      {
        iret = Serialization::get_int32(data, data_len, pos, &spec_time_);
      }
      if (TFS_SUCCESS == iret)
      {
        iret = Serialization::get_int32(data, data_len, pos, &status_);
      }
      if (TFS_SUCCESS == iret)
      {
        iret = Serialization::get_int32(data, data_len, pos, &note_interval_);
      }
      if (TFS_SUCCESS == iret)
      {
        iret = Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&type_));
      }
      return iret;
    }

    bool ExpireTaskInfo::operator < (const ExpireTaskInfo& rh) const
    {
      if (type_ < rh.type_) return true;
      if (type_ > rh.type_) return false;
      if (spec_time_ < rh.spec_time_) return true;
      if (spec_time_ > rh.spec_time_) return  false;
      if (assign_no_ < rh.assign_no_) return true;
      if (assign_no_ > rh.assign_no_) return false;
      if (alive_total_ < rh.alive_total_) return true;
      if (alive_total_ > rh.alive_total_) return false;
      return false;
    }


    int ExpireDefine::transfer_time(const int32_t time, int32_t *p_days_secs, int32_t *p_hours_secs)
    {
      int ret = TFS_SUCCESS;
      if (p_days_secs == NULL || p_hours_secs == NULL)
      {
        return TFS_ERROR;
      }
      else
      {
        int32_t days_int= time / 60 / 60 / 24;
        *p_days_secs = days_int * 60 * 60 * 24;
        *p_hours_secs = time - *p_days_secs;
      }

      return ret;
    }

    //ServerExpireTask
    //ServerExpireTask::ServerExpireTask():server_id_(0){}

    //ServerExpireTask::ServerExpireTask(const uint64_t id, const ExpireTaskInfo& t)
        //:server_id_(id), task_(t){}

    int64_t ServerExpireTask::length() const
    {
      int64_t len = INT64_SIZE + task_.length();
      return len;
    }

    int ServerExpireTask::serialize(char *data, const int64_t data_len, int64_t &pos) const
    {
      int iret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        iret = Serialization::set_int64(data, data_len, pos, server_id_);
      }
      if (TFS_SUCCESS == iret)
      {
        iret = task_.serialize(data, data_len, pos);
      }
      return iret;
    }

    int ServerExpireTask::deserialize(const char *data, const int64_t data_len, int64_t &pos)
    {
      int32_t iret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        iret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&server_id_));
      }
      if (TFS_SUCCESS == iret)
      {
        iret = task_.deserialize(data, data_len, pos);
      }
      return iret;
    }

  }//common end
}// tfs end

