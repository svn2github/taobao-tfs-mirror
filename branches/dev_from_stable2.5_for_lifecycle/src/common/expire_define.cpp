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
    const char ExpireDefine::DELIMITER_EXPIRE = 10;
    const char ExpireDefine::KEY_TYPE_ORI_TARGET = 11;
    const char ExpireDefine::KEY_TYPE_ORI_NAME = 12;
    const char ExpireDefine::KEY_TYPE_ORI_NOTE = 13;
    const char ExpireDefine::KEY_TYPE_S3 = 14;
    const int32_t ExpireDefine::HASH_BUCKET_NUM = 10243;

    int ExpireDefine::dserialize_ori_tfs_target_key(const char *data, const int32_t size,
        int32_t* p_days_secs, int32_t* p_hours_secs,
        int32_t* p_hash_mod, int32_t *p_file_type, std::string *file_name)
    {
      int ret = (data != NULL && size > 0 && p_days_secs != NULL && p_hours_secs != NULL &&
          p_hash_mod != NULL && p_file_type != NULL && file_name->size() > 0) ? TFS_SUCCESS : TFS_ERROR;

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
        if (TFS_SUCCESS == ret && (pos + 4) <= size)
        {
          ret = Serialization::char_to_int32(data + pos, size - pos, *p_days_secs);
          pos = pos + 4;
        }

        //hash_mod
        if (TFS_SUCCESS == ret && (pos + 4) <= size)
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
        if (TFS_SUCCESS == ret && (pos + 4) <= size)
        {
          ret = Serialization::char_to_int32(data + pos, size - pos, *p_hours_secs);
          pos = pos + 4;
        }

        //file_type
        if (TFS_SUCCESS == ret && (pos + 4) <= size)
        {
          ret = Serialization::char_to_int32(data + pos, size - pos, *p_file_type);
          pos = pos + 4;
        }

        //file_name
        int64_t len = 0;
        if (TFS_SUCCESS == ret)
        {
          ret = Serialization:: get_string(data + pos, size - pos, len, (*file_name));
        }
        pos = pos + len;

      }
      return ret;
    }

    int ExpireDefine::serialize_ori_tfs_target_key(const int32_t days_secs, const int32_t hours_secs,
            const int32_t hash_mod, const int32_t file_type, const std::string &file_name,
            const int32_t key_type, KvKey *key, char *data, const int32_t size)
    {
      int ret = (hash_mod >= 0 && hash_mod < 10243 && days_secs > 0 && hours_secs > 0
                 && key != NULL &&  data != NULL ) ? TFS_SUCCESS : TFS_ERROR;
      int64_t pos = 0;
      if(TFS_SUCCESS == ret)
      {
        //key type ori tfs key
        if (TFS_SUCCESS == ret && (pos + 1) < size)
        {
          data[pos++] = ExpireDefine::KEY_TYPE_ORI_TARGET;
        }
        else
        {
          ret = TFS_ERROR;
        }

        //days_secs
        if (TFS_SUCCESS == ret && (pos + 4) < size)
        {
          ret = Serialization::int32_to_char(data + pos, size, days_secs);
          pos = pos + 4;
        }

        //hash_mod
        if (TFS_SUCCESS == ret && (pos + 4) < size)
        {
          ret = Serialization::int32_to_char(data + pos, size, hash_mod);
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

        //hours_secs
        if (TFS_SUCCESS == ret && (pos + 4) < size)
        {
          ret = Serialization::int32_to_char(data + pos, size, hours_secs);
          pos = pos + 4;
        }

        //custom file is 2; usual file is 1;
        if (TFS_SUCCESS == ret && (pos + 4) < size)
        {
          ret = Serialization::int32_to_char(data + pos, size, file_type);
          pos = pos + 4;
        }

        //file name
        int64_t len = 0;
        if (file_type == 1 || file_type == 2 )
        {
          ret = Serialization::set_string(data + pos, size, len, file_name);
          pos = pos + len;
        }
      }

      if (TFS_SUCCESS == ret)
      {
        key->key_ = data;
        key->key_size_ = pos;
        key->key_type_ = key_type;
      }
      return ret;
    }

    int ExpireDefine::serialize_ori_tfs_note_key(const uint64_t local_ipport, const int32_t num_es,
            const int32_t task_time, const int32_t hash_bucket_num, const int64_t sum_file_num,
            const int32_t key_type, KvKey *key, char *data, const int32_t size)
    {
      int ret = (local_ipport > 0 && hash_bucket_num < 10243
                 && key != NULL &&  data != NULL ) ? TFS_SUCCESS : TFS_ERROR;
      int64_t pos = 0;
      if(TFS_SUCCESS == ret)
      {
        //key type ori tfs note key
        if (TFS_SUCCESS == ret && (pos + 1) < size)
        {
          data[pos++] = ExpireDefine::KEY_TYPE_ORI_NOTE;
        }
        else
        {
          ret = TFS_ERROR;
        }

        //local_ipport
        if (TFS_SUCCESS == ret && (pos + 8) < size)
        {
          ret = Serialization::int64_to_char(data + pos, size, local_ipport);
          pos = pos + 8;
        }

        //num_es
        if (TFS_SUCCESS == ret && (pos + 4) < size)
        {
          ret = Serialization::int32_to_char(data + pos, size, num_es);
          pos = pos + 4;
        }

        //task_time
        if (TFS_SUCCESS == ret && (pos + 4) < size)
        {
          ret = Serialization::int32_to_char(data + pos, size, task_time);
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
          ret = Serialization::int32_to_char(data + pos, size, hash_bucket_num);
          pos = pos + 4;
        }

        //sum_file_num
        if (TFS_SUCCESS == ret && (pos + 8) < size)
        {
          ret = Serialization::int64_to_char(data + pos, size, sum_file_num);
          pos = pos + 8;
        }


      }

      if (TFS_SUCCESS == ret)
      {
        key->key_ = data;
        key->key_size_ = pos;
        key->key_type_ = key_type;
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

