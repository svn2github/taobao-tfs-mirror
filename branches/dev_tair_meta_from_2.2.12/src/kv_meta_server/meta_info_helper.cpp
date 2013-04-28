/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: meta_server_service.h 49 2011-08-08 09:58:57Z nayan@taobao.com $
 *
 * Authors:
 *   daoan <daoan@taobao.com>
 *      - initial release
 *
 */


#include "meta_info_helper.h"
#include <malloc.h>
#include "tairengine_helper.h"
#include "common/session_util.h"
using namespace std;
namespace tfs
{
  using namespace common;
  namespace kvmetaserver
  {
    const int TFS_INFO_BUFF_SIZE = 128;
    const int VALUE_BUFF_SIZE = 1024*1024;
    const int KV_VALUE_BUFF_SIZE = 1024*1024;
    const int32_t KEY_BUFF_SIZE = 512 + 8 + 8;
    const int32_t SCAN_LIMIT = 500;
    const int32_t MESS_LIMIT = 10;
    const int64_t INT64_INFI = 0x7FFFFFFFFFFFFFFF;
    const char DELIMITER_1 = 7;
    const char DELIMITER_2 = 2;
    enum
    {
      MODE_REQ_LIMIT = 1,
      MODE_KV_LIMIT = 2,
    };
    enum
    {
      CMD_RANGE_ALL = 1,
      CMD_RANGE_VALUE_ONLY,
      CMD_RANGE_KEY_ONLY,
    };
    MetaInfoHelper::MetaInfoHelper()
    {
      kv_engine_helper_ = new TairEngineHelper();
    }

    MetaInfoHelper::~MetaInfoHelper()
    {
      delete kv_engine_helper_;
      kv_engine_helper_ = NULL;
    }

    int MetaInfoHelper::init()
    {
      int ret = TFS_ERROR;
      if (NULL != kv_engine_helper_)
      {
        ret = kv_engine_helper_->init();
      }
      return ret;
    }

    int MetaInfoHelper::serialize_key(const std::string &bucket_name, const std::string &file_name,
                                      const int64_t offset, KvKey *key, char *key_buff, const int32_t buff_size, int32_t key_type)
    {
      int ret = (bucket_name.size() > 0 && file_name.size() > 0 && offset >= 0
                 && key != NULL &&  key_buff != NULL ) ? TFS_SUCCESS : TFS_ERROR;
      int64_t pos = 0;
      if(TFS_SUCCESS == ret)
      {
        //bucket
        int64_t bucket_name_size = static_cast<int64_t>(bucket_name.size());
        if(pos + bucket_name_size < buff_size)
        {
          memcpy(key_buff + pos, bucket_name.data(), bucket_name.size());
          pos += bucket_name_size;
        }
        else
        {
          ret = TFS_ERROR;
        }
        //DELIMITER
        if (TFS_SUCCESS == ret && (pos + 1) < buff_size)
        {
          key_buff[pos++] = KvKey::DELIMITER;
        }
        else
        {
          ret = TFS_ERROR;
        }
        //filename
        int64_t file_name_size = static_cast<int64_t>(file_name.size());
        if(pos + file_name_size < buff_size)
        {
          memcpy(key_buff + pos, file_name.data(), file_name.size());
          pos += file_name_size;
        }
        else
        {
          ret = TFS_ERROR;
        }
        //DELIMITER
        if (TFS_SUCCESS == ret && (pos + 1) < buff_size)
        {
          key_buff[pos++] = KvKey::DELIMITER;
        }
        else
        {
          ret = TFS_ERROR;
        }
        //version
        int64_t version = 0;
        if (TFS_SUCCESS == ret && (pos + 8) < buff_size)
        {
          ret = Serialization::int64_to_char(key_buff + pos, buff_size, version);
          pos = pos + 8;
        }
        //DELIMITER
        if (TFS_SUCCESS == ret && (pos + 1) < buff_size)
        {
          key_buff[pos++] = KvKey::DELIMITER;
        }
        else
        {
          ret = TFS_ERROR;
        }
        //offset
        if (TFS_SUCCESS == ret && (pos + 8) < buff_size)
        {
          ret = Serialization::int64_to_char(key_buff + pos, buff_size, offset);
          pos = pos + 8;
        }
      }

      if (TFS_SUCCESS == ret)
      {
        key->key_ = key_buff;
        key->key_size_ = pos;
        key->key_type_ = key_type;
      }
      return ret;
    }

    int MetaInfoHelper::serialize_key_ex(const std::string &file_name, const int64_t offset, KvKey *key,
        char *key_buff, const int32_t buff_size, int32_t key_type)
    {
      TBSYS_LOG(DEBUG, "part offset: %"PRI64_PREFIX"d", offset);

      int ret = (file_name.size() > 0 && offset >= 0
                 && key != NULL &&  key_buff != NULL ) ? TFS_SUCCESS : TFS_ERROR;
      int64_t pos = 0;
      if (TFS_SUCCESS == ret)
      {
        //filename
        int64_t file_name_size = static_cast<int64_t>(file_name.size());
        if(pos + file_name_size < buff_size)
        {
          memcpy(key_buff + pos, file_name.data(), file_name.size());
          pos += file_name_size;
        }
        else
        {
          ret = TFS_ERROR;
        }
        //DELIMITER
        if (TFS_SUCCESS == ret && (pos + 1) < buff_size)
        {
          key_buff[pos++] = KvKey::DELIMITER;
        }
        else
        {
          ret = TFS_ERROR;
        }
        //version
        int64_t version = 0;
        if (TFS_SUCCESS == ret && (pos + 8) < buff_size)
        {
          ret = Serialization::int64_to_char(key_buff + pos, buff_size, version);
          pos = pos + 8;
        }
        //DELIMITER
        if (TFS_SUCCESS == ret && (pos + 1) < buff_size)
        {
          key_buff[pos++] = KvKey::DELIMITER;
        }
        else
        {
          ret = TFS_ERROR;
        }
        //offset
        if (TFS_SUCCESS == ret && (pos + 8) < buff_size)
        {
          ret = Serialization::int64_to_char(key_buff + pos, buff_size, offset);
          pos = pos + 8;
        }
      }

      if (TFS_SUCCESS == ret)
      {
        key->key_ = key_buff;
        key->key_size_ = pos;
        key->key_type_ = key_type;
      }
      return ret;
    }

    int MetaInfoHelper::serialize_upload_key(const std::string &file_name, const std::string &upload_id,
        const int64_t offset, const int64_t part_num, KvKey *key, char *key_buff,
        const int32_t buff_size, int32_t key_type)
    {
      int ret = (file_name.size() > 0 && upload_id.size() > 0 && offset >= 0
          && part_num >= 0 && key != NULL &&  key_buff != NULL ) ? TFS_SUCCESS : TFS_ERROR;
      int64_t pos = 0;
      if (TFS_SUCCESS == ret)
      {
        //multipart_upload_key
        if (pos + 1 < buff_size)
        {
          key_buff[pos++] = MULTIPART_UPLOAD_KEY;
        }
        else
        {
          ret = TFS_ERROR;
        }

        //upload_id
        int64_t upload_id_size = static_cast<int64_t>(upload_id.size());
        if (TFS_SUCCESS == ret && pos + upload_id_size < buff_size)
        {
          memcpy(key_buff + pos, upload_id.data(), upload_id_size);
          pos += upload_id_size;
        }
        else
        {
          ret = TFS_ERROR;
        }
        //DELIMITER
        if (TFS_SUCCESS == ret && (pos + 1) < buff_size)
        {
          key_buff[pos++] = KvKey::DELIMITER;
        }
        else
        {
          ret = TFS_ERROR;
        }
        //filename
        int64_t file_name_size = static_cast<int64_t>(file_name.size());
        if (TFS_SUCCESS == ret && pos + file_name_size < buff_size)
        {
          memcpy(key_buff + pos, file_name.data(), file_name_size);
          pos += file_name_size;
        }
        else
        {
          ret = TFS_ERROR;
        }
        //DELIMITER
        if (TFS_SUCCESS == ret && (pos + 1) < buff_size)
        {
          key_buff[pos++] = KvKey::DELIMITER;
        }
        else
        {
          ret = TFS_ERROR;
        }
        //version
        int64_t version = 0;
        if (TFS_SUCCESS == ret && (pos + 8) < buff_size)
        {
          ret = Serialization::int64_to_char(key_buff + pos, buff_size, version);
          pos = pos + 8;
        }
        //DELIMITER
        if (TFS_SUCCESS == ret && (pos + 1) < buff_size)
        {
          key_buff[pos++] = KvKey::DELIMITER;
        }
        else
        {
          ret = TFS_ERROR;
        }
        //part_num
        if (TFS_SUCCESS == ret && (pos + 8) < buff_size)
        {
          ret = Serialization::int64_to_char(key_buff + pos, buff_size, part_num);
          pos = pos + 8;
        }
        //DELIMITER
        if (TFS_SUCCESS == ret && (pos + 1) < buff_size)
        {
          key_buff[pos++] = KvKey::DELIMITER;
        }
        else
        {
          ret = TFS_ERROR;
        }
        //offset
        if (TFS_SUCCESS == ret && (pos + 8) < buff_size)
        {
          ret = Serialization::int64_to_char(key_buff + pos, buff_size, offset);
          pos = pos + 8;
        }
      }

      if (TFS_SUCCESS == ret)
      {
        key->key_ = key_buff;
        key->key_size_ = pos;
        key->key_type_ = key_type;
      }
      return ret;
    }


    /*----------------------------object part-----------------------------*/
    int MetaInfoHelper::head_object(const string &bucket_name,
        const string &file_name, ObjectInfo *object_info_zero)
    {
      int ret = (bucket_name.size() > 0 && file_name.size() > 0 &&
          NULL != object_info_zero) ? TFS_SUCCESS : TFS_ERROR;

      if (TFS_SUCCESS == ret)
      {
        ret = get_object_part(bucket_name, file_name, 0, object_info_zero, NULL);
      }

      return ret;
    }

    int MetaInfoHelper::put_object_ex(const string &bucket_name, const string &file_name,
        const int64_t offset, const ObjectInfo &object_info, const int64_t lock_version)
    {
      //op key
      int ret = TFS_SUCCESS;
      char *key_buff = (char*)malloc(KEY_BUFF_SIZE);
      char *value_buff = (char*)malloc(VALUE_BUFF_SIZE);

      if(NULL == key_buff || NULL == value_buff)
      {
        ret = TFS_ERROR;
      }
      KvKey key;
      if (TFS_SUCCESS == ret)
      {
         ret = serialize_key(bucket_name, file_name, offset, &key, key_buff, KEY_BUFF_SIZE, KvKey::KEY_TYPE_OBJECT);
      }

      //op value
      int64_t pos = 0;
      if (TFS_SUCCESS == ret)
      {
        ret = object_info.serialize(value_buff, VALUE_BUFF_SIZE, pos);
      }

      KvMemValue kv_value;
      if (TFS_SUCCESS == ret)
      {
        kv_value.set_data(value_buff, pos);
        ret = kv_engine_helper_->put_key(key, kv_value, lock_version);
      }

      if (NULL != value_buff)
      {
        free(value_buff);
        value_buff = NULL;
      }

      if (NULL != key_buff)
      {
        free(key_buff);
        key_buff = NULL;
      }

      return ret;
    }

    int MetaInfoHelper::put_object_part(const string &bucket_name, const string &file_name,
        const ObjectInfo &object_info)
    {
      int ret = TFS_SUCCESS;
      ObjectInfo object_info_part;
      int64_t offset = 0;
      for (size_t i = 0; i < object_info.v_tfs_file_info_.size(); i++)
      {
        offset = object_info.v_tfs_file_info_[i].offset_;
        // skip object info zero
        if (0 == offset)
        {
          continue;
        }

        object_info_part.v_tfs_file_info_.clear();
        object_info_part.v_tfs_file_info_.push_back(object_info.v_tfs_file_info_[i]);

        TBSYS_LOG(DEBUG, "will put object part, bucekt_name: %s, object_name: %s",
            bucket_name.c_str(), file_name.c_str());
        object_info_part.dump();
        ret = put_object_ex(bucket_name, file_name, offset, object_info_part, 0);

        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "put bucket_name: %s, file_name: %s, offset: %"PRI64_PREFIX"d part fail",
              bucket_name.c_str(), file_name.c_str(), offset);
          break;
        }
      }

      return ret;
    }

    int MetaInfoHelper::put_object_zero(const string &bucket_name,
        const string &file_name, ObjectInfo *object_info_zero,
        int64_t *offset, const int64_t length, int64_t version,
        const bool is_append)
    {
      int ret = TFS_SUCCESS;
      int32_t retry = VERSION_ERROR_RETRY_COUNT;
      bool is_old_data = false;
      //-5 means transfer data from mysql
      if (-5 == object_info_zero->meta_info_.max_tfs_file_size_)
      {
        is_old_data = true;
        object_info_zero->meta_info_.max_tfs_file_size_ = 2048;
      }
      do
      {
        if (!is_old_data)
        {
          object_info_zero->meta_info_.modify_time_ = static_cast<int64_t>(time(NULL));
        }

        TBSYS_LOG(DEBUG, "will put object info zero, bucket: %s, object: %s",
            bucket_name.c_str(), file_name.c_str());
        object_info_zero->dump();
        ret = put_object_ex(bucket_name, file_name, 0, *object_info_zero, version);
        if (EXIT_KV_RETURN_VERSION_ERROR == ret)
        {
          TBSYS_LOG(INFO, "put object zero version conflict, bucket: %s, object: %s",
              bucket_name.c_str(), file_name.c_str());
          ObjectInfo curr_object_info_zero;
          int tmp_ret = get_object_part(bucket_name, file_name, 0, &curr_object_info_zero, &version);

          if (TFS_SUCCESS != tmp_ret)
          {
            TBSYS_LOG(WARN, "get object zero fail, ret: %d, bucket: %s, object: %s",
                tmp_ret, bucket_name.c_str(), file_name.c_str());
            break;
          }
          else
          {
            if (is_append)
            {
              // assumption failed
              if (curr_object_info_zero.meta_info_.big_file_size_ > 0)
              {
                object_info_zero->v_tfs_file_info_ = curr_object_info_zero.v_tfs_file_info_;
              }
              // get real offset
              *offset = curr_object_info_zero.meta_info_.big_file_size_;
              object_info_zero->meta_info_ = curr_object_info_zero.meta_info_;
              object_info_zero->meta_info_.big_file_size_ += length;
            }
            else
            {
              if (0 != *offset)
              {
                object_info_zero->v_tfs_file_info_ = curr_object_info_zero.v_tfs_file_info_;
                object_info_zero->meta_info_ = curr_object_info_zero.meta_info_;
              }
              else
              {
                if (!curr_object_info_zero.v_tfs_file_info_.empty()
                    && !object_info_zero->v_tfs_file_info_.empty())
                {
                  TBSYS_LOG(WARN, "object info zero conflict found");
                }
              }

              int64_t curr_file_size = curr_object_info_zero.meta_info_.big_file_size_;
              object_info_zero->meta_info_.big_file_size_ =
                  curr_file_size > (*offset + length) ? curr_file_size : (*offset + length);
            }
            if (!object_info_zero->has_customize_info_)
            {
              object_info_zero->has_customize_info_ = curr_object_info_zero.has_customize_info_;
              object_info_zero->customize_info_ = curr_object_info_zero.customize_info_;
            }
          }
        }
      }while (retry-- && EXIT_KV_RETURN_VERSION_ERROR == ret);

      return ret;
    }

    int MetaInfoHelper::put_object(const std::string &bucket_name,
        const std::string &file_name,
        ObjectInfo &object_info, const UserInfo &user_info)
    {
      int ret = (bucket_name.size() > 0 && file_name.size() > 0) ? TFS_SUCCESS : TFS_ERROR;

      // check bucket whether exist
      if (TFS_SUCCESS == ret)
      {
        BucketMetaInfo bucket_meta_info;
        ret = head_bucket(bucket_name, &bucket_meta_info, NULL);
        TBSYS_LOG(DEBUG, "head bucket, bucket: %s, object: %s, ret: %d",
            bucket_name.c_str(), file_name.c_str(), ret);
      }

      ObjectInfo object_info_zero;
      int64_t offset = 0;
      if (object_info.v_tfs_file_info_.size() > 0)
      {
        offset = object_info.v_tfs_file_info_.front().offset_;
      }

      int64_t version = MAX_VERSION;
      bool is_append = (-1 == offset);
      // assume this is object info zero
      if (is_append)
      {
        offset = 0;
        if (!object_info.v_tfs_file_info_.empty())
        {
          object_info.v_tfs_file_info_[0].offset_ = 0;
        }
      }

      /*trick for transfer data*/
      if (-5 == object_info.meta_info_.max_tfs_file_size_)
      {
        object_info_zero.meta_info_.modify_time_ = object_info.meta_info_.modify_time_;
        object_info_zero.meta_info_.max_tfs_file_size_ = -5;
      }

      if (TFS_SUCCESS == ret)
      {
        uint64_t length = 0;
        for (size_t i = 0; i < object_info.v_tfs_file_info_.size(); i++)
        {
          length += object_info.v_tfs_file_info_[i].file_size_;
        }

        TBSYS_LOG(DEBUG, "will put object, bucekt: %s, object: %s, "
            "offset: %"PRI64_PREFIX"d, length: %"PRI64_PREFIX"d",
            bucket_name.c_str(), file_name.c_str(), offset, length);

        object_info_zero.has_meta_info_ = true;
        object_info_zero.meta_info_.big_file_size_ = offset + length;
        if (0 == offset)
        {
          object_info_zero.meta_info_.owner_id_ = user_info.owner_id_;
          if (object_info.meta_info_.max_tfs_file_size_ > PARTNUM_BASE)
          {
            object_info_zero.meta_info_.max_tfs_file_size_ = object_info.meta_info_.max_tfs_file_size_;
          }
          // identify old data
          if (-5 == object_info.meta_info_.max_tfs_file_size_)
          {
            object_info_zero.meta_info_.create_time_ = object_info.meta_info_.create_time_;
          }
          else
          {
            object_info_zero.meta_info_.create_time_ = static_cast<int64_t>(time(NULL));
          }

          //TODO: check this
          object_info_zero.has_customize_info_ = object_info.has_customize_info_;

          object_info_zero.customize_info_ = object_info.customize_info_;

          if (!object_info.v_tfs_file_info_.empty())
          {
            object_info_zero.v_tfs_file_info_.push_back(object_info.v_tfs_file_info_.front());
          }
        }

        //check whether overlap
        if (!is_append)
        {
          //check offset & length
          int32_t limit = MAX_LIMIT;
          int32_t res_size = -1;
          vector<KvValue*> kv_value_keys;
          vector<KvValue*> kv_value_values;

          KvKey pkey;
          pkey.key_ = bucket_name.c_str();
          pkey.key_size_ = bucket_name.length();
          pkey.key_type_ = KvKey::KEY_TYPE_BUCKET;

          string tmp_start_key = file_name;
          bool loop = true;
          do
          {
            ret = get_range(pkey, tmp_start_key, 0, limit, &kv_value_keys, &kv_value_values, &res_size);
            if (TFS_SUCCESS == ret)
            {
              for (int i = 0; i < res_size; ++i)
              {
                int64_t tmp_offset = 0;
                int64_t tmp_length = 0;
                string tmp_bucket_name;
                string tmp_object_name;
                string k(kv_value_keys[i]->get_data(), kv_value_keys[i]->get_size());
                string v(kv_value_values[i]->get_data(), kv_value_values[i]->get_size());

                ret = deserialize_key(k.c_str(), k.length(), &tmp_bucket_name, &tmp_object_name, &tmp_offset, &version);
                if (EXIT_MULTIPART_TYPE_KEY == ret)
                {
                  continue;
                }
                else if (TFS_SUCCESS != ret)
                {
                  TBSYS_LOG(ERROR, "deserialize from %s fail", k.c_str());
                }
                else if (tmp_object_name.compare(file_name) > 0)
                {
                  loop = false;
                  break;
                }
                else if (tmp_object_name.compare(file_name) == 0)
                {
                  ObjectInfo object_info;
                  int64_t pos = 0;
                  ret = object_info.deserialize(v.c_str(), v.length(), pos);
                  if (TFS_SUCCESS == ret)
                  {
                    for (size_t i = 0; i < object_info.v_tfs_file_info_.size(); i++)
                    {
                      tmp_length += object_info.v_tfs_file_info_[i].file_size_;
                    }
                  }

                  if (!(offset + static_cast<int64_t>(length) <= tmp_offset || tmp_offset + tmp_length <= offset))
                  {
                    ret = EXIT_OBJECT_OVERLAP;
                    break;
                  }

                  tmp_start_key.assign(k.c_str(), k.length());
                }
              }
            }

            if (res_size < limit || TFS_SUCCESS != ret)
            {
              loop = false;
            }

            //delete for tair
            for (int i = 0; i < res_size; ++i)
            {
              kv_value_keys[i]->free();
              kv_value_values[i]->free();
            }
            kv_value_keys.clear();
            kv_value_values.clear();
          }while (loop);
        }

        if (TFS_SUCCESS == ret)
        {
          ret = put_object_zero(bucket_name, file_name, &object_info_zero,
              &offset, length, version, is_append);
        }

        if (TFS_SUCCESS == ret)
        {
          bool need_put_part = false;
          if (object_info.v_tfs_file_info_.size() > 1
              || 0 != offset)
          {
            need_put_part = true;
          }
          if (need_put_part)
          {
            if (is_append)
            {
              // update offsets
              int64_t tmp_offset = offset;
              for (size_t i = 0; i < object_info.v_tfs_file_info_.size(); i++)
              {
                object_info.v_tfs_file_info_[i].offset_ = tmp_offset;
                tmp_offset += object_info.v_tfs_file_info_[i].file_size_;
              }
            }

            ret = put_object_part(bucket_name, file_name, object_info);
            TBSYS_LOG(DEBUG, "put object part ret: %d, bucekt_name: %s, object_name: %s, "
                "offset: %"PRI64_PREFIX"d, length: %"PRI64_PREFIX"d",
                ret, bucket_name.c_str(), file_name.c_str(), offset, length);
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "put object zero failed, ret: %d, bucekt: %s, object: %s, "
              "offset: %"PRI64_PREFIX"d, length: %"PRI64_PREFIX"d",
              ret, bucket_name.c_str(), file_name.c_str(), offset, length);
        }
      }

      return ret;
    }

    int MetaInfoHelper::get_object_part(const std::string &bucket_name,
        const std::string &file_name,
        const int64_t offset,
        common::ObjectInfo *object_info,
        int64_t *lock_version)
    {
      int ret = (bucket_name.size() > 0 && file_name.size() > 0
          && offset >= 0 && NULL != object_info) ? TFS_SUCCESS : TFS_ERROR;
      //op key
      char *key_buff = NULL;
      key_buff = (char*) malloc(KEY_BUFF_SIZE);
      if (NULL == key_buff)
      {
        ret = TFS_ERROR;
      }
      KvKey key;
      if (TFS_SUCCESS == ret)
      {
        ret = serialize_key(bucket_name, file_name, offset, &key, key_buff, KEY_BUFF_SIZE, KvKey::KEY_TYPE_OBJECT);
      }

      //op value
      KvValue *kv_value = NULL;
      if (TFS_SUCCESS == ret)
      {
        ret = kv_engine_helper_->get_key(key, &kv_value, lock_version);
        if (TFS_SUCCESS != ret)
        {
          ret = EXIT_OBJECT_NOT_EXIST;
        }
      }

      int64_t pos = 0;
      if (TFS_SUCCESS == ret)
      {
        ret = object_info->deserialize(kv_value->get_data(), kv_value->get_size(), pos);
      }

      if (NULL != kv_value)
      {
        kv_value->free();
      }

      if (NULL != key_buff)
      {
        free(key_buff);
        key_buff = NULL;
      }

      return ret;
    }

    int MetaInfoHelper::get_object(const std::string &bucket_name,
        const std::string &file_name, const int64_t offset,
        const int64_t length,
        common::ObjectInfo *object_info, bool* still_have)
    {
      int ret = (bucket_name.size() > 0 && file_name.size() > 0 && length >= 0
          && offset >= 0 && object_info != NULL && still_have != NULL) ? TFS_SUCCESS : TFS_ERROR;

      common::ObjectInfo object_info_zero;
      if (TFS_SUCCESS == ret)
      {
        int64_t version = 0;
        int64_t offset_zero = 0;
        ret = get_object_part(bucket_name, file_name, offset_zero, &object_info_zero, &version);
        *object_info = object_info_zero;
        *still_have = false;
        if (TAIR_RETURN_DATA_NOT_EXIST == ret)
        {
          TBSYS_LOG(ERROR, "object not exist");
          ret = EXIT_OBJECT_NOT_EXIST;
        }
      }
      if (TFS_SUCCESS == ret)
      {
        if (offset > object_info_zero.meta_info_.big_file_size_)
        {
          TBSYS_LOG(ERROR, "req offset is out of big_file_size_");
          ret = EXIT_KV_RETURN_DATA_NOT_EXIST;
        }
      }

      if (TFS_SUCCESS == ret)
      {
        bool is_big_file = false;
        if (object_info_zero.v_tfs_file_info_.size() > 0)
        {
          if (offset + length <= object_info_zero.v_tfs_file_info_[0].file_size_)
          {
            is_big_file = false;
          }
          else
          {
            is_big_file = true;
          }
        }
        else
        {
          is_big_file = true;
        }

        if (is_big_file)//big file
        {
          //op key
          char *start_key_buff = NULL;
          if (TFS_SUCCESS == ret)
          {
            start_key_buff = (char*) malloc(KEY_BUFF_SIZE);
          }
          if (NULL == start_key_buff)
          {
            ret = TFS_ERROR;
          }
          char *end_key_buff = NULL;
          if (ret == TFS_SUCCESS)
          {
            end_key_buff = (char*) malloc(KEY_BUFF_SIZE);
          }
          if (NULL == end_key_buff)
          {
            ret = TFS_ERROR;
          }
          KvKey start_key;
          KvKey end_key;
          int64_t start_offset = offset - MAX_SEGMENT_SIZE > 0 ? offset - MAX_SEGMENT_SIZE : 0;
          int64_t end_offset = offset + length;
          if (TFS_SUCCESS == ret)
          {
            ret = serialize_key(bucket_name, file_name, start_offset,
                &start_key, start_key_buff, KEY_BUFF_SIZE, KvKey::KEY_TYPE_OBJECT);
          }
          if (TFS_SUCCESS == ret)
          {
            ret = serialize_key(bucket_name, file_name, end_offset,
                &end_key, end_key_buff, KEY_BUFF_SIZE, KvKey::KEY_TYPE_OBJECT);
          }

          //op value

          int32_t i;
          int32_t first = 0;
          bool go_on = true;
          short scan_type = CMD_RANGE_VALUE_ONLY;//only scan value
          vector<KvValue*> kv_value_keys;
          vector<KvValue*> kv_value_values;
          object_info->v_tfs_file_info_.clear();
          int32_t valid_result = 0;

          while (go_on)
          {
            int32_t result_size = 0;
            int64_t last_offset = 0;
            ret = kv_engine_helper_->scan_keys(start_key, end_key, SCAN_LIMIT, first,
                &kv_value_keys, &kv_value_values, &result_size, scan_type);
            if (EXIT_KV_RETURN_DATA_NOT_EXIST == ret)
            {//metainfo exist but data not exist
              ret = TFS_SUCCESS;
            }
            for(i = 0; i < result_size; ++i)
            {
              common::ObjectInfo tmp_object_info;
              //value get
              int64_t pos = 0;
              tmp_object_info.deserialize(kv_value_values[i]->get_data(),
                                       kv_value_values[i]->get_size(), pos);
              if (tmp_object_info.v_tfs_file_info_.size() > 0)
              {
                last_offset = tmp_object_info.v_tfs_file_info_[0].offset_;
                if (tmp_object_info.v_tfs_file_info_[0].offset_ + tmp_object_info.v_tfs_file_info_[0].file_size_ <= offset)
                {//invalid frag
                  continue;
                }
                // now vector max == 1
                object_info->v_tfs_file_info_.push_back(tmp_object_info.v_tfs_file_info_[0]);
                valid_result++;
                if (valid_result >= MESS_LIMIT)
                {
                  break;
                }
              }
            }
            TBSYS_LOG(DEBUG, "this time result_size is: %d", result_size);

            if(result_size == SCAN_LIMIT && valid_result < MESS_LIMIT)
            {
              first = 1;
              ret = serialize_key(bucket_name, file_name, last_offset,
                                  &start_key, start_key_buff, KEY_BUFF_SIZE, KvKey::KEY_TYPE_OBJECT);
            }
            else
            {
              go_on = false;
            }

            for(i = 0; i < result_size; ++i)//free tair
            {
              kv_value_values[i]->free();
            }
            kv_value_values.clear();
          }//end while

          if (NULL != start_key_buff)
          {
            free(start_key_buff);
            start_key_buff = NULL;
          }
          if (NULL != end_key_buff)
          {
            free(end_key_buff);
            end_key_buff = NULL;
          }
          int32_t vec_tfs_size = static_cast<int32_t>(object_info->v_tfs_file_info_.size());

          if (MESS_LIMIT == vec_tfs_size)
          {
            *still_have = true;
          }
        }//end big file
      }//end success
      return ret;
    }

    int MetaInfoHelper::del_object(const std::string& bucket_name, const std::string& file_name,
        common::ObjectInfo *object_info, bool* still_have)
    {
      int ret = (bucket_name.size() > 0 && file_name.size() > 0) ? TFS_SUCCESS : TFS_ERROR;
      *still_have = false;
      //op key
      char *start_key_buff = NULL;
      if (TFS_SUCCESS == ret)
      {
        start_key_buff = (char*) malloc(KEY_BUFF_SIZE);
      }
      if (NULL == start_key_buff)
      {
        ret = TFS_ERROR;
      }
      char *end_key_buff = NULL;
      if (TFS_SUCCESS == ret)
      {
        end_key_buff = (char*) malloc(KEY_BUFF_SIZE);
      }
      if (NULL == end_key_buff)
      {
        ret = TFS_ERROR;
      }
      KvKey start_key;
      KvKey end_key;
      int64_t start_offset = 0;
      int64_t end_offset = INT64_INFI;
      if (TFS_SUCCESS == ret)
      {
        ret = serialize_key(bucket_name, file_name, start_offset,
              &start_key, start_key_buff, KEY_BUFF_SIZE, KvKey::KEY_TYPE_OBJECT);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = serialize_key(bucket_name, file_name, end_offset,
              &end_key, end_key_buff, KEY_BUFF_SIZE, KvKey::KEY_TYPE_OBJECT);
      }

      int32_t limit = MESS_LIMIT;
      int32_t i;
      int32_t first = 0;
      short scan_type = CMD_RANGE_ALL;
      vector<KvValue*> kv_value_keys;
      vector<KvValue*> kv_value_values;
      object_info->v_tfs_file_info_.clear();
      vector<KvKey> vec_keys;

      int32_t result_size = 0;
      if (TFS_SUCCESS == ret)
      {
        ret = kv_engine_helper_->scan_keys(start_key, end_key, limit + 1, first,
          &kv_value_keys, &kv_value_values, &result_size, scan_type);
        TBSYS_LOG(DEBUG, "del object, bucekt_name: %s, object_name: %s, "
            "scan ret: %d, limit: %d, result size: %d",
            bucket_name.c_str(), file_name.c_str(), ret, limit + 1, result_size);
        if (TFS_SUCCESS == ret && result_size == 0)
        {
          ret = EXIT_OBJECT_NOT_EXIST;
        }
        if (result_size == limit + 1)
        {
          result_size -= 1;
          *still_have = true;
        }
        if(TFS_SUCCESS == ret)
        {
          for(i = 0; i < result_size; ++i)
          {
            //key get
            KvKey tmp_key;
            tmp_key.key_ = kv_value_keys[i]->get_data();
            tmp_key.key_size_ = kv_value_keys[i]->get_size();
            tmp_key.key_type_ = KvKey::KEY_TYPE_OBJECT;
            vec_keys.push_back(tmp_key);

            //value get
            common::ObjectInfo tmp_object_info;
            int64_t pos = 0;
            if(TFS_SUCCESS == ret)
            {
              ret = tmp_object_info.deserialize(kv_value_values[i]->get_data(),
                                     kv_value_values[i]->get_size(), pos);
            }
            if(TFS_SUCCESS == ret)
            {
              //j now max == 1
              for (size_t j = 0; j < tmp_object_info.v_tfs_file_info_.size(); j++)
              {
                TBSYS_LOG(DEBUG, "del tfs file info:");
                tmp_object_info.v_tfs_file_info_[j].dump();
                object_info->v_tfs_file_info_.push_back(tmp_object_info.v_tfs_file_info_[j]);
              }
            }
          }
        }

        //del from tair
        if(TFS_SUCCESS == ret && result_size > 0)
        {
           ret = kv_engine_helper_->delete_keys(vec_keys);
        }
        for(i = 0; i < result_size; ++i)//free tair
        {
          kv_value_keys[i]->free();
          kv_value_values[i]->free();
        }
        kv_value_keys.clear();
        kv_value_values.clear();
      }

      if(NULL != start_key_buff)
      {
        free(start_key_buff);
        start_key_buff = NULL;
      }
      if(NULL != end_key_buff)
      {
        free(end_key_buff);
        end_key_buff = NULL;
      }
      return ret;
    }
    /*----------------------------bucket part-----------------------------*/

     int MetaInfoHelper::get_common_prefix(const char *key, const string& prefix,
        const char delimiter, bool *prefix_flag, bool *common_flag, int *common_end_pos)
    {
      int ret = TFS_SUCCESS;
      if (NULL == prefix_flag || NULL == common_flag || NULL == common_end_pos)
      {
        ret = TFS_ERROR;
      }

      if (TFS_SUCCESS == ret)
      {
        *prefix_flag = false;
        *common_flag = false;
        *common_end_pos = -1;

        int start_pos = 0;
        if (!prefix.empty())
        {
          if (strncmp(key, prefix.c_str(), prefix.length()) == 0)
          {
            *prefix_flag = true;
            start_pos = prefix.length();
          }
        }
        else
        {
          *prefix_flag = true;
        }

        if (*prefix_flag && DEFAULT_CHAR != delimiter)
        {
          for (size_t j = start_pos; j < strlen(key); j++)
          {
            if (*(key+j) == delimiter)
            {
              *common_flag = true;
              *common_end_pos = j;
              break;
            }
          }
        }
      }

      return ret;
    }

    int MetaInfoHelper::get_range(const KvKey &pkey, const string &start_key,
          const int32_t offset, const int32_t limit, vector<KvValue*> *kv_value_keys,
          vector<KvValue*> *kv_value_values, int32_t *result_size)
    {
      int ret = TFS_SUCCESS;

      short scan_type = CMD_RANGE_ALL;//scan key and value
      KvKey start_obj_key;
      KvKey end_obj_key;

      string skey(pkey.key_);
      skey += KvKey::DELIMITER;
      if (!start_key.empty())
      {
        skey += start_key;
      }
      start_obj_key.key_ = skey.c_str();
      start_obj_key.key_size_ = skey.length();
      start_obj_key.key_type_ = KvKey::KEY_TYPE_OBJECT;

      string ekey(pkey.key_);
      ekey += KvKey::DELIMITER;
      char max_int[8];
      memset(max_int, 0xff, 8);
      ekey.append(max_int, 8);

      end_obj_key.key_ = ekey.c_str();
      end_obj_key.key_size_ = ekey.length();
      end_obj_key.key_type_ = KvKey::KEY_TYPE_OBJECT;

      ret = kv_engine_helper_->scan_keys(start_obj_key, end_obj_key, limit, offset, kv_value_keys, kv_value_values, result_size, scan_type);

      return ret;
    }

    int MetaInfoHelper::deserialize_key(const char *key, const int32_t key_size, string *bucket_name,
        string *object_name, int64_t *offset, int64_t *version)
    {
      int ret = (key != NULL && key_size > 0 && bucket_name != NULL && object_name != NULL &&
          version != NULL && offset != NULL) ? TFS_SUCCESS : TFS_ERROR;

      if (TFS_SUCCESS == ret)
      {
        char *pos = const_cast<char*>(key);
        do
        {
          if (KvKey::DELIMITER == *pos)
          {
            break;
          }
          pos++;
        } while(pos - key < key_size);
        int64_t bucket_name_size = pos - key;
        bucket_name->assign(key, bucket_name_size);

        pos++;

        do
        {
          if (KvKey::DELIMITER == *pos)
          {
            break;
          }
          pos++;
        } while(pos - key < key_size);

        int64_t object_name_size = pos - key - bucket_name_size - 1;
        object_name->assign(key + bucket_name_size + 1, object_name_size);

        if ((*object_name)[0] == MULTIPART_UPLOAD_KEY)
        {
          ret = EXIT_MULTIPART_TYPE_KEY;
        }

        pos++;

        if (TFS_SUCCESS == ret && (pos + 8) <= key + key_size)
        {
          ret = Serialization::char_to_int64(pos, key + key_size - pos, *version);
          pos = pos + 8;
        }

        pos++;

        if (TFS_SUCCESS == ret && (pos + 8) <= key + key_size)
        {
          ret = Serialization::char_to_int64(pos, key + key_size - pos, *offset);
          pos = pos + 8;
        }
      }


      return ret;
    }

    int MetaInfoHelper::deserialize_upload_key(const char *key, const int32_t key_size,
        string *bucket_name, string *upload_id, string *object_name,
        int64_t *version, int64_t *part_num, int64_t *offset)
    {
      int ret = (key != NULL && key_size > 0 && bucket_name != NULL
          && upload_id != NULL && object_name != NULL
          && version != NULL && part_num != NULL && offset != NULL) ? TFS_SUCCESS : TFS_ERROR;

      if (TFS_SUCCESS == ret)
      {
        char *pos = const_cast<char*>(key);
        do
        {
          if (KvKey::DELIMITER == *pos)
          {
            break;
          }
          pos++;
        } while(pos - key < key_size);
        int64_t bucket_name_size = pos - key;
        bucket_name->assign(key, bucket_name_size);

        pos++;

        if (*pos != MULTIPART_UPLOAD_KEY)
        {
          ret = EXIT_NOT_MULTIPART_TYPE_KEY;
        }

        //upload_id's first char is 2
        int64_t upload_id_size = -1;
        if (TFS_SUCCESS == ret)
        {
          do
          {
            if (KvKey::DELIMITER == *pos)
            {
              break;
            }
            pos++;
          } while(pos - key < key_size);

          upload_id_size = pos - key - bucket_name_size - 1;
          upload_id->assign(key + bucket_name_size + 2, upload_id_size - 1);
        }

        pos++;

        if (TFS_SUCCESS == ret)
        {
          do
          {
            if (KvKey::DELIMITER == *pos)
            {
              break;
            }
            pos++;
          } while(pos - key < key_size);

          int64_t object_name_size = pos - key - bucket_name_size - upload_id_size - 1;
          object_name->assign(key + bucket_name_size + upload_id_size + 1, object_name_size);
        }

        pos++;

        if (TFS_SUCCESS == ret && (pos + 8) <= key + key_size)
        {
          ret = Serialization::char_to_int64(pos, key + key_size - pos, *version);
          pos = pos + 8;
        }

        pos++;

        if (TFS_SUCCESS == ret && (pos + 8) <= key + key_size)
        {
          ret = Serialization::char_to_int64(pos, key + key_size - pos, *part_num);
          pos = pos + 8;
        }

        pos++;

        if (TFS_SUCCESS == ret && (pos + 8) <= key + key_size)
        {
          ret = Serialization::char_to_int64(pos, key + key_size - pos, *offset);
          pos = pos + 8;
        }
      }


      return ret;
    }

    int MetaInfoHelper::group_upload_objects(const string &object_name, const string &upload_id,
        const string &v, const string &prefix, const char delimiter,
        vector<ObjectUploadInfo> *v_object_upload_info, set<string> *s_common_prefix)
    {
      UNUSED(v);
      UNUSED(v_object_upload_info);
      int ret = TFS_SUCCESS;
      int common_pos = -1;

      bool prefix_flag = false;
      bool common_flag = false;

      ret = get_common_prefix(object_name.c_str(), prefix, delimiter, &prefix_flag, &common_flag, &common_pos);

      if (TFS_SUCCESS == ret)
      {
        if (common_flag)
        {
          string common_prefix(object_name.substr(0, common_pos+1));
          s_common_prefix->insert(common_prefix);
        }
        else if (prefix_flag)
        {
          ObjectInfo object_info;
          ObjectUploadInfo object_upload_info;
          int64_t pos = 0;
          ret = object_info.deserialize(v.c_str(), v.length(), pos);
          if (TFS_SUCCESS == ret)
          {
            object_upload_info.object_name_ = object_name;
            object_upload_info.upload_id_ = upload_id;
            object_upload_info.owner_id_ = object_info.meta_info_.owner_id_;
            v_object_upload_info->push_back(object_upload_info);
          }
        }
      }

      return ret;
    }


    int MetaInfoHelper::group_objects(const string &object_name, const string &v,
        const string &prefix, const char delimiter,
        vector<ObjectMetaInfo> *v_object_meta_info, vector<string> *v_object_name, set<string> *s_common_prefix)
    {
      int ret = TFS_SUCCESS;
      int common_pos = -1;

      bool prefix_flag = false;
      bool common_flag = false;

      ret = get_common_prefix(object_name.c_str(), prefix, delimiter, &prefix_flag, &common_flag, &common_pos);

      if (TFS_SUCCESS == ret)
      {
        if (common_flag)
        {
          string common_prefix(object_name.substr(0, common_pos+1));
          s_common_prefix->insert(common_prefix);
        }
        else if (prefix_flag)
        {
          ObjectInfo object_info;
          int64_t pos = 0;
          ret = object_info.deserialize(v.c_str(), v.length(), pos);
          if (TFS_SUCCESS == ret)
          {
            v_object_meta_info->push_back(object_info.meta_info_);
            v_object_name->push_back(object_name);
          }
        }
      }

      return ret;
    }

    int MetaInfoHelper::list_objects(const KvKey& pkey, const std::string& prefix,
        const std::string& start_key, const char delimiter, int32_t *limit,
        std::vector<common::ObjectMetaInfo>* v_object_meta_info, common::VSTRING* v_object_name,
        std::set<std::string>* s_common_prefix, int8_t* is_truncated)
    {
      int ret = TFS_SUCCESS;

      if (NULL == v_object_meta_info ||
          NULL == v_object_name ||
          NULL == s_common_prefix ||
          NULL == is_truncated ||
          NULL == limit)
      {
        ret = TFS_ERROR;
      }

      if (*limit > MAX_LIMIT or *limit < 0)
      {
        TBSYS_LOG(WARN, "limit: %d will be cutoff", *limit);
        *limit = MAX_LIMIT;
      }

      if (TFS_SUCCESS == ret)
      {
        v_object_meta_info->clear();
        v_object_name->clear();
        s_common_prefix->clear();

        int32_t limit_size = *limit;
        *is_truncated = 0;

        vector<KvValue*> kv_value_keys;
        vector<KvValue*> kv_value_values;

        string temp_start_key(start_key);

        bool loop = true;
        bool first_loop = true;
        do
        {
          int32_t res_size = -1;
          int32_t actual_size = static_cast<int32_t>(v_object_name->size()) +
            static_cast<int32_t>(s_common_prefix->size());

          limit_size = *limit - actual_size;

          int32_t extra = first_loop ? 2 : 1;
          ret = get_range(pkey, temp_start_key, 0, limit_size + extra,
              &kv_value_keys, &kv_value_values, &res_size);
          // error
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "get range fail, ret: %d", ret);
            break;
          }

          TBSYS_LOG(DEBUG, "get range once, res_size: %d, limit_size: %d", res_size, limit_size);

          if (res_size == 0)
          {
            break;
          }
          else if (res_size < limit_size + extra)
          {
            loop = false;
          }

          string object_name;
          string bucket_name;
          int64_t offset = -1;
          int64_t version = -1;

          for (int i = 0; i < res_size; i++)
          {
            string k(kv_value_keys[i]->get_data(), kv_value_keys[i]->get_size());
            string v(kv_value_values[i]->get_data(), kv_value_values[i]->get_size());

            ret = deserialize_key(k.c_str(), k.length(), &bucket_name, &object_name, &offset, &version);
            if (EXIT_MULTIPART_TYPE_KEY == ret)
            {
              continue;
            }
            else if (TFS_SUCCESS != ret)
            {
              TBSYS_LOG(ERROR, "deserialize from %s fail", k.c_str());
            }
            else if (offset == 0)
            {
              if (!first_loop)
              {
                ret = group_objects(object_name, v, prefix, delimiter,
                    v_object_meta_info, v_object_name, s_common_prefix);
              }
              else if (object_name.compare(start_key) != 0)
              {
                ret = group_objects(object_name, v, prefix, delimiter,
                    v_object_meta_info, v_object_name, s_common_prefix);
              }

              if (TFS_SUCCESS != ret)
              {
                TBSYS_LOG(ERROR, "group objects fail, ret: %d", ret);
              }
            }

            if (TFS_SUCCESS != ret)
            {
              loop = false;
              break;
            }

            if (static_cast<int32_t>(s_common_prefix->size()) +
                static_cast<int32_t>(v_object_name->size()) >= *limit)
            {
              loop = false;
              *is_truncated = 1;
              break;
            }
          }

          if (loop)
          {
            KvKey key;
            char key_buff[KEY_BUFF_SIZE];
            offset = INT64_MAX;
            ret = serialize_key_ex(object_name, offset, &key, key_buff, KEY_BUFF_SIZE, KvKey::KEY_TYPE_OBJECT);
            if (TFS_SUCCESS == ret)
            {
              temp_start_key.assign(key.key_, key.key_size_);
            }
            else
            {
              TBSYS_LOG(INFO, "serialize sub key error");
              loop = false;
            }
          }

          //delete for tair
          for (int i = 0; i < res_size; ++i)
          {
            kv_value_keys[i]->free();
            kv_value_values[i]->free();
          }
          kv_value_keys.clear();
          kv_value_values.clear();
          first_loop = false;
        } while (loop);// end of while
      }// end of if
      return ret;
    }// end of func

    int MetaInfoHelper::head_bucket(const std::string &bucket_name,
        common::BucketMetaInfo *bucket_meta_info, int64_t *version)
    {
      int ret = TFS_SUCCESS;

      KvKey key;
      key.key_ = bucket_name.c_str();
      key.key_size_ = bucket_name.length();
      key.key_type_ = KvKey::KEY_TYPE_BUCKET;

      KvValue *value = NULL;
      if (TFS_SUCCESS == ret)
      {
        ret = kv_engine_helper_->get_key(key, &value, version);
      }
      if (ret == EXIT_KV_RETURN_DATA_NOT_EXIST)
      {
        ret = EXIT_BUCKET_NOT_EXIST;
      }

      if (TFS_SUCCESS == ret)
      {
        int64_t pos = 0;
        ret = bucket_meta_info->deserialize(value->get_data(), value->get_size(), pos);
      }

      if (NULL != value)
      {
        value->free();
      }

      return ret;
    }

    int MetaInfoHelper::put_bucket_ex(const string &bucket_name, const BucketMetaInfo &bucket_meta_info,
        int64_t lock_version)
    {
      int ret = TFS_SUCCESS;

      KvKey key;
      key.key_ = bucket_name.c_str();
      key.key_size_ = bucket_name.length();
      key.key_type_ = KvKey::KEY_TYPE_BUCKET;

      char *kv_value_bucket_info_buff = NULL;
      kv_value_bucket_info_buff = (char*) malloc(KV_VALUE_BUFF_SIZE);
      if (NULL == kv_value_bucket_info_buff)
      {
        ret = TFS_ERROR;
      }

      int64_t pos = 0;
      if (TFS_SUCCESS == ret)
      {
        ret = bucket_meta_info.serialize(kv_value_bucket_info_buff, KV_VALUE_BUFF_SIZE, pos);
      }

      KvMemValue value;
      if (TFS_SUCCESS == ret)
      {
        value.set_data(kv_value_bucket_info_buff, pos);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = kv_engine_helper_->put_key(key, value, lock_version);
      }

      if (NULL != kv_value_bucket_info_buff)
      {
        free(kv_value_bucket_info_buff);
        kv_value_bucket_info_buff = NULL;
      }
      return ret;
    }

    int MetaInfoHelper::put_bucket(const std::string& bucket_name, common::BucketMetaInfo& bucket_meta_info,
        const common::UserInfo &user_info)
    {
      int64_t now_time = static_cast<int64_t>(time(NULL));
      bucket_meta_info.set_create_time(now_time);
      int ret = TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        BucketMetaInfo tmp_bucket_meta_info;
        ret = head_bucket(bucket_name, &tmp_bucket_meta_info, NULL);
        if (TFS_SUCCESS == ret)
        {
          TBSYS_LOG(INFO, "bucket: %s has existed", bucket_name.c_str());
          ret = EXIT_BUCKET_EXIST;
        }
        else if (EXIT_BUCKET_NOT_EXIST == ret)
        {
          ret = TFS_SUCCESS;
        }
      }

      if (TFS_SUCCESS == ret)
      {
        bucket_meta_info.owner_id_ = user_info.owner_id_;
        int64_t ver = MAX_VERSION;
        ret = put_bucket_ex(bucket_name, bucket_meta_info, ver);
      }

      return ret;
    }

    int MetaInfoHelper::put_bucket_tag(const std::string& bucket_name, const common::MAP_STRING &bucket_tag_map)
    {
      int ret = TFS_SUCCESS;

      BucketMetaInfo new_bucket_meta_info;
      int64_t version = -1;
      if (TFS_SUCCESS == ret)
      {
        ret = head_bucket(bucket_name, &new_bucket_meta_info, &version);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(INFO, "bucket: %s has not existed", bucket_name.c_str());
          ret = EXIT_BUCKET_NOT_EXIST;
        }
      }

      if (TFS_SUCCESS == ret)
      {
        if (!new_bucket_meta_info.has_tag_info_)
        {
          new_bucket_meta_info.has_tag_info_ = true;
          new_bucket_meta_info.bucket_tag_map_ = bucket_tag_map;
        }
        else
        {
          MAP_STRING_ITER iter = bucket_tag_map.begin();
          bool insert_success = false;
          for (; iter != bucket_tag_map.end() && TFS_SUCCESS == ret; iter++)
          {
            if (static_cast<int32_t>(new_bucket_meta_info.bucket_tag_map_.size()) > MAX_BUCKET_TAG_SIZE)
            {
              ret = EXIT_TAG_KEY_OVER_LIMIT;
              TBSYS_LOG(INFO, "the bucket: %s has %d keys of tag", bucket_name.c_str(), MAX_BUCKET_TAG_SIZE);
              continue;
            }

            insert_success = (new_bucket_meta_info.bucket_tag_map_.insert(std::make_pair(iter->first, iter->second))).second;

            if (!insert_success)
            {
              ret = EXIT_TAG_KEY_EXIST;
              TBSYS_LOG(INFO, "bucket: %s tag key: %s maybe exist, put fail",
                  bucket_name.c_str(), (iter->first).c_str());
            }
          }
        }
      }

      if (TFS_SUCCESS == ret)
      {
        ret = put_bucket_ex(bucket_name, new_bucket_meta_info, version);
      }

      return ret;
    }

    int MetaInfoHelper::get_bucket(const std::string& bucket_name, const std::string& prefix,
        const std::string& start_key, const char delimiter, int32_t *limit,
        vector<ObjectMetaInfo>* v_object_meta_info, VSTRING* v_object_name, set<string>* s_common_prefix,
        int8_t* is_truncated)
    {
      int ret = TFS_SUCCESS;

      KvKey pkey;
      pkey.key_ = bucket_name.c_str();
      pkey.key_size_ = bucket_name.length();
      pkey.key_type_ = KvKey::KEY_TYPE_BUCKET;

      TBSYS_LOG(DEBUG, "get bucket: %s, prefix: %s, start_key: %s, delimiter: %c",
                bucket_name.c_str(), prefix.c_str(), start_key.c_str(), delimiter);
      // check bucket whether exist
      if (TFS_SUCCESS == ret)
      {
        BucketMetaInfo bucket_meta_info;
        ret = head_bucket(bucket_name, &bucket_meta_info, NULL);
        TBSYS_LOG(INFO, "head bucket: %s, ret: %d", bucket_name.c_str(), ret);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = list_objects(pkey, prefix, start_key, delimiter, limit,
          v_object_meta_info, v_object_name, s_common_prefix, is_truncated);
      }

      return ret;
    }

    int MetaInfoHelper::get_bucket_tag(const string& bucket_name, MAP_STRING *bucket_tag_map)
    {
      int ret = TFS_SUCCESS;

      BucketMetaInfo bucket_meta_info;
      if (TFS_SUCCESS == ret)
      {
        ret = head_bucket(bucket_name, &bucket_meta_info, NULL);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(INFO, "bucket: %s has not existed", bucket_name.c_str());
          ret = EXIT_BUCKET_NOT_EXIST;
        }
      }

      if (TFS_SUCCESS == ret)
      {
        if (!bucket_meta_info.has_tag_info_)
        {
          ret = EXIT_BUCKET_TAG_NOT_EXIST;
          TBSYS_LOG(INFO, "bucket: %s has no tag set, del fail", bucket_name.c_str());
        }
        else
        {
          *bucket_tag_map = bucket_meta_info.bucket_tag_map_;
        }
      }

      return ret;
    }

    int MetaInfoHelper::del_bucket(const string& bucket_name)
    {
      int ret = TFS_SUCCESS;
      KvKey pkey;
      pkey.key_ = bucket_name.c_str();
      pkey.key_size_ = bucket_name.length();
      pkey.key_type_ = KvKey::KEY_TYPE_BUCKET;

      int32_t limit = MAX_LIMIT;
      int32_t res_size = -1;
      vector<KvValue*> kv_value_keys;
      vector<KvValue*> kv_value_values;

      ret = get_range(pkey, "", 0, limit, &kv_value_keys, &kv_value_values, &res_size);
      if (res_size == 0 && TFS_SUCCESS == ret)
      {
        TBSYS_LOG(DEBUG, "bucket: %s is empty", bucket_name.c_str());
      }
      else
      {
        TBSYS_LOG(ERROR, "delete bucket: %s failed! bucket is not empty", bucket_name.c_str());
        ret = EXIT_DELETE_DIR_WITH_FILE_ERROR;
      }

      if (TFS_SUCCESS == ret)
      {
        ret = kv_engine_helper_->delete_key(pkey);
      }

      //delete for tair
      for (int i = 0; i < res_size; ++i)
      {
        kv_value_keys[i]->free();
        kv_value_values[i]->free();
      }
      kv_value_keys.clear();
      kv_value_values.clear();
      return ret;
    }
    int MetaInfoHelper::list_upload_objects(const KvKey& pkey, const std::string& prefix,
        const std::string &start_key, const std::string &start_id, const char delimiter,
        const int32_t &limit, common::ListMultipartObjectResult *list_multipart_object_result)
    {
      int ret = TFS_SUCCESS;

      if (NULL == list_multipart_object_result)
      {
        ret = TFS_ERROR;
      }

      if (limit > MAX_LIMIT or limit < 0)
      {
        TBSYS_LOG(WARN, "limit: %d will be cutoff", limit);
        list_multipart_object_result->limit_ = MAX_LIMIT;
      }
      else
      {
        list_multipart_object_result->limit_ = limit;
      }

      if (TFS_SUCCESS == ret)
      {
        list_multipart_object_result->v_object_upload_info_.clear();
        list_multipart_object_result->s_common_prefix_.clear();

        int32_t limit_size = list_multipart_object_result->limit_;
        list_multipart_object_result->is_truncated_ = false;

        vector<KvValue*> kv_value_keys;
        vector<KvValue*> kv_value_values;

        //multipart object form: $upload_id*object_name*version*partnum*offset
        string temp_start_key;
        temp_start_key += MULTIPART_UPLOAD_KEY;
        temp_start_key += start_id;

        bool loop = true;
        bool first_loop = true;
        do
        {
          int32_t res_size = -1;
          int32_t actual_size = static_cast<int32_t>(list_multipart_object_result->v_object_upload_info_.size()) +
            static_cast<int32_t>(list_multipart_object_result->s_common_prefix_.size());

          limit_size = limit - actual_size;

          int32_t extra = first_loop ? 2 : 1;
          ret = get_range(pkey, temp_start_key, 0, limit_size + extra,
              &kv_value_keys, &kv_value_values, &res_size);
          // error
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "get range fail, ret: %d", ret);
            break;
          }

          TBSYS_LOG(DEBUG, "get range once, res_size: %d, limit_size: %d", res_size, limit_size);

          if (res_size == 0)
          {
            break;
          }
          else if (res_size < limit_size + extra)
          {
            loop = false;
          }

          string upload_id;
          string object_name;
          string bucket_name;

          int64_t part_num = -1;
          int64_t offset = -1;
          int64_t version = -1;

          for (int i = 0; i < res_size; i++)
          {
            string k(kv_value_keys[i]->get_data(), kv_value_keys[i]->get_size());
            string v(kv_value_values[i]->get_data(), kv_value_values[i]->get_size());

            ret = deserialize_upload_key(k.c_str(), k.length(), &bucket_name, &upload_id,
                &object_name, &version, &part_num, &offset);

            if (EXIT_NOT_MULTIPART_TYPE_KEY == ret)
            {
              continue;
            }
            else if (TFS_SUCCESS != ret)
            {
              TBSYS_LOG(ERROR, "deserialize from %s fail", k.c_str());
            }
            else if (offset == 0 && object_name.compare(start_key) >= 0)
            {
              if (!first_loop)
              {
                ret = group_upload_objects(object_name, upload_id, v, prefix, delimiter,
                    &(list_multipart_object_result->v_object_upload_info_),
                    &(list_multipart_object_result->s_common_prefix_));
              }
              else if (object_name.compare(start_key) != 0)
              {
                ret = group_upload_objects(object_name, upload_id, v, prefix, delimiter,
                    &list_multipart_object_result->v_object_upload_info_,
                    &list_multipart_object_result->s_common_prefix_);
              }

              if (TFS_SUCCESS != ret)
              {
                TBSYS_LOG(ERROR, "group objects fail, ret: %d", ret);
              }
            }

            if (TFS_SUCCESS != ret)
            {
              loop = false;
              break;
            }

            if (static_cast<int32_t>(list_multipart_object_result->s_common_prefix_.size()) +
                static_cast<int32_t>(list_multipart_object_result->v_object_upload_info_.size())
                >= list_multipart_object_result->limit_)
            {
              loop = false;
              list_multipart_object_result->is_truncated_ = true;
              vector<ObjectUploadInfo>::const_iterator iter = list_multipart_object_result->v_object_upload_info_.end();
              list_multipart_object_result->next_start_key_ = (iter -1)->object_name_;
              list_multipart_object_result->next_start_id_ = (iter - 1)->upload_id_;
              break;
            }
          }

          if (loop)
          {
            KvKey key;
            char key_buff[KEY_BUFF_SIZE];
            part_num = INT64_MAX;
            offset = INT64_MAX;
            ret = serialize_upload_key(object_name, upload_id, offset, part_num, &key,
                key_buff, KEY_BUFF_SIZE, KvKey::KEY_TYPE_OBJECT);
            if (TFS_SUCCESS == ret)
            {
              temp_start_key.assign(key.key_, key.key_size_);
            }
            else
            {
              TBSYS_LOG(INFO, "serialize sub key error");
              loop = false;
            }
          }

          //delete for tair
          for (int i = 0; i < res_size; ++i)
          {
            kv_value_keys[i]->free();
            kv_value_values[i]->free();
          }
          kv_value_keys.clear();
          kv_value_values.clear();
          first_loop = false;
        } while (loop);// end of while
      }// end of if
      return ret;

    }

    int MetaInfoHelper::list_multipart_object(const std::string &bucket_name, const std::string &prefix, const std::string &start_key, const std::string &start_id, const char delimiter, const int32_t &limit, common::ListMultipartObjectResult *list_multipart_object_result)
    {
      int ret = TFS_SUCCESS;

      KvKey pkey;
      pkey.key_ = bucket_name.c_str();
      pkey.key_size_ = bucket_name.length();
      pkey.key_type_ = KvKey::KEY_TYPE_BUCKET;

      TBSYS_LOG(DEBUG, "get bucket: %s, prefix: %s, start_key: %s, start_id: %s, delimiter: %c",
                bucket_name.c_str(), prefix.c_str(), start_key.c_str(), start_id.c_str(), delimiter);
      // check bucket whether exist
      if (TFS_SUCCESS == ret)
      {
        BucketMetaInfo bucket_meta_info;
        ret = head_bucket(bucket_name, &bucket_meta_info, NULL);
        TBSYS_LOG(INFO, "head bucket: %s, ret: %d", bucket_name.c_str(), ret);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = list_upload_objects(pkey, prefix, start_key, start_id, delimiter, limit, list_multipart_object_result);
      }

      return ret;
    }

    int MetaInfoHelper::del_bucket_tag(const string& bucket_name)
    {
      int ret = TFS_SUCCESS;

      BucketMetaInfo bucket_meta_info;
      int64_t version = -1;
      if (TFS_SUCCESS == ret)
      {
        ret = head_bucket(bucket_name, &bucket_meta_info, &version);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(INFO, "bucket: %s has not existed", bucket_name.c_str());
          ret = EXIT_BUCKET_NOT_EXIST;
        }
      }

      if (TFS_SUCCESS == ret)
      {
        if (!bucket_meta_info.has_tag_info_)
        {
          ret = EXIT_BUCKET_TAG_NOT_EXIST;
          TBSYS_LOG(INFO, "bucket: %s has no tag set, del fail", bucket_name.c_str());
        }
        else
        {
          bucket_meta_info.bucket_tag_map_.clear();
          bucket_meta_info.has_tag_info_ = false;
        }
      }

      if (TFS_SUCCESS == ret)
      {
        ret = put_bucket_ex(bucket_name, bucket_meta_info, version);
      }

      return ret;
    }

    //about bucket acl
    int MetaInfoHelper::put_bucket_acl(const string& bucket_name, const MAP_STRING_INT &bucket_acl_map)
    {
      int ret = TFS_SUCCESS;

      BucketMetaInfo new_bucket_meta_info;
      int64_t version = -1;
      if (TFS_SUCCESS == ret)
      {
        ret = head_bucket(bucket_name, &new_bucket_meta_info, &version);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(INFO, "bucket: %s has not existed", bucket_name.c_str());
          ret = EXIT_BUCKET_NOT_EXIST;
        }
      }

      if (TFS_SUCCESS == ret)
      {
        new_bucket_meta_info.bucket_acl_map_.clear();
        new_bucket_meta_info.bucket_acl_map_ = bucket_acl_map;
        ret = put_bucket_ex(bucket_name, new_bucket_meta_info, version);
      }

      return ret;
    }

    int MetaInfoHelper::get_bucket_acl(const string& bucket_name, MAP_STRING_INT *bucket_acl_map)
    {
      int ret = TFS_SUCCESS;

      BucketMetaInfo bucket_meta_info;
      if (TFS_SUCCESS == ret)
      {
        ret = head_bucket(bucket_name, &bucket_meta_info, NULL);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(INFO, "bucket: %s has not existed", bucket_name.c_str());
          ret = EXIT_BUCKET_NOT_EXIST;
        }
      }

      if (TFS_SUCCESS == ret)
      {
        *bucket_acl_map = bucket_meta_info.bucket_acl_map_;
      }

      return ret;
    }

    /*----------------------------multi part-----------------------------*/
    int MetaInfoHelper::joint_multi_objectname(const std::string &file_name,
        const std::string &upload_id, const int32_t part_num, std::string &new_objectname)
    {
      int ret = (file_name.size() > 0 &&
                 upload_id.size() > 0 && part_num <= PARTNUM_MAX && part_num >= PARTNUM_MIN - 1) ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        char str[10];
        sprintf(str, "%05d", part_num);
        string str_num(str);
        new_objectname = DELIMITER_2 + upload_id + DELIMITER_1 + file_name + DELIMITER_1 + str_num;
      }
      else
      {
        TBSYS_LOG(ERROR, "joint fail");
      }
      return ret;
    }
/*
    int MetaInfoHelper::joint_multi_objectname_for_check(const std::string &file_name,
        const std::string &upload_id, const int32_t part_num, std::string &new_objectname)
    {
      int ret = (file_name.size() > 0 &&
                 upload_id.size() > 0 && part_num < 10000 && part_num >= 0) ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        char str[10];
        sprintf(str, "%05d", part_num);
        string str_num(str);
        const char DELIMITER_1 = 7;
        const char DELIMITER_2 = 2;
        new_objectname = DELIMITER_2 + upload_id + DELIMITER_1 + file_name + DELIMITER_1 + DELIMITER_2 + str_num;
      }
      return ret;
    }
*/
    int MetaInfoHelper::is_equal_v_part_num(const VINT32& v_part_num_kv, const VINT32& v_part_num)
    {
      int32_t ret = TFS_SUCCESS;
      if (v_part_num_kv.size() == v_part_num.size())
      {
        for (size_t i = 0; i < v_part_num.size(); ++i)
        {
          if (v_part_num_kv[i] != v_part_num[i])
          {
            ret = EXIT_MULITIPART_LIST_DIFF;
            break;
          }
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "SIZE is diff : user:%d , kv:%d",v_part_num.size() , v_part_num_kv.size());
        ret = EXIT_MULITIPART_LIST_DIFF;
      }
      return ret;
    }

    int MetaInfoHelper::get_v_partnum_kv(const std::string& bucket_name,
                const std::string& file_name, const std::string &upload_id, std::vector<int32_t>* const p_v_part_num)
    {
      int ret = (bucket_name.size() > 0 && file_name.size() > 0 &&
          upload_id.size() > 0 && p_v_part_num != NULL) ? TFS_SUCCESS : TFS_ERROR;

      if (TFS_SUCCESS == ret)
      {
        //op key
        char *start_key_buff = NULL;
        if (TFS_SUCCESS == ret)
        {
          start_key_buff = (char*) malloc(KEY_BUFF_SIZE);
        }
        if (NULL == start_key_buff)
        {
          ret = TFS_ERROR;
        }
        char *end_key_buff = NULL;
        if (TFS_SUCCESS == ret)
        {
          end_key_buff = (char*) malloc(KEY_BUFF_SIZE);
        }
        if (NULL == end_key_buff)
        {
          ret = TFS_ERROR;
        }
        KvKey start_key;
        KvKey end_key;
        int64_t start_offset = 0;
        int64_t end_offset = 0;
        string check_objectname_start;
        string check_objectname_end;
        if (TFS_SUCCESS == ret)
        {
          ret = joint_multi_objectname(file_name, upload_id, PARTNUM_MIN, check_objectname_start);
        }
        if (TFS_SUCCESS == ret)
        {
          ret = joint_multi_objectname(file_name, upload_id, PARTNUM_MAX, check_objectname_end);
        }
        if (TFS_SUCCESS == ret)
        {
          ret = serialize_key(bucket_name, check_objectname_start, start_offset,
                &start_key, start_key_buff, KEY_BUFF_SIZE, KvKey::KEY_TYPE_OBJECT);
        }
        else
        {
          TBSYS_LOG(ERROR, "error serialize_key", ret);
        }

        if (TFS_SUCCESS == ret)
        {
          ret = serialize_key(bucket_name, check_objectname_end, end_offset,
                &end_key, end_key_buff, KEY_BUFF_SIZE, KvKey::KEY_TYPE_OBJECT);
        }
        else
        {
          TBSYS_LOG(ERROR, "error serialize_key", ret);
        }

        int32_t i;
        int32_t first = 0;
        bool go_on = true;
        short scan_type = CMD_RANGE_VALUE_ONLY;//only scan value
        vector<KvValue*> kv_value_keys;
        vector<KvValue*> kv_value_values;
        p_v_part_num->clear();

        while (go_on)
        {
          int32_t result_size = 0;
          ret = kv_engine_helper_->scan_keys(start_key, end_key, SCAN_LIMIT, first,
              &kv_value_keys, &kv_value_values, &result_size, scan_type);
          if (EXIT_KV_RETURN_DATA_NOT_EXIST == ret)
          {//metainfo exist but data not exist
            ret = TFS_SUCCESS;
          }
          for(i = 0; i < result_size; ++i)
          {
            common::ObjectInfo tmp_object_info;
            //value get
            int64_t pos = 0;
            tmp_object_info.deserialize(kv_value_values[i]->get_data(),
                                     kv_value_values[i]->get_size(), pos);
            if (tmp_object_info.v_tfs_file_info_.size() > 0)
            {
              if (tmp_object_info.meta_info_.max_tfs_file_size_ > PARTNUM_BASE && tmp_object_info.v_tfs_file_info_[0].offset_ == 0)
              {
                if (p_v_part_num->size() > 0)
                {
                  if (tmp_object_info.meta_info_.max_tfs_file_size_ - PARTNUM_BASE == p_v_part_num->back())
                  {
                    continue;
                  }
                }
                p_v_part_num->push_back(tmp_object_info.meta_info_.max_tfs_file_size_ - PARTNUM_BASE);
                TBSYS_LOG(DEBUG, "this time part_num is =========: %d", tmp_object_info.meta_info_.max_tfs_file_size_ - PARTNUM_BASE);
              }
            }
          }

          TBSYS_LOG(DEBUG, "this time result_size is: %d", result_size);

          if(result_size == SCAN_LIMIT)
          {
            if (TFS_SUCCESS == ret)
            {
              ret = joint_multi_objectname(file_name, upload_id, p_v_part_num->back() + 1, check_objectname_start);
            }
            if (TFS_SUCCESS == ret)
            {
              ret = serialize_key(bucket_name, check_objectname_start, start_offset,
                    &start_key, start_key_buff, KEY_BUFF_SIZE, KvKey::KEY_TYPE_OBJECT);
            }
          }
          else
          {
            go_on = false;
          }

          for(i = 0; i < result_size; ++i)//free tair
          {
            kv_value_values[i]->free();
          }
          kv_value_values.clear();
        }//end while

        if (NULL != start_key_buff)
        {
          free(start_key_buff);
          start_key_buff = NULL;
        }
        if (NULL != end_key_buff)
        {
          free(end_key_buff);
          end_key_buff = NULL;
        }
      }
      return ret;
    }
    int MetaInfoHelper::init_multipart(const std::string& bucket_name,
                const std::string& file_name, std::string* upload_id)
    {
      int ret = (bucket_name.size() > 0 && file_name.size() > 0 &&
          NULL != upload_id ) ? TFS_SUCCESS : TFS_ERROR;

      // check bucket whether exist
      if (TFS_SUCCESS == ret)
      {
        BucketMetaInfo bucket_meta_info;
        ret = head_bucket(bucket_name, &bucket_meta_info, NULL);
        TBSYS_LOG(DEBUG, "head bucket, bucket: %s, object: %s, ret: %d",
            bucket_name.c_str(), file_name.c_str(), ret);
      }

      if (TFS_SUCCESS == ret)
      {
        ObjectInfo object_info_zero;
        ret = head_object(bucket_name, file_name, &object_info_zero);
      }
      if (EXIT_OBJECT_NOT_EXIST == ret)
      {
        string new_objectname;
        int32_t part_num = 0;

        SessionUtil::gene_session_id(static_cast<int32_t>(bucket_name.size()), static_cast<int64_t>(file_name.size()), *upload_id);
        // put partnum = 0 key as checkkey
        ret = joint_multi_objectname(file_name, *upload_id, part_num, new_objectname);
        if (TFS_SUCCESS == ret)
        {
          ObjectInfo object_info;
          UserInfo user_info;
          ret = put_object(bucket_name, new_objectname, object_info, user_info);
        }
      }
      else if (TFS_SUCCESS == ret)
      {
        ret = EXIT_OBJECT_EXIST;
      }
      return ret;
    }

    int MetaInfoHelper::upload_multipart(const std::string &bucket_name,
        const std::string &file_name, const std::string &upload_id, const int32_t part_num,
        ObjectInfo &object_info, const UserInfo &user_info)
    {
      int ret = (bucket_name.size() > 0 && file_name.size() > 0 &&
                 upload_id.size() > 0 && part_num <= PARTNUM_MAX && part_num >= PARTNUM_MIN) ? TFS_SUCCESS : TFS_ERROR;
      //first check uploadid is vaild;
      string check_objectname;
      if (TFS_SUCCESS == ret)
      {
        ret = joint_multi_objectname(file_name, upload_id, 0, check_objectname);
      }
      if (TFS_SUCCESS == ret)
      {
        common::ObjectInfo object_info_zero;
        int64_t version = 0;
        int64_t offset_zero = 0;
        ret = get_object_part(bucket_name, check_objectname, offset_zero, &object_info_zero, &version);
        if (TFS_SUCCESS != ret)
        {
          if (EXIT_OBJECT_NOT_EXIST == ret)
          {
            TBSYS_LOG(ERROR, "upload multipart fail object noexist or uploadid is wrong ret : %d", ret);
          }
          else
          {
            TBSYS_LOG(ERROR, "upload multipart other error ret : %d", ret);
          }
        }
      }
      //put this frag
      string new_objectname;
      if (TFS_SUCCESS == ret)
      {
        ret = joint_multi_objectname(file_name, upload_id, part_num, new_objectname);
      }
      if (TFS_SUCCESS == ret)
      {
        if (object_info.v_tfs_file_info_.size() > 0)
        {
          //put partnum to meta_info_.max_tfs_file_size_ of first frag
          if (0 == object_info.v_tfs_file_info_.front().offset_)
          {
            object_info.meta_info_.max_tfs_file_size_ = PARTNUM_BASE + part_num;
          }
        }
        ret = put_object(bucket_name, new_objectname, object_info, user_info);
      }
      return ret;
    }

    int MetaInfoHelper::complete_multipart(const std::string &bucket_name,
        const std::string &file_name, const std::string &upload_id, const std::vector<int32_t>& v_part_num)
    {
      int ret = (bucket_name.size() > 0 && file_name.size() > 0 &&
                 upload_id.size() > 0) ? TFS_SUCCESS : TFS_ERROR;
      //first check uploadid is vaild;
      string check_objectname;
      if (TFS_SUCCESS == ret)
      {
        ret = joint_multi_objectname(file_name, upload_id, 0, check_objectname);
      }
      if (TFS_SUCCESS == ret)
      {
        common::ObjectInfo object_info_zero;
        int64_t version = 0;
        int64_t offset_zero = 0;
        ret = get_object_part(bucket_name, check_objectname, offset_zero, &object_info_zero, &version);
      }
      std::vector<int32_t> v_part_num_kv;
      if (TFS_SUCCESS == ret)
      {
        ret = get_v_partnum_kv(bucket_name, file_name, upload_id, &v_part_num_kv);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = is_equal_v_part_num(v_part_num_kv, v_part_num);
      }
      bool still_have = false;
      if (TFS_SUCCESS == ret)
      {
        int64_t total_offset = 0;
        int64_t one = 1;//must define ; in 1<<40 ,1 is int32 so...
        int64_t total_length = one << 40;//max 1TB
        size_t i = 0;
        for (i = 0; i < v_part_num.size(); ++i)
        {
          ret = joint_multi_objectname(file_name, upload_id, v_part_num[i], check_objectname);

          still_have = false;
          int64_t left_length = total_length;
          int64_t read_length = 0;
          int64_t cur_offset = 0;
          common::UserInfo user_info;
          if (TFS_SUCCESS == ret)
          {
            do
            {
              still_have = false;
              common::ObjectInfo object_info;
              ret = get_object(bucket_name, check_objectname, cur_offset, left_length, &object_info, &still_have);
              for (size_t j = 0; j < object_info.v_tfs_file_info_.size(); ++j)
              {
                common::ObjectInfo object_info_one;
                if (i == 0 && j == 0)
                {
                  object_info_one.has_meta_info_ = true;
                  object_info_one.has_customize_info_ = true;
                  object_info_one.customize_info_ = object_info.customize_info_;
                  user_info.owner_id_ = object_info.meta_info_.owner_id_;
                }
                object_info.v_tfs_file_info_[j].offset_ += total_offset;
                object_info_one.v_tfs_file_info_.push_back(object_info.v_tfs_file_info_[j]);

                ret = put_object(bucket_name, file_name, object_info_one, user_info);
                if (TFS_SUCCESS != ret)
                {
                  TBSYS_LOG(ERROR, "complete multipart input fail ret : %d", ret);
                  //TODO
                }

                cur_offset += object_info.v_tfs_file_info_[j].file_size_;
                read_length += object_info.v_tfs_file_info_[j].file_size_;
                left_length -= object_info.v_tfs_file_info_[j].file_size_;
              }
            }while(left_length > 0 && still_have);
          }

          total_offset += read_length;
          total_length -= read_length;

          do
          {
            still_have = false;
            common::ObjectInfo object_info_del;
            ret = del_object(bucket_name, check_objectname, &object_info_del, &still_have);
          }while(still_have);
        }
        if (TFS_SUCCESS == ret && i == v_part_num.size())
        {
          ret = joint_multi_objectname(file_name, upload_id, 0, check_objectname);
          if (TFS_SUCCESS == ret)
          {
            common::ObjectInfo object_info_tdel;
            ret = del_object(bucket_name, check_objectname, &object_info_tdel, &still_have);
          }
        }
      }

      return ret;
    }

    int MetaInfoHelper::list_multipart(const std::string& bucket_name,
                const std::string& file_name, const std::string &upload_id, std::vector<int32_t>* const p_v_part_num)
    {
      int ret = (bucket_name.size() > 0 && file_name.size() > 0 &&
          upload_id.size() > 0 && p_v_part_num != NULL) ? TFS_SUCCESS : TFS_ERROR;

      string check_objectname;
      if (TFS_SUCCESS == ret)
      {
        ret = joint_multi_objectname(file_name, upload_id, 0, check_objectname);
      }
      if (TFS_SUCCESS == ret)
      {
        common::ObjectInfo object_info_zero;
        int64_t version = 0;
        int64_t offset_zero = 0;
        ret = get_object_part(bucket_name, check_objectname, offset_zero, &object_info_zero, &version);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = get_v_partnum_kv(bucket_name, file_name, upload_id, p_v_part_num);
      }

      return ret;
    }

    int MetaInfoHelper::abort_multipart(const std::string &bucket_name,
        const std::string &file_name, const std::string &upload_id, common::ObjectInfo *object_info, bool* still_have)
    {
      int ret = (bucket_name.size() > 0 && file_name.size() > 0 &&
                 upload_id.size() > 0) ? TFS_SUCCESS : TFS_ERROR;
                 /*
      string check_objectname;
      if (TFS_SUCCESS == ret)
      {
        ret = joint_multi_objectname(file_name, upload_id, 0, check_objectname);
      }
      if (TFS_SUCCESS == ret)
      {
        common::ObjectInfo object_info_zero;
        int64_t version = 0;
        int64_t offset_zero = 0;
        ret = get_object_part(bucket_name, check_objectname, offset_zero, &object_info_zero, &version);
      }
      */
      if (TFS_SUCCESS == ret)
      {
        *still_have = false;
        //op key
        char *start_key_buff = NULL;
        if (TFS_SUCCESS == ret)
        {
          start_key_buff = (char*) malloc(KEY_BUFF_SIZE);
        }
        if (NULL == start_key_buff)
        {
          ret = TFS_ERROR;
        }
        char *end_key_buff = NULL;
        if (TFS_SUCCESS == ret)
        {
          end_key_buff = (char*) malloc(KEY_BUFF_SIZE);
        }
        if (NULL == end_key_buff)
        {
          ret = TFS_ERROR;
        }
        KvKey start_key;
        KvKey end_key;
        int64_t start_offset = 0;
        int64_t end_offset = INT64_INFI;

        string newobjectname_start;
        string newobjectname_end;
        if (TFS_SUCCESS == ret)
        {
          ret = joint_multi_objectname(file_name, upload_id, PARTNUM_MIN, newobjectname_start);
        }
        if (TFS_SUCCESS == ret)
        {
          ret = joint_multi_objectname(file_name, upload_id, PARTNUM_MAX, newobjectname_end);
        }
        if (TFS_SUCCESS == ret)
        {
          ret = serialize_key(bucket_name, newobjectname_start, start_offset,
                &start_key, start_key_buff, KEY_BUFF_SIZE, KvKey::KEY_TYPE_OBJECT);
        }
        if (TFS_SUCCESS == ret)
        {
          ret = serialize_key(bucket_name, newobjectname_end, end_offset,
                &end_key, end_key_buff, KEY_BUFF_SIZE, KvKey::KEY_TYPE_OBJECT);
        }
        int32_t limit = MESS_LIMIT;
        int32_t i;
        int32_t first = 0;
        short scan_type = CMD_RANGE_ALL;
        vector<KvValue*> kv_value_keys;
        vector<KvValue*> kv_value_values;
        object_info->v_tfs_file_info_.clear();
        vector<KvKey> vec_keys;

        int32_t result_size = 0;
        if (TFS_SUCCESS == ret)
        {
          ret = kv_engine_helper_->scan_keys(start_key, end_key, limit + 1, first,
            &kv_value_keys, &kv_value_values, &result_size, scan_type);
          if (TFS_SUCCESS == ret && result_size == 0)
          {
            ret = EXIT_OBJECT_NOT_EXIST;
          }
          if (result_size == limit + 1)
          {
            result_size -= 1;
            *still_have = true;
          }

          if(TFS_SUCCESS == ret)
          {
            for(i = 0; i < result_size; ++i)
            {
              //key get
              KvKey tmp_key;
              tmp_key.key_ = kv_value_keys[i]->get_data();
              tmp_key.key_size_ = kv_value_keys[i]->get_size();
              tmp_key.key_type_ = KvKey::KEY_TYPE_OBJECT;
              vec_keys.push_back(tmp_key);

              //value get
              common::ObjectInfo tmp_object_info;
              int64_t pos = 0;
              if(TFS_SUCCESS == ret)
              {
                ret = tmp_object_info.deserialize(kv_value_values[i]->get_data(),
                                       kv_value_values[i]->get_size(), pos);
              }
              if(TFS_SUCCESS == ret)
              {
                //j now max == 1
                for (size_t j = 0; j < tmp_object_info.v_tfs_file_info_.size(); j++)
                {
                  object_info->v_tfs_file_info_.push_back(tmp_object_info.v_tfs_file_info_[j]);
                }
              }
            }
            TBSYS_LOG(DEBUG, "this time result_size is: %d", result_size);
          }
          else
          {
            TBSYS_LOG(ERROR, "del partnum key fail ret:%d", ret);
          }

          //del from tair
          if(TFS_SUCCESS == ret && result_size > 0)
          {
             ret = kv_engine_helper_->delete_keys(vec_keys);
          }
          for(i = 0; i < result_size; ++i)//free tair
          {
            kv_value_keys[i]->free();
            kv_value_values[i]->free();
          }
          kv_value_keys.clear();
          kv_value_values.clear();
        }

        if(NULL != start_key_buff)
        {
          free(start_key_buff);
          start_key_buff = NULL;
        }
        if(NULL != end_key_buff)
        {
          free(end_key_buff);
          end_key_buff = NULL;
        }
      }
      else if (EXIT_OBJECT_NOT_EXIST == ret)
      {
        TBSYS_LOG(ERROR,"multipart noexist or uploadid is wrong");
      }
      else
      {
        TBSYS_LOG(ERROR,"other error ret:%d", ret);
      }

      return ret;
    }

  }// end for kvmetaserver
}// end for tfs

