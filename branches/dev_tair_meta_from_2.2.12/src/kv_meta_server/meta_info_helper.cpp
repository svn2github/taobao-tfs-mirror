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

      TBSYS_LOG(DEBUG, "part offset: %"PRI64_PREFIX"d", offset);

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
      if(TFS_SUCCESS == ret)
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
        ret = get_single_value(bucket_name, file_name, 0, object_info_zero, 0);
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

      if (NULL != value_buff)
      {
        free(key_buff);
        key_buff = NULL;
      }

      return ret;
    }

    int MetaInfoHelper::put_object_segment(const string &bucket_name, const string &file_name,
        const ObjectInfo &object_info, ObjectInfo *object_info_zero)
    {
      int ret = TFS_SUCCESS;
      for (size_t i = 0; i < object_info.v_tfs_file_info_.size(); i++)
      {
        int64_t tmp_offset = object_info.v_tfs_file_info_[i].offset_;
        ObjectInfo part_object_info;
        if (tmp_offset == 0)
        {
          object_info_zero->has_meta_info_ = true;
          object_info_zero->has_customize_info_ = object_info.has_customize_info_;
          object_info_zero->customize_info_ = object_info.customize_info_;

          object_info_zero->meta_info_ = object_info.meta_info_;
          object_info_zero->meta_info_.create_time_ = static_cast<int64_t>(time(NULL));
          object_info_zero->v_tfs_file_info_.clear();
          object_info_zero->v_tfs_file_info_.push_back(object_info.v_tfs_file_info_[i]);
        }
        else
        {
          part_object_info.v_tfs_file_info_.clear();
          part_object_info.v_tfs_file_info_.push_back(object_info.v_tfs_file_info_[i]);
          ret = put_object_ex(bucket_name, file_name, tmp_offset, part_object_info, 0);
        }

        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "put bucket_name: %s, file_name: %s, offset: %"PRI64_PREFIX"d part fail",
              bucket_name.c_str(), file_name.c_str(), tmp_offset);
          break;
        }
      }

      return ret;
    }

    int MetaInfoHelper::update_object_head(const string &bucket_name, const string &file_name,
        ObjectInfo *object_info_zero, const int64_t offset, const int64_t length)
    {
      int ret = TFS_SUCCESS;

      int32_t retry = VERSION_ERROR_RETRY_COUNT;
      int64_t ver = 1<<15 - 1;
      ObjectInfo tmp_object_info_zero;

      do
      {
        if (length + offset > tmp_object_info_zero.meta_info_.big_file_size_ )
        {
          object_info_zero->meta_info_.big_file_size_ = length + offset;
        }

        object_info_zero->meta_info_.modify_time_ = static_cast<int64_t>(time(NULL));
        object_info_zero->has_meta_info_ = true;

        ret = put_object_ex(bucket_name, file_name, 0, *object_info_zero, ver);
        if (EXIT_TAIR_VERSION_ERROR == ret)
        {
          TBSYS_LOG(INFO, "%s", "update object metainfo version conflict");
          ret = get_single_value(bucket_name, file_name, 0, &tmp_object_info_zero, &ver);

          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "get bucket: %s, object: %s metainfo fail, ret: %d",
                bucket_name.c_str(), file_name.c_str(), ret);
          }
          else
          {
            ret = EXIT_TAIR_VERSION_ERROR;
          }

          if (tmp_object_info_zero.v_tfs_file_info_.size() > 0)
          {
            if (object_info_zero->v_tfs_file_info_.size() > 0)
            {
              TBSYS_LOG(WARN, "conflict found. drop new one");
              object_info_zero->v_tfs_file_info_.clear();
            }
          }
          else
          {
            tmp_object_info_zero.v_tfs_file_info_ = object_info_zero->v_tfs_file_info_;
          }
          *object_info_zero = tmp_object_info_zero;
        }
      }while (retry-- && EXIT_TAIR_VERSION_ERROR == ret);

      return ret;
    }

    int MetaInfoHelper::put_object(const std::string &bucket_name,
        const std::string &file_name,
        const int64_t offset,
        const int64_t length,
        const ObjectInfo &object_info)
    {
      int ret = (bucket_name.size() > 0 && file_name.size() > 0 &&
          offset >= 0  && length >= 0) ? TFS_SUCCESS : TFS_ERROR;

      // check bucket whether exist
      if (TFS_SUCCESS == ret)
      {
        BucketMetaInfo bucket_meta_info;
        ret = head_bucket(bucket_name, &bucket_meta_info);
        TBSYS_LOG(INFO, "head bucket: %s, ret: %d", bucket_name.c_str(), ret);
      }

      ObjectInfo object_info_zero;
      if (TFS_SUCCESS == ret)
      {
        ret = put_object_segment(bucket_name, file_name, object_info, &object_info_zero);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = update_object_head(bucket_name, file_name, &object_info_zero, offset, length);
      }

      return ret;
    }

    int MetaInfoHelper::get_single_value(const std::string &bucket_name,
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
          && offset >= 0 && object_info != NULL) ? TFS_SUCCESS : TFS_ERROR;

      common::ObjectInfo object_info_zero;
      if(TFS_SUCCESS == ret)
      {
        int64_t version = 0;
        int64_t offset_zero = 0;
        ret = get_single_value(bucket_name, file_name, offset_zero, &object_info_zero, &version);
        *object_info = object_info_zero;
        *still_have = false;
        if(TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "object is noexist");
        }
      }
      if(TFS_SUCCESS == ret)
      {
        if(offset > object_info_zero.meta_info_.big_file_size_)
        {
          TBSYS_LOG(ERROR, "req offset is out of big_file_size_");
          ret = EXIT_KV_RETURN_DATA_NOT_EXIST;
        }
      }
      if(TFS_SUCCESS == ret)
      {
        bool is_big_file = false;
        if(object_info_zero.v_tfs_file_info_.size() > 0)
        {
          if(offset + length <= object_info_zero.v_tfs_file_info_[0].file_size_)
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
        if(is_big_file == true )//big file
        {
          //op key
          char *start_key_buff = NULL;
          if(TFS_SUCCESS == ret)
          {
            start_key_buff = (char*) malloc(KEY_BUFF_SIZE);
          }
          if(NULL == start_key_buff)
          {
            ret = TFS_ERROR;
          }
          char *end_key_buff = NULL;
          if(ret == TFS_SUCCESS)
          {
            end_key_buff = (char*) malloc(KEY_BUFF_SIZE);
          }
          if(NULL == end_key_buff)
          {
            ret = TFS_ERROR;
          }
          KvKey start_key;
          KvKey end_key;
          int64_t start_offset = offset - MAX_SEGMENT_SIZE > 0 ? offset - MAX_SEGMENT_SIZE : 0;
          int64_t end_offset = offset + length;
          if(TFS_SUCCESS == ret)
          {
            ret = serialize_key(bucket_name, file_name, start_offset,
                  &start_key, start_key_buff, KEY_BUFF_SIZE, KvKey::KEY_TYPE_OBJECT);
          }
          if(TFS_SUCCESS == ret)
          {
            ret = serialize_key(bucket_name, file_name, end_offset,
                  &end_key, end_key_buff, KEY_BUFF_SIZE, KvKey::KEY_TYPE_OBJECT);
          }

          //op value
          int32_t limit = 0;
          int32_t limit_mode = 0;
          int32_t limit_res = MESS_LIMIT;
          if(SCAN_LIMIT >= MESS_LIMIT)
          {
            limit = MESS_LIMIT;
            limit_mode = MODE_REQ_LIMIT;
          }
          else
          {
            limit = SCAN_LIMIT;
            limit_mode = MODE_KV_LIMIT;
          }

          int32_t i;
          int32_t first = 0;
          bool go_on = true;
          short scan_type = CMD_RANGE_VALUE_ONLY;//only scan value
          vector<KvValue*> kv_value_keys;
          vector<KvValue*> kv_value_values;
          object_info->v_tfs_file_info_.clear();

          while(go_on == true)
          {
            int32_t result_size = 0;
            ret = kv_engine_helper_->scan_keys(start_key, end_key, limit, first,
                &kv_value_keys, &kv_value_values, &result_size, scan_type);
            for(i = 0; i < result_size; ++i)
            {
              common::ObjectInfo tmp_object_info;
              //value get
              int64_t pos = 0;
              tmp_object_info.deserialize(kv_value_values[i]->get_data(),
                                       kv_value_values[i]->get_size(), pos);
              //j now max == 1
              for (size_t j = 0; j < tmp_object_info.v_tfs_file_info_.size(); j++)
              {
                object_info->v_tfs_file_info_.push_back(tmp_object_info.v_tfs_file_info_[j]);
              }

            }
            TBSYS_LOG(ERROR, "this time result_size is: %d", result_size);
            if(result_size >= limit && limit_mode == MODE_KV_LIMIT)
            {
              first = 1;
              limit_res -= result_size;
              limit = min(limit_res, limit);
              if(0 == limit)//message full
              {
                go_on = false;
              }
              ret = serialize_key(bucket_name, file_name, object_info->v_tfs_file_info_.back().offset_,
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
          int32_t vec_tfs_size = static_cast<int32_t>(object_info->v_tfs_file_info_.size());
          if(MESS_LIMIT == vec_tfs_size)
          {
            *still_have = true;
          }
        }//end big file
      }//end success
      return ret;
    }

    int MetaInfoHelper::del_object(const std::string& bucket_name, const std::string& file_name)
    {
      int ret = (bucket_name.size() > 0 && file_name.size() > 0) ? TFS_SUCCESS : TFS_ERROR;
      string real_key(bucket_name + KvKey::DELIMITER + file_name);
      real_key += KvKey::DELIMITER;
      real_key += "0" ; //version_id;
      real_key += KvKey::DELIMITER;
      real_key += "0" ; //offset;

      KvKey key;
      key.key_ = real_key.c_str();
      key.key_size_ = real_key.length();
      key.key_type_ = KvKey::KEY_TYPE_OBJECT;
      ret = kv_engine_helper_->delete_key(key);

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

      ret = kv_engine_helper_->scan_keys(start_obj_key, end_obj_key, limit, offset, kv_value_keys, kv_value_values, result_size, scan_type);

      return ret;
    }

    int MetaInfoHelper::deserialize_key(const char *key, const int32_t key_size, string *object_name,
        int64_t *offset, int64_t *version)
    {
      int ret = (key != NULL && key_size > 0 &&  object_name != NULL &&
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

        int64_t object_name_size = pos - key;

        object_name->assign(key, object_name_size);

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

    int MetaInfoHelper::match_objects(const string &k, const string &v, const string &prefix, const char delimiter,
        vector<ObjectMetaInfo> *v_object_meta_info, vector<string> *v_object_name, set<string> *s_common_prefix)
    {
      int ret = TFS_SUCCESS;
      int common_pos = -1;

      bool prefix_flag = false;
      bool common_flag = false;

      string object_name;
      int64_t offset = -1;
      int64_t version = -1;


      //deserialze from object_name/offset/version
      ret = deserialize_key(k.c_str(), k.length(), &object_name, &offset, &version);

      ret = get_common_prefix(object_name.c_str(), prefix, delimiter, &prefix_flag, &common_flag, &common_pos);

      if (TFS_SUCCESS == ret)
      {
        if (common_flag)
        {
          string common_prefix(object_name.substr(0, common_pos+1));
          s_common_prefix->insert(common_prefix);
        }
        else if (prefix_flag && offset == 0)
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
          const std::string& start_key, const char delimiter, const int32_t limit,
          std::vector<common::ObjectMetaInfo>* v_object_meta_info, common::VSTRING* v_object_name,
          std::set<std::string>* s_common_prefix, int8_t* is_truncated)
    {
      int ret = TFS_SUCCESS;

      if (NULL == v_object_meta_info ||
          NULL == v_object_name ||
          NULL == s_common_prefix ||
          NULL == is_truncated)
      {
        ret = TFS_ERROR;
      }

      if (limit > MAX_LIMIT || limit < 0)
      {
        TBSYS_LOG(ERROR, "%s", "limit param error");
        ret = TFS_ERROR;
      }

      if (TFS_SUCCESS == ret)
      {
        v_object_meta_info->clear();
        v_object_name->clear();
        s_common_prefix->clear();

        bool first_loop = true;
        int32_t limit_size = limit;
        *is_truncated = 0;

        vector<KvValue*> kv_value_keys;
        vector<KvValue*> kv_value_values;

        string temp_start_key(start_key);

        bool need_out = true;
        while (need_out)
        {
          int32_t res_size = -1;
          int32_t actual_size = static_cast<int32_t>(v_object_name->size()) +
            static_cast<int32_t>(s_common_prefix->size());

          limit_size = limit - actual_size;

          ret = get_range(pkey, temp_start_key,  first_loop ? 0 : 1, limit_size + 1,
                          &kv_value_keys, &kv_value_values, &res_size);
          // error
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "get range fail, ret: %d", ret);
            break;
          }

          if (res_size == 0)
          {
            break;
          }

          for (int i = 0; need_out && i < res_size; i++)
          {
            need_out = static_cast<int32_t>(s_common_prefix->size()) +
              static_cast<int32_t>(v_object_name->size()) < limit;
            if (need_out)
            {
              string k(kv_value_keys[i]->get_data(), kv_value_keys[i]->get_size());
              string v(kv_value_values[i]->get_data(), kv_value_values[i]->get_size());
              match_objects(k, v, prefix, delimiter, v_object_meta_info, v_object_name, s_common_prefix);
            }
            else if (i < res_size -1)
            {
              *is_truncated = 1;
              break;
            }
          }

          if (need_out)
          {
            first_loop = false;
            temp_start_key = string(kv_value_keys[res_size-1]->get_data(), kv_value_keys[res_size-1]->get_size());
          }

          //delete for tair
          for (int i = 0; i < res_size; ++i)
          {
            kv_value_keys[i]->free();
            kv_value_values[i]->free();
          }
          kv_value_keys.clear();
          kv_value_values.clear();
        }// end of while
      }// end of if
      return ret;
    }// end of func

    int MetaInfoHelper::head_bucket(const std::string &bucket_name, common::BucketMetaInfo *bucket_meta_info)
    {
      //TODO for test now
      int ret = TFS_SUCCESS;

      KvKey key;
      key.key_ = bucket_name.c_str();
      key.key_size_ = bucket_name.length();
      key.key_type_ = KvKey::KEY_TYPE_BUCKET;

      KvValue *value = NULL;
      int64_t version = 0;
      if (TFS_SUCCESS == ret)
      {
        ret = kv_engine_helper_->get_key(key, &value, &version);
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

    int MetaInfoHelper::put_bucket(const std::string& bucket_name, const common::BucketMetaInfo& bucket_meta_info)
    {
      //TODO for test now
      int ret = TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        BucketMetaInfo tmp_bucket_meta_info;
        ret = head_bucket(bucket_name, &tmp_bucket_meta_info);
        if (TFS_SUCCESS == ret)
        {
          TBSYS_LOG(INFO, "bucket: %s has existed", bucket_name.c_str());
          ret = EXIT_WITH_BUCKET_REPEAT_PUT;
        }
        else if (EXIT_BUCKET_NOT_EXIST == ret)
        {
          ret = TFS_SUCCESS;
        }
      }

      if (TFS_SUCCESS == ret)
      {
        KvKey key;
        key.key_ = bucket_name.c_str();
        key.key_size_ = bucket_name.length();
        key.key_type_ = KvKey::KEY_TYPE_BUCKET;

        char *kv_value_bucket_info_buff = NULL;
        kv_value_bucket_info_buff = (char*) malloc(KV_VALUE_BUFF_SIZE);
        if(NULL == kv_value_bucket_info_buff)
        {
          ret = TFS_ERROR;
        }

        int64_t pos = 0;
        if(TFS_SUCCESS == ret)
        {
          ret = bucket_meta_info.serialize(kv_value_bucket_info_buff, KV_VALUE_BUFF_SIZE, pos);
        }

        KvMemValue value;
        if (ret == TFS_SUCCESS)
        {
          value.set_data(kv_value_bucket_info_buff, pos);
        }

        if (TFS_SUCCESS == ret)
        {
          ret = kv_engine_helper_->put_key(key, value, 0);
        }
        free(kv_value_bucket_info_buff);
      }

      return ret;
    }

    int MetaInfoHelper::get_bucket(const std::string& bucket_name, const std::string& prefix,
        const std::string& start_key, const char delimiter, const int32_t limit,
        vector<ObjectMetaInfo>* v_object_meta_info, VSTRING* v_object_name, set<string>* s_common_prefix,
        int8_t* is_truncated)
    {
      //TODO for test now
      int ret = TFS_SUCCESS;

      KvKey pkey;
      pkey.key_ = bucket_name.c_str();
      pkey.key_size_ = bucket_name.length();
      pkey.key_type_ = KvKey::KEY_TYPE_BUCKET;

      ret = list_objects(pkey, prefix, start_key, delimiter, limit,
          v_object_meta_info, v_object_name, s_common_prefix, is_truncated);

      return ret;
    }

    int MetaInfoHelper::del_bucket(const string& bucket_name)
    {
      //TODO for test now
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

  }// end for kvmetaserver
}// end for tfs

