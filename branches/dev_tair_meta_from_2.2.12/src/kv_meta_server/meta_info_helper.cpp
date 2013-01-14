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
    const int KEY_BUFF_SIZE = 512 + 8 + 8;
    const int SCAN_LIMIT = 500;
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

    int MetaInfoHelper::put_meta(const std::string& bucket_name, const std::string& file_name,
         const common::TfsFileInfo& tfs_file_info)
    {
      //TODO for test now
      int ret = TFS_SUCCESS;

      string real_key(bucket_name + KvKey::DELIMITER + file_name);
      real_key += KvKey::DELIMITER;
      real_key += "0" ; //version_id;
      real_key += KvKey::DELIMITER;
      real_key += "0" ; //offset;

      KvKey key;
      key.key_ = real_key.c_str();
      key.key_size_ = real_key.length();
      key.key_type_ = KvKey::KEY_TYPE_OBJECT;
      char tfs_info_buff[TFS_INFO_BUFF_SIZE];
      int64_t pos = 0;
      assert(TFS_SUCCESS == tfs_file_info.serialize(tfs_info_buff,
            TFS_INFO_BUFF_SIZE, pos));
      string value(tfs_info_buff, pos);
      ret = kv_engine_helper_->put_key(key, value, 0);
      return ret;
    }

    int MetaInfoHelper::get_meta(const std::string& bucket_name, const std::string& file_name,
        common::TfsFileInfo* tfs_file_info)
    {
      UNUSED(bucket_name);
      UNUSED(file_name);
      UNUSED(tfs_file_info);
      return TFS_SUCCESS;
    }

    //----------------------------
    int MetaInfoHelper::serialize_key(const std::string &bucket_name,
                                   const std::string &file_name, const int64_t &offset,
                                   KvKey *key, char **key_buff, int32_t key_type)
    {
      int ret = (bucket_name.size() > 0 && file_name.size() > 0) ? TFS_SUCCESS : TFS_ERROR;
      int64_t pos = 0;
      if(TFS_SUCCESS == ret)
      {
        int64_t version = 0;
        if (TFS_SUCCESS == ret)
        {
          ret = Serialization::set_string(*key_buff, KEY_BUFF_SIZE, pos, bucket_name);
        }
        if (TFS_SUCCESS == ret)
        {
          ret = Serialization::set_string(*key_buff, KEY_BUFF_SIZE, pos, file_name);
        }
        if (TFS_SUCCESS == ret)
        {
          ret = Serialization::int64_to_char((*key_buff) + pos, KEY_BUFF_SIZE, version);
          pos = pos + 8;
        }
        if (TFS_SUCCESS == ret)
        {
          ret = Serialization::int64_to_char((*key_buff) + pos, KEY_BUFF_SIZE, offset);
          pos = pos + 8;
        }
      }
      if(TFS_SUCCESS == ret)
      {
        key->key_ = *key_buff;
        key->key_size_ = pos;
        key->key_type_ = key_type;
      }
      return ret;
    }



    /*----------------------------object part-----------------------------*/
    int MetaInfoHelper::put_object(const std::string &bucket_name,
                                   const std::string &file_name,
                                   const int64_t &offset,
                                   common::ObjectInfo &object_info)
    {
      TBSYS_LOG(ERROR, "we at Metainfohelp put object");
      TBSYS_LOG(DEBUG, "offset is %"PRI64_PREFIX"d", offset);
      int ret = (bucket_name.size() > 0 && file_name.size() > 0) ? TFS_SUCCESS : TFS_ERROR;

      if(0 < offset)// is not big file `s zero part
      {
        common::ObjectInfo object_info_zero;

        int64_t zero = 0;

        ret = get_single_value(bucket_name, file_name, zero, &object_info_zero);

        if( TFS_ERROR== ret)// zero non-existent
        {
          object_info_zero.meta_info_.big_file_size_ = object_info.v_tfs_file_info_[0].file_size_ + offset;

          ret = put_object(bucket_name, file_name, zero, object_info_zero);

        }
        else if((object_info.v_tfs_file_info_[0].file_size_ + offset) > object_info_zero.meta_info_.big_file_size_)
        {
          object_info_zero.meta_info_.big_file_size_ = object_info.v_tfs_file_info_[0].file_size_ + offset;

          ret = put_object(bucket_name, file_name, zero, object_info_zero);
        }

      }
      else if(0 > offset)
      {
        TBSYS_LOG(DEBUG, "offset error %"PRI64_PREFIX"d", offset);
      }
      //op key
      char *key_buff = NULL;
      key_buff = (char*) malloc(KEY_BUFF_SIZE);
      if(NULL == key_buff)
      {
        ret = TFS_ERROR;
      }
      KvKey key;
      serialize_key(bucket_name, file_name, offset, &key, &key_buff, KvKey::KEY_TYPE_OBJECT);

      //op value
      char *value_buff = NULL;
      value_buff = (char*) malloc(VALUE_BUFF_SIZE);
      if(NULL == value_buff)
      {
        ret = TFS_ERROR;
      }
      int64_t pos = 0;
      object_info.serialize(value_buff, VALUE_BUFF_SIZE, pos);

      KvMemValue kv_value;
      if(ret == TFS_SUCCESS)
      {
        kv_value.set_data(value_buff, pos);
      }
      int64_t ver = 0;
      if(TFS_SUCCESS == ret)
      {
        ret = kv_engine_helper_->put_key(key, kv_value, ver);
      }
      free(value_buff);
      free(key_buff);

      return ret;
    }

     int MetaInfoHelper::get_single_value(const std::string &bucket_name,
                                          const std::string &file_name,
                                          const int64_t &offset,
                                          common::ObjectInfo *object_info)
    {
      int ret = (bucket_name.size() > 0 && file_name.size() > 0 && offset >= 0) ? TFS_SUCCESS : TFS_ERROR;
      //op key
      char *key_buff = NULL;
      key_buff = (char*) malloc(KEY_BUFF_SIZE);
      if(NULL == key_buff)
      {
        ret = TFS_ERROR;
      }
      KvKey key;
      serialize_key(bucket_name, file_name, offset, &key, &key_buff, KvKey::KEY_TYPE_OBJECT);

      //op value
      KvValue *kv_value = NULL;
      int64_t version;

      ret = kv_engine_helper_->get_key(key, &kv_value, &version);
      int64_t pos = 0;
      object_info->deserialize(kv_value->get_data(), kv_value->get_size(), pos);
      kv_value->free();
      free(key_buff);

      return ret;
    }

    int MetaInfoHelper::get_object(const std::string &bucket_name,
                                   const std::string &file_name, const int64_t &offset,
                                   common::ObjectInfo *object_info)
    {
      //TODO check object_info;
      int ret = (bucket_name.size() > 0 && file_name.size() > 0 && offset >= 0) ? TFS_SUCCESS : TFS_ERROR;
      TBSYS_LOG(ERROR, "we at Metainfohelp get object");
      //op key
      char *start_key_buff = NULL;
      start_key_buff = (char*) malloc(KEY_BUFF_SIZE);
      if(NULL == start_key_buff)
      {
        ret = TFS_ERROR;
      }
      char *end_key_buff = NULL;
      end_key_buff = (char*) malloc(KEY_BUFF_SIZE);
      if(NULL == end_key_buff)
      {
        ret = TFS_ERROR;
      }
      KvKey start_key;
      KvKey end_key;
      int64_t start_offset = 0;
      int64_t end_offset = 5555555555;
      serialize_key(bucket_name, file_name, start_offset, &start_key, &start_key_buff, KvKey::KEY_TYPE_OBJECT);
      serialize_key(bucket_name, file_name, end_offset, &end_key, &end_key_buff, KvKey::KEY_TYPE_OBJECT);

      //op value
      int32_t limit = SCAN_LIMIT;
      uint32_t i;
      int32_t first = 0;
      uint32_t result_size = 0;
      vector<KvValue*> kv_value_keys;
      vector<KvValue*> kv_value_values;

      while(first > -1)
      {
        ret = kv_engine_helper_->scan_keys(start_key, end_key, limit,
                                           &first, &kv_value_keys, &kv_value_values, &result_size);
        for(i = 0; i < result_size; ++i)
        {
          common::ObjectInfo tmp_object_info;
          int64_t pos = 0;
          //value get
          tmp_object_info.deserialize(kv_value_values[i]->get_data(),
                                   kv_value_values[i]->get_size(), pos);
          if (0 == i)
          {
            *object_info = tmp_object_info;
          }
          else
          {
            for (size_t j = 0; j < tmp_object_info.v_tfs_file_info_.size(); j++)
            {
              object_info->v_tfs_file_info_.push_back(tmp_object_info.v_tfs_file_info_[j]);
            }
          }
        }

        if(first > -1)
        {
          first = 1;
          serialize_key(bucket_name, file_name, object_info->v_tfs_file_info_.back().offset_ , &start_key, &start_key_buff, KvKey::KEY_TYPE_OBJECT);
        }
        for(i = 0; i < result_size; ++i)//free tair
        {
          //kv_value_keys[i]->free();
          kv_value_values[i]->free();
        }
        //kv_value_keys.clear();//reset
        kv_value_values.clear();
      }
      free(start_key_buff);
      free(end_key_buff);
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
          const int32_t offset, const int32_t limit, vector<KvKey> *vec_keys,
          vector<string> *vec_realkeys,
          vector<string> *vec_values, int32_t *result_size)
    {
      int ret = TFS_SUCCESS;

      KvKey start_obj_key;
      KvKey end_obj_key;

      if (!start_key.empty())
      {
        string skey(pkey.key_);
        skey += KvKey::DELIMITER;
        skey += start_key;

        start_obj_key.key_ = skey.c_str();
        start_obj_key.key_size_ = skey.length();
        start_obj_key.key_type_ = KvKey::KEY_TYPE_OBJECT;
      }

      ret = kv_engine_helper_->scan_keys(start_obj_key, end_obj_key, offset, limit, vec_keys, vec_realkeys, vec_values, result_size);

      return ret;
    }


    int MetaInfoHelper::list_objects_ex(const char *k, const char *v, const string &prefix, const char delimiter,
        vector<ObjectMetaInfo> *v_object_meta_info, vector<string> *v_object_name, set<string> *s_common_prefix)
    {
      int ret = TFS_SUCCESS;
      int common_pos = -1;

      bool prefix_flag = false;
      bool common_flag = false;

      ret = get_common_prefix(k, prefix, delimiter, &prefix_flag, &common_flag, &common_pos);

      if (TFS_SUCCESS == ret)
      {
        string object_name(k);
        if (common_flag)
        {
          string common_prefix(object_name.substr(0, common_pos+1));
          s_common_prefix->insert(common_prefix);
        }
        else if (prefix_flag)
        {
          v_object_name->push_back(object_name);

          TfsFileInfo tfs_file_info;
          ObjectMetaInfo object_meta_info;
          CustomizeInfo customize_info;

          int64_t pos = 0;
          tfs_file_info.deserialize(v, strlen(v), pos);
          object_meta_info.deserialize(v, strlen(v), pos);
          customize_info.deserialize(v, strlen(v), pos);
          v_object_meta_info->push_back(object_meta_info);
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
        VSTRING vec_realkeys;
        VSTRING vec_values;
        vector<KvKey> vec_keys;

        string temp_start_key(start_key);

        bool need_out = true;
        while (need_out)
        {
          int res_size = -1;
          int32_t actual_size = static_cast<int32_t>(v_object_name->size()) +
            static_cast<int32_t>(s_common_prefix->size());

          limit_size = limit - actual_size;

          ret = get_range(pkey, temp_start_key, first_loop ? 0 : 1,
              limit_size + 1, &vec_keys, &vec_realkeys, &vec_values, &res_size);

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
              const char* k = vec_realkeys[i].c_str();
              const char* v = vec_values[i].c_str();
              list_objects_ex(k, v, prefix, delimiter, v_object_meta_info, v_object_name, s_common_prefix);
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
            temp_start_key = vec_realkeys[res_size-1];
          }
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

      string value;
      if(TFS_SUCCESS == ret)
      {
        ret = kv_engine_helper_->get_key(key, &value, 0);
      }

      if(TFS_SUCCESS == ret)
      {
        int64_t pos = 0;
        ret = bucket_meta_info->deserialize(value.c_str(), KV_VALUE_BUFF_SIZE, pos);
      }

      return ret;
    }

    int MetaInfoHelper::put_bucket(const std::string& bucket_name, const common::BucketMetaInfo& bucket_meta_info)
    {
      //TODO for test now
      int ret = TFS_SUCCESS;

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
        ret =  bucket_meta_info.serialize(kv_value_bucket_info_buff, KV_VALUE_BUFF_SIZE, pos);
      }
      if(TFS_SUCCESS == ret)
      {
        string value(kv_value_bucket_info_buff, pos);
        ret = kv_engine_helper_->put_key(key, value, 0);
      }
      free(kv_value_bucket_info_buff);

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
      ret = kv_engine_helper_->delete_key(pkey);
      return ret;
    }

  }// end for kvmetaserver
}// end for tfs

