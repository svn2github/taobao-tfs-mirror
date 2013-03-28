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
        ret = head_bucket(bucket_name, &bucket_meta_info);
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

        ret = put_object_zero(bucket_name, file_name, &object_info_zero,
            &offset, length, version, is_append);

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

      ret = kv_engine_helper_->scan_keys(start_obj_key, end_obj_key, limit, offset, kv_value_keys, kv_value_values, result_size, scan_type);

      return ret;
    }

    int MetaInfoHelper::deserialize_key(const char *key, const int32_t key_size, string *bucket_name, string *object_name,
        int64_t *offset, int64_t *version)
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

        int32_t limit_size = limit;
        *is_truncated = 0;

        vector<KvValue*> kv_value_keys;
        vector<KvValue*> kv_value_values;

        string temp_start_key(start_key);

        bool loop = true;
        do
        {
          int32_t res_size = -1;
          int32_t actual_size = static_cast<int32_t>(v_object_name->size()) +
            static_cast<int32_t>(s_common_prefix->size());

          limit_size = limit - actual_size;

          ret = get_range(pkey, temp_start_key,  0, limit_size + 1,
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
          else if (res_size < limit_size + 1)
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
            if (TFS_SUCCESS != ret)
            {
              TBSYS_LOG(ERROR, "deserialize from %s fail", k.c_str());
            }
            else if (offset == 0)
            {
              ret = group_objects(object_name, v, prefix, delimiter, v_object_meta_info, v_object_name, s_common_prefix);
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
                static_cast<int32_t>(v_object_name->size()) >= limit)
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
        } while (loop);// end of while
      }// end of if
      return ret;
    }// end of func

    int MetaInfoHelper::head_bucket(const std::string &bucket_name, common::BucketMetaInfo *bucket_meta_info)
    {
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
        ret = head_bucket(bucket_name, &tmp_bucket_meta_info);
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

    int MetaInfoHelper::get_bucket(const std::string& bucket_name, const std::string& prefix,
        const std::string& start_key, const char delimiter, const int32_t limit,
        vector<ObjectMetaInfo>* v_object_meta_info, VSTRING* v_object_name, set<string>* s_common_prefix,
        int8_t* is_truncated)
    {
      int ret = TFS_SUCCESS;

      KvKey pkey;
      pkey.key_ = bucket_name.c_str();
      pkey.key_size_ = bucket_name.length();
      pkey.key_type_ = KvKey::KEY_TYPE_BUCKET;

      TBSYS_LOG(DEBUG, "get bucket: %s, prefix: %s, start_key: %s, delimiter: %c, limit: %d",
                bucket_name.c_str(), prefix.c_str(), start_key.c_str(), delimiter, limit);
      // check bucket whether exist
      if (TFS_SUCCESS == ret)
      {
        BucketMetaInfo bucket_meta_info;
        ret = head_bucket(bucket_name, &bucket_meta_info);
        TBSYS_LOG(INFO, "head bucket: %s, ret: %d", bucket_name.c_str(), ret);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = list_objects(pkey, prefix, start_key, delimiter, limit,
          v_object_meta_info, v_object_name, s_common_prefix, is_truncated);
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

  }// end for kvmetaserver
}// end for tfs

