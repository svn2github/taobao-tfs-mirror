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

#include "kv_meta_define.h"
#include "serialization.h"
#include "stream.h"

namespace tfs
{
  namespace common
  {
    const char KvDefine::PERIOD = '.';
    const char KvDefine::DASH = '-';
    const char KvDefine::DEFAULT_CHAR = 7;
    const int32_t KvDefine::MAX_LIMIT = 1000;
    const int32_t KvDefine::MAX_BUCKETS_COUNT = 100;
    const int32_t KvDefine::VERSION_ERROR_RETRY_COUNT = 3;
    const int64_t KvDefine::MAX_VERSION = (1L<<30) - 1;
    const int64_t KvDefine::ADMIN_ID = 8;

    TfsFileInfo::TfsFileInfo()
      :cluster_id_(0), block_id_(0), file_id_(0), offset_(0), file_size_(0)
    { }

    TfsFileInfo::TfsFileInfo(int32_t cluster_id, int64_t block_id, int64_t file_id, int64_t offset, int64_t size):
          cluster_id_(cluster_id), block_id_(block_id), file_id_(file_id), offset_(offset), file_size_(size)
              {}

    int64_t TfsFileInfo::length() const
    {
      // add 6 tag to self-interpret
      return INT64_SIZE * 4 + INT_SIZE + 6 * INT_SIZE;
    }

    void TfsFileInfo::dump() const
    {
      TBSYS_LOG(DEBUG, "TfsFileInfo: [block_id: %"PRI64_PREFIX"d, file_id: %"PRI64_PREFIX"d, "
                "cluster_id: %d, file_size: %"PRI64_PREFIX"d, offset: %"PRI64_PREFIX"d]",
                 block_id_, file_id_, cluster_id_, file_size_, offset_);
    }

    int TfsFileInfo::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, TFS_FILE_INFO_CLUSTER_ID_TAG);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, cluster_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, TFS_FILE_INFO_BLOCK_ID_TAG);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, block_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, TFS_FILE_INFO_FILE_ID_TAG);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, file_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, TFS_FILE_INFO_OFFSET_TAG);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, offset_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, TFS_FILE_INFO_FILE_SIZE_TAG);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, file_size_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, END_TAG);
      }
      return ret;
    }

    int TfsFileInfo::deserialize(const char* data, const int64_t data_len, int64_t& pos)
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
            case TFS_FILE_INFO_CLUSTER_ID_TAG:
              ret = Serialization::get_int32(data, data_len, pos, &cluster_id_);
              break;
            case TFS_FILE_INFO_BLOCK_ID_TAG:
              ret = Serialization::get_int64(data, data_len, pos, &block_id_);
              break;
            case TFS_FILE_INFO_FILE_ID_TAG:
              ret = Serialization::get_int64(data, data_len, pos, &file_id_);
              break;
            case TFS_FILE_INFO_OFFSET_TAG:
              ret = Serialization::get_int64(data, data_len, pos, &offset_);
              break;
            case TFS_FILE_INFO_FILE_SIZE_TAG:
              ret = Serialization::get_int64(data, data_len, pos, &file_size_);
              break;
            case END_TAG:
              ;
              break;
            default:
              TBSYS_LOG(ERROR, "tfs file info: %d can't self-interpret", type_tag);
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

    //object meta info
    ObjectMetaInfo::ObjectMetaInfo()
      :create_time_(0), modify_time_(0),
      max_tfs_file_size_(2048), big_file_size_(0),
      owner_id_(0)
    {}

    int64_t ObjectMetaInfo::length() const
    {
      return INT_SIZE  + INT64_SIZE * 4 + 6 * INT_SIZE;
    }

    void ObjectMetaInfo::dump() const
    {
      TBSYS_LOG(DEBUG, "ObjectMetaInfo: [create_time: %"PRI64_PREFIX"d, modify_time: %"PRI64_PREFIX"d, "
          "big_file_size: %"PRI64_PREFIX"d, max_tfs_file_size: %d, owner_id_: %"PRI64_PREFIX"d]",
          create_time_, modify_time_, big_file_size_, max_tfs_file_size_, owner_id_);
    }

    int ObjectMetaInfo::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, OBJECT_META_INFO_CREATE_TIME_TAG);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, create_time_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, OBJECT_META_INFO_MODIFY_TIME_TAG);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, modify_time_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, OBJECT_META_INFO_BIG_FILE_SIZE_TAG);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, big_file_size_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, OBJECT_META_INFO_MAX_TFS_FILE_SIZE_TAG);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, max_tfs_file_size_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, OBJECT_META_INFO_OWNER_ID_TAG);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, owner_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, END_TAG);
      }

      return ret;
    }

    int ObjectMetaInfo::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;

      while (TFS_SUCCESS == ret)
      {
        int32_t type_tag = 0;
        ret = Serialization::get_int32(data, data_len, pos, &type_tag);

        if (TFS_SUCCESS == ret)
        {
          switch (type_tag)
          {
            case OBJECT_META_INFO_CREATE_TIME_TAG:
              ret = Serialization::get_int64(data, data_len, pos, &create_time_);
              break;
            case OBJECT_META_INFO_MODIFY_TIME_TAG:
              ret = Serialization::get_int64(data, data_len, pos, &modify_time_);
              break;
            case OBJECT_META_INFO_BIG_FILE_SIZE_TAG:
              ret = Serialization::get_int64(data, data_len, pos, &big_file_size_);
              break;
            case OBJECT_META_INFO_MAX_TFS_FILE_SIZE_TAG:
              ret = Serialization::get_int32(data, data_len, pos, &max_tfs_file_size_);
              break;
            case OBJECT_META_INFO_OWNER_ID_TAG:
              ret = Serialization::get_int64(data, data_len, pos, &owner_id_);
              break;
            case END_TAG:
              ;
              break;
            default:
              TBSYS_LOG(ERROR, "object meta info: %d can't self-interpret", type_tag);
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


    //customizeinfo
    UserMetadata::UserMetadata()
    { }

    int64_t UserMetadata::length() const
    {
      int64_t len = INT_SIZE;
      MAP_STRING_STRING_ITER iter = meta_data_.begin();
      for (; iter != meta_data_.end(); iter++)
      {
        len += common::Serialization::get_string_length(iter->first);
        len += common::Serialization::get_string_length(iter->second);
      }

      return len;
    }

    void UserMetadata::dump() const
    {
      TBSYS_LOG(DEBUG, "UserMetadata: [meta_count: %"PRI64_PREFIX"d]", meta_data_.size());

      if (meta_data_.size() > 0)
      {
        MAP_STRING_STRING_ITER iter = meta_data_.begin();
        for (; iter != meta_data_.end(); iter++)
        {
          TBSYS_LOG(DEBUG, "UserMetadata: [key: %s value: %s]",iter->first.c_str(), iter->second.c_str());
        }
      }
    }

    int UserMetadata::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;

      if (TFS_SUCCESS == ret) {

        int32_t meta_count = static_cast<int32_t>(meta_data_.size());
        ret = Serialization::set_int32(data, data_len, pos, meta_count);

        if (TFS_SUCCESS == ret && meta_count > 0)
        {
          MAP_STRING_STRING_ITER iter = meta_data_.begin();
          for (; iter != meta_data_.end() && TFS_SUCCESS == ret; iter++)
          {
            ret = Serialization::set_string(data, data_len, pos, iter->first);
            if (TFS_SUCCESS == ret)
            {
              ret = Serialization::set_string(data, data_len, pos, iter->second);
            }
          }
        }
      }

      return ret;
    }

    int UserMetadata::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;

      int32_t meta_count = 0;

      ret = Serialization::get_int32(data, data_len, pos, &meta_count);

      if (TFS_SUCCESS == ret && meta_count > 0)
      {
        std::string key;
        std::string value;
        for (int32_t i = 0; i < meta_count && TFS_SUCCESS == ret; i++)
        {
          ret = Serialization::get_string(data, data_len, pos, key);
          if (TFS_SUCCESS == ret)
          {
            ret = Serialization::get_string(data, data_len, pos, value);
          }

          if (TFS_SUCCESS == ret)
          {
            meta_data_.insert(std::make_pair(key, value));
          }
        }
      }

      return ret;
    }

    //object meta info
    ObjectInfo::ObjectInfo()
      : has_meta_info_(false), has_user_metadata_(false)
    {v_tfs_file_info_.clear();}

    int64_t ObjectInfo::length() const
    {
      return (INT8_SIZE * 2 + INT_SIZE +
          (v_tfs_file_info_.size() * (INT64_SIZE * 4 + INT_SIZE + 6 * INT_SIZE)) +
          (has_meta_info_ ? (INT_SIZE + meta_info_.length()) : 0) +
          (has_user_metadata_ ? (INT_SIZE + user_metadata_.length()) : 0) +
          4 * INT_SIZE);
    }

    void ObjectInfo::dump() const
    {
      TBSYS_LOG(DEBUG, "ObjectInfo: [has_meta_info: %d, "
          "has_user_metadata: %d]",  has_meta_info_, has_user_metadata_);
      if (has_meta_info_)
      {
        meta_info_.dump();
      }
      if (has_user_metadata_)
      {
        user_metadata_.dump();
      }
      for (size_t i = 0; i < v_tfs_file_info_.size(); i++)
      {
        v_tfs_file_info_.at(i).dump();
      }
    }

    int ObjectInfo::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, OBJECT_INFO_V_TFS_FILE_INFO_TAG);
      }
      if (TFS_SUCCESS == ret)
      {
        int32_t tfs_file_count = static_cast<int32_t>(v_tfs_file_info_.size());
        common::Serialization::set_int32(data, data_len, pos, tfs_file_count);
        std::vector<common::TfsFileInfo>::const_iterator iter = v_tfs_file_info_.begin();
        for (; iter != v_tfs_file_info_.end(); iter++)
        {
          (*iter).serialize(data, data_len, pos);
        }
        ret = common::TFS_SUCCESS;
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, OBJECT_INFO_HAS_META_INFO_TAG);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int8(data, data_len, pos, has_meta_info_);
      }
      if (TFS_SUCCESS == ret && has_meta_info_)
      {
        ret = Serialization::set_int32(data, data_len, pos, OBJECT_INFO_META_INFO_TAG);
        if (TFS_SUCCESS == ret)
        {
          ret = meta_info_.serialize(data, data_len, pos);
        }
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, OBJECT_INFO_HAS_USER_METADATA_TAG);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int8(data, data_len, pos, has_user_metadata_);
      }
      if (TFS_SUCCESS == ret && has_user_metadata_)
      {
        ret = Serialization::set_int32(data, data_len, pos, OBJECT_INFO_USER_METADATA_TAG);
        if (TFS_SUCCESS == ret)
        {
          ret = user_metadata_.serialize(data, data_len, pos);
        }
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, END_TAG);
      }

      return ret;
    }

    int ObjectInfo::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;

      while (TFS_SUCCESS == ret)
      {
        int32_t type_tag = 0;
        ret = Serialization::get_int32(data, data_len, pos, &type_tag);

        if (TFS_SUCCESS == ret)
        {
          switch (type_tag)
          {
            case OBJECT_INFO_V_TFS_FILE_INFO_TAG:
              if (TFS_SUCCESS == ret)
              {
                int32_t tfs_file_count = 0;
                ret = common::Serialization::get_int32(data, data_len, pos, &tfs_file_count);
                if (TFS_SUCCESS == ret)
                {
                  TfsFileInfo tmp;
                  int i = 0;
                  for (; i < tfs_file_count && TFS_SUCCESS == ret; i++)
                  {
                    ret = tmp.deserialize(data, data_len, pos);
                    v_tfs_file_info_.push_back(tmp);
                  }
                }
              }
              break;
            case OBJECT_INFO_HAS_META_INFO_TAG:
              ret = Serialization::get_int8(data, data_len, pos, reinterpret_cast<int8_t*>(&has_meta_info_));
              break;
            case OBJECT_INFO_META_INFO_TAG:
              ret = meta_info_.deserialize(data, data_len, pos);
              break;
            case OBJECT_INFO_HAS_USER_METADATA_TAG:
              ret = Serialization::get_int8(data, data_len, pos, reinterpret_cast<int8_t*>(&has_user_metadata_));
              break;
            case OBJECT_INFO_USER_METADATA_TAG:
              ret = user_metadata_.deserialize(data, data_len, pos);
              break;
            case END_TAG:
              ;
              break;
            default:
              TBSYS_LOG(ERROR, "object info: %d can't self-interpret", type_tag);
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

    //bucketmetainfo
    BucketMetaInfo::BucketMetaInfo()
      :create_time_(0), owner_id_(0)
    {}

    int64_t BucketMetaInfo::length() const
    {
      //create_time_ owner_id_
      int64_t len = INT64_SIZE * 2 + INT_SIZE * 2;
       //add bucket acl map
      len += INT_SIZE * 2;//tag and and size
      len += (INT64_SIZE+INT_SIZE) * bucket_acl_map_.size();
      len += INT_SIZE; //end tag

      return len;
    }

    int BucketMetaInfo::serialize(char *data, const int64_t data_len, int64_t &pos) const
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, BUCKET_META_INFO_CREATE_TIME_TAG);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, create_time_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, BUCKET_META_INFO_OWNER_ID_TAG);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, owner_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, BUCKET_META_INFO_BUCKET_ACL_MAP_TAG);

        if (TFS_SUCCESS == ret)
        {
          int32_t size = bucket_acl_map_.size();
          ret = Serialization::set_int32(data, data_len, pos, size);
        }
        if (TFS_SUCCESS == ret)
        {
          MAP_INT64_INT_ITER iter = bucket_acl_map_.begin();
          for (; iter != bucket_acl_map_.end() && TFS_SUCCESS == ret; iter++)
          {
            ret = Serialization::set_int64(data, data_len, pos, iter->first);
            if (TFS_SUCCESS == ret)
            {
              ret = Serialization::set_int32(data, data_len, pos, iter->second);
            }
          }
        }
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, END_TAG);
      }
      return ret;
    }

    int BucketMetaInfo::deserialize(const char *data, const int64_t data_len, int64_t &pos)
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
            case BUCKET_META_INFO_CREATE_TIME_TAG:
              ret = Serialization::get_int64(data, data_len, pos, &create_time_);
              break;
            case BUCKET_META_INFO_OWNER_ID_TAG:
              ret = Serialization::get_int64(data, data_len, pos, &owner_id_);
              break;
            case BUCKET_META_INFO_BUCKET_ACL_MAP_TAG:
              if (TFS_SUCCESS == ret)
              {
                int32_t size = -1;
                ret = Serialization::get_int32(data, data_len, pos, &size);

                if (TFS_SUCCESS == ret)
                {
                  int64_t key;
                  int32_t value;
                  for (int32_t i = 0; i < size && TFS_SUCCESS == ret; i++)
                  {
                    ret = Serialization::get_int64(data, data_len, pos, &key);
                    if (TFS_SUCCESS == ret)
                    {
                      ret = Serialization::get_int32(data, data_len, pos, &value);
                    }

                    if (TFS_SUCCESS == ret)
                    {
                      bucket_acl_map_.insert(std::make_pair(key, value));
                    }
                  }
                }
              }
              break;
            case END_TAG:
              ;
              break;
            default:
              TBSYS_LOG(ERROR, "bucket meta info: %d can't self-interpret", type_tag);
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

    //userinfo
    UserInfo::UserInfo()
      :owner_id_(0)
    {}
    int64_t UserInfo::length() const
    {
      return INT64_SIZE * 1 + INT_SIZE * 2;
    }

    int UserInfo::serialize(char *data, const int64_t data_len, int64_t &pos) const
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, USER_INFO_OWNER_ID_TAG);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, owner_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, END_TAG);
      }
      return ret;
    }

    int UserInfo::deserialize(const char *data, const int64_t data_len, int64_t &pos)
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
            case USER_INFO_OWNER_ID_TAG:
              ret = Serialization::get_int64(data, data_len, pos, &owner_id_);
              break;
            case END_TAG:
              ;
              break;
            default:
              TBSYS_LOG(ERROR, "user info: %d can't self-interpret", type_tag);
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

    //BucketsResult
    BucketsResult::BucketsResult()
      :owner_id_(0)
    {}

    int64_t BucketsResult::length() const
    {
      int64_t len = INT64_SIZE;

      //owner_id_tag
      len += INT_SIZE;

      //buckets_result_tag + map_size + end_tag
      len += 3 * INT_SIZE;

      MAP_BUCKET_INFO_ITER iter = bucket_info_map_.begin();
      for (; iter != bucket_info_map_.end(); iter++)
      {
        len += common::Serialization::get_string_length(iter->first);
        len += (iter->second).length();
      }

      return len;
    }

    int BucketsResult::serialize(char *data, const int64_t data_len, int64_t &pos) const
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, BUCKETS_RESULT_OWNER_ID_TAG);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, owner_id_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, BUCKETS_RESULT_BUCKET_INFO_MAP_TAG);
        if (TFS_SUCCESS == ret)
        {
          int32_t size = bucket_info_map_.size();
          ret = Serialization::set_int32(data, data_len, pos, size);
        }

        if (TFS_SUCCESS == ret)
        {
          MAP_BUCKET_INFO_ITER iter = bucket_info_map_.begin();
          for (; iter != bucket_info_map_.end() && TFS_SUCCESS == ret; iter++)
          {
            ret = Serialization::set_string(data, data_len, pos, iter->first);
            if (TFS_SUCCESS == ret)
            {
              (iter->second).serialize(data, data_len, pos);
            }
          }
        }
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, END_TAG);
      }

      return ret;
    }

    int BucketsResult::deserialize(const char *data, const int64_t data_len, int64_t &pos)
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
            case BUCKETS_RESULT_OWNER_ID_TAG:
              ret = Serialization::get_int64(data, data_len, pos, &owner_id_);
              break;
            case BUCKETS_RESULT_BUCKET_INFO_MAP_TAG:
              int32_t size;
              ret = Serialization::get_int32(data, data_len, pos, &size);

              if (TFS_SUCCESS == ret)
              {
                std::string key;
                BucketMetaInfo value;
                for (int32_t i = 0; i < size && TFS_SUCCESS == ret; i++)
                {
                  ret = Serialization::get_string(data, data_len, pos, key);
                  if (TFS_SUCCESS == ret)
                  {
                    ret = value.deserialize(data, data_len, pos);
                  }

                  if (TFS_SUCCESS == ret)
                  {
                    bucket_info_map_.insert(std::make_pair(key, value));
                  }
                }
              }
              break;
            case END_TAG:
              ;
              break;
            default:
              TBSYS_LOG(ERROR, "buckets result: %d can't self-interpret", type_tag);
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

  }
}

