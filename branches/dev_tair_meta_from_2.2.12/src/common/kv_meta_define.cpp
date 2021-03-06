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
      int ret = NULL != data/* && data_len - pos >= length()*/ ? TFS_SUCCESS : TFS_ERROR;

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
    CustomizeInfo::CustomizeInfo()
    {
      meta_data_.clear();
    }

    int64_t CustomizeInfo::length() const
    {
      int64_t len = 0;
      len += INT_SIZE;

      if (meta_data_.size() > 0)
      {
        MAP_STRING_ITER iter = meta_data_.begin();
        for (; iter != meta_data_.end(); iter++)
        {
          len += common::Serialization::get_string_length(iter->first);
          len += common::Serialization::get_string_length(iter->second);
        }
      }

      return len + INT_SIZE * 2;
    }

    void CustomizeInfo::dump() const
    {
      int32_t meta_size = meta_data_.size();
      if (meta_size > 0)
      {
        MAP_STRING_ITER iter = meta_data_.begin();
        for (; iter != meta_data_.end(); iter++)
        {
          TBSYS_LOG(DEBUG, "CustomizeInfo: [key: %s, value: %s]", iter->first.c_str(), iter->second.c_str());
        }
      }
    }

    int CustomizeInfo::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, CUSTOMIZE_INFO_META_DATA_TAG);
      }

      int32_t meta_size = static_cast<int32_t>(meta_data_.size());
      ret = Serialization::set_int32(data, data_len, pos, meta_size);

      if (TFS_SUCCESS == ret && meta_size > 0)
      {
        MAP_STRING_ITER iter = meta_data_.begin();
        for (; iter != meta_data_.end() && TFS_SUCCESS == ret; iter++)
        {
          ret = Serialization::set_string(data, data_len, pos, iter->first);
          if (TFS_SUCCESS == ret)
          {
            ret = Serialization::set_string(data, data_len, pos, iter->second);
          }
        }
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, END_TAG);
      }
      return ret;
    }

    int CustomizeInfo::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int ret = NULL != data/* && data_len - pos >= length()*/ ? TFS_SUCCESS : TFS_ERROR;

      while (TFS_SUCCESS == ret)
      {
        int32_t type_tag = 0;
        ret = Serialization::get_int32(data, data_len, pos, &type_tag);

        int32_t size = 0;
        if (TFS_SUCCESS == ret)
        {
          switch (type_tag)
          {
            case CUSTOMIZE_INFO_META_DATA_TAG:
              ret = Serialization::get_int32(data, data_len, pos, &size);

              if (TFS_SUCCESS == ret && size > 0)
              {
                std::string key;
                std::string value;
                for (int32_t i = 0; i < size && TFS_SUCCESS == ret; i++)
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
              break;
            case END_TAG:
              ;
              break;
            default:
              TBSYS_LOG(ERROR, "customize info: %d can't self-interpret", type_tag);
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
    ObjectInfo::ObjectInfo()
      : has_meta_info_(false), has_customize_info_(false)
    {v_tfs_file_info_.clear();}

    int64_t ObjectInfo::length() const
    {
      return (INT8_SIZE * 2 + INT_SIZE +
          (v_tfs_file_info_.size() * (INT64_SIZE * 4 + INT_SIZE + 6 * INT_SIZE)) +
          (has_meta_info_ ? (INT_SIZE + meta_info_.length()) : 0) +
          (has_customize_info_ ? (INT_SIZE + customize_info_.length()) : 0) +
          4 * INT_SIZE);
    }

    void ObjectInfo::dump() const
    {
      TBSYS_LOG(DEBUG, "ObjectInfo: [has_meta_info: %d, "
          "has_customize_info: %d]",  has_meta_info_, has_customize_info_);
      if (has_meta_info_)
      {
        meta_info_.dump();
      }
      if (has_customize_info_)
      {
        customize_info_.dump();
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
        ret = Serialization::set_int32(data, data_len, pos, OBJECT_INFO_HAS_CUSTOMIZE_INFO_TAG);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int8(data, data_len, pos, has_customize_info_);
      }
      if (TFS_SUCCESS == ret && has_customize_info_)
      {
        ret = Serialization::set_int32(data, data_len, pos, OBJECT_INFO_CUSTOMIZE_INFO_TAG);
        if (TFS_SUCCESS == ret)
        {
          ret = customize_info_.serialize(data, data_len, pos);
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
      int ret = NULL != data/* && data_len - pos >= length()*/ ? TFS_SUCCESS : TFS_ERROR;

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
              if (has_meta_info_)
              {
                ret = meta_info_.deserialize(data, data_len, pos);
              }
              break;
            case OBJECT_INFO_HAS_CUSTOMIZE_INFO_TAG:
              ret = Serialization::get_int8(data, data_len, pos, reinterpret_cast<int8_t*>(&has_customize_info_));
              break;
            case OBJECT_INFO_CUSTOMIZE_INFO_TAG:
              if (has_customize_info_)
              {
                ret = customize_info_.deserialize(data, data_len, pos);
              }
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
      :create_time_(0), owner_id_(0), has_tag_info_(false), logging_status_(false)
    {}

    int64_t BucketMetaInfo::length() const
    {
      int64_t len = INT64_SIZE * 2 + INT_SIZE * 3;
      //int64_t len = INT64_SIZE * 2 + INT_SIZE * 4 + INT8_SIZE;

      if (has_tag_info_)
      {
        len += INT_SIZE * 2;
        MAP_STRING_ITER iter = bucket_tag_map_.begin();
        for (; iter != bucket_tag_map_.end(); iter++)
        {
          len += common::Serialization::get_string_length(iter->first);
          len += common::Serialization::get_string_length(iter->second);
        }
      }

      //add bucket acl map
      /*
      len += INT_SIZE * 2;
      MAP_INT64_INT_ITER iter = bucket_acl_map_.begin();
      for (; iter != bucket_acl_map_.end(); iter++)
      {
        len += INT64_SIZE;
        len += INT_SIZE;
      }
      */
      //add bucket logging
      /*
      len += INT_SIZE * 3; //tag
      len += INT8_SIZE;
      len += common::Serialization::get_string_length(target_bucket_name_);
      len += common::Serialization::get_string_length(target_prefix_);
      */
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
        ret = Serialization::set_int32(data, data_len, pos, BUCKET_META_INFO_HAS_TAG_INFO_TAG);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int8(data, data_len, pos, has_tag_info_);
      }
      if (TFS_SUCCESS == ret && has_tag_info_)
      {
        ret = Serialization::set_int32(data, data_len, pos, BUCKET_META_INFO_BUCKET_TAG_MAP_TAG);
        if (TFS_SUCCESS == ret)
        {
          int32_t size = bucket_tag_map_.size();
          ret = Serialization::set_int32(data, data_len, pos, size);
        }

        if (TFS_SUCCESS == ret)
        {
          MAP_STRING_ITER iter = bucket_tag_map_.begin();
          for (; iter != bucket_tag_map_.end() && TFS_SUCCESS == ret; iter++)
          {
            ret = Serialization::set_string(data, data_len, pos, iter->first);
            if (TFS_SUCCESS == ret)
            {
              ret = Serialization::set_string(data, data_len, pos, iter->second);
            }
          }
        }
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
        ret = Serialization::set_int32(data, data_len, pos, BUCKET_META_INFO_LOGGING_STATUS_TAG);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int8(data, data_len, pos, logging_status_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, BUCKET_META_INFO_TARGET_BUCKET_NAME_TAG);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_string(data, data_len, pos, target_bucket_name_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, BUCKET_META_INFO_TARGET_PREFIX_TAG);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_string(data, data_len, pos, target_prefix_);
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
            case BUCKET_META_INFO_HAS_TAG_INFO_TAG:
              ret = Serialization::get_int8(data, data_len, pos, reinterpret_cast<int8_t*>(&has_tag_info_));
              break;
            case BUCKET_META_INFO_BUCKET_TAG_MAP_TAG:
              if (has_tag_info_)
              {
                int32_t size = 0;
                ret = Serialization::get_int32(data, data_len, pos, &size);

                if (TFS_SUCCESS == ret)
                {
                  std::string key;
                  std::string value;
                  for (int32_t i = 0; i < size && TFS_SUCCESS == ret; i++)
                  {
                    ret = Serialization::get_string(data, data_len, pos, key);
                    if (TFS_SUCCESS == ret)
                    {
                      ret = Serialization::get_string(data, data_len, pos, value);
                    }

                    if (TFS_SUCCESS == ret)
                    {
                      bucket_tag_map_.insert(std::make_pair(key, value));
                    }
                  }
                }
              }
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
            case BUCKET_META_INFO_LOGGING_STATUS_TAG:
              ret = Serialization::get_int8(data, data_len, pos, reinterpret_cast<int8_t*>(&logging_status_));
              break;
            case BUCKET_META_INFO_TARGET_BUCKET_NAME_TAG:
              ret = Serialization::get_string(data, data_len, pos, target_bucket_name_);
              break;
            case BUCKET_META_INFO_TARGET_PREFIX_TAG:
              ret = Serialization::get_string(data, data_len, pos, target_prefix_);
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

    //AuthorizeValueInfo
    AuthorizeValueInfo::AuthorizeValueInfo()
    {}
    int64_t AuthorizeValueInfo::length() const
    {
      return Serialization::get_string_length(access_secret_key_) + Serialization::get_string_length(user_name_);
    }

    int AuthorizeValueInfo::serialize(char *data, const int64_t data_len, int64_t &pos) const
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_string(data, data_len, pos, access_secret_key_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_string(data, data_len, pos, user_name_);
      }
      return ret;
    }

    int AuthorizeValueInfo::deserialize(const char *data, const int64_t data_len, int64_t &pos)
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_string(data, data_len, pos, access_secret_key_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_string(data, data_len, pos, user_name_);
      }
      return ret;
    }

    //ObjectUploadInfo
    ObjectUploadInfo::ObjectUploadInfo()
      :owner_id_(0)
    {}
    int64_t ObjectUploadInfo::length() const
    {
      return INT64_SIZE
        + Serialization::get_string_length(object_name_)
        + Serialization::get_string_length(upload_id_)
        + 4 * INT_SIZE;
    }

    int ObjectUploadInfo::serialize(char *data, const int64_t data_len, int64_t &pos) const
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, OBJECT_UPLOAD_INFO_OBJECT_NAME_TAG);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_string(data, data_len, pos, object_name_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, OBJECT_UPLOAD_INFO_UPLOAD_ID_TAG);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_string(data, data_len, pos, upload_id_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, OBJECT_UPLOAD_INFO_OWNER_ID_TAG);
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

    int ObjectUploadInfo::deserialize(const char *data, const int64_t data_len, int64_t &pos)
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
            case OBJECT_UPLOAD_INFO_OBJECT_NAME_TAG:
              ret = Serialization::get_string(data, data_len, pos, object_name_);
              break;
            case OBJECT_UPLOAD_INFO_UPLOAD_ID_TAG:
              ret = Serialization::get_string(data, data_len, pos, upload_id_);
              break;
            case OBJECT_UPLOAD_INFO_OWNER_ID_TAG:
              ret = Serialization::get_int64(data, data_len, pos, &owner_id_);
              break;
            case END_TAG:
              ;
              break;
            default:
              TBSYS_LOG(ERROR, "object upload info: %d can't self-interpret", type_tag);
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

    //list_multipart_object_result
    ListMultipartObjectResult::ListMultipartObjectResult()
      :limit_(0), delimiter_(DEFAULT_CHAR), is_truncated_(false)
    {
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

    //DeleteResult
    DeleteResult::DeleteResult()
    {}

    int64_t DeleteResult::length() const
    {
      int64_t len = 0;

      //v_suc_obj_tag + v_fail_obj_tag + v_fail_obj_tag + end_tag
      len += 4 * INT_SIZE;

      len += Serialization::get_vstring_length(v_suc_objects_);
      len += Serialization::get_vstring_length(v_fail_objects_);
      len += Serialization::get_vstring_length(v_fail_msg_);

      return len;
    }

    int DeleteResult::serialize(char *data, const int64_t data_len, int64_t &pos) const
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, DELETE_RESULT_V_SUC_OBJECTS_TAG);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_vstring(data, data_len, pos, v_suc_objects_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, DELETE_RESULT_V_FAIL_OBJECTS_TAG);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_vstring(data, data_len, pos, v_fail_objects_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, DELETE_RESULT_V_FAIL_MSG_TAG);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_vstring(data, data_len, pos, v_fail_msg_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, END_TAG);
      }

      return ret;
    }

    int DeleteResult::deserialize(const char *data, const int64_t data_len, int64_t &pos)
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
            case DELETE_RESULT_V_SUC_OBJECTS_TAG:
              ret = Serialization::get_vstring(data, data_len, pos, v_suc_objects_);
              break;
            case DELETE_RESULT_V_FAIL_OBJECTS_TAG:
              ret = Serialization::get_vstring(data, data_len, pos, v_fail_objects_);
              break;
            case DELETE_RESULT_V_FAIL_MSG_TAG:
              ret = Serialization::get_vstring(data, data_len, pos, v_fail_msg_);
              break;
            case END_TAG:
              ;
              break;
            default:
              TBSYS_LOG(ERROR, "delete result: %d can't self-interpret", type_tag);
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

