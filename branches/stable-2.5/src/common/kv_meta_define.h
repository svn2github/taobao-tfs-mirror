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
#ifndef TFS_COMMON_KV_META_DEFINE_H_
#define TFS_COMMON_KV_META_DEFINE_H_

#include <vector>
#include <map>
#include <vector>
#include <string>
#include "define.h"
#include "meta_define.h"

namespace tfs
{
  namespace common
  {

    typedef std::map<std::string, std::string> META_MAP_STRING;
    typedef std::map<std::string, std::string>::const_iterator CMETA_MAP_STRING_ITER;
    const int32_t USER_METADATA_LENGTH = 2048;

    class KvDefine
    {
      public:
        static const char PERIOD;
        static const char DASH;
        static const char DEFAULT_CHAR;
        static const int32_t MAX_LIMIT;
        static const int32_t VERSION_ERROR_RETRY_COUNT;
        static const int64_t MAX_VERSION;
    };

    struct TfsFileInfo
    {
      TfsFileInfo();
      TfsFileInfo(int32_t cluster_id, int64_t block_id, int64_t file_id, int64_t offset, int64_t size);
      int64_t length() const;
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      void dump() const;

      int32_t cluster_id_;

      int64_t block_id_;

      int64_t file_id_;

      int64_t offset_;

      int64_t file_size_;
    };

    struct ObjectMetaInfo
    {
      ObjectMetaInfo();
      int64_t length() const;
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      void dump() const;

      int64_t create_time_;

      int64_t modify_time_;

      int32_t max_tfs_file_size_;

      int64_t big_file_size_;

      int64_t owner_id_;
    };

    struct UserMetadata
    {
      UserMetadata();
      int64_t length() const;
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      void dump() const;

      const META_MAP_STRING& get_meta_data() const
      {
        return meta_data_;
      }

      META_MAP_STRING& get_mutable_meta_data()
      {
        return meta_data_;
      }

      void set_meta_data(const META_MAP_STRING& meta_data)
      {
        meta_data_ = meta_data;
      }

      void clear_meta_data()
      {
        meta_data_.clear();
      }

      const bool is_meta_data_excessed() const
      {
        return length() > USER_METADATA_LENGTH;
      }

      META_MAP_STRING meta_data_;
    };

    struct ObjectInfo
    {
      ObjectInfo();
      int64_t length() const;
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      void dump() const;

      const UserMetadata& get_user_metadata() const
      {
        return user_metadata_;
      }

      UserMetadata& get_mutable_user_metadata()
      {
        return user_metadata_;
      }

      void set_user_metadata(const UserMetadata& user_metadata)
      {
        user_metadata_ = user_metadata;
      }

      bool has_meta_info_;

      bool has_user_metadata_;

      ObjectMetaInfo meta_info_;

      std::vector<TfsFileInfo> v_tfs_file_info_;

      UserMetadata user_metadata_;

    };

    struct BucketMetaInfo
    {
      BucketMetaInfo();
      int64_t length() const;
      int serialize(char *data, const int64_t data_len, int64_t &pos) const;
      int deserialize(const char *data, const int64_t data_len, int64_t &pos);

      void set_create_time(const int64_t create_time)
      {
        create_time_ = create_time;
      }

      int64_t create_time_;
      int64_t owner_id_;
    };

    struct UserInfo
    {
      UserInfo();
      int64_t length() const;
      int serialize(char *data, const int64_t data_len, int64_t &pos) const;
      int deserialize(const char *data, const int64_t data_len, int64_t &pos);

      int64_t owner_id_;
    };

  }
}

#endif
