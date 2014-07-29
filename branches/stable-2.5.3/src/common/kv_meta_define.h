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
#include <string>
#include <map>
#include "define.h"
#include "meta_define.h"

namespace tfs
{
  namespace common
  {
    typedef std::map<std::string, std::string> MAP_STRING;
    typedef std::map<std::string, int32_t> MAP_STRING_INT;
    typedef std::map<std::string, int32_t>::const_iterator  MAP_STRING_INT_ITER;
    typedef std::map<int64_t, int32_t> MAP_INT64_INT;
    typedef std::map<int64_t, int32_t>::const_iterator MAP_INT64_INT_ITER;

    class KvDefine
    {
      public:
      static const char PERIOD;
      static const char DASH;
      static const char DEFAULT_CHAR;
      static const int32_t MAX_LIMIT;
      static const int32_t MAX_BUCKETS_COUNT;
      static const int32_t VERSION_ERROR_RETRY_COUNT;
      static const int64_t MAX_VERSION;
      static const int64_t ADMIN_ID;
    };

    enum CANNED_ACL
    {
      PRIVATE  = 0,
      PUBLIC_READ = 1,
      PUBLIC_READ_WRITE = 2,
      AUTHENTICATED_READ = 3, //0-3 use for bucket and object
      BUCKET_OWNER_READ = 4,  //just for object
      BUCKET_OWNER_FULL_CONTROL = 5, //just for object
      LOG_DELIVERY_WRITE = 6 //just for bucket
    };

    enum PERMISSION
    {
      READ = 1,
      WRITE = 2,
      READ_ACP = 4,
      WRITE_ACP = 8,
      FULL_CONTROL = READ | WRITE | READ_ACP | WRITE_ACP
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

    struct CustomizeInfo
    {
      CustomizeInfo();
      int64_t length() const;
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      void dump() const;

      std::string otag_;
    };

    struct ObjectInfo
    {
      ObjectInfo();
      int64_t length() const;
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      void dump() const;

      bool has_meta_info_;

      bool has_customize_info_;

      ObjectMetaInfo meta_info_;

      std::vector<TfsFileInfo> v_tfs_file_info_;

      CustomizeInfo customize_info_;
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
      MAP_INT64_INT bucket_acl_map_;
    };

    struct UserInfo
    {
      UserInfo();
      int64_t length() const;
      int serialize(char *data, const int64_t data_len, int64_t &pos) const;
      int deserialize(const char *data, const int64_t data_len, int64_t &pos);

      int64_t owner_id_;
    };

    typedef std::map<std::string, BucketMetaInfo> MAP_BUCKET_INFO;
    typedef std::map<std::string, BucketMetaInfo>::const_iterator MAP_BUCKET_INFO_ITER;
    struct BucketsResult
    {
      BucketsResult();
      int64_t length() const;
      int serialize(char *data, const int64_t data_len, int64_t &pos) const;
      int deserialize(const char *data, const int64_t data_len, int64_t &pos);
      int64_t owner_id_;
      MAP_BUCKET_INFO bucket_info_map_;
    };
  }
}

#endif
