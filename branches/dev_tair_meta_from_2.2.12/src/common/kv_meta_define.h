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
#include <set>
#include <string>
#include "define.h"
#include "meta_define.h"

namespace tfs
{
  namespace common
  {
    const char PERIOD = '.';
    const char DASH = '-';
    const char DEFAULT_CHAR = 7;
    const char MULTIPART_UPLOAD_KEY = 2;
    const int32_t MAX_OWN_BUCKETS_SIZE = 100;
    const int32_t MAX_LIMIT = 1000;
    const int32_t VERSION_ERROR_RETRY_COUNT = 3;
    const int64_t MAX_VERSION = (1L<<16) - 1;
    const int32_t MAX_BUCKET_TAG_SIZE = 10;

    const int32_t MAX_TAG_KEY_LEN = 128;
    const int32_t MAX_TAG_VALUE_LEN = 256;
    const int32_t MAX_OBJECT_NAME_SIZE = 1024;
    const int32_t MAX_CUSTOMIZE_INFO_SIZE = 2*(1<<10);
    typedef std::map<std::string, std::string> MAP_STRING;
    typedef std::map<std::string, std::string>::const_iterator MAP_STRING_ITER;
    typedef std::map<std::string, int32_t> MAP_STRING_INT;
    typedef std::map<std::string, int32_t>::const_iterator  MAP_STRING_INT_ITER;
    typedef std::map<int64_t, int32_t> MAP_INT64_INT;
    typedef std::map<int64_t, int32_t>::const_iterator MAP_INT64_INT_ITER;

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

    /*------------MULTIPART-------------------------------*/
    const int32_t PARTNUM_BASE = 10000000;//because no meta_info_.max_tfs_file_size_ > PARTNUM_BASE
    const int32_t PARTNUM_MIN = 1;
    const int32_t PARTNUM_MAX = 10000;
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

      // meta_data_ (size of key and value not over 2k)
      MAP_STRING meta_data_;
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

      bool has_tag_info_;
      MAP_STRING bucket_tag_map_;
      MAP_INT64_INT bucket_acl_map_;

      bool logging_status_;
      std::string target_bucket_name_;
      std::string target_prefix_;
    };

    struct UserInfo
    {
      UserInfo();
      int64_t length() const;
      int serialize(char *data, const int64_t data_len, int64_t &pos) const;
      int deserialize(const char *data, const int64_t data_len, int64_t &pos);

      int64_t owner_id_;
    };

    struct ObjectUploadInfo
    {
      ObjectUploadInfo();
      int64_t length() const;
      int serialize(char *data, const int64_t data_len, int64_t &pos) const;
      int deserialize(const char *data, const int64_t data_len, int64_t &pos);

      std::string object_name_;
      std::string upload_id_;
      int64_t owner_id_;
    };

    struct ListMultipartObjectResult
    {
      ListMultipartObjectResult();

      std::string bucket_name_;
      std::string start_key_;
      std::string start_id_;
      std::string next_start_key_;
      std::string next_start_id_;

      std::set<std::string> s_common_prefix_;
      std::vector<common::ObjectUploadInfo> v_object_upload_info_;
      int32_t limit_;
      char delimiter_;
      bool is_truncated_;
    };

    struct AuthorizeValueInfo
    {
      AuthorizeValueInfo();
      int64_t length() const;
      int serialize(char *data, const int64_t data_len, int64_t &pos) const;
      int deserialize(const char *data, const int64_t data_len, int64_t &pos);
      std::string access_secret_key_;
      std::string user_name_;
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

    struct DeleteResult
    {
      DeleteResult();
      int64_t length() const;
      int serialize(char *data, const int64_t data_len, int64_t &pos) const;
      int deserialize(const char *data, const int64_t data_len, int64_t &pos);

      std::vector<std::string> v_suc_objects_;
      std::vector<std::string> v_fail_objects_;
      std::vector<std::string> v_fail_msg_;
    };
  }
}

#endif
