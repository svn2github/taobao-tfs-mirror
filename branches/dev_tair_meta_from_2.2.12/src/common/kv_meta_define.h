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

#include <string>
#include "define.h"
#include "meta_define.h"

namespace tfs
{
  namespace common
  {
    enum identify_id
    {
      //TfsFileInfo struct
      TFS_FILE_INFO_BLOCK_ID_TAG = 101,
      TFS_FILE_INFO_FILE_ID_TAG = 102,
      TFS_FILE_INFO_CLUSTER_ID_TAG = 103,
      TFS_FILE_INFO_OFFSET_TAG = 104,
      TFS_FILE_INFO_FILE_SIZE_TAG = 105,


      //ObjectMetaInfo struct
      OBJECT_META_INFO_CREATE_TIME_TAG = 201,
      OBJECT_META_INFO_MODIFY_TIME_TAG = 202,
      OBJECT_META_INFO_MAX_TFS_FILE_SIZE_TAG = 203,
      OBJECT_META_INFO_BIG_FILE_SIZE_TAG = 204,
      OBJECT_META_INFO_OWNER_ID_TAG = 205,

      //CustomizeInfo struct
      CUSTOMIZE_INFO_OTAG_TAG = 301,

      //ObjectInfo struct
      OBJECT_INFO_HAS_META_INFO_TAG = 401,
      OBJECT_INFO_HAS_CUSTOMIZE_INFO_TAG = 402,
      OBJECT_INFO_META_INFO_TAG = 403,
      OBJECT_INFO_V_TFS_FILE_INFO_TAG = 404,
      OBJECT_INFO_CUSTOMIZE_INFO_TAG = 405,

      //BucketMetaInfo struct
      BUCKET_META_INFO_CREATE_TIME_TAG = 501,
      BUCKET_META_INFO_OWNER_ID_TAG = 502,

      //UserInfo
      USER_INFO_OWNER_ID_TAG = 801,

      //End TAG
      END_TAG = 999
    };

    const char PERIOD = '.';
    const char DASH = '-';
    const char DEFAULT_CHAR = 7;
    const int32_t MAX_LIMIT = 1000;
    const int32_t VERSION_ERROR_RETRY_COUNT = 3;
    const int64_t MAX_VERSION = 1<<15 - 1;

    struct TfsFileInfo
    {
      TfsFileInfo();
      int64_t length() const;
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      void dump() const;

      int64_t block_id_;

      int64_t file_id_;

      int32_t cluster_id_;

      int64_t file_size_;

      int64_t offset_;
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
