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
      TFS_FILE_INFO_BLOCK_ID_TAG = 0,
      TFS_FILE_INFO_FILE_ID_TAG,
      TFS_FILE_INFO_CLUSTER_ID_TAG,
      TFS_FILE_INFO_OFFSET_TAG,
      TFS_FILE_INFO_FILE_SIZE_TAG,

      //ObjectMetaInfo struct
      OBJECT_META_INFO_CREATE_TIME_TAG,
      OBJECT_META_INFO_MODIFY_TIME_TAG,
      OBJECT_META_INFO_MAX_TFS_FILE_SIZE_TAG,
      OBJECT_META_INFO_BIG_FILE_SIZE_TAG,

      //CustomizeInfo struct
      CUSTOMIZE_INFO_OTAG_TAG,

      //ObjectInfo struct
      OBJECT_INFO_HAS_META_INFO_TAG,
      OBJECT_INFO_HAS_CUSTOMIZE_INFO_TAG,
      OBJECT_INFO_META_INFO_TAG,
      OBJECT_INFO_V_TFS_FILE_INFO_TAG,
      OBJECT_INFO_CUSTOMIZE_INFO_TAG,

      //BucketMetaInfo struct
      BUCKET_META_INFO_CREATE_TIME_TAG
    };

    const char PERIOD = '.';
    const char DASH = '-';
    const char DEFAULT_CHAR = 7;
    const int32_t MAX_LIMIT = 1000;
    const int32_t MAX_RETRY_COUNT = 3;

    struct TfsFileInfo
    {
      TfsFileInfo();
      int64_t length() const;
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      void dump() const;

      int32_t block_id_tag_;
      int64_t block_id_;

      int32_t file_id_tag_;
      int64_t file_id_;

      int32_t cluster_id_tag_;
      int32_t cluster_id_;

      int32_t file_size_tag_;
      int64_t file_size_;

      int32_t offset_tag_;
      int64_t offset_;
    };

    struct ObjectMetaInfo
    {
      ObjectMetaInfo();
      int64_t length() const;
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      void dump() const;

      int32_t create_time_tag_;
      int64_t create_time_;

      int32_t modify_time_tag_;
      int64_t modify_time_;

      int32_t max_tfs_file_size_tag_;
      int32_t max_tfs_file_size_;

      int32_t big_file_size_tag_;
      int64_t big_file_size_;
    };

    struct CustomizeInfo
    {
      CustomizeInfo();
      int64_t length() const;
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);

      int32_t otag_tag_;
      std::string otag_;
    };

    struct ObjectInfo
    {
      ObjectInfo();
      int64_t length() const;
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      void dump() const;

      int32_t has_meta_info_tag_;
      bool has_meta_info_;

      int32_t has_customize_info_tag_;
      bool has_customize_info_;

      int32_t meta_info_tag_;
      ObjectMetaInfo meta_info_;

      int32_t v_tfs_file_info_tag_;
      std::vector<TfsFileInfo> v_tfs_file_info_;

      int32_t customize_info_tag_;
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

      int32_t create_time_tag_;
      int64_t create_time_;
    };

  }
}

#endif
