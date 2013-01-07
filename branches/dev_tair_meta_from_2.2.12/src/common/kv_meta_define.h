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

#include "define.h"
#include <string>

namespace tfs
{
  namespace common
  {
    const char PERIOD = '.';
    const char DASH = '-';
    const int32_t MAX_LIMIT = 1000;
    struct TfsFileInfo
    {
      TfsFileInfo();
      int64_t length() const;
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);

      int64_t block_id_;
      int64_t file_id_;
      int32_t cluster_id_;
    };

    struct ObjectMetaInfo
    {
      ObjectMetaInfo();
      int64_t length() const;
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);

      int64_t create_time_;
      int64_t modify_time_;
      int64_t file_size_;
      int32_t max_tfs_file_size_;
    };

    struct CustomizeInfo
    {
      CustomizeInfo();
      int64_t length() const;
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);

      std::string otag_;
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
    };
  }
}

#endif
