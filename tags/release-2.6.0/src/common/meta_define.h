/*
 * (C) 2007-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   mingyan <mingyan.zc@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_COMMON_META_DEFINE_H_
#define TFS_COMMON_META_DEFINE_H_

#include "define.h"

namespace tfs
{
  namespace common
  {
    class Stream;
    struct FragMeta
    {
      FragMeta();
      FragMeta(const uint32_t block_id, const uint64_t file_id, const int64_t offset, const int32_t size);

      static int64_t get_length();
      bool operator < (const FragMeta& right) const;

      // for store
      int serialize(char* data, const int64_t buff_len, int64_t& pos) const;
      int deserialize(char* data, const int64_t data_len, int64_t& pos);
      // for packet
      int serialize(common::Stream& output) const;
      int deserialize(common::Stream& input);

      uint64_t file_id_;
      int64_t offset_;
      uint32_t block_id_;
      int32_t size_;
    };
  }
}

#endif
