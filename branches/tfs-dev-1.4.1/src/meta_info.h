/*
* (C) 2007-2010 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
*
* Version: $Id
*
* Authors:
*   daoan <daoan@taobao.com>
*      - initial release
*
*/

#ifndef TFS_NAMEMETASERVER_METAINFO_H_
#define TFS_NAMEMETASERVER_METAINFO_H_
#include <vector>
#include <string>
#include "common/serialization.h"

namespace tfs
{
  namespace namemetaserver 
  {
    struct FragMeta
    {
      int64_t offset_;
      uint64_t file_id_;
      int32_t size_;
      uint32_t block_id_;
      int64_t get_length() const
      {
        return 2 * sizeof(int64_t) + 2 * sizeof(int32_t);
      }
      int serialize(char* data, int64_t& length, int64_t& pos) const
      {
        length = get_length();
        common::Serialization::set_int64(data, length, pos, offset_);
        common::Serialization::set_int64(data, length, pos, file_id_);
        common::Serialization::set_int32(data, length, pos, size_);
        common::Serialization::set_int32(data, length, pos, block_id_);
        return common::TFS_SUCCESS;
      }
    };
    struct FragInfo
    {
      FragInfo() : cluster_id_(0)
        {
        }
      FragInfo(std::vector<FragMeta> v_frag_meta) :
        v_frag_meta_(v_frag_meta)
        {
        }
      int32_t cluster_id_;
      std::vector<FragMeta> v_frag_meta_;
      int64_t get_length() const
      {
        return sizeof(int32_t) * 2 + v_frag_meta_.size() * sizeof(FragInfo);
      }
      int serialize(char* data, int64_t& length, int64_t& pos) const
      {
        length = get_length();
        common::Serialization::set_int32(data, length, pos, v_frag_meta_.size());
        common::Serialization::set_int32(data, length, pos, cluster_id_);
        std::vector<FragMeta>::const_iterator iter = v_frag_meta_.begin();
        for (; iter != v_frag_meta_.end(); iter++)
        {
          (*iter).serialize(data, length, pos);
        }
        return common::TFS_SUCCESS;
      }
    };
    struct MetaInfo
    {
      public:
        MetaInfo():
          pid_(-1), id_(0), create_time_(0), modify_time_(0),
          size_(0), ver_no_(0)
      {
      }
      MetaInfo(FragInfo frag_info):
          pid_(-1), id_(0), create_time_(0), modify_time_(0),
          size_(0), ver_no_(0), frag_info_(frag_info)
        {
        }
      
      int64_t pid_;
      int64_t id_;
      int32_t create_time_;
      int32_t modify_time_;
      int64_t size_;
      int16_t ver_no_;
      std::string name_;
      FragInfo frag_info_;
      int32_t get_frag_length()
      {
        return frag_info_.get_length();
      }
      int get_frag_info(char* data, int64_t& length, int64_t& pos)
      {
        return frag_info_.serialize(data, length, pos);
      }
    };
  }
}

#endif
