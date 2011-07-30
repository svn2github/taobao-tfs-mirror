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
#include "common/stream.h"

namespace tfs
{
  namespace namemetaserver
  {
    struct FragMeta
    {
      FragMeta(): offset_(0), file_id_(0), size_(0), block_id_(0)
      { };
      FragMeta(int64_t offset, uint64_t file_id, int32_t size, uint32_t block_id): offset_(offset), file_id_(file_id), size_(size), block_id_(block_id)
      { };
      int64_t offset_;
      uint64_t file_id_;
      int32_t size_;
      uint32_t block_id_;
      static int64_t get_length()
      {
        return 2 * sizeof(int64_t) + 2 * sizeof(int32_t);
      }
      int serialize(char* data, const int64_t buff_len, int64_t& pos) const
      {
        int ret = common::TFS_ERROR;
        if (buff_len - pos >= get_length())
        {
          common::Serialization::set_int64(data, buff_len, pos, offset_);
          common::Serialization::set_int64(data, buff_len, pos, file_id_);
          common::Serialization::set_int32(data, buff_len, pos, size_);
          common::Serialization::set_int32(data, buff_len, pos, block_id_);
          ret = common::TFS_SUCCESS;
        }
        return ret;
      }

      int serialize(common::Stream& output) const
      {
        int ret = common::TFS_ERROR;
        if (output.get_free_length() > get_length())
        {
          ret = output.set_int64(offset_);
          if (common::TFS_SUCCESS == ret)
          {
            ret = output.set_int64(file_id_);
          }
          if (common::TFS_SUCCESS == ret)
          {
            ret = output.set_int32(size_);
          }
          if (common::TFS_SUCCESS == ret)
          {
            ret = output.set_int32(block_id_);
          }
        }
        return ret;
      }
      int deserialize(char* data, const int64_t data_len, int64_t& pos)
      {
        int ret = common::Serialization::get_int64(data, data_len, pos, &offset_);
        if (common::TFS_SUCCESS == ret)
        {
          ret = common::Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&file_id_));
        }
        if (common::TFS_SUCCESS == ret)
        {
          ret = common::Serialization::get_int32(data, data_len, pos, &size_);
        }
        if (common::TFS_SUCCESS == ret)
        {
          ret = common::Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&block_id_));
        }
        return ret;
      }
      int deserialize(common::Stream& input)
      {
        int ret = input.get_int64(&offset_);
        if (common::TFS_SUCCESS == ret)
        {
          ret = input.get_int64(reinterpret_cast<int64_t*>(&file_id_));
        }
        if (common::TFS_SUCCESS == ret)
        {
          ret = input.get_int32(&size_);
        }
        if (common::TFS_SUCCESS == ret)
        {
          ret = input.get_int32(reinterpret_cast<int32_t*>(&block_id_));
        }
        return ret;
      }
      bool operator < (const FragMeta& right) const
      {
        return offset_ < right.offset_;
      }
    };

    class FragInfo
    {
      public:
      FragInfo():
        cluster_id_(0), had_been_split_(false)
      {
      }
      explicit FragInfo(const std::vector<FragMeta>& v_frag_meta):
        cluster_id_(0), had_been_split_(false),
        v_frag_meta_(v_frag_meta)
      {
      }
      int32_t cluster_id_;
      bool had_been_split_;
      std::vector<FragMeta> v_frag_meta_;
      int64_t get_length() const
      {
        return sizeof(int32_t) * 2 + v_frag_meta_.size() * FragMeta::get_length();
      }
      int serialize(char* data, const int64_t buff_len, int64_t& pos) const
      {
        int ret = common::TFS_ERROR;
        if (buff_len - pos >= get_length())
        {
          int32_t frag_count = static_cast<int32_t>(v_frag_meta_.size());
          assert(frag_count  > 0 );
          if (had_been_split_)
          {
            frag_count |= (1 << 31);
          }
          common::Serialization::set_int32(data, buff_len, pos, frag_count);
          common::Serialization::set_int32(data, buff_len, pos, cluster_id_);
          std::vector<FragMeta>::const_iterator iter = v_frag_meta_.begin();
          for (; iter != v_frag_meta_.end(); iter++)
          {
            (*iter).serialize(data, buff_len, pos);
          }
          ret = common::TFS_SUCCESS;
        }
        return ret;
      }
      int serialize(common::Stream& output) const
      {
        int ret = common::TFS_ERROR;
        if (output.get_free_length() >= get_length())
        {
          int32_t frag_count = static_cast<int32_t>(v_frag_meta_.size());
          assert(frag_count  > 0 );
          if (had_been_split_)
          {
            frag_count |= (1 << 31);
          }
          ret = output.set_int32(frag_count);
          if (common::TFS_SUCCESS == ret)
          {
            ret = output.set_int32(cluster_id_);
          }
          std::vector<FragMeta>::const_iterator iter = v_frag_meta_.begin();
          for (; iter != v_frag_meta_.end(); iter++)
          {
            if (common::TFS_SUCCESS == ret)
            {
              ret = (*iter).serialize(output);
            }
          }
        }
        return ret;
      }
      int deserialize(char* data, const int64_t data_len, int64_t& pos)
      {
        int32_t frg_count = 0;
        int ret = common::Serialization::get_int32(data, data_len, pos, &frg_count);
        if (common::TFS_SUCCESS == ret)
        {
          had_been_split_ = (frg_count >> 31);
          frg_count = frg_count & ~(1 << 31);
          ret = common::Serialization::get_int32(data, data_len, pos, &cluster_id_);
        }
        if (common::TFS_SUCCESS == ret)
        {
          v_frag_meta_.clear();
          FragMeta tmp;
          for (int i = 0; i < frg_count && common::TFS_SUCCESS == ret; i ++)
          {
            ret = tmp.deserialize(data, data_len, pos);
            v_frag_meta_.push_back(tmp);
          }
        }
        return ret;
      }
      int deserialize(common::Stream& input)
      {
        int32_t frg_count = 0;
        int ret = input.get_int32(&frg_count);
        if (common::TFS_SUCCESS == ret)
        {
          had_been_split_ = (frg_count >> 31);
          frg_count = frg_count & ~(1 << 31);
          ret = input.get_int32(&cluster_id_);
        }
        if (common::TFS_SUCCESS == ret)
        {
          v_frag_meta_.clear();
          FragMeta tmp;
          for (int i = 0; i < frg_count && common::TFS_SUCCESS == ret; i ++)
          {
            ret = tmp.deserialize(input);
            v_frag_meta_.push_back(tmp);
          }
        }
        return ret;
      }
      int64_t get_last_offset() const
      {
        int64_t last_offset = 0;
        if (!v_frag_meta_.empty())
        {
          std::vector<FragMeta>::const_iterator it = v_frag_meta_.end() - 1;
          last_offset = it->offset_ + it->size_;
        }
        return last_offset;
      }
      void dump() const
      {
        fprintf(stdout, "cluster_id: %d\n", cluster_id_);
        fprintf(stdout, "had_been_split: %d\n", had_been_split_);
        fprintf(stdout, "frag_size : %zd\n", v_frag_meta_.size());

        std::vector<FragMeta>::const_iterator iter = v_frag_meta_.begin();
        for (; iter != v_frag_meta_.end(); iter++)
        {
          fprintf(stdout, "offset: %"PRI64_PREFIX"d  ", (*iter).offset_);
          fprintf(stdout, "file_id: %"PRI64_PREFIX"u  ", (*iter).file_id_);
          fprintf(stdout, "size: %d  ", (*iter).size_);
          fprintf(stdout, "block_id: %u\n", (*iter).block_id_);
        }
      }
    };
    class MetaInfo
    {
      public:
        MetaInfo():
          pid_(-1), id_(0), create_time_(0), modify_time_(0),
          size_(0), ver_no_(0)
      {
      }
        explicit MetaInfo(FragInfo frag_info):
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
    };
  }
}

#endif
