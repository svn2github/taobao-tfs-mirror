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
 *   chuyu <chuyu@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_NAMEMETASERVER_META_SERVER_DEFINE_H_
#define TFS_NAMEMETASERVER_META_SERVER_DEFINE_H_

#include <vector>
#include <string>
#include "serialization.h"
#include "stream.h"

namespace tfs
{
  namespace common
  {
    enum FileType
    {
      NORMAL_FILE = 1,
      DIRECTORY = 2,
      PWRITE_FILE = 3
    };

    struct FragMeta
    {
      FragMeta();
      FragMeta(uint32_t block_id, uint64_t file_id, int64_t offset, int32_t size);

      static int64_t get_length();
      bool operator < (const FragMeta& right) const;

      // for store
      int serialize(char* data, const int64_t buff_len, int64_t& pos) const;
      int deserialize(char* data, const int64_t data_len, int64_t& pos);
      // for packet
      int serialize(common::Stream& output) const;
      int deserialize(common::Stream& input);

      uint32_t block_id_;
      uint64_t file_id_;
      int64_t offset_;
      int32_t size_;
    };

    class FragInfo
    {
    public:
      FragInfo();
      explicit FragInfo(const std::vector<FragMeta>& v_frag_meta);
      ~FragInfo();

      int64_t get_length() const;
      // for store
      int serialize(char* data, const int64_t buff_len, int64_t& pos) const;
      int deserialize(char* data, const int64_t data_len, int64_t& pos);
      // for packet
      int serialize(common::Stream& output) const;
      int deserialize(common::Stream& input);
      // get last offset current fraginfo hold
      int64_t get_last_offset() const;
      void dump() const;
      void push_back(FragInfo& new_frag_info);

      int32_t cluster_id_;
      bool had_been_split_;
      std::vector<FragMeta> v_frag_meta_;
    };

    class FileMetaInfo
    {
    public:
      int64_t pid_;
      int64_t id_;
      int32_t create_time_;
      int32_t modify_time_;
      int64_t size_;
      int16_t ver_no_;
      std::string name_;

      FileMetaInfo();
      FileMetaInfo(const FileMetaInfo& file_info);
      ~FileMetaInfo();

      // name format.
      // ----------------------
      // | namelen |   name   |
      // |   char  | namelen  |
      // ----------------------
      const char* get_real_name() const;
      bool is_file() const;
      // length to serialize
      int64_t length() const;
      int serialize(common::Stream& output) const;
      int deserialize(common::Stream& input);
    };

    class MetaInfo
    {
    public:
      MetaInfo();
      MetaInfo(const MetaInfo& meta_info);
      explicit MetaInfo(FragInfo& frag_info);

      void copy_no_frag(const MetaInfo& meta_info);
      int64_t length() const;
      int serialize(common::Stream& output) const;
      int deserialize(common::Stream& input);

      inline int64_t  get_size() const
      {
        return file_info_.size_;
      }
      inline bool empty() const
      {
        return file_info_.name_.empty();
      }
      inline int64_t get_last_offset() const
      {
        return frag_info_.get_last_offset();
      }
      inline int64_t get_pid() const
      {
        return file_info_.pid_;
      }
      inline int64_t get_id() const
      {
        return file_info_.id_;
      }
      inline const char* get_name() const
      {
        return file_info_.name_.c_str();
      }
      inline int32_t get_name_len() const
      {
        return file_info_.name_.length();
      }
      inline void add_frag_meta(const FragMeta& frag_meta)
      {
        frag_info_.v_frag_meta_.push_back(frag_meta);
      }

      FileMetaInfo file_info_;
      FragInfo frag_info_;
    };

    const int32_t MAX_FILE_PATH_LEN = 512;
    const int32_t MAX_META_FILE_NAME_LEN = 255;
    // threshhold count when should split
    const int32_t SOFT_MAX_FRAG_META_COUNT = 1024;
    const int32_t MAX_FRAG_INFO_SIZE = 65535;
    const int32_t MAX_FRAG_META_COUNT = MAX_FRAG_INFO_SIZE/sizeof(FragMeta) - 1;
    // MetaInfo or FragInfo return count once
    const int32_t MAX_OUT_INFO_COUNT = 500;
    const int32_t ROW_LIMIT = 5;

  }
}

#endif
