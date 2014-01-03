/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: fsname.h 5 2010-09-29 07:44:56Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_CLIENTV2_FSNAME_H_
#define TFS_CLIENTV2_FSNAME_H_

#include <string>
#include "common/internal.h"

namespace tfs
{
  namespace clientv2
  {
    struct FileBitsV1
    {
      uint32_t block_id_;
      uint32_t seq_id_;
      uint32_t suffix_;
    };

    struct FileBitsV2
    {
      uint64_t block_id_;
      uint32_t seq_id_;
      uint32_t suffix_;
      char reserve_[2];
    };

    class FSName
    {
    public:
      FSName();
      FSName(const uint64_t block_id, const uint64_t file_id, const int32_t cluster_id = 0);
      FSName(const char *file_name, const char* suffix = NULL, const int32_t cluster_id = 0);
      virtual ~FSName();

      const char* get_name(const bool large_flag = false);
      void set_name(const char* file_name, const char* suffix = NULL, const int32_t cluster_id = 0);
      void set_suffix(const char* suffix);
      std::string to_string();

      common::TfsFileType check_file_type(const char* tfs_name);

      inline bool is_valid() const
      {
        return is_valid_;
      }

      inline void set_block_id(const uint64_t id)
      {
        if (common::TFS_FILE_NAME_V1 == version_)
        {
          filev1_.block_id_ = id;
        }
        else
        {
          filev2_.block_id_ = id;
        }
      }

      inline uint64_t get_block_id() const
      {
        if (common::TFS_FILE_NAME_V1 == version_)
        {
          return filev1_.block_id_;
        }
        else
        {
          return filev2_.block_id_;
        }
      }

      inline void set_seq_id(const uint32_t id)
      {
        if (common::TFS_FILE_NAME_V1 == version_)
        {
          filev1_.seq_id_ = id;
        }
        else
        {
          filev2_.seq_id_ = id;
        }
      }

      inline uint32_t get_seq_id() const
      {
        if (common::TFS_FILE_NAME_V1 == version_)
        {
          return filev1_.seq_id_;
        }
        else
        {
          return filev2_.seq_id_;
        }
      }

      inline void set_suffix(const uint32_t id)
      {
        if (common::TFS_FILE_NAME_V1 == version_)
        {
          filev1_.suffix_ = id;
        }
        else
        {
          filev2_.suffix_ = id;
        }
      }

      inline uint32_t get_suffix() const
      {
        if (common::TFS_FILE_NAME_V1 == version_)
        {
          return filev1_.suffix_;
        }
        else
        {
          return filev2_.suffix_;
        }
      }

      inline void set_file_id(const uint64_t id)
      {
        if (common::TFS_FILE_NAME_V1 == version_)
        {
          filev1_.suffix_ = (id >> 32);
          filev1_.seq_id_ = (id & 0xFFFFFFFF);
        }
        else
        {
          filev2_.suffix_ = (id >> 32);
          filev2_.seq_id_ = (id & 0xFFFFFFFF);
        }
      }

      inline uint64_t get_file_id()
      {
        if (common::TFS_FILE_NAME_V1 == version_)
        {
          uint64_t id = filev1_.suffix_;
          return ((id << 32) | filev1_.seq_id_);
        }
        else
        {
          uint64_t id = filev2_.suffix_;
          return ((id << 32) | filev2_.seq_id_);
        }
      }

      inline void set_cluster_id(const int32_t cluster_id)
      {
        cluster_id_ = cluster_id;
      }

      inline int32_t get_cluster_id() const
      {
        return cluster_id_;
      }

    private:
      common::TfsFileNameVersion version_;
      bool is_valid_;
      FileBitsV1 filev1_;
      FileBitsV2 filev2_;
      int32_t cluster_id_;
      char file_name_[common::TFS_FILE_LEN_V2];
    };
  }
}
#endif
