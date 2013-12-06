/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: fsname.cpp 19 2010-10-18 05:48:09Z nayan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include <tbsys.h>
#include <string.h>

#include "encode.h"
#include "fsname.h"

using namespace tfs::common;
using namespace tbsys;
using namespace tfs::clientv2::coding;

namespace tfs
{
  namespace clientv2
  {
    static const char TFS_FILE_VERSION_CHAR_V2 = 'B';
    static const TfsFileNameVersion CURRENT_TFS_FILE_NAME_VERSION = TFS_FILE_NAME_V1;

    static int32_t hash(const char *str)
    {
      if (str == NULL)
      {
        return 0;
      }
      int32_t len = strlen(str);
      int32_t h = 0;
      int32_t i = 0;
      for (i = 0; i < len; ++i)
      {
        h += str[i];
        h *= 7;
      }
      return (h | 0x80000000);
    }

    FSName::FSName() : version_(CURRENT_TFS_FILE_NAME_VERSION), is_valid_(true), cluster_id_(0)
    {
      file_name_[0] = '\0';
      memset(&filev1_, 0, sizeof(FileBitsV1));
      memset(&filev2_, 0, sizeof(FileBitsV2));
    }

    FSName::FSName(const uint64_t block_id, const uint64_t file_id, const int32_t cluster_id) :
      version_(CURRENT_TFS_FILE_NAME_VERSION), is_valid_(true), cluster_id_(cluster_id)
    {
      set_block_id(block_id);
      set_file_id(file_id);
      file_name_[0] = '\0';
    }

    FSName::FSName(const char* file_name, const char* suffix, const int32_t cluster_id) :
      version_(CURRENT_TFS_FILE_NAME_VERSION), is_valid_(true)
    {
      set_name(file_name, suffix, cluster_id);
    }

    FSName::~FSName()
    {

    }

    void FSName::set_name(const char* file_name, const char* suffix, const int32_t cluster_id)
    {
      file_name_[0] = '\0';
      cluster_id_ = cluster_id;
      memset(&filev1_, 0, sizeof(FileBitsV1));
      memset(&filev2_, 0, sizeof(FileBitsV2));

      if (NULL != file_name && file_name[0] != '\0')
      {
        TfsFileType file_type = check_file_type(file_name);
        if (INVALID_TFS_FILE_TYPE == file_type)
        {
          is_valid_ = false;
        }
        else
        {
          int32_t file_name_len = 0;
          if (TFS_FILE_NAME_V1 == version_)
          {
            file_name_len = FILE_NAME_LEN;
            decode(file_name + 2, (char*) &filev1_, file_name_len - 2);
          }
          else
          {
            file_name_len = FILE_NAME_LEN_V2;
            decode(file_name + 3, (char*) &filev2_, file_name_len - 3);
          }

          if (NULL == suffix)
          {
            suffix = file_name + file_name_len;
          }
          set_suffix(suffix);
          if (0 == cluster_id_)
          {
            cluster_id_ = file_name[1] - '0';
          }
        }
      }
    }

    const char* FSName::get_name(const bool large_flag)
    {
      if (file_name_[0] == '\0')
      {
        int32_t file_name_len = 0;
        if (TFS_FILE_NAME_V1 == version_)
        {
          file_name_len = FILE_NAME_LEN;
          encode((char*) &filev1_, file_name_ + 2, sizeof(FileBitsV1));
        }
        else
        {
          file_name_len = FILE_NAME_LEN_V2;
          encode((char*) &filev2_, file_name_ + 3, sizeof(FileBitsV2));
        }

        if (large_flag)
        {
          file_name_[0] = LARGE_TFS_FILE_KEY_CHAR;
        }
        else
        {
          file_name_[0] = SMALL_TFS_FILE_KEY_CHAR;
        }

        if (TFS_FILE_NAME_V2 == version_)
        {
          file_name_[1] = TFS_FILE_VERSION_CHAR_V2;
          file_name_[2] = static_cast<char> ('0' + cluster_id_);
        }
        else
        {
          file_name_[1] = static_cast<char> ('0' + cluster_id_);
        }
        file_name_[file_name_len] = '\0';
      }

      return file_name_;
    }

    void FSName::set_suffix(const char *suffix)
    {
      if (TFS_FILE_NAME_V1 == version_)
      {
        if ((suffix != NULL) && (suffix[0] != '\0'))
        {
          filev1_.suffix_ = hash(suffix);
        }
      }
      else
      {
        if ((suffix != NULL) && (suffix[0] != '\0'))
        {
          filev2_.suffix_ = hash(suffix);
        }
      }
    }

    string FSName::to_string()
    {
      char buffer[256];
      snprintf(buffer, 256, "block_id: %"PRI64_PREFIX"u, file_id: %"PRI64_PREFIX"u, seq_id: %u, suffix: %u, name: %s",
               get_block_id(), get_file_id(), get_seq_id(), get_suffix(), get_name());
      return string(buffer);
    }

    TfsFileType FSName::check_file_type(const char* tfs_name)
    {
      TfsFileType file_type = INVALID_TFS_FILE_TYPE;
      if (NULL != tfs_name &&
          static_cast<int32_t>(strlen(tfs_name)) >= FILE_NAME_LEN)
      {
        if (TFS_FILE_VERSION_CHAR_V2 == tfs_name[1])
        {
          version_ = TFS_FILE_NAME_V2;
        }
        else
        {
          version_ = TFS_FILE_NAME_V1;
        }

        if (SMALL_TFS_FILE_KEY_CHAR == tfs_name[0])
        {
          file_type = SMALL_TFS_FILE_TYPE;
        }
        else
        {
          TBSYS_LOG(WARN, "big file currently not supported in clientv2\n");
        }
      }
      return file_type;
    }
  }
}
