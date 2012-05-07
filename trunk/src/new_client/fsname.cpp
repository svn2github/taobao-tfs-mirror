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
using namespace tfs::client::coding;

namespace tfs
{
  namespace client
  {
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

    FSName::FSName() : is_valid_(true), cluster_id_(0)
    {
      file_name_[0] = '\0';
      memset(&file_, 0, sizeof(FileBits));
    }

    FSName::FSName(const uint32_t block_id, const uint64_t file_id, const int32_t cluster_id) :
      is_valid_(true), cluster_id_(cluster_id)
    {
      file_.block_id_ = block_id;
      set_file_id(file_id);
      file_name_[0] = '\0';
    }

    FSName::FSName(const char* file_name, const char* suffix, const int32_t cluster_id) : is_valid_(true)
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
      memset(&file_, 0, sizeof(FileBits));

      if (NULL != file_name && file_name[0] != '\0')
      {
        if (check_file_type(file_name) == INVALID_TFS_FILE_TYPE)
        {
          is_valid_ = false;
        }
        else
        {
          decode(file_name + 2, (char*) &file_);
          if (NULL == suffix)
          {
            suffix = file_name + FILE_NAME_LEN;
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
        encode((char*) &file_, file_name_ + 2);
        if (large_flag)
        {
          file_name_[0] = LARGE_TFS_FILE_KEY_CHAR;
        }
        else
        {
          file_name_[0] = SMALL_TFS_FILE_KEY_CHAR;
        }
        file_name_[1] = static_cast<char> ('0' + cluster_id_);
        file_name_[FILE_NAME_LEN] = '\0';
      }

      return file_name_;
    }

    void FSName::set_suffix(const char *suffix)
    {
      if ((suffix != NULL) && (suffix[0] != '\0'))
      {
        file_.suffix_ = hash(suffix);
      }
    }

    string FSName::to_string()
    {
      char buffer[256];
      snprintf(buffer, 256, "block_id: %u, file_id: %"PRI64_PREFIX"u, seq_id: %u, suffix: %u, name: %s",
               file_.block_id_, get_file_id(), file_.seq_id_, file_.suffix_, get_name());
      return string(buffer);
    }

    TfsFileType FSName::check_file_type(const char* tfs_name)
    {
      TfsFileType file_type = INVALID_TFS_FILE_TYPE;
      if (NULL != tfs_name &&
          static_cast<int32_t>(strlen(tfs_name)) >= FILE_NAME_LEN)
      {
        if (LARGE_TFS_FILE_KEY_CHAR == tfs_name[0])
        {
          file_type = LARGE_TFS_FILE_TYPE;
        }
        else if (SMALL_TFS_FILE_KEY_CHAR == tfs_name[0])
        {
          file_type = SMALL_TFS_FILE_TYPE;
        }
      }
      return file_type;
    }
  }
}
