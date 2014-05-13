/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: sync_file_base.h 1511 2012-06-06 02:22:22Z chuyu@taobao.com $
 *
 * Authors:
 *   chuyu <chuyu@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_TOOLS_SYNCFILEBASE_H_
#define TFS_TOOLS_SYNCFILEBASE_H_

#include <stdio.h>
#include <stdlib.h>
#include <tbsys.h>
#include <TbThread.h>
#include <vector>
#include <string>
#include <sys/types.h>
#include <dirent.h>
#include <libgen.h>
#include <Mutex.h>
#include <Memory.hpp>

#include "common/directory_op.h"
#include "common/internal.h"
#include "common/func.h"

static const int32_t MAX_READ_DATA_SIZE = 81920;
static const int32_t MAX_LINE_SIZE = 256;

int rename_file(const char* file_path);

struct LogFile
{
  LogFile(char* file) : fp_(NULL), file_(file){}
  FILE* fp_;
  const char* file_;

  bool init_log_file(const char* dir)
  {
    bool ret = true;
    if(NULL == dir)
    {
      fprintf(stderr, "log dir is null\n:");
      ret = false;
    }
    else
    {
      char file_path[256];
      snprintf(file_path, 256, "%s/%s", dir, file_);
      rename_file(file_path);
      fp_ = fopen(file_path, "wb");
      if (NULL == fp_)
      {
        fprintf(stderr, "open file fail %s : %s\n:",  file_path, strerror(errno));
        ret = false;
      }
    }
    return ret;
  }
};

enum SyncResult
{
  SYNC_SUCCESS = 1,
  SYNC_FAILED = 2,
  SYNC_NOTHING = 3
};

struct SyncStat
{
  SyncStat() : total_count_(0), success_count_(0), fail_count_(0), unsync_count_(0)
  {
  }
  void dump(FILE* fp)
  {
    fprintf(fp, "total file count: %"PRI64_PREFIX"d, "
        "succ_count: %"PRI64_PREFIX"d, fail_count: %"PRI64_PREFIX"d, unsync_count: %"PRI64_PREFIX"d\n",
        total_count_, success_count_, fail_count_, unsync_count_);
  }
  int64_t total_count_;
  int64_t success_count_;
  int64_t fail_count_;
  int64_t unsync_count_;
};

class SyncFileBase
{
  public:
    SyncFileBase(std::string src_ns_addr, std::string dest_ns_addr);
    ~SyncFileBase();
    int cmp_and_sync_file(const std::string& file_name, const int32_t timestamp, SyncResult& result, const bool force, const bool unlink);
    int cmp_and_sync_file(const uint64_t block_id, const tfs::common::FileInfoV2& source_file_info, const tfs::common::FileInfoV2& dest_file_info,
      const int32_t timestamp, SyncResult& result, std::string& file_name, const bool force, const bool unlink);
    int unlink_file(const std::string& file_name, const int32_t flag);
    int copy_file(const std::string& file_name, const int32_t status);

  private:
    int sync_file(const std::string& file_name, const tfs::common::TfsFileStat& source_stat, const tfs::common::TfsFileStat& dest_stat, const int32_t timestamp, const bool force, const bool unlink, SyncResult& result);
    inline void get_filestat_from_file_info(const tfs::common::FileInfoV2& file_info, tfs::common::TfsFileStat& buf);
    int get_filestat_from_file_name(const std::string& file_name, const std::string& ns_addr, tfs::common::TfsFileStat& buf);

  private:
    std::string src_ns_addr_;
    std::string dest_ns_addr_;
};
#endif
