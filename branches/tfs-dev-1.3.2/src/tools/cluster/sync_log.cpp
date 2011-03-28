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
#include <stdio.h>
#include <stdlib.h>
#include <tbsys.h>
#include <vector>
#include <string>
#include <sys/types.h>
#include <dirent.h>
#include <libgen.h>

#include "common/func.h"
#include "common/directory_op.h"
#include "client/fsname.h"
#include "client/tfs_client_api.h"

using namespace std;
using namespace tfs::client;
using namespace tfs::common;

struct SyncFileInfo
{
  string file_name_;
  int32_t action_;
};
struct SyncStat
{
  int64_t total_count_;
  int64_t success_count_;
  int64_t fail_count_;
};
struct LogFile
{
  FILE** fp_;
  const char* file_;
};
enum CmpStat
{
  SAME_FILE = 1,
  DIFF_FILE = 2,
  DIFF_FLAG = 4
};
typedef vector<SyncFileInfo> FILE_VEC;
struct SyncStat sync_stat;
int get_file_list(const string& log_file, FILE_VEC& name_vec);
int sync_file(FILE_VEC& name_vec, const string& modify_time);
int cmp_file_info(const string& file_name, const string& modify_time);
char* str_match(char* data, const char* prefix);

static const int32_t MAX_READ_SIZE = 81920;
static const int32_t MAX_READ_LEN = 256;
TfsClient source_tfs_client_;
TfsClient dest_tfs_client_;
string source_ns_ip_ = "", dest_ns_ip_ = "";
FILE *g_write_succ = NULL, *g_write_fail = NULL, *g_unlink_succ = NULL, *g_unlink_fail = NULL;

struct LogFile g_log_fp[] =
{
  {&g_write_succ, "write_succ_file"},
  {&g_write_fail, "write_fail_file"},
  {&g_unlink_succ, "unlink_succ_file"},
  {&g_unlink_fail, "unlink_fail_file"},
  {NULL, NULL}
};

int init_log_file(char* dir_path)
{
  for (int i = 0; g_log_fp[i].file_; i++)
  {
    char file_path[256];
    snprintf(file_path, 256, "%s%s", dir_path, g_log_fp[i].file_);
    *g_log_fp[i].fp_ = fopen(file_path, "w");
    if (!*g_log_fp[i].fp_)
    {
      printf("open file fail %s : %s\n:", g_log_fp[i].file_, strerror(errno));
      return TFS_ERROR;
    }
  }
  return TFS_SUCCESS;
}

static void usage(const char* name)
{
  fprintf(stderr, "Usage: %s -s -d -f [-g] [-l] [-h]\n", name);
  fprintf(stderr, "       -s source ns ip port\n");
  fprintf(stderr, "       -d dest ns ip port\n");
  fprintf(stderr, "       -f file name, assign a file to sync\n");
  fprintf(stderr, "       -g log file name, redirect log info to log file, optional\n");
  fprintf(stderr, "       -l log file level, set log file level, optional\n");
  fprintf(stderr, "       -h help\n");
  exit(TFS_ERROR);
}

int main(int argc, char* argv[])
{
  int32_t i;
  string file_name = "";
  string modify_time = "";
  string log_file = "sync_report.log";
  string level = "info";

  // analyze arguments
  while ((i = getopt(argc, argv, "s:d:f:m:g:l:h")) != EOF)
  {
    switch (i)
    {
    case 's':
      source_ns_ip_ = optarg;
      break;
    case 'd':
      dest_ns_ip_ = optarg;
      break;
    case 'f':
      file_name = optarg;
      break;
    case 'm':
      modify_time = optarg;
      break;
    case 'g':
      log_file = optarg;
      break;
    case 'l':
      level = optarg;
      break;
    case 'h':
    default:
      usage(argv[0]);
    }
  }

  if ((source_ns_ip_.empty())
      || (source_ns_ip_.compare(" ") == 0)
      || dest_ns_ip_.empty()
      || (dest_ns_ip_.compare(" ") == 0)
      || file_name.empty()
      || (file_name.compare(" ") == 0)
      || modify_time.length() < 8)
  {
    usage(argv[0]);
  }

  modify_time += "000000";

  if ((level != "info") && (level != "debug") && (level != "error") && (level != "warn"))
  {
    fprintf(stderr, "level(info | debug | error | warn) set error\n");
    return TFS_ERROR;
  }

  char base_path[256];
  char log_path[256];
  snprintf(base_path, 256, "%s%s", dirname(argv[0]), "/log/");
  DirectoryOp::create_directory(base_path);
  init_log_file(base_path);

  snprintf(log_path, 256, "%s%s", base_path, log_file.c_str());
  if (strlen(log_path) != 0 && access(log_path, R_OK) == 0)
  {
    char old_log_file[256];
    sprintf(old_log_file, "%s.%s", log_path, Func::time_to_str(time(NULL), 1).c_str());
    rename(log_path, old_log_file);
  }
  else if (!DirectoryOp::create_full_path(log_path, true))
  {
    TBSYS_LOG(ERROR, "create file(%s)'s directory failed", log_path);
    return TFS_ERROR;
  }

  TBSYS_LOGGER.setFileName(log_path, true);
  TBSYS_LOGGER.setMaxFileSize(1024 * 1024 * 1024);
  TBSYS_LOGGER.setLogLevel(level.c_str());

  source_tfs_client_.initialize(source_ns_ip_.c_str());
  dest_tfs_client_.initialize(dest_ns_ip_.c_str());

  memset(&sync_stat, 0, sizeof(sync_stat));
  FILE_VEC name_vec;
  name_vec.clear();

  get_file_list(file_name, name_vec);
  if (name_vec.size() > 0)
  {
    sync_file(name_vec, modify_time);
    sync_stat.total_count_ = name_vec.size();
  }

  fprintf(stdout, "TOTAL COUNT: %"PRI64_PREFIX"d, SUCCESS COUNT: %"PRI64_PREFIX"d, FAIL COUNT: %"PRI64_PREFIX"d\n",
      sync_stat.total_count_, sync_stat.success_count_, sync_stat.fail_count_);
  fprintf(stdout, "LOG FILE: %s\n", log_path);

  return TFS_SUCCESS;
}
int get_file_list(const string& log_file, FILE_VEC& name_vec)
{
  int ret = TFS_ERROR;
  FILE* fp = fopen (log_file.c_str(), "r");
  if (fp == NULL)
  {
    TBSYS_LOG(ERROR, "open file(%s) fail,errors(%s)", log_file.c_str(), strerror(errno));
  }
  else
  {
    char info[MAX_READ_LEN];
    while(!feof(fp))
    {
      memset(info, 0, MAX_READ_LEN);
      fgets (info, MAX_READ_LEN, fp);
      if (strlen(info) > 0)
      {
        char* block = str_match(info, "blockid: ");
        uint32_t block_id = static_cast<uint32_t>(atoi(block));
        char* file = str_match(block + strlen(block) + 1, "fileid: ");
        uint64_t file_id = strtoull(file, NULL, 10);
        char* a = str_match(file + strlen(file) + 1, "action: ");
        int32_t action = -1;
        if (a != NULL)
        {
          action = atoi(a);
        }
        FSName fs;
        fs.set_block_id(block_id);
        fs.set_file_id(file_id);
        SyncFileInfo sync_finfo;
        sync_finfo.file_name_ = string(fs.get_name());
        sync_finfo.action_ = action;
        name_vec.push_back(sync_finfo);
        TBSYS_LOG(INFO, "block_id: %u, file_id: %"PRI64_PREFIX"u, action:%d, name: %s\n", block_id, file_id, action, fs.get_name());
      }
    }
    fclose (fp);
    ret = TFS_SUCCESS;
  }
  return ret;
}

int sync_file(FILE_VEC& name_vec, const string& modify_time)
{
  int ret = TFS_SUCCESS;
  FILE_VEC::iterator iter = name_vec.begin();
  for (; iter != name_vec.end(); iter++)
  {
    string file_name = (*iter).file_name_;
    int32_t action = (*iter).action_;
    int32_t re_action = -1;
    switch (action)
    {
      case DELETE:
        re_action = UNDELETE;
        break;
      case CONCEAL:
        re_action = REVEAL;
        break;
      default:
        break;
    }
    int ret = source_tfs_client_.tfs_open(file_name.c_str(), NULL, READ_MODE);
    int cmp_stat = DIFF_FILE;
    if (ret == TFS_SUCCESS)
    {
      int32_t total_len = 0;
      // when dest file is different, write new file to dest.
      cmp_stat = cmp_file_info(file_name, modify_time);
      if (cmp_stat & DIFF_FILE)
      {
        if (re_action != -1)
        {
          source_tfs_client_.unlink(file_name.c_str(), NULL, re_action);
        }
        ret = dest_tfs_client_.tfs_open(file_name.c_str(), NULL, WRITE_MODE | NEWBLK_MODE);
        char data[MAX_READ_SIZE];
        int32_t rlen = 0;

        for (;;)
        {
          FileInfo file_info;
          rlen = source_tfs_client_.tfs_read_v2(data, MAX_READ_SIZE, &file_info);
          if (rlen < 0)
          {
            TBSYS_LOG(ERROR, "read tfsfile(%s) fail.", file_name.c_str());
            ret = TFS_ERROR;
            break;
          }
          if (rlen == 0)
          {
            break;
          }
          total_len += rlen;
          if (dest_tfs_client_.tfs_write(data, rlen) != rlen)
          {
            TBSYS_LOG(ERROR, "write tfsfile(%s) fail.", file_name.c_str());
            ret = TFS_ERROR;
            break;
          }
        }
      }
      if (ret == TFS_SUCCESS && (dest_tfs_client_.tfs_close() == TFS_SUCCESS))
      {
        if ((action == DELETE) || (action == CONCEAL))
        {
          if(cmp_stat & DIFF_FILE)
          {
            ret = source_tfs_client_.unlink(file_name.c_str(), NULL, action);
          }
          if ((dest_tfs_client_.unlink(file_name.c_str(), NULL, action) != TFS_SUCCESS) || (ret != TFS_SUCCESS))
          {
            TBSYS_LOG(ERROR, "dest file(%s) del(%d) failed", file_name.c_str(), action);
            TBSYS_LOG(ERROR, "sync file(%s) from(%s) to dest(%s) failed.", file_name.c_str(),
               source_ns_ip_.c_str(), dest_ns_ip_.c_str());
            sync_stat.fail_count_++;
            fprintf(g_unlink_fail, "%s\n", file_name.c_str());
          }
          else
          {
             sync_stat.success_count_++;
             fprintf(g_unlink_succ, "%s\n", file_name.c_str());
          }
        }
        else
        {
          TBSYS_LOG(INFO, "sync file(%s) from(%s) to dest(%s) success.", file_name.c_str(),
              source_ns_ip_.c_str(), dest_ns_ip_.c_str());
          sync_stat.success_count_++;
          fprintf(g_write_succ, "%s\n", file_name.c_str());
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "sync file(%s) from(%s) to dest(%s) failed.", file_name.c_str(),
            source_ns_ip_.c_str(), dest_ns_ip_.c_str());
        sync_stat.fail_count_++;
        if (action != -1)
        {
          fprintf(g_unlink_fail, "%s\n", file_name.c_str());
        }
        else
        {
          fprintf(g_write_fail, "%s\n", file_name.c_str());
        }
      }
      source_tfs_client_.tfs_close();
    }
    else
    {
      TBSYS_LOG(ERROR, "get file(%s) from source ns error", file_name.c_str());
       sync_stat.fail_count_++;
      fprintf(g_write_fail, "%s\n", file_name.c_str());
    }
  }
  return ret;
}

int cmp_file_info(const string& file_name, const string& modify_time)
{
  int ret = DIFF_FILE;
  FileInfo source_buf;
  FileInfo dest_buf;
  int iret = dest_tfs_client_.tfs_open(file_name.c_str(), NULL, READ_MODE);
  if (iret == TFS_SUCCESS)
  {
    iret = dest_tfs_client_.tfs_stat(&dest_buf, READ_MODE);
    if (iret == TFS_SUCCESS)
    {
      iret = source_tfs_client_.tfs_stat(&source_buf, READ_MODE);
      if (iret == TFS_SUCCESS)
      {
        if (((dest_buf.id_ == source_buf.id_)
            && (dest_buf.size_ == source_buf.size_)
            && (dest_buf.crc_ == source_buf.crc_))
           || dest_buf.modify_time_ >= tbsys::CTimeUtil::strToTime(const_cast<char*>(modify_time.c_str())))
        {
          if (dest_buf.flag_ == source_buf.flag_)
          {
            TBSYS_LOG(INFO, "tfs file(%s) exists or modify time(%s) more than(%s)",
                file_name.c_str(), Func::time_to_str(dest_buf.modify_time_).c_str(), modify_time.c_str());
            ret = SAME_FILE;
          }
          else
          {
            TBSYS_LOG(INFO, "tfs file(%s) exists, but wrong status(source:%d -> dest:%d)",
                file_name.c_str(), source_buf.flag_, dest_buf.flag_);
            ret = DIFF_FLAG;
          }
        }
      }
    }
    dest_tfs_client_.tfs_close();
  }
  return ret;
}

char* str_match(char* data, const char* prefix)
{
  if (data == NULL || prefix == NULL)
  {
    return NULL;
  }
  char* pch = strstr(data, prefix);
  if (pch != NULL)
  {
    pch += strlen(prefix);
    char* tmp = strchr(pch , ',');
    if (tmp != NULL)
    {
      *tmp = '\0';
    }
  }

  return pch;
}
