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
#include <TbThread.h>
#include <vector>
#include <string>
#include <sys/types.h>
#include <dirent.h>
#include <libgen.h>
#include <Mutex.h>
#include <Memory.hpp>

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
enum CmpFlag
{
  SKIP_FLAG = 1,
  SYNC_FLAG = 2,
  ERROR_FLAG = 4
};
enum OpAction
{
  WRITE_ACTION = 1,
  CONCEAL_ACTION = 2,
  REVERT_ACTION = 4
};
typedef vector<SyncFileInfo> FILE_VEC;
typedef vector<SyncFileInfo>::iterator FILE_VEC_ITER;
typedef map<uint32_t, FILE_VEC> SYNC_FILE_MAP;
typedef map<uint32_t, FILE_VEC>::iterator SYNC_FILE_MAP_ITER;
tbutil::Mutex g_mutex_;
SyncStat g_sync_stat_;
int get_file_list(const string& log_file, SYNC_FILE_MAP& sync_file_map);
void get_revert_action(int32_t source_flag, int32_t dest_flag, VINT& action_set);
int sync_file(TfsClient& source_tfs_client, TfsClient& dest_tfs_client, FILE_VEC& name_vec, const string& modify_time);
int cmp_file_info(TfsClient& source_tfs_client, TfsClient& dest_tfs_client, SyncFileInfo& sync_finfo, const string& modify_time, VINT& action_set);
char* str_match(char* data, const char* prefix);

class WorkThread : public tbutil::Thread
{
  public:
    WorkThread(string& source_ns_ip, string& dest_ns_ip, string& modify_time):
      modify_time_(modify_time)
  {
    source_tfs_client_.initialize(source_ns_ip.c_str());
    dest_tfs_client_.initialize(dest_ns_ip.c_str());
  }
    virtual ~WorkThread()
    {

    }

    void wait_for_shut_down()
    {
      join();
    }

    void destroy()
    {
      destroy_ = true;
    }

    virtual void run()
    {
      sync_file(source_tfs_client_, dest_tfs_client_, name_vec_, modify_time_);
    }

    void push_back(FILE_VEC name_vec)
    {
      name_vec_.insert(name_vec_.end(), name_vec.begin(), name_vec.end());
    }

  private:
    WorkThread(const WorkThread&);
    WorkThread& operator=(const WorkThread&);
    TfsClient source_tfs_client_;
    TfsClient dest_tfs_client_;
    FILE_VEC name_vec_;
    string modify_time_;
    bool destroy_;
};
typedef tbutil::Handle<WorkThread> WorkThreadPtr;
static WorkThreadPtr* gworks = NULL;
static int32_t thread_count = 1;
static const int32_t MAX_READ_SIZE = 81920;
static const int32_t MAX_READ_LEN = 256;
string source_ns_ip_ = "", dest_ns_ip_ = "";
FILE *g_sync_succ = NULL, *g_sync_fail = NULL;

struct LogFile g_log_fp[] =
{
  {&g_sync_succ, "sync_succ_file"},
  {&g_sync_fail, "sync_fail_file"},
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

static void interrupt_callback(int signal)
{
  TBSYS_LOG(INFO, "application signal[%d]", signal);
  switch( signal )
  {
    case SIGTERM:
    case SIGINT:
    default:
      if (gworks != NULL)
      {
        for (int32_t i = 0; i < thread_count; ++i)
        {
          if (gworks != 0)
          {
            gworks[i]->destroy();
          }
        }
      }
      break;
  }
}
int main(int argc, char* argv[])
{
  int32_t i;
  string source_ns_ip = "", dest_ns_ip = "";
  string file_name = "";
  string modify_time = "";
  string log_file = "sync_report.log";
  string level = "info";

  // analyze arguments
  while ((i = getopt(argc, argv, "s:d:f:m:t:g:l:h")) != EOF)
  {
    switch (i)
    {
      case 's':
        source_ns_ip = optarg;
        break;
      case 'd':
        dest_ns_ip = optarg;
        break;
      case 'f':
        file_name = optarg;
        break;
      case 'm':
        modify_time = optarg;
        break;
      case 't':
        thread_count = atoi(optarg);
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

  if ((source_ns_ip.empty())
      || (source_ns_ip.compare(" ") == 0)
      || dest_ns_ip.empty()
      || (dest_ns_ip.compare(" ") == 0)
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


  memset(&g_sync_stat_, 0, sizeof(g_sync_stat_));
  SYNC_FILE_MAP sync_file_map;

  get_file_list(file_name, sync_file_map);
  if (sync_file_map.size() > 0)
  {
    gworks = new WorkThreadPtr[thread_count];
    int32_t i = 0;
    for (; i < thread_count; ++i)
    {
      gworks[i] = new WorkThread(source_ns_ip, dest_ns_ip, modify_time);
    }
    int32_t index = 0;
    int64_t count = 0;
    SYNC_FILE_MAP_ITER iter = sync_file_map.begin();
    for (; iter != sync_file_map.end(); iter++)
    {
      index = count % thread_count;
      gworks[index]->push_back(iter->second);
      ++count;
    }
    for (i = 0; i < thread_count; ++i)
    {
      gworks[i]->start();
    }

    signal(SIGHUP, interrupt_callback);
    signal(SIGINT, interrupt_callback);
    signal(SIGTERM, interrupt_callback);
    signal(SIGUSR1, interrupt_callback);

    for (i = 0; i < thread_count; ++i)
    {
      gworks[i]->wait_for_shut_down();
    }

    tbsys::gDeleteA(gworks);
  }

  for (i = 0; g_log_fp[i].fp_; i++)
  {
    fclose(*g_log_fp[i].fp_);
  }


  fprintf(stdout, "TOTAL COUNT: %"PRI64_PREFIX"d, SUCCESS COUNT: %"PRI64_PREFIX"d, FAIL COUNT: %"PRI64_PREFIX"d\n",
      g_sync_stat_.total_count_, g_sync_stat_.success_count_, g_sync_stat_.fail_count_);
  fprintf(stdout, "LOG FILE: %s\n", log_path);

  return TFS_SUCCESS;
}
int get_file_list(const string& log_file, SYNC_FILE_MAP& sync_file_map)
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
        SYNC_FILE_MAP_ITER iter = sync_file_map.find(block_id);
        if (iter != sync_file_map.end())
        {
          iter->second.push_back(sync_finfo);
        }
        else
        {
          FILE_VEC name_vec;
          name_vec.push_back(sync_finfo);
          sync_file_map.insert(make_pair(block_id, name_vec));
        }

        g_sync_stat_.total_count_++;
        TBSYS_LOG(INFO, "block_id: %u, file_id: %"PRI64_PREFIX"u, action:%d, name: %s\n", block_id, file_id, action, fs.get_name());
      }
    }
    fclose (fp);
    ret = TFS_SUCCESS;
  }
  return ret;
}

int sync_file(TfsClient& source_tfs_client, TfsClient& dest_tfs_client, FILE_VEC& name_vec, const string& modify_time)
{
  int iret = TFS_SUCCESS;
  FILE_VEC::iterator iter = name_vec.begin();
  for (; iter != name_vec.end(); iter++)
  {
    string file_name = (*iter).file_name_;
    VINT action_set;
    // compare file info
    int cmp_stat = SYNC_FLAG;
    cmp_stat = cmp_file_info(source_tfs_client, dest_tfs_client, (*iter), modify_time, action_set);
    // need do sync file
    if (cmp_stat & SYNC_FLAG)
    {
      int action = iter->action_;
      if (action & WRITE_ACTION)
      {
        if (action & CONCEAL_ACTION)
        {
          source_tfs_client.unlink(file_name.c_str(), NULL, REVEAL);
          usleep(20000);
        }

        bool first_flag = true;
        char data[MAX_READ_SIZE];
        int32_t rlen = 0;

        FileInfo file_info;
        int ret = source_tfs_client.tfs_open(file_name.c_str(), NULL, READ_MODE);
        if (ret != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "open source tfsfile fail, filename: %s, ret: %d", file_name.c_str(), ret);
        }
        else
        {
          for (;;)
          {
            rlen = source_tfs_client.tfs_read_v2(data, MAX_READ_SIZE, &file_info);
            if (rlen < 0)
            {
              TBSYS_LOG(ERROR, "read tfsfile fail, filename: %s, datalen: %d", file_name.c_str(), rlen);
              ret = TFS_ERROR;
              break;
            }
            if (rlen == 0)
            {
              break;
            }

            if (first_flag)
            {
              ret = dest_tfs_client.tfs_open(file_name.c_str(), NULL, WRITE_MODE | NEWBLK_MODE);
              if (ret != TFS_SUCCESS)
              {
                TBSYS_LOG(ERROR, "open dest tfsfile fail, filename: %s, ret: %d", file_name.c_str(), ret);
                break;
              }
              first_flag = false;
            }

            if (dest_tfs_client.tfs_write(data, rlen) != rlen)
            {
              TBSYS_LOG(ERROR, "write tfsfile fail, filename: %s, datalen: %d", file_name.c_str(), rlen);
              ret = TFS_ERROR;
              break;
            }

            if (rlen < MAX_READ_SIZE)
            {
              break;
            }
          }
        }

        source_tfs_client.tfs_close();

        if (ret == TFS_SUCCESS)
        {
          if ((ret = dest_tfs_client.tfs_close()) != TFS_SUCCESS)
          {
            TBSYS_LOG(ERROR, "close dest tfsfile fail, filename: %s, ret: %d", file_name.c_str(), ret);
            iret = TFS_ERROR;
          }
        }
        else
        {
          iret = TFS_ERROR;
        }

        if (action & CONCEAL_ACTION)
        {
          if ((source_tfs_client.unlink(file_name.c_str(), NULL, CONCEAL) != TFS_SUCCESS))
          {
            TBSYS_LOG(ERROR, "source file(%s) hide failed", file_name.c_str());
            iret = TFS_ERROR;
          }

          if (ret == TFS_SUCCESS)
          {
            if ((dest_tfs_client.unlink(file_name.c_str(), NULL, CONCEAL) != TFS_SUCCESS))
            {
              TBSYS_LOG(ERROR, "dest file(%s) hide failed", file_name.c_str());
              iret = TFS_ERROR;
            }
          }
        }
      }
      if (action & REVERT_ACTION)
      {
        iret = TFS_SUCCESS;
        int32_t size = static_cast<int32_t>(action_set.size());
        for (int32_t i = 0; i < size; i++)
        {
          if ((dest_tfs_client.unlink(file_name.c_str(), NULL, action_set[i]) != TFS_SUCCESS))
          {
            TBSYS_LOG(ERROR, "dest file(%s) unlink(%d) failed", file_name.c_str(), action_set[i]);
            iret = TFS_ERROR;
          }
          if (i < (size - 1))
          {
            usleep(20000);
          }
        }
      }
    }
    else if (cmp_stat & SKIP_FLAG)
    {
      iret = TFS_SUCCESS;
    }
    else if (cmp_stat & ERROR_FLAG)
    {
      iret = TFS_ERROR;
    }

    if (iret == TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "sync file(%s) from(%s) to dest(%s) succeed.", file_name.c_str(),
          source_ns_ip_.c_str(), dest_ns_ip_.c_str());
      {
        tbutil::Mutex::Lock lock(g_mutex_);
        g_sync_stat_.success_count_++;
      }
      fprintf(g_sync_succ, "%s\n", file_name.c_str());
    }
    else
    {
      TBSYS_LOG(ERROR, "sync file(%s) from(%s) to dest(%s) failed.", file_name.c_str(),
          source_ns_ip_.c_str(), dest_ns_ip_.c_str());
      {
        tbutil::Mutex::Lock lock(g_mutex_);
        g_sync_stat_.fail_count_++;
      }
      fprintf(g_sync_fail, "%s\n", file_name.c_str());
    }

  }
  return iret;
}

int cmp_file_info(TfsClient& source_tfs_client, TfsClient& dest_tfs_client, SyncFileInfo& sync_finfo, const string& modify_time, VINT& action_set)
{
  int ret = SYNC_FLAG;
  FileInfo source_buf;
  FileInfo dest_buf;
  string file_name = sync_finfo.file_name_;
  int iret = source_tfs_client.tfs_open(file_name.c_str(), NULL, READ_MODE);
  if (iret == TFS_SUCCESS)
  {
    iret = source_tfs_client.tfs_stat(&source_buf, READ_MODE);
    if (iret == TFS_SUCCESS)
    {
      TBSYS_LOG(DEBUG, "source stat(%s) success: %d", file_name.c_str(), source_buf.flag_);
      // source file not been delete
      if ((source_buf.flag_ != 1) && (source_buf.flag_ != 5))
      {
        iret = dest_tfs_client.tfs_open(file_name.c_str(), NULL, READ_MODE);
        if (iret == TFS_SUCCESS)
        {
          TBSYS_LOG(DEBUG, "dest open(%s) success", file_name.c_str());
          iret = dest_tfs_client.tfs_stat(&dest_buf, READ_MODE);
          if (iret == TFS_SUCCESS) // dest file exist
          {
            TBSYS_LOG(DEBUG, "dest stat(%s) success: %d", file_name.c_str(), dest_buf.flag_);
            if (((dest_buf.id_ == source_buf.id_)
                  && (dest_buf.size_ == source_buf.size_)
                  && (dest_buf.crc_ == source_buf.crc_))
                || dest_buf.modify_time_ >= tbsys::CTimeUtil::strToTime(const_cast<char*>(modify_time.c_str())))
            {
              if (dest_buf.flag_ == source_buf.flag_)
              {
                TBSYS_LOG(INFO, "tfs dest file(%s) exists or modify time(%s) more than(%s)",
                    file_name.c_str(), Func::time_to_str(dest_buf.modify_time_).c_str(), modify_time.c_str());
                ret = SKIP_FLAG;
              }
              else
              {
                TBSYS_LOG(INFO, "tfs dest file(%s) exists, but in diff status(source:%d -> dest:%d)",
                    file_name.c_str(), source_buf.flag_, dest_buf.flag_);
                get_revert_action(source_buf.flag_, dest_buf.flag_, action_set);
                sync_finfo.action_ = REVERT_ACTION;
                ret = SYNC_FLAG;
              }
            }
            else // dest file is diff
            {
              if (source_buf.flag_ == 0)
              {
                sync_finfo.action_ = WRITE_ACTION;
              }
              else if (source_buf.flag_ == 4)
              {
                sync_finfo.action_ = WRITE_ACTION | CONCEAL_ACTION;
              }
              TBSYS_LOG(INFO, "dest file(%s) is diff from source file, source file stat(%d), do action(%d)", file_name.c_str(), source_buf.flag_, sync_finfo.action_);
            }
          }
          else // dest file not exist
          {
            TBSYS_LOG(DEBUG, "dest file(%s) not exist : %d", file_name.c_str(), iret);
            if (source_buf.flag_ == 0)
            {
              sync_finfo.action_ = WRITE_ACTION;
            }
            else if (source_buf.flag_ == 4)
            {
              sync_finfo.action_ = WRITE_ACTION | CONCEAL_ACTION;
            }
            TBSYS_LOG(INFO, "tfs file(%s) not exists, source file stat(%d), do action(%d)", file_name.c_str(), source_buf.flag_, sync_finfo.action_);
            ret = SYNC_FLAG;
          }
          dest_tfs_client.tfs_close();
        }
        else
        {
          if (source_buf.flag_ == 0)
          {
            sync_finfo.action_ = WRITE_ACTION;
          }
          else if (source_buf.flag_ == 4)
          {
            sync_finfo.action_ = WRITE_ACTION | CONCEAL_ACTION;
          }
          TBSYS_LOG(INFO, "open dest tfs file(%s) fail, ret(%d)", file_name.c_str(), iret);
          ret = SYNC_FLAG;
        }
      }
      else
      {
        TBSYS_LOG(INFO, "source tfsfile(%s) has been deleted", file_name.c_str());
        ret = SKIP_FLAG;
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "stat source tfsfile(%s) error, ret: %d", file_name.c_str(), iret);
      ret = ERROR_FLAG;
    }
    source_tfs_client.tfs_close();
  }
  else
  {
    TBSYS_LOG(ERROR, "open source tfsfile(%s) error", file_name.c_str());
    ret = ERROR_FLAG;
  }
  TBSYS_LOG(DEBUG, "cmp file (%s): ret: %d, action: %d, action_set: %d", file_name.c_str(), ret, sync_finfo.action_, action_set.size());
  return ret;
}
void get_revert_action(int32_t source_flag, int32_t dest_flag, VINT& action_set)
{
  if (source_flag == 0)
  {
    if (dest_flag == 1)
    {
      action_set.push_back(UNDELETE);
    }
    else if (dest_flag == 4)
    {
      action_set.push_back(REVEAL);
    }
  }
  else if (source_flag == 4)
  {
    if (dest_flag == 1)
    {
      action_set.push_back(UNDELETE);
      action_set.push_back(CONCEAL);
    }
    else if (dest_flag == 0)
    {
      action_set.push_back(CONCEAL);
    }
  }
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
