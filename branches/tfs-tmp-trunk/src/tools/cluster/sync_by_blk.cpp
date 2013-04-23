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

#include <set>
#include "tools/util/util.h"
#include "sync_file_base.h"
#include "common/client_manager.h"
#include "common/status_message.h"
#include "message/block_info_message.h"
#include "message/server_status_message.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::client;
using namespace tfs::tools;

typedef vector<uint64_t> BLOCK_ID_VEC;
typedef vector<uint64_t>::iterator BLOCK_ID_VEC_ITER;
typedef vector<std::string> FILE_NAME_VEC;
typedef vector<std::string>::iterator FILE_NAME_VEC_ITER;

struct BlockFileInfo
{
  BlockFileInfo(const uint64_t block_id, const FileInfo& file_info)
    : block_id_(block_id)
  {
    memcpy(&file_info_, &file_info, sizeof(file_info));
  }
  ~BlockFileInfo()
  {
  }
  uint64_t block_id_;
  FileInfo file_info_;
};

typedef vector<BlockFileInfo> BLOCK_FILE_INFO_VEC;
typedef vector<BlockFileInfo>::iterator BLOCK_FILE_INFO_VEC_ITER;

struct SyncStat
{
  int64_t total_count_;
  int64_t actual_count_;
  int64_t success_count_;
  int64_t fail_count_;
  int64_t unsync_count_;
  int64_t del_count_;
};
struct LogFile
{
  FILE** fp_;
  const char* file_;
};

tbutil::Mutex g_mutex_;
SyncStat g_sync_stat_;
static int32_t thread_count = 1;
static const int32_t MAX_READ_LEN = 256;
FILE *g_blk_done= NULL;
FILE *g_file_succ = NULL, *g_file_fail = NULL, *g_file_unsync = NULL, *g_file_del = NULL;

//int get_server_status();
//int get_diff_block();
//int get_file_list(const string& ns_addr, const uint64_t block_id, BLOCK_FILE_INFO_VEC& v_block_file_info);
int get_file_list(const string& ns_addr, const uint64_t block_id, FILE_NAME_VEC& v_file_name);
int sync_file(const string& src_ns_addr, const string& dest_ns_addr, const uint64_t block_id, const FileInfo& file_info, const bool force, const int32_t modify_time);

static void get_diff_file_infos(std::multiset<std::string>& src_file_name, std::multiset<std::string>& dest_file_name, std::multiset<std::string>& out_file_name)
{
  std::multiset<std::string>::const_iterator iter = dest_file_name.begin();
  for (; iter != dest_file_name.end(); ++iter)
  {
    std::multiset<std::string>::const_iterator it = src_file_name.find((*iter));
    if (it == src_file_name.end())
    {
      TBSYS_LOG(DEBUG, "dest file need to be deleted. file_name: %s", (*iter).c_str());
      fprintf(g_file_del, "%s\n", (*iter).c_str());
      out_file_name.insert((*iter));
    }
  }
}

static void unlink_file_list(std::multiset<std::string>& out_v_file_name, const std::string& ns_addr)
{
  int64_t size = 0;
  std::multiset<std::string>::const_iterator iter = out_v_file_name.begin();
  for (; iter != out_v_file_name.end(); ++iter)
  {
    TfsClientImpl::Instance()->unlink(size, (*iter).c_str(), NULL, ns_addr.c_str());
    fprintf(g_file_del, "%s\n", (*iter).c_str());
    tbutil::Mutex::Lock lock(g_mutex_);
    g_sync_stat_.del_count_++;
  }
}

class WorkThread : public tbutil::Thread
{
  public:
    WorkThread(const string& src_ns_addr, const string& dest_ns_addr, const bool need_remove_file, const bool force, const int32_t modify_time):
      src_ns_addr_(src_ns_addr), dest_ns_addr_(dest_ns_addr), need_remove_file_(need_remove_file), force_(force), modify_time_(modify_time), destroy_(false)
    {
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
      std::vector<uint64_t>::const_iterator iter = v_block_id_.begin();
      for (; iter != v_block_id_.end() && !destroy_; ++iter)
      {
        uint64_t block_id = (*iter);
        bool block_done = false;
        TBSYS_LOG(INFO, "sync block started. blockid: %"PRI64_PREFIX"u", block_id);

        std::set<FileInfo, CompareFileInfoByFileId> files;
        int32_t ret = Util::read_file_infos(src_ns_addr_, block_id, files, 0);
        if (TFS_SUCCESS == ret)
        {
          std::set<FileInfo, CompareFileInfoByFileId>::const_iterator it = files.begin();
          for (; it != files.end() && !destroy_; ++it)
          {
            sync_file(src_ns_addr_, dest_ns_addr_, block_id, (*it), force_, modify_time_);
          }
          block_done = it == files.end();
          if (need_remove_file_)
          {
            std::multiset<std::string> src_file_names, dest_file_names, diff_file_names;
            Util::read_file_infos(src_ns_addr_, block_id, src_file_names, 0);
            TBSYS_LOG(INFO, "source file list size: %zd", src_file_names.size());

            Util::read_file_infos(dest_ns_addr_, block_id, dest_file_names, 1);
            TBSYS_LOG(INFO, "dest file list size: %zd", src_file_names.size());

            get_diff_file_infos(src_file_names, dest_file_names, diff_file_names);
            TBSYS_LOG(INFO, "diff file list size: %zd", diff_file_names.size());

            unlink_file_list(diff_file_names, dest_ns_addr_);
            TBSYS_LOG(INFO, "unlink file list size: %zd", diff_file_names.size());
          }
        }
        else
        {
          TBSYS_LOG(WARN, "get file infos fail, block: %"PRI64_PREFIX"u", block_id);
        }

        if (block_done)
        {
          fprintf(g_blk_done, "%"PRI64_PREFIX"u\n", block_id);
          TBSYS_LOG(INFO, "sync block finished. blockid: %"PRI64_PREFIX"u", block_id);
        }
      }
    }

    void push_back(uint64_t block_id)
    {
      v_block_id_.push_back(block_id);
    }
  private:
    WorkThread(const WorkThread&);
    string src_ns_addr_;
    string dest_ns_addr_;
    bool need_remove_file_;
    bool force_;
    BLOCK_ID_VEC v_block_id_;
    int32_t modify_time_;
    bool destroy_;
};
typedef tbutil::Handle<WorkThread> WorkThreadPtr;

static WorkThreadPtr* gworks = NULL;

struct LogFile g_log_fp[] =
{
  {&g_blk_done, "sync_done_blk"},
  {&g_file_succ, "sync_succ_file"},
  {&g_file_fail, "sync_fail_file"},
  {&g_file_unsync, "sync_unsync_file"},
  {&g_file_del, "sync_del_file"},
  {NULL, NULL}
};
static string sync_file_log("sync_by_blk.log");

int rename_file(const char* file_path)
{
  int ret = TFS_SUCCESS;
  if (access(file_path, F_OK) == 0)
  {
    char old_file_path[256];
    snprintf(old_file_path, 256, "%s.%s", file_path, Func::time_to_str(time(NULL), 1).c_str());
    ret = rename(file_path, old_file_path);
  }
  return ret;
}

int init_log_file(const char* dir_path)
{
  for (int i = 0; g_log_fp[i].file_; i++)
  {
    char file_path[256];
    snprintf(file_path, 256, "%s/%s", dir_path, g_log_fp[i].file_);
    rename_file(file_path);
    *g_log_fp[i].fp_ = fopen(file_path, "wb");
    if (!*g_log_fp[i].fp_)
    {
      printf("open file fail %s : %s\n:", g_log_fp[i].file_, strerror(errno));
      return TFS_ERROR;
    }
  }
  char log_file_path[256];
  snprintf(log_file_path, 256, "%s/%s", dir_path, sync_file_log.c_str());
  rename_file(log_file_path);
  TBSYS_LOGGER.setFileName(log_file_path, true);
  TBSYS_LOGGER.setMaxFileSize(1024 * 1024 * 1024);

  return TFS_SUCCESS;
}

static void usage(const char* name)
{
  fprintf(stderr, "Usage: %s -s src_addr -d dest_addr -f blk_list [-m modify_time] [-t thread_count] [-p log_path] [-e] [-u] [-l level] [-h]\n", name);
  fprintf(stderr, "       -s source ns ip port\n");
  fprintf(stderr, "       -d dest ns ip port\n");
  fprintf(stderr, "       -f block list file, assign a file to sync\n");
  fprintf(stderr, "       -m modify time, the file modified after it will be ignored. ex: 20121220, default is tomorrow, optional\n");
  fprintf(stderr, "       -e force flag, need strong consistency, optional\n");
  fprintf(stderr, "       -u flag, need delete redundent file in dest cluster, optional\n");
  fprintf(stderr, "       -p the dir path of log file and result logs, both absolute and relative path are ok, if path not exists, it will be created. default is under logs/, optional\n");
  fprintf(stderr, "       -l log file level, set log file level to (debug|info|warn|error), default is info, optional\n");
  fprintf(stderr, "       -h help\n");
  fprintf(stderr, "ex: ./sync_by_blk -s 168.192.1.1:3000 -d 168.192.1.2:3000 -f blk_list -m 20121220\n");
  fprintf(stderr, "    ./sync_by_blk -s 168.192.1.1:3000 -d 168.192.1.2:3000 -f blk_list -m 20121220 -p t1m -t 5\n");
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
        for (int32_t i = 0; g_log_fp[i].file_; i++)
        {
          if (!*g_log_fp[i].fp_)
          {
            fflush(*g_log_fp[i].fp_);
          }
        }
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

string get_day(const int32_t days, const bool is_after)
{
  time_t now = time((time_t*)NULL);
  time_t end_time;
  if (is_after)
  {
    end_time = now + 86400 * days;
  }
  else
  {
    end_time = now - 86400 * days;
  }
  char tmp[64];
  struct tm *ttime;
  ttime = localtime(&end_time);
  strftime(tmp,64,"%Y%m%d",ttime);
  return string(tmp);
}

int main(int argc, char* argv[])
{
  int32_t i;
  string src_ns_addr, dest_ns_addr;
  string block_list_file;
  bool force = false;
  bool need_remove_file = false;
  string modify_time = get_day(1, true);
  string log_path("logs/");
  string level("info");

  // analyze arguments
  while ((i = getopt(argc, argv, "s:d:f:eum:t:p:l:h")) != EOF)
  {
    switch (i)
    {
      case 's':
        src_ns_addr = optarg;
        break;
      case 'd':
        dest_ns_addr = optarg;
        break;
      case 'f':
        block_list_file = optarg;
        break;
      case 'e':
        force = true;
        break;
      case 'u':
        need_remove_file = true;
        break;
      case 'm':
        modify_time = optarg;
        break;
      case 't':
        thread_count = atoi(optarg);
        break;
      case 'p':
        log_path = optarg;
        break;
      case 'l':
        level = optarg;
        break;
      case 'h':
      default:
        usage(argv[0]);
    }
  }

  if (src_ns_addr.empty()
      || dest_ns_addr.empty()
      || block_list_file.empty()
      || modify_time.empty())
  {
    usage(argv[0]);
  }

  if ((level != "info") && (level != "debug") && (level != "error") && (level != "warn"))
  {
    fprintf(stderr, "level(info | debug | error | warn) set error\n");
    return TFS_ERROR;
  }

  if ((access(log_path.c_str(), F_OK) == -1) && (!DirectoryOp::create_full_path(log_path.c_str())))
  {
    TBSYS_LOG(ERROR, "create log file path failed. log_path: %s", log_path.c_str());
    return TFS_ERROR;
  }

  modify_time += "000000";
  init_log_file(log_path.c_str());
  TBSYS_LOGGER.setLogLevel(level.c_str());

  TfsClientImpl::Instance()->initialize(NULL, DEFAULT_BLOCK_CACHE_TIME, 1000, false);

  BLOCK_ID_VEC v_block_id;
  if (! block_list_file.empty())
  {
    FILE *fp = fopen(block_list_file.c_str(), "rb");
    if (fp)
    {
      char tbuf[256];
      while(fgets(tbuf, 256, fp))
      {
        uint64_t block_id = strtoull(tbuf, NULL, 10);
        v_block_id.push_back(block_id);
      }
      fclose(fp);
    }
  }
  TBSYS_LOG(INFO, "blockid list size: %zd", v_block_id.size());

  memset(&g_sync_stat_, 0, sizeof(g_sync_stat_));

  if (v_block_id.size() > 0)
  {
    gworks = new WorkThreadPtr[thread_count];
    int32_t i = 0;
    int32_t time = tbsys::CTimeUtil::strToTime(const_cast<char*>(modify_time.c_str()));
    for (; i < thread_count; ++i)
    {
      gworks[i] = new WorkThread(src_ns_addr, dest_ns_addr, need_remove_file, force, time);
    }
    int32_t index = 0;
    int64_t count = 0;
    BLOCK_ID_VEC_ITER iter = v_block_id.begin();
    for (; iter != v_block_id.end(); iter++)
    {
      index = count % thread_count;
      gworks[index]->push_back(*iter);
      ++count;
    }
    for (i = 0; i < thread_count; ++i)
    {
      gworks[i]->start();
    }

    signal(SIGTERM, interrupt_callback);
    signal(SIGINT, interrupt_callback);

    for (i = 0; i < thread_count; ++i)
    {
      gworks[i]->wait_for_shut_down();
    }

    tbsys::gDeleteA(gworks);
  }

  for (i = 0; g_log_fp[i].fp_; i++)
  {
    if (*g_log_fp[i].fp_ != NULL)
    {
      fclose(*g_log_fp[i].fp_);
      *g_log_fp[i].fp_ = NULL;
    }
  }

  fprintf(stdout, "total_count: %"PRI64_PREFIX"d, actual_count: %"PRI64_PREFIX"d, "
      "succ_count: %"PRI64_PREFIX"d, fail_count: %"PRI64_PREFIX"d, unsync_count: %"PRI64_PREFIX"d, del_count: %"PRI64_PREFIX"d\n",
      g_sync_stat_.total_count_, g_sync_stat_.actual_count_,
      g_sync_stat_.success_count_, g_sync_stat_.fail_count_, g_sync_stat_.unsync_count_, g_sync_stat_.del_count_);
  fprintf(stdout, "log and result path: %s\n", log_path.c_str());

  return TFS_SUCCESS;
}
int sync_file(const string& src_ns_addr, const string& dest_ns_addr, const uint64_t block_id, const FileInfo& file_info,
    const bool force, const int32_t modify_time)
{
  int ret = TFS_SUCCESS;

  SyncFileBase sync_op;
  sync_op.initialize(src_ns_addr, dest_ns_addr);
  SyncAction sync_action;

  // compare file info
  ret = sync_op.cmp_file_info(block_id, file_info, sync_action, force, modify_time);

  FSName fsname(block_id, file_info.id_, TfsClientImpl::Instance()->get_cluster_id(src_ns_addr.c_str()));
  string file_name = string(fsname.get_name());
  // do sync file
  if (ret == TFS_SUCCESS)
  {
    ret = sync_op.do_action(file_name, sync_action);
  }
  if (ret == TFS_SUCCESS)
  {
    if (sync_action.size() == 0)
    {
      TBSYS_LOG(DEBUG, "sync file(%s) succeed without action.", file_name.c_str());
      {
        tbutil::Mutex::Lock lock(g_mutex_);
        g_sync_stat_.unsync_count_++;
      }
      fprintf(g_file_unsync, "%s\n", file_name.c_str());
    }
    else
    {
      TBSYS_LOG(DEBUG, "sync file(%s) succeed.", file_name.c_str());
      {
        tbutil::Mutex::Lock lock(g_mutex_);
        g_sync_stat_.success_count_++;
      }
      fprintf(g_file_succ, "%s\n", file_name.c_str());
    }
  }
  else
  {
    TBSYS_LOG(DEBUG, "sync file(%s) failed.", file_name.c_str());
    {
      tbutil::Mutex::Lock lock(g_mutex_);
      g_sync_stat_.fail_count_++;
    }
    fprintf(g_file_fail, "%s\n", file_name.c_str());
  }
  g_sync_stat_.actual_count_++;

  return ret;
}
