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
#include "sync_file_base.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::client;

typedef vector<string> VEC_FILE_NAME;
typedef vector<string>::iterator VEC_FILE_NAME_ITER;
struct SyncStat
{
  int64_t total_count_;
  int64_t actual_count_;
  int64_t success_count_;
  int64_t fail_count_;
  int64_t unsync_count_;
};
struct LogFile
{
  FILE** fp_;
  const char* file_;
};

tbutil::Mutex g_mutex_;
SyncStat g_sync_stat_;

int get_file_list(const string& log_file, VEC_FILE_NAME& name_set);
int sync_file(const string& src_ns_addr, const string& dest_ns_addr, VEC_FILE_NAME& name_set, const bool force, const int32_t modify_time);
void trim(const char* input, char* output);

class WorkThread : public tbutil::Thread
{
  public:
    WorkThread(const string& src_ns_addr, const string& dest_ns_addr, const bool force, const int32_t modify_time):
      src_ns_addr_(src_ns_addr), dest_ns_addr_(dest_ns_addr), force_(force), modify_time_(modify_time), destroy_(false)
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
      if (! destroy_)
      {
        sync_file(src_ns_addr_, dest_ns_addr_, name_set_, force_, modify_time_);
      }
    }

    void push_back(string& file_name)
    {
      name_set_.push_back(file_name);
    }

  private:
    WorkThread(const WorkThread&);
    WorkThread& operator=(const WorkThread&);
    string src_ns_addr_;
    string dest_ns_addr_;
    VEC_FILE_NAME name_set_;
    bool force_;
    int32_t modify_time_;
    bool destroy_;
};
typedef tbutil::Handle<WorkThread> WorkThreadPtr;
static WorkThreadPtr* gworks = NULL;
static int32_t thread_count = 1;
static const int32_t MAX_READ_LEN = 256;
FILE *g_blk_done= NULL;
FILE *g_file_succ = NULL, *g_file_fail = NULL, *g_file_unsync = NULL;

struct LogFile g_log_fp[] =
{
  {&g_blk_done, "sync_done_blk"},
  {&g_file_succ, "sync_succ_file"},
  {&g_file_fail, "sync_fail_file"},
  {&g_file_unsync, "sync_unsync_file"},
  {NULL, NULL}
};
static string sync_file_log("sync_by_file.log");

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
  fprintf(stderr, "       -f file list to sync\n");
  fprintf(stderr, "       -m modify time, the file modified after it will be ignored. ex: 20121220, default is tomorrow, optional\n");
  fprintf(stderr, "       -e force flag, need strong consistency, optional\n");
  fprintf(stderr, "       -p the dir path of log file and result logs, both absolute and relative path are ok, if path not exists, it will be created. default is under logs/, optional\n");
  fprintf(stderr, "       -l log file level, set log file level to (debug|info|warn|error), default is info, optional\n");
  fprintf(stderr, "       -h help\n");
  fprintf(stderr, "   ex: %s -s 168.192.1.1:3000 -d 168.192.1.2:3000 -f file_list -m 20121220\n", name);
  fprintf(stderr, "       %s -s 168.192.1.1:3000 -d 168.192.1.2:3000 -f file_list -m 20121220 -p t1m -t 5\n", name);
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
  string file_list;
  bool force = false;
  string log_path("logs/");
  string level("info");
  string modify_time = get_day(1, true);

  // analyze arguments
  while ((i = getopt(argc, argv, "s:d:f:m:t:p:l:eh")) != EOF)
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
        file_list = optarg;
        break;
      case 'e':
        force = true;
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
      || file_list.empty()
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

  TfsClientImpl::Instance()->initialize(NULL, DEFAULT_BLOCK_CACHE_TIME, DEFAULT_BLOCK_CACHE_ITEMS, false);

  memset(&g_sync_stat_, 0, sizeof(g_sync_stat_));

  VEC_FILE_NAME name_set;
  if (!file_list.empty())
  {
    get_file_list(file_list, name_set);
  }
  if (name_set.size() > 0)
  {
    gworks = new WorkThreadPtr[thread_count];
    int32_t i = 0;
    int32_t time = tbsys::CTimeUtil::strToTime(const_cast<char*>(modify_time.c_str()));
    for (; i < thread_count; ++i)
    {
      gworks[i] = new WorkThread(src_ns_addr, dest_ns_addr, force, time);
    }
    int32_t index = 0;
    int64_t count = 0;
    VEC_FILE_NAME_ITER iter = name_set.begin();
    for (; iter != name_set.end(); iter++)
    {
      index = count % thread_count;
      gworks[index]->push_back(*iter);
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

  fprintf(stdout, "total_count: %"PRI64_PREFIX"d, actual_count: %"PRI64_PREFIX"d, "
      "succ_count: %"PRI64_PREFIX"d, fail_count: %"PRI64_PREFIX"d, unsync_count: %"PRI64_PREFIX"d\n",
      g_sync_stat_.total_count_, g_sync_stat_.actual_count_,
      g_sync_stat_.success_count_, g_sync_stat_.fail_count_, g_sync_stat_.unsync_count_);
  fprintf(stdout, "log and result path: %s\n", log_path.c_str());

  return TFS_SUCCESS;
}
int get_file_list(const string& log_file, VEC_FILE_NAME& name_set)
{
  int ret = TFS_ERROR;
  FILE* fp = fopen (log_file.c_str(), "r");
  if (fp == NULL)
  {
    TBSYS_LOG(ERROR, "open file(%s) fail,errors(%s)", log_file.c_str(), strerror(errno));
  }
  else
  {
    char name[MAX_READ_LEN];
    char buf[MAX_READ_LEN];
    while(!feof(fp))
    {
      memset(buf, 0, MAX_READ_LEN);
      fgets (buf, MAX_READ_LEN, fp);
      trim(buf, name);
      if (strlen(name) > 0)
      {
        name_set.push_back(string(name));
        TBSYS_LOG(DEBUG, "file name: %s\n", name);
      }
    }
    fclose (fp);
    ret = TFS_SUCCESS;
  }
  return ret;
}
int sync_file(const string& src_ns_addr, const string& dest_ns_addr, VEC_FILE_NAME& name_set, const bool force, const int32_t modify_time)
{
  int ret = TFS_SUCCESS;
  SyncFileBase sync_op;
  sync_op.initialize(src_ns_addr, dest_ns_addr);
  VEC_FILE_NAME_ITER iter = name_set.begin();
  for (; iter != name_set.end(); iter++)
  {
    string file_name = (*iter);
    SyncAction sync_action;

    // compare file info
    ret = sync_op.cmp_file_info(file_name, sync_action, force, modify_time);

    // do sync file
    if (ret != TFS_ERROR)
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
  }
  return ret;
}

void trim(const char* input, char* output)
{
  if (output == NULL)
  {
    fprintf(stderr, "output str is null");
  }
  else
  {
    int32_t size = strlen(input);
    int32_t j = 0;
    for (int32_t i = 0; i < size; i++)
    {
      if (!isspace(input[i]))
      {
        output[j] = input[i];
        j++;
      }
    }
    output[j] = '\0';
  }
}
