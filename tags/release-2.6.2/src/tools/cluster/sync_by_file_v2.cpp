/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: sync_by_file.cpp 2312 2013-06-13 08:46:08Z duanfei $
 *
 * Authors:
 *   chuyu <chuyu@taobao.com>
 *      - initial release
 *
 */

#include "tools/util/util.h"
#include "tools/util/tool_util.h"
#include "tools/util/base_worker.h"
#include "sync_file_base_v2.h"
#include "clientv2/tfs_client_impl_v2.h"
#include "common/func.h"
#include "common/error_msg.h"

#include <sys/types.h>
#include <sys/syscall.h>

using namespace std;
using namespace tfs::common;
using namespace tfs::clientv2;
using namespace tfs::tools;

class SyncByFileManger;
class SyncByFileWorker: public BaseWorker
{
  public:
    SyncByFileWorker(SyncByFileManger* manager) : manager_(manager) {}
    ~SyncByFileWorker(){}
    virtual int process(string& line);
    SyncByFileManger* manager_;
};


class SyncByFileManger: public BaseWorkerManager
{
  public:
    SyncByFileManger(bool force = false, bool unlink = false)
      : force_(force), unlink_(unlink),
        log_file_succ_("sync_succ_file"), log_file_fail_("sync_fail_file"), log_file_unsync_("sync_unsync_file")
    {
    }
    ~SyncByFileManger(){}
    virtual int begin();
    virtual void end();
    BaseWorker* create_worker();
    void write_log_file(const string& tfs_file_name, const SyncResult& result);

    bool need_force_sync()
    {
      return force_;
    }

    bool need_unlink_sync()
    {
      return unlink_;
    }

  private:
    void destroy_log_file(LogFile& log_file);
    virtual void usage(const char* app_name);

    bool force_;
    bool unlink_;
    SyncStat sync_stat_;//记录各种操作结果的数目信息
    LogFile log_file_succ_;//记录同步成功的文件名
    LogFile log_file_fail_;
    LogFile log_file_unsync_;
    tbutil::Mutex mutex_;
};


void SyncByFileManger::usage(const char* app_name)
{
  char *options =
    "-s           source server ip:port\n"
    "-d           dest server ip:port\n"
    "-f           input file name\n"
    "-m           timestamp eg: 20130610, optional, default next day 0'clock\n"
    "-i           sleep interval (ms), optional, default 0\n"
    "-e           force flag, need strong consistency(crc), optional\n"
    "-r           unlink flag, need sync unlink file to dest cluster, optional\n"
    "-t           thread count, optional, defaul 1\n"
    "-l           log level, optional, default info\n"
    "-p           output directory, optional, default ./logs\n"
    "-v           print version information\n"
    "-h           print help information\n"
    "signal       SIGUSR1 inc sleep interval 1000ms\n"
    "             SIGUSR2 dec sleep interval 1000ms\n";
  fprintf(stderr, "%s usage:\n%s", app_name, options);
  exit(-1);
}

// trim left and right whitespace/tab/line break
char* trim_line(char* s)
{
  if(NULL == s)
  {
    fprintf(stderr, "trim str is null\n");
  }
  else
  {
    while(*s == ' ' || *s == '\t')
      ++s;
    char* p = s + strlen(s) - 1;
    while(p >= s && (*p == ' ' || *p == '\n'  || *s == '\t'))//fgets line including the trailing '\n'
      --p;
    *(p+1) = '\0';
  }
  return s;
}


void SyncByFileManger::destroy_log_file(LogFile& log_file)
{
  if (NULL != log_file.fp_)
  {
    fclose(log_file.fp_);
    log_file.fp_ = NULL;
  }
}

void SyncByFileManger::write_log_file(const string& tfs_file_name, const SyncResult& result)
{
  if ( !tfs_file_name.empty() )
  {
    tbutil::Mutex::Lock lock(mutex_);
    if (SYNC_SUCCESS == result)
    {
      sync_stat_.success_count_++;
      fprintf(log_file_succ_.fp_, "%s\n", tfs_file_name.c_str());
    }
    else if (SYNC_FAILED == result)
    {
      TBSYS_LOG(WARN, "sync file(%s) failed.", tfs_file_name.c_str());
      sync_stat_.fail_count_++;
      fprintf(log_file_fail_.fp_, "%s\n", tfs_file_name.c_str());
    }
    else
    {
      sync_stat_.unsync_count_++;
      fprintf(log_file_unsync_.fp_, "%s\n", tfs_file_name.c_str());
    }
    sync_stat_.total_count_++;
  }
  else
  {
    TBSYS_LOG(WARN, "sync tfs_file_name is empty");
  }
}


int SyncByFileWorker::process(string& line)
{
  int ret = TFS_SUCCESS;
  if ( !line.empty() )
  {
    if(line.size() > (unsigned int)MAX_LINE_SIZE)
    {
      TBSYS_LOG(ERROR, "file name(%s) length more than MAX_LINE_SIZE:%d.", line.c_str(), MAX_LINE_SIZE);
      ret = EXIT_INVALID_FILE_NAME;
    }
    else
    {
      char name[MAX_LINE_SIZE+1] = {0};
      line.copy(name, line.size());
      string tfs_file_name( trim_line(name) );

      SyncFileBase sync_file_op(src_addr_, dest_addr_);
      SyncResult result = SYNC_NOTHING;
      ret = sync_file_op.cmp_and_sync_file(tfs_file_name, timestamp_, result, manager_->need_force_sync(), manager_->need_unlink_sync());
      manager_->write_log_file(tfs_file_name, result);
      TBSYS_LOG(DEBUG, "sync file: %s finished, %s", tfs_file_name.c_str(), (TFS_SUCCESS == ret) ? "success" : "failed");
    }
  }
  else
  {
    ret = TFS_ERROR;
    TBSYS_LOG(ERROR, "this line is a null string in putfile");
  }
  return ret;
}

int SyncByFileManger::begin()
{
  srandom((unsigned)time(NULL));
  int ret = TfsClientImplV2::Instance()->initialize();
  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "TfsClientImplV2 initialize failed, ret:%d", ret);
  }
  else
  {
    if ( !log_file_succ_.init_log_file(get_output_dir().c_str())
        || !log_file_fail_.init_log_file(get_output_dir().c_str())
        || !log_file_unsync_.init_log_file(get_output_dir().c_str()))
    {
      TBSYS_LOG(ERROR, "init sync log file failed.");
      ret = EXIT_OPEN_FILE_ERROR;
    }
  }
  return ret;
}


void SyncByFileManger::end()
{
  TfsClientImplV2::Instance()->destroy();
  destroy_log_file(log_file_succ_);
  destroy_log_file(log_file_fail_);
  destroy_log_file(log_file_unsync_);

  //rotato log默认标准输出全部到log
  char file_path[256];
  snprintf(file_path, 256, "%s/%s", get_output_dir().c_str(), "result");
  FILE* result_fp = fopen(file_path, "wb");
  if(NULL != result_fp)
  {
    sync_stat_.dump(result_fp);
    fclose(result_fp);
  }
  else
  {
    TBSYS_LOG(ERROR, "open result file:%s to write fail", file_path);
  }
  TBSYS_LOG(INFO, "log and result output path: %s\n", get_output_dir().c_str());
}

BaseWorker* SyncByFileManger::create_worker()
{
  return new SyncByFileWorker(this);
}

int main(int argc, char* argv[])
{
  bool force = false;
  bool unlink = false;
  int idx = 0;
  while (idx < argc)
  {
    if (strcmp(argv[idx], "-e")== 0)
    {
      force = true;
      for (int i = idx; i + 1 < argc; ++i)
      {
        argv[i] = argv[i + 1];
      }
      argc -= 1;
    }
    else if (strcmp(argv[idx], "-r")== 0)
    {
      unlink = true;
      for (int i = idx; i + 1 < argc; ++i)
      {
        argv[i] = argv[i + 1];
      }
      argc -= 1;
    }
    else
    {
      ++idx;
    }
  }
  SyncByFileManger work_manager(force, unlink);
  return work_manager.main(argc, argv);
}


