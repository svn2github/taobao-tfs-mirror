/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: compare_crc.cpp 445 2011-06-08 09:27:48Z nayan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include <stdlib.h>
#include <libgen.h>
#include <TbThread.h>
#include "common/internal.h"
#include "common/directory_op.h"
#include "common/new_client.h"
#include "common/client_manager.h"
#include "common/status_message.h"
#include "message/server_status_message.h"
#include "message/block_info_message.h"
#include "message/block_info_message_v2.h"
#include "new_client/tfs_client_impl.h"
#include "new_client/fsname.h"
#include "tools/util/tool_util.h"
#include "tools/util/util.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::client;
using namespace tfs::message;
using namespace tfs::tools;

enum CompareResult
{
  COMPARE_SUCCESS,
  COMPARE_FAIL,
  COMPARE_DIFF
};

static const int32_t OP_FILE = 0;
static const int32_t OP_BLOCK = 1;
static bool compare_crc(const FileInfo& left, const FileInfo& right)
{
  return left.crc_ == right.crc_;
}
static CompareResult compare_crc_by_filename(const std::string& left_ns_addr, const std::string& right_ns_addr,
  const std::string& filename, const int64_t modify_time, const int32_t force)
{
  CompareResult cmp_result = COMPARE_SUCCESS;
  FileInfo left, right;
  memset(&left, 0, sizeof(left));
  memset(&right, 0, sizeof(right));
  int32_t left_ret  = Util::read_file_info(left_ns_addr, filename, left);
  int32_t right_ret = Util::read_file_info(right_ns_addr, filename, right);
  int32_t ret = ((left_ret == TFS_SUCCESS || left_ret == META_FLAG_ABNORMAL) && (right_ret == TFS_SUCCESS || right_ret == META_FLAG_ABNORMAL)) ? TFS_SUCCESS : EXIT_CHECK_CRC_ERROR;
  if (TFS_SUCCESS != ret)
  {
    cmp_result = COMPARE_FAIL;
    TBSYS_LOG(WARN, "get file %s file information error, left_ret: %d, right_ret: %d", filename.c_str(), left_ret, right_ret);
  }
  else
  {
    if (left.modify_time_ < modify_time)
    {
      if (TFS_SUCCESS == left_ret && TFS_SUCCESS == right_ret)
      {
        ret = compare_crc(left, right) ? TFS_SUCCESS : EXIT_CHECK_CRC_ERROR;
      }

      if (TFS_SUCCESS == left_ret && META_FLAG_ABNORMAL == right_ret)
      {
        ret = FILE_STATUS_ERROR;
        TBSYS_LOG(INFO, "file %s status not consistency, left: %d, right: %d", filename.c_str(), left.flag_, right.flag_);
      }

      if (META_FLAG_ABNORMAL == left_ret && TFS_SUCCESS == right_ret)
      {
        ret = left.flag_ == FILE_STATUS_DELETE ? TFS_SUCCESS : FILE_STATUS_ERROR;
        if (TFS_SUCCESS != ret)
          TBSYS_LOG(INFO, "file %s status not consistency, left: %d, right: %d", filename.c_str(), left.flag_, right.flag_);
      }

      if (META_FLAG_ABNORMAL == left_ret && META_FLAG_ABNORMAL == right_ret)
      {
        ret = left.flag_ == right.flag_ ? TFS_SUCCESS : FILE_STATUS_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = compare_crc(left, right) ? TFS_SUCCESS : EXIT_CHECK_CRC_ERROR;
        }
        else
        {
          TBSYS_LOG(INFO, "file %s status not consistency, left: %d, right: %d",
              filename.c_str(), left.flag_, right.flag_);
        }
      }

      if (TFS_SUCCESS == ret && left.flag_ != FILE_STATUS_DELETE && 1 == force)
      {
        uint32_t left_crc = 0;
        uint32_t right_crc = 0;
        left_ret  = Util::read_file_real_crc(left_ns_addr, filename, left_crc);
        right_ret = Util::read_file_real_crc(right_ns_addr, filename, right_crc);
        ret = (left_ret == TFS_SUCCESS && right_ret == TFS_SUCCESS) ? TFS_SUCCESS : EXIT_CHECK_CRC_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = left_crc == right_crc ? TFS_SUCCESS : EXIT_CHECK_CRC_ERROR;
        }
      }

      if (TFS_SUCCESS != ret)
      {
        cmp_result = COMPARE_DIFF;
      }
    }
  }
  return cmp_result;
}

static CompareResult compare_crc_by_block_id(const std::string& left_ns_addr, const std::string& right_ns_addr,
  const uint64_t block, const int64_t modify_time, const int32_t force)
{
  CompareResult cmp_result = COMPARE_SUCCESS;
  std::set<FileInfo, CompareFileInfoByFileId > left, right;
  int32_t left_ret = Util::read_file_infos(left_ns_addr, block, left, 0);
  int32_t right_ret = Util::read_file_infos(right_ns_addr, block, right, 1);
  int32_t ret = (left_ret == TFS_SUCCESS && right_ret == TFS_SUCCESS) ? TFS_SUCCESS : EXIT_FILE_INFO_ERROR;
  if (TFS_SUCCESS != ret)
  {
    cmp_result = COMPARE_FAIL;
    TBSYS_LOG(WARN, "get file informations erorr, block: %"PRI64_PREFIX"u, left_ret: %d, right_ret: %d", block, left_ret, right_ret);
  }
  else
  {
    // when we successfully got two filelist, the result must be SUCCESS or DIFF
    std::set<FileInfo, CompareFileInfoByFileId>::iterator iter = left.begin();
    std::set<FileInfo, CompareFileInfoByFileId>::iterator it;
    for (; iter != left.end(); ++iter)  // compare every file in left
    {
      if ((*iter).modify_time_ < modify_time) // ignore modified file after transfer
      {
        // ignore deleted or invalid file
        ret = ((*iter).flag_ & (FILE_STATUS_DELETE | FILE_STATUS_INVALID)) ? TFS_SUCCESS : FILE_STATUS_ERROR;
        if (TFS_SUCCESS != ret)
        {
          it = right.find((*iter));
          ret = (it == right.end()) ? FILE_STATUS_ERROR : TFS_SUCCESS;
          if (TFS_SUCCESS == ret)
          {
            ret = (*iter).flag_ == (*it).flag_ ? TFS_SUCCESS : FILE_STATUS_ERROR;
            if (TFS_SUCCESS == ret)
            {
              ret = compare_crc((*iter), (*it)) ? TFS_SUCCESS : EXIT_CHECK_CRC_ERROR;
            }
            right.erase(it);
            if (TFS_SUCCESS == ret && 1 == force)
            {
              uint32_t left_crc = 0;
              uint32_t right_crc = 0;
              FSName name(block, (*iter).id_);
              left_ret  = Util::read_file_real_crc(left_ns_addr, name.get_name(), left_crc);
              right_ret = Util::read_file_real_crc(right_ns_addr, name.get_name(), right_crc);
              //TBSYS_LOG(INFO, "BLOCK: %lu, FILEID: %lu, left_crc : %u, right_crc: %u, left_ret: %d, right_ret: %d", block, (*iter).id_, left_crc, right_crc, left_ret, right_ret);
              ret = (left_ret == TFS_SUCCESS && right_ret == TFS_SUCCESS) ? TFS_SUCCESS : EXIT_CHECK_CRC_ERROR;
              if (TFS_SUCCESS == ret)
              {
                ret = left_crc == right_crc ? TFS_SUCCESS : EXIT_CHECK_CRC_ERROR;
              }
            }

            if (TFS_SUCCESS != ret)
            {
              cmp_result = COMPARE_DIFF;
              TBSYS_LOG(INFO, "DIFF: blockid: %lu, fileid: %lu, left_status: %d,"
                  "right_status: %d, left_crc : %u, right_crc: %u",
                  block, (*iter).id_, iter->flag_, it->flag_, iter->crc_, it->crc_);
            }
          }
          else
          {
            cmp_result = COMPARE_DIFF;
            TBSYS_LOG(INFO, "MORE: blockid: %lu, fileid: %lu, left_status: %d, left_crc : %u",
                block, iter->id_, iter->flag_, iter->crc_);
          }
        }
        else
        {
          right.erase((*iter));
        }
      }
    }

    // some file modified after transfer in source cluster
    // so it's normal the right is not empty after compare
    it = right.begin();
    for ( ; it != right.end(); it++)
    {
      TBSYS_LOG(INFO, "LESS: blockid: %lu, fileid: %lu, right_status: %d, right_crc : %u",
          block, it->id_, it->flag_, it->crc_);
    }
  }
  return cmp_result;
}

class WorkThread: public tbutil::Thread
{
 public:
   WorkThread(const int32_t type, const std::string& left_ns_addr, const std::string& right_ns_addr,
      const int64_t modify_time, std::vector<std::string>& inputs, FILE* success_fp, FILE* fail_fp, FILE* diff_fp, const int32_t force, const int32_t sleep_ms):
      type_(type),
      left_ns_addr_(left_ns_addr),
      right_ns_addr_(right_ns_addr),
      modify_time_(modify_time),
      inputs_(inputs),
      success_fp_(success_fp),
      fail_fp_(fail_fp),
      diff_fp_(diff_fp),
      force_(force),
      sleep_ms_(sleep_ms),
      stop_(false)
   {

   }

   ~WorkThread()
   {

   }

    void reload_interval(int flag)
    {
      switch (flag)
      {
        case 1:
          sleep_ms_ *= 2;
          break;
        case -1:
          sleep_ms_ /= 2;
          break;
        default:
          TBSYS_LOG(WARN, "invalid flag");
      }
      TBSYS_LOG(INFO, "reload sleep interval, flag: %d, sleep_ms: %d", flag, sleep_ms_);
    }

    void destroy()
    {
      stop_ = true;
    }

   void run()
   {
      std::vector<std::string>::iterator iter = inputs_.begin();
      if (type_ == OP_FILE)
      {
        for (; iter != inputs_.end() && !stop_; ++iter)
        {
          CompareResult result = compare_crc_by_filename(left_ns_addr_, right_ns_addr_, (*iter), modify_time_, force_);
          FILE* fp = NULL;
          if (COMPARE_SUCCESS == result)
          {
            fp = success_fp_;
          }
          else if (COMPARE_FAIL == result)
          {
            fp = fail_fp_;
          }
          else
          {
            fp = diff_fp_;
          }
          fprintf(fp, "%s", (*iter).c_str());
          fflush(fp);
          usleep(sleep_ms_ * 1000);
        }
      }

      if (type_ == OP_BLOCK)
      {
        uint64_t block = 0;
        for (; iter != inputs_.end() && !stop_; ++iter)
        {
          block = atoll((*iter).c_str());
          CompareResult result = compare_crc_by_block_id(left_ns_addr_, right_ns_addr_, block, modify_time_, force_);
          FILE* fp = NULL;
          if (COMPARE_SUCCESS == result)
          {
            fp = success_fp_;
          }
          else if (COMPARE_FAIL == result)
          {
            fp = fail_fp_;
          }
          else
          {
            fp = diff_fp_;
          }

          fprintf(fp, "%s", (*iter).c_str());
          fflush(fp);
          usleep(sleep_ms_* 1000);
        }
      }
   }
private:
  int32_t type_;
  std::string left_ns_addr_;
  std::string right_ns_addr_;
  int64_t modify_time_;
  std::vector<std::string>& inputs_;
  FILE* success_fp_;
  FILE* fail_fp_;
  FILE* diff_fp_;
  int32_t force_;
  int32_t sleep_ms_;
  bool stop_;
};
typedef tbutil::Handle<WorkThread> WorkThreadPtr;

static int32_t thread_num = 1;
static WorkThreadPtr* work_threads = NULL;

static void interruptcallback(int signal)
{
  TBSYS_LOG(INFO, "receive signal: %d", signal);
  bool destory_flag = false;
  int reload_flag = 0;
  switch (signal)
  {
    case SIGUSR1:
      reload_flag = 1;
      break;
    case SIGUSR2:
      reload_flag = -1;
      break;
    case SIGTERM:
    case SIGINT:
    default:
      destory_flag = true;
      break;
  }

  if (work_threads != NULL)
  {
    for (int32_t i = 0; i < thread_num; ++i)
    {
      if (work_threads[i] != 0)
      {
        if (destory_flag)
        {
          work_threads[i]->destroy();
        }
        else if (0 != reload_flag)
        {
          work_threads[i]->reload_interval(reload_flag);
        }
      }
    }
  }
}

void usage(const char* name)
{
  fprintf(stderr,"Usage:\n%s -o old_ns_ip:port -n new_ns_ip:port -f input "
      "-m modify_time(20110315183500) -t thread_num -k type -s interval(ms)"
      "-r ouput_dir -l log_level -d(daemon) -c[-h]\n", name);
  fprintf(stderr, " -k type\n");
  fprintf(stderr, "    0: compare by file\n");
  fprintf(stderr, "    1: compare by block\n");
  fprintf(stderr, " -r output direcotry\n");
  fprintf(stderr, "    all the output file and log file will be in this dir\n");
  fprintf(stderr, " -c\n");
  fprintf(stderr, "    if turn this flag on, data crc will be compared\n");
  fprintf(stderr, " receive signal\n");
  fprintf(stderr, "     USR1: increase sleep interval to double\n");
  fprintf(stderr, "     USR1: decrease sleep interval to half\n");

  exit(-1);
}

int main(int argc, char** argv)
{
  int ret = TFS_SUCCESS;
  int32_t i = 0, real_read_check_crc = 0; // default one thread
  int64_t modify_time = 0;
  int32_t sleep_ms = 0;
  int32_t op_type = OP_FILE;
  bool daemon_flag = false;
  std::string old_ns_addr(""), new_ns_addr(""), file_path(""), work_dir(""), log_level("info");
  while ((i = getopt(argc, argv, "o:n:f:m:t:k:s:l:r:dch")) != EOF)
  {
    switch (i)
    {
      case 'o':
        old_ns_addr = optarg;
        break;
      case 'n':
        new_ns_addr = optarg;
        break;
      case 'f':
        file_path   = optarg;
        break;
      case 'k':
        op_type = atoi(optarg);
        break;
      case 'm':
        modify_time = tbsys::CTimeUtil::strToTime(optarg);
        break;
      case 't':
        thread_num = atoi(optarg);
        break;
      case 'l':
        log_level = optarg;
        break;
      case 'c':
        real_read_check_crc  = 1;
        break;
      case 'd':
        daemon_flag = true;
        break;
      case 's':
        sleep_ms = atoi(optarg);
        break;
      case 'r':
        work_dir = optarg;
        break;
      case 'h':
      default:
        usage(argv[0]);
    }
  }
  if (old_ns_addr == ""
      || new_ns_addr == ""
      || file_path == ""
      || modify_time < 0
      || sleep_ms < 0)
  {
    usage(argv[0]);
  }

  char log_path[256];
  if (work_dir == "") // use default output dir
  {
    snprintf(log_path, 256, "%s%s", dirname(argv[0]), "/cmp_log");
  }
  else
  {
    snprintf(log_path, 256, "%s", work_dir.c_str());
  }
  DirectoryOp::create_directory(log_path);

  // init log and daemon
  if (TFS_SUCCESS == ret)
  {
    string log_file = string(log_path) + string("/compare_crc.log");
    string pid_file = string(log_path) + string("/compare_crc.pid");
    if (log_file.size() != 0)
    {
      TBSYS_LOGGER.rotateLog(log_file.c_str());
    }
    TBSYS_LOGGER.setMaxFileSize(1024 * 1024 * 1024);
    TBSYS_LOGGER.setLogLevel(log_level.c_str());

    int pid = 0;
    if (daemon_flag)
    {
      pid = Func::start_daemon(pid_file.c_str(), log_file.c_str());
    }

    // parent process just exit
    if (0 != pid)
    {
      return 0;
    }
  }

  char path[256];
  snprintf(path, 256, "%s%s", log_path, "/success");
  FILE* success_fp = fopen(path, "w");
  memset(path, 0, 256);
  snprintf(path, 256, "%s%s", log_path, "/fail");
  FILE* fail_fp = fopen(path, "w");
  memset(path, 0, 256);
  snprintf(path, 256, "%s%s", log_path, "/diff");
  FILE* diff_fp = fopen(path, "a");  // append to one file for simplicity
  ret = (NULL == success_fp || NULL == fail_fp || NULL == diff_fp) ? EXIT_OPEN_FILE_ERROR : TFS_SUCCESS;
  std::vector<std::string> params[thread_num];
  if (TFS_SUCCESS == ret)
  {
    FILE* fp = fopen(file_path.c_str(), "r");
    ret = (NULL == fp) ? EXIT_OPEN_FILE_ERROR : TFS_SUCCESS;
    if (TFS_SUCCESS == ret)
    {
      char line[256];
      while (NULL != fgets(line, 256, fp))
      {
        int32_t index = random() % thread_num;
        params[index].push_back(line);
      }
    }
    if (NULL != fp)
      ::fclose(fp);
  }

  if (TFS_SUCCESS == ret)
  {
    ret = TfsClientImpl::Instance()->initialize(NULL, DEFAULT_BLOCK_CACHE_TIME, DEFAULT_BLOCK_CACHE_ITEMS, false);
  }

  if (TFS_SUCCESS == ret)
  {
    int32_t index = 0;
    work_threads = new WorkThreadPtr[thread_num];
    for (index = 0; index < thread_num; ++index)
    {
      work_threads[index] = new WorkThread(op_type, old_ns_addr, new_ns_addr, modify_time, params[index], success_fp, fail_fp, diff_fp, real_read_check_crc, sleep_ms);
      work_threads[index]->start();
    }

    signal(SIGPIPE, SIG_IGN);
    signal(SIGHUP, SIG_IGN);
    signal(SIGINT, interruptcallback);
    signal(SIGTERM, interruptcallback);
    signal(SIGUSR1, interruptcallback);
    signal(SIGUSR2, interruptcallback);

    for (index = 0; index < thread_num; ++index)
    {
      work_threads[index]->join();
    }

    tbsys::gDeleteA(work_threads);
  }

  if (NULL != success_fp)
    ::fclose(success_fp);
  if (NULL != fail_fp)
    ::fclose(fail_fp);
  if (NULL != diff_fp)
    ::fclose(diff_fp);

  //TBSYS_LOG(WARN, "total_count: %"PRI64_PREFIX"d, succ_count: %"PRI64_PREFIX"d, fail_count: %"PRI64_PREFIX"d"
  //    ", error_count: %"PRI64_PREFIX"d, unsync_count: %"PRI64_PREFIX"d, skip_count: %"PRI64_PREFIX"d",
  //    cmp_stat_.total_count_, cmp_stat_.succ_count_, cmp_stat_.fail_count_, cmp_stat_.error_count_, cmp_stat_.unsync_count_,
  //    cmp_stat_.skip_count_);
  return ret;
}

