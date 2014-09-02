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
#include "common/version.h"
#include "common/client_manager.h"
#include "common/status_message.h"
#include "message/server_status_message.h"
#include "message/block_info_message.h"
#include "message/block_info_message_v2.h"
#include "clientv2/tfs_client_impl_v2.h"
#include "clientv2/fsname.h"
#include "tools/util/tool_util.h"
#include "tools/util/util.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::clientv2;
using namespace tfs::message;
using namespace tfs::tools;

enum CompareResult
{
  COMPARE_SUCCESS,
  COMPARE_FAIL,
  COMPARE_DIFF,
  COMPARE_IGNORE
};

static const int32_t OP_FILE = 0;
static const int32_t OP_BLOCK = 1;
static bool compare_finfo(const FileInfoV2& left, const FileInfoV2& right)
{
  return left.status_ == right.status_ && left.crc_ == right.crc_ && left.size_ == right.size_;
}

static CompareResult compare_crc_by_filename(const std::string& left_ns_addr, const std::string& right_ns_addr,
  const std::string& filename, const int64_t modify_time, const bool force)
{
  CompareResult cmp_result = COMPARE_SUCCESS;
  FileInfoV2 left, right;
  memset(&left, 0, sizeof(left));
  memset(&right, 0, sizeof(right));
  int32_t left_ret = TFS_SUCCESS;
  int32_t right_ret = TFS_SUCCESS;
  left_ret  = Util::read_file_info_v2(left_ns_addr, filename, left);
  if (TFS_SUCCESS == left_ret)
  {
    if ((left.modify_time_ >= modify_time) || (left.status_ & FILE_STATUS_DELETE))// 删除文件不需要transfer迁移
    {
      cmp_result = COMPARE_IGNORE;
    }
    else // no modified file should keep consistent completely
    {
      right_ret = Util::read_file_info_v2(right_ns_addr, filename, right);
      if (TFS_SUCCESS == right_ret)
      {
        if ( !compare_finfo(left, right) )
        {
          cmp_result = COMPARE_DIFF;
          TBSYS_LOG(WARN, "DIFF: filename: %s, fileid: %"PRI64_PREFIX"u, left_status: %d,"
            "right_status: %d, left_crc: %u, right_crc: %u, left_size : %d, right_size: %d",
            filename.c_str(), left.id_, left.status_, right.status_, left.crc_, right.crc_, left.size_, right.size_);
        }
        else if (force)
        {
          uint32_t left_crc = 0;
          uint32_t right_crc = 0;
          left_ret  = Util::read_file_real_crc_v2(left_ns_addr, filename, left_crc, true);// 可能是FILE_STATUS_CONCEAL
          right_ret = Util::read_file_real_crc_v2(right_ns_addr, filename, right_crc, true);
          if (left_ret != TFS_SUCCESS || right_ret != TFS_SUCCESS)
          {
            cmp_result = COMPARE_FAIL;
            TBSYS_LOG(WARN, "read real crc fail, fatal error, filename: %s, left_ret: %d, right_ret: %d", filename.c_str(), left_ret, right_ret);
          }
          else if (left_crc != right_crc)
          {
            cmp_result = COMPARE_DIFF;
            TBSYS_LOG(WARN, "DIFF: filename: %s, fileid: %"PRI64_PREFIX"u, left_real_crc: %u, right_real_crc: %u, left_status: %d,"
              "right_status: %d, left_crc : %u, right_crc: %u, left_size : %d, right_size: %d", filename.c_str(), left.id_, left_crc, right_crc,
              left.status_, right.status_, left.crc_, right.crc_, left.size_, right.size_);
          }//else COMPARE_SUCCESS
        }//else COMPARE_SUCCESS
      }
      else if (EXIT_META_NOT_FOUND_ERROR == right_ret)
      {
        cmp_result = COMPARE_DIFF;
        TBSYS_LOG(WARN, "MORE: filename: %s, fileid: %"PRI64_PREFIX"u, left_status: %d, left_crc: %u, left_size: %d",
          filename.c_str(), left.id_, left.status_, left.crc_, left.size_);
      }
      else
      {
        cmp_result = COMPARE_FAIL;
        TBSYS_LOG(WARN, "read right file fail, filename: %s, right_ret: %d", filename.c_str(), right_ret);
      }
    }
  }
  else if (EXIT_META_NOT_FOUND_ERROR == left_ret)
  {
    cmp_result = COMPARE_IGNORE;// right can stay any status
  }
  else
  {
    cmp_result = COMPARE_FAIL;
    TBSYS_LOG(WARN, "read left file fail, filename: %s, left_ret: %d", filename.c_str(), left_ret);
  }

  return cmp_result;
}


static CompareResult compare_crc_by_block_id(const string& left_ns_addr, const string& right_ns_addr,
  const uint64_t block_id, const int64_t modify_time, const bool force)
{
  CompareResult cmp_result = COMPARE_SUCCESS;
  std::vector<FileInfoV2> left, right;
  int left_ret, right_ret;

  left_ret = ToolUtil::read_file_infos_v2(Func::get_host_ip(left_ns_addr.c_str()), block_id, left);
  right_ret = ToolUtil::read_file_infos_v2(Func::get_host_ip(right_ns_addr.c_str()), block_id, right);
  if (TFS_SUCCESS != left_ret || TFS_SUCCESS != right_ret)
  {
    if (TFS_SUCCESS == left_ret && EXIT_BLOCK_NOT_FOUND == right_ret) // 不对称, block_list来自left集群, left肯定是transfer迁移的源集群吧
    {
      if(left.size() > 0)
      {
        vector<FileInfoV2>::iterator left_iter = left.begin();
        for (; left_iter != left.end(); ++left_iter)  // compare every file in left
        {
          if ( (*left_iter).modify_time_ >= modify_time || ((*left_iter).status_ & FILE_STATUS_DELETE) ) // ignore modified(new) file after transfer and deleted file
          {
            continue;
          }
          else
          {
            cmp_result = COMPARE_DIFF;
            TBSYS_LOG(WARN, "MORE: blockid: %lu, fileid: %lu, left_status: %d, left_crc : %u", block_id, left_iter->id_, left_iter->status_, left_iter->crc_);
          }
        }
        if (cmp_result != COMPARE_DIFF)
        {
          cmp_result = COMPARE_IGNORE;
          TBSYS_LOG(WARN, "MORE: blockid: %lu, left files size: %zd, ignore the block", block_id, left.size());
        }
      }//else COMPARE_SUCCESS
    }
    else
    {
      cmp_result = COMPARE_FAIL;
      TBSYS_LOG(WARN, "get file infos erorr, blockd: %"PRI64_PREFIX"u, left_ret: %d, right_ret: %d", block_id, left_ret, right_ret);
    }
  }
  else // both get file infos success
  {
    set<FileInfoV2, CompareFileInfoV2ByFileId > right_finfos;
    right_finfos.insert(right.begin(), right.end());

    uint32_t ignore_count = 0;
    vector<FileInfoV2>::iterator left_iter = left.begin();
    for (; left_iter != left.end(); ++left_iter)  // compare every file in left
    {
      if ( (*left_iter).modify_time_ >= modify_time || ((*left_iter).status_ & FILE_STATUS_DELETE) ) // ignore modified file after transfer and deleted file
      {
        ++ignore_count;
        right_finfos.erase((*left_iter));
      }
      else // need to compare
      {
        set<FileInfoV2, CompareFileInfoV2ByFileId>::iterator right_iter;
        right_iter = right_finfos.find((*left_iter));
        if (right_iter != right_finfos.end())
        {
          // no modified file should keep consistent completely
          if ( !compare_finfo((*left_iter), (*right_iter)) )
          {
            cmp_result = COMPARE_DIFF;
            TBSYS_LOG(WARN, "DIFF: blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, left_status: %d, "
              "right_status: %d, left_crc: %u, right_crc: %u, left_size : %d, right_size: %d",
              block_id, (*left_iter).id_, left_iter->status_, right_iter->status_, left_iter->crc_, right_iter->crc_, left_iter->size_, right_iter->size_);
          }
          else if (force)
          {
            uint32_t left_crc = 0;
            uint32_t right_crc = 0;
            FSName name(block_id, (*left_iter).id_);
            left_ret  = Util::read_file_real_crc_v2(left_ns_addr, name.get_name(), left_crc, true);// FILE_STATUS_CONCEAL
            right_ret = Util::read_file_real_crc_v2(right_ns_addr, name.get_name(), right_crc, true);
            if (left_ret != TFS_SUCCESS || right_ret != TFS_SUCCESS)
            {
              cmp_result = COMPARE_FAIL;
              TBSYS_LOG(WARN, "read file fail, fatal erorr, blockid: %"PRI64_PREFIX"u, fileid: %lu, left_ret: %d, right_ret: %d",
                  block_id, (*left_iter).id_, left_ret, right_ret);
              break;// stop compare, fail quit
            }
            else if (left_crc != right_crc)
            {
              cmp_result = COMPARE_DIFF;
              TBSYS_LOG(WARN, "DIFF: blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, left_real_crc: %u, right_real_crc: %u, left_status: %d,"
                "right_status: %d, left_crc : %u, right_crc: %u, left_size : %d, right_size: %d", block_id, (*left_iter).id_, left_crc, right_crc,
                left_iter->status_, right_iter->status_, left_iter->crc_, right_iter->crc_, left_iter->size_, right_iter->size_);
            }//else COMPARE_SUCCESS
          }
          //else COMPARE_SUCCESS

          right_finfos.erase(right_iter);
        }
        else
        {
          cmp_result = COMPARE_DIFF;
          TBSYS_LOG(WARN, "MORE: blockid: %lu, fileid: %lu, left_status: %d, left_crc : %u", block_id, left_iter->id_, left_iter->status_, left_iter->crc_);
        }
      }
    }// end for
    if (ignore_count == left.size()) // all files of block_id are ignored
    {
      cmp_result = COMPARE_IGNORE;
    }

    // some left files are compact after transfer and we can't get their fileinfos, so the right is not empty after compare
    if (COMPARE_FAIL != cmp_result)// COMPARE_DIFF or COMPARE_SUCCESS or COMPARE_IGNORE
    {
      set<FileInfoV2, CompareFileInfoV2ByFileId>::iterator it;
      it = right_finfos.begin();
      for ( ; it != right_finfos.end(); it++)
      {
        TBSYS_LOG(WARN, "LESS: blockid: %lu, fileid: %lu, right_status: %d, right_crc : %u",
            block_id, it->id_, it->status_, it->crc_);
      }
    }
  }

  return cmp_result;
}

class WorkThread: public tbutil::Thread
{
 public:
   WorkThread(const int32_t type, const std::string& left_ns_addr, const std::string& right_ns_addr,
      const int64_t modify_time, std::vector<std::string>& inputs, FILE* success_fp, FILE* fail_fp, FILE* diff_fp, FILE* ignore_fp, const bool force, const int32_t sleep_ms):
      type_(type),
      left_ns_addr_(left_ns_addr),
      right_ns_addr_(right_ns_addr),
      modify_time_(modify_time),
      inputs_(inputs),
      success_fp_(success_fp),
      fail_fp_(fail_fp),
      diff_fp_(diff_fp),
      ignore_fp_(ignore_fp),
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
          else if (COMPARE_DIFF == result)
          {
            fp = diff_fp_;
          }
          else
          {
            fp = ignore_fp_;
          }
          fprintf(fp, "%s\n", (*iter).c_str());
          fflush(fp);
          usleep(sleep_ms_ * 1000);
        }
      }

      if (type_ == OP_BLOCK)
      {
        uint64_t block_id = 0;
        for (; iter != inputs_.end() && !stop_; ++iter)
        {
          block_id = atoll((*iter).c_str());
          CompareResult result = compare_crc_by_block_id(left_ns_addr_, right_ns_addr_, block_id, modify_time_, force_);
          FILE* fp = NULL;
          if (COMPARE_SUCCESS == result)
          {
            fp = success_fp_;
          }
          else if (COMPARE_FAIL == result)
          {
            fp = fail_fp_;
          }
          else if (COMPARE_DIFF == result)
          {
            fp = diff_fp_;
          }
          else
          {
            fp = ignore_fp_;
          }

          fprintf(fp, "%s\n", (*iter).c_str());
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
  FILE* ignore_fp_;
  bool force_;
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
  fprintf(stderr,"Usage:\n%s -o old_ns_ip:port -n new_ns_ip:port -f input -k type -c "
      "-m modify_time(20110315183500) -t thread_num -s interval(ms) "
      "-r ouput_dir -l log_level -d(daemon) [-h] [-v]\n", name);
  fprintf(stderr, " -k type\n");
  fprintf(stderr, "    0: compare by file\n");
  fprintf(stderr, "    1: compare by block\n");
  fprintf(stderr, " -r output direcotry, default cmp_log\n");
  fprintf(stderr, "    all the output file and log file will be in this dir\n");
  fprintf(stderr, " -c\n");
  fprintf(stderr, "    if turn this flag on, data crc will be compared\n");
  fprintf(stderr, " receive signal\n");
  fprintf(stderr, "     USR1: increase sleep interval to double\n");
  fprintf(stderr, "     USR1: decrease sleep interval to half\n");

  exit(-1);
}

void version(const char* app_name)
{
  fprintf(stderr, "%s %s\n", app_name, Version::get_build_description());
  exit(0);
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
    while(p >= s && (*p == ' ' || *p == '\n' || *s == '\t'))//fgets line include '\n'
      --p;
    *(p+1) = '\0';
  }
  return s;
}

int main(int argc, char** argv)
{
  int ret = TFS_SUCCESS;
  int32_t i = 0;
  // default one thread
  bool need_real_read_check_crc = false;
  int64_t modify_time = 0;
  int32_t sleep_ms = 0;
  int32_t op_type = OP_FILE;
  bool daemon_flag = false;
  std::string old_ns_addr(""), new_ns_addr(""), file_path(""), work_dir(""), log_level("info");
  while ((i = getopt(argc, argv, "o:n:f:m:t:k:s:l:r:dchv")) != EOF)
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
        need_real_read_check_crc  = true;
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
      case 'v':
        version(argv[0]);
        break;
      case 'h':
      default:
        usage(argv[0]);
    }
  }
  if (old_ns_addr == ""
      || new_ns_addr == ""
      || file_path == ""
      || modify_time <= 0
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
  FILE* diff_fp = fopen(path, "a");// append to one file for simplicity
  memset(path, 0, 256);
  snprintf(path, 256, "%s%s", log_path, "/ignore");
  FILE* ignore_fp = fopen(path, "w");
  ret = (NULL == success_fp || NULL == fail_fp || NULL == diff_fp || NULL == ignore_fp) ? EXIT_OPEN_FILE_ERROR : TFS_SUCCESS;
  std::vector<std::string> params[thread_num];
  if (TFS_SUCCESS == ret)
  {
    FILE* fp = fopen(file_path.c_str(), "r");
    if (NULL == fp)
    {
      ret = EXIT_OPEN_FILE_ERROR;
      fprintf(stderr, "input file: %s open failed\n", file_path.c_str());
    }
    if (TFS_SUCCESS == ret)
    {
      char line[256];
      srandom((unsigned int)time(NULL));
      while (NULL != fgets(line, 256, fp))
      {
        int32_t index = random() % thread_num;
        char* str = trim_line(line);// filename需要保证没有多余无用符号
        if (strlen(str) > 0 )
        {
          params[index].push_back(str);
        }
      }
    }
    if (NULL != fp)
      ::fclose(fp);
  }

  if (TFS_SUCCESS == ret)
  {
    ret = TfsClientImplV2::Instance()->initialize(NULL);
  }

  if (TFS_SUCCESS == ret)
  {
    int32_t index = 0;
    work_threads = new WorkThreadPtr[thread_num];
    for (index = 0; index < thread_num; ++index)
    {
      work_threads[index] = new WorkThread(op_type, old_ns_addr, new_ns_addr, modify_time, params[index], success_fp, fail_fp, diff_fp, ignore_fp,
          need_real_read_check_crc, sleep_ms);
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
    TfsClientImplV2::Instance()->destroy();
  }

  if (NULL != success_fp)
    ::fclose(success_fp);
  if (NULL != fail_fp)
    ::fclose(fail_fp);
  if (NULL != diff_fp)
    ::fclose(diff_fp);

  return ret;
}

