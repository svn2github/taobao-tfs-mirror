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

using namespace std;
using namespace tfs::common;
using namespace tfs::client;
using namespace tfs::message;
using namespace tfs::tools;

static const int32_t META_FLAG_ABNORMAL = -9800;
static const int32_t FILE_STATUS_ERROR  = -9801;
static const int32_t OP_FILE = 0;
static const int32_t OP_BLOCK = 1;
struct CompareFileInfoByFileId
{
  bool operator()(const FileInfo& left, const FileInfo& right) const
  {
    return left.id_ < right.id_;
  }
};

static int read_file_infos(const std::string& ns_addr, const uint64_t block, std::set<FileInfo, CompareFileInfoByFileId>& files, const int32_t version)
{
  int32_t ret = (INVALID_BLOCK_ID == block) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
  if (TFS_SUCCESS == ret)
  {
    std::vector<uint64_t> servers;
    ToolUtil::get_block_ds_list(Func::get_host_ip(ns_addr.c_str()), block, servers);
    ret = servers.empty() ? EXIT_DATASERVER_NOT_FOUND : TFS_SUCCESS;
    if (TFS_SUCCESS == ret)
    {
      int32_t index = random() % servers.size();
      uint64_t server = servers[index];
      GetServerStatusMessage req_msg;
      req_msg.set_status_type(GSS_BLOCK_FILE_INFO);
      req_msg.set_return_row(block);
      req_msg.set_from_row(block);
      tbnet::Packet* ret_msg= NULL;
      NewClient* client = NewClientManager::get_instance().create_client();
      ret = send_msg_to_server(server, client, &req_msg, ret_msg);
      if (TFS_SUCCESS == ret)
      {
        if (0 == version)
          ret = ret_msg->getPCode() == BLOCK_FILE_INFO_MESSAGE ? TFS_SUCCESS : EXIT_UNKNOWN_MSGTYPE;
        else if (1 == version)
          ret = ret_msg->getPCode() == BLOCK_FILE_INFO_MESSAGE_V2 ? TFS_SUCCESS : EXIT_UNKNOWN_MSGTYPE;
        else
          ret = EXIT_UNKNOWN_MSGTYPE;
        if (TFS_SUCCESS != ret)
        {
          ret = ret_msg->getPCode() == STATUS_MESSAGE ? EXIT_FILE_INFO_ERROR : EXIT_UNKNOWN_MSGTYPE;
          if (EXIT_FILE_INFO_ERROR == ret)
          {
            StatusMessage* st = dynamic_cast<StatusMessage*>(ret_msg);
            TBSYS_LOG(WARN, "read file infos error: %s", st->get_error());
          }
        }
      }
      if (TFS_SUCCESS == ret)
      {
        if (0 == version)
        {
          BlockFileInfoMessage* bfm = dynamic_cast<BlockFileInfoMessage*>(ret_msg);
          assert(NULL != bfm);
          std::vector<FileInfo>::iterator iter = bfm->get_fileinfo_list().begin();
          for (; iter != bfm->get_fileinfo_list().end(); ++iter)
            files.insert((*iter));
        }
        else if (1 == version)
        {
          BlockFileInfoMessageV2* bfm = dynamic_cast<BlockFileInfoMessageV2*>(ret_msg);
          assert(NULL != bfm);
          std::vector<FileInfoV2>::iterator iter = bfm->get_fileinfo_list()->begin();
          for (; iter != bfm->get_fileinfo_list()->end(); ++iter)
          {
            FileInfo info;
            info.id_ = (*iter).id_;
            info.offset_ = (*iter).offset_;
            info.size_= (*iter).size_;
            info.modify_time_= (*iter).modify_time_;
            info.create_time_= (*iter).create_time_;
            info.flag_ = (*iter).status_;
            info.crc_  = (*iter).crc_;
            files.insert(info);
          }
        }
      }
      NewClientManager::get_instance().destroy_client(client);
    }
  }
  return ret;
}

static int read_file_info(const std::string& ns_addr, const std::string& filename, FileInfo& info)
{
  int32_t fd = TfsClientImpl::Instance()->open(filename.c_str(), NULL, ns_addr.c_str(), T_READ);
  int32_t ret = fd < 0 ? fd : TFS_SUCCESS;
  if (TFS_SUCCESS == ret)
  {
    TfsFileStat stat;
    ret = TfsClientImpl::Instance()->fstat(fd, &stat, FORCE_STAT);
    if (TFS_SUCCESS == ret)
    {
      info.id_ = stat.file_id_;
      info.offset_ = stat.offset_;
      info.size_ = stat.size_;
      info.usize_ = stat.usize_;
      info.modify_time_ = stat.modify_time_;
      info.create_time_ = stat.create_time_;
      info.flag_ = stat.flag_;
      info.crc_ = stat.crc_;
      ret = (stat.flag_ == 1 || stat.flag_ == 4 || stat.flag_ == 5) ? META_FLAG_ABNORMAL : TFS_SUCCESS;
    }
    TfsClientImpl::Instance()->close(fd);
  }
  return ret;
}

static int read_file_real_crc(const std::string& ns_addr, const std::string& filename, uint32_t& crc)
{
  crc = 0;
  int32_t fd = TfsClientImpl::Instance()->open(filename.c_str(), NULL, ns_addr.c_str(), T_READ);
  int32_t ret = fd < 0 ? fd : TFS_SUCCESS;
  if (TFS_SUCCESS == ret)
  {
    int32_t total = 0;
    char data[MAX_READ_SIZE]={'\0'};
    TfsFileStat stat;
    while (true)
    {
      int32_t rlen = TfsClientImpl::Instance()->readv2(fd, data, MAX_READ_SIZE, &stat);
      ret = rlen < 0 ? rlen : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        if (0 == rlen)
          break;
        total += rlen;
        crc = Func::crc(crc, data, rlen);
        //Func::hex_dump(data, 10, true, TBSYS_LOG_LEVEL_INFO);
        //TBSYS_LOG(INFO, "FILENAME : %s, READ LENGTH: %d, crc: %u", filename.c_str(), rlen, crc);
      }
    }
    TfsClientImpl::Instance()->close(fd);
  }
  return ret;
}

static bool compare_crc(const FileInfo& left, const FileInfo& right)
{
  return left.crc_ == right.crc_;
}

static int compare_crc_by_filename(const std::string& left_ns_addr, const std::string& right_ns_addr,
  const std::string& filename, const int64_t modify_time, const int32_t force)
{
  FileInfo left, right;
  memset(&left, 0, sizeof(left));
  memset(&right, 0, sizeof(right));
  int32_t left_ret  = read_file_info(left_ns_addr, filename, left);
  int32_t right_ret = read_file_info(right_ns_addr, filename, right);
  int32_t ret = ((left_ret == TFS_SUCCESS || left_ret == META_FLAG_ABNORMAL) && (right_ret == TFS_SUCCESS || right_ret == META_FLAG_ABNORMAL)) ? TFS_SUCCESS : EXIT_CHECK_CRC_ERROR;
  if (TFS_SUCCESS != ret)
  {
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
          TBSYS_LOG(INFO, "file %s status not consistency, left: %d, right: %d", filename.c_str(), left.flag_, right.flag_);
        }
      }

      if (TFS_SUCCESS == ret && left.flag_ != FILE_STATUS_DELETE && 1 == force)
      {
        uint32_t left_crc = 0;
        uint32_t right_crc = 0;
        left_ret  = read_file_real_crc(left_ns_addr, filename, left_crc);
        right_ret = read_file_real_crc(right_ns_addr, filename, right_crc);
        ret = (left_ret == TFS_SUCCESS && right_ret == TFS_SUCCESS) ? TFS_SUCCESS : EXIT_CHECK_CRC_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = left_crc == right_crc ? TFS_SUCCESS : EXIT_CHECK_CRC_ERROR;
        }
      }
    }
  }
  return ret;
}

int compare_crc_by_block_id(const std::string& left_ns_addr, const std::string& right_ns_addr,
  const uint64_t block, const int64_t modify_time, const int32_t force)
{
  std::set<FileInfo, CompareFileInfoByFileId > left, right;
  int32_t left_ret = read_file_infos(left_ns_addr, block, left, 0);
  int32_t right_ret = read_file_infos(right_ns_addr, block, right, 1);
  int32_t ret = (left_ret == TFS_SUCCESS && right_ret == TFS_SUCCESS) ? TFS_SUCCESS : EXIT_FILE_INFO_ERROR;
  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "get file informations erorr, block: %"PRI64_PREFIX"u, left_ret: %d, right_ret: %d", block, left_ret, right_ret);
  }
  else
  {
    std::set<FileInfo, CompareFileInfoByFileId>::iterator iter = left.begin();
    std::set<FileInfo, CompareFileInfoByFileId>::iterator it;
    for (; iter != left.end() && TFS_SUCCESS == ret; ++iter)
    {
      //TBSYS_LOG(INFO, "BLOCK: %lu, LEFT : %zd, right: %zd, %d == > %ld", block, left.size(), right.size(),(*iter).modify_time_, modify_time);
      if ((*iter).modify_time_ < modify_time)
      {
        //TBSYS_LOG(INFO, "FLAG: %d", (*iter).flag_);
        ret = (*iter).flag_ == FILE_STATUS_DELETE ? TFS_SUCCESS : FILE_STATUS_ERROR;
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
            //TBSYS_LOG(INFO, "compare_crc result: %d", ret);
            if (TFS_SUCCESS == ret && 1 == force)
            {
              uint32_t left_crc = 0;
              uint32_t right_crc = 0;
              FSName name(block, (*iter).id_);
              left_ret  = read_file_real_crc(left_ns_addr, name.get_name(), left_crc);
              right_ret = read_file_real_crc(right_ns_addr, name.get_name(), right_crc);
              //TBSYS_LOG(INFO, "BLOCK: %lu, FILEID: %lu, left_crc : %u, right_crc: %u, left_ret: %d, right_ret: %d", block, (*iter).id_, left_crc, right_crc, left_ret, right_ret);
              ret = (left_ret == TFS_SUCCESS && right_ret == TFS_SUCCESS) ? TFS_SUCCESS : EXIT_CHECK_CRC_ERROR;
              if (TFS_SUCCESS == ret)
              {
                ret = left_crc == right_crc ? TFS_SUCCESS : EXIT_CHECK_CRC_ERROR;
              }
            }
          }
        }
        else
        {
          right.erase((*iter));
        }
      }
    }

    if (TFS_SUCCESS == ret)
    {
      if (!right.empty())
        TBSYS_LOG(WARN, "block : %"PRI64_PREFIX"u, files not consistency, right size: %zd", block, right.size());
    }
  }
  return ret;
}

class WorkThread: public tbutil::Thread
{
 public:
   WorkThread(const int32_t type, const std::string& left_ns_addr, const std::string& right_ns_addr,
      const int64_t modify_time, std::vector<std::string>& inputs, FILE* success_fp, FILE* fail_fp, const int32_t force):
      type_(type),
      left_ns_addr_(left_ns_addr),
      right_ns_addr_(right_ns_addr),
      modify_time_(modify_time),
      inputs_(inputs),
      success_fp_(success_fp),
      fail_fp_(fail_fp),
      force_(force)
   {

   }

   ~WorkThread()
   {

   }

   void run()
   {
      int32_t ret = TFS_SUCCESS;
      std::vector<std::string>::iterator iter = inputs_.begin();
      if (type_ == OP_FILE)
      {
        for (; iter != inputs_.end(); ++iter)
        {
          ret = compare_crc_by_filename(left_ns_addr_, right_ns_addr_, (*iter), modify_time_, force_);
          FILE* fp = TFS_SUCCESS == ret ? success_fp_ : fail_fp_;
          fprintf(fp, "%s", (*iter).c_str());
          fflush(fp);
        }
      }

      if (type_ == OP_BLOCK)
      {
        uint64_t block = 0;
        for (; iter != inputs_.end(); ++iter)
        {
          block = atoll((*iter).c_str());
          ret = compare_crc_by_block_id(left_ns_addr_, right_ns_addr_, block, modify_time_, force_);
          //TBSYS_LOG(INFO, "COMPARE CRC BLOCK : %lu, ret: %d", block, ret);
          FILE* fp = TFS_SUCCESS == ret ? success_fp_ : fail_fp_;
          fprintf(fp, "%s", (*iter).c_str());
          fflush(fp);
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
  int32_t force_;
};
typedef tbutil::Handle<WorkThread> WorkThreadPtr;

void usage(const char* name)
{
  fprintf(stderr,"Usage:\n%s -o old_ns_ip:port -n new_ns_ip:port -f input "\
      "-m modify_time(20110315|20110315183500) -t thread_num -k type -c[-h]\n", name);
  exit(-1);
}

int main(int argc, char** argv)
{
  int32_t thread_num = 1, i = 0, real_read_check_crc = 0; // default one thread
  int64_t modify_time = 0;
  int32_t op_type = OP_FILE;
  std::string old_ns_addr(""), new_ns_addr(""), file_path(""), log_level("info");
  while ((i = getopt(argc, argv, "o:n:f:m:t:k:l:ch")) != EOF)
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
      case 'c':
        real_read_check_crc  = 1;
        break;
      case 'h':
      default:
        usage(argv[0]);
    }
  }
  if (old_ns_addr == ""
      || new_ns_addr == ""
      || file_path == ""
      || modify_time < 0)
  {
    usage(argv[0]);
  }

  TBSYS_LOGGER.setLogLevel(log_level.c_str());
  char log_path[256];
  snprintf(log_path, 256, "%s%s", dirname(argv[0]), "/cmp_log");
  DirectoryOp::create_directory(log_path);
  char path[256];
  snprintf(path, 256, "%s%s", log_path, "/success");
  FILE* success_fp = fopen(path, "w");
  memset(path, 0, 256);
  snprintf(path, 256, "%s%s", log_path, "/fail");
  FILE* fail_fp = fopen(path, "w");
  int32_t ret = (NULL == success_fp || NULL == fail_fp) ? EXIT_OPEN_FILE_ERROR : TFS_SUCCESS;
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
    WorkThreadPtr *work_threads = new WorkThreadPtr[thread_num];
    for (index = 0; index < thread_num; ++index)
    {
      work_threads[index] = new WorkThread(op_type, old_ns_addr, new_ns_addr, modify_time, params[index], success_fp, fail_fp, real_read_check_crc);
      work_threads[index]->start();
    }

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

  //TBSYS_LOG(WARN, "total_count: %"PRI64_PREFIX"d, succ_count: %"PRI64_PREFIX"d, fail_count: %"PRI64_PREFIX"d"
  //    ", error_count: %"PRI64_PREFIX"d, unsync_count: %"PRI64_PREFIX"d, skip_count: %"PRI64_PREFIX"d",
  //    cmp_stat_.total_count_, cmp_stat_.succ_count_, cmp_stat_.fail_count_, cmp_stat_.error_count_, cmp_stat_.unsync_count_,
  //    cmp_stat_.skip_count_);
  return ret;
}

