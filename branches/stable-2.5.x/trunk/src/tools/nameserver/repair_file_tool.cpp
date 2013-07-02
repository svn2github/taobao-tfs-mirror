/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *   linqing <linqing.zyd@taobao.com>
 *      - initial release
 */

#include <stdio.h>
#include <vector>
#include <algorithm>
#include <TbThread.h>
#include "common/directory_op.h"
#include "common/internal.h"
#include "common/error_msg.h"
#include "common/version.h"
#include "common/func.h"
#include "common/base_packet_streamer.h"
#include "common/base_packet.h"
#include "common/client_manager.h"
#include "message/message_factory.h"
#include "tools/util/tool_util.h"
#include "tools/util/ds_lib.h"
#include "new_client/tfs_client_impl.h"
#include "new_client/fsname.h"
#include "dataserver/ds_define.h"

using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::tools;
using namespace tfs::client;
using namespace tfs::dataserver;
using namespace std;

void usage(const char* app_name)
{
  char *options =
    "-s           nameserver ip:port\n"
    "-f           file list\n"
    "-r           randomly choose a replica\n"
    "-i           sleep interval (ms)\n"
    "-o           output directory\n"
    "-t           thread count\n"
    "-l           log level\n"
    "-v           version information\n"
    "-h           help\n";
  fprintf(stderr, "%s usage:\n%s", app_name, options);
  exit(-1);
}

void version()
{
  fprintf(stderr, Version::get_build_description());
  exit(0);
}

class WorkThread: public tbutil::Thread
{
 public:
   WorkThread(const string& ns_addr,
       FILE* success_fp, FILE* fail_fp, const int32_t sleep_ms, const bool random):
     ns_addr_(ns_addr),
     success_fp_(success_fp),
     fail_fp_(fail_fp),
     sleep_ms_(sleep_ms),
     random_(random)
   {

   }

   void add(const string& file)
   {
     input_.push_back(file);
   }

   void run()
   {
     vector<string>::iterator iter = input_.begin();
     for ( ; iter != input_.end(); iter++)
     {
       FSName fsname(iter->c_str());
       uint64_t block_id = fsname.get_block_id();
       uint64_t file_id = fsname.get_file_id();
       VUINT64 replicas;
       map<uint64_t, FileInfo> finfos;
       int ret = ToolUtil::get_block_ds_list(Func::get_host_ip(ns_addr_.c_str()), block_id, replicas);
       if (TFS_SUCCESS == ret)
       {
         ret = read_file_info(replicas, block_id, file_id, finfos);
       }

       if (TFS_SUCCESS == ret)
       {
         map<uint64_t, FileInfo>::iterator fit = choose_replica(finfos, random_);
         ret = repair_file(fit->first, block_id, fit->second, *iter);
       }

       FILE* fp = (TFS_SUCCESS == ret) ? success_fp_ : fail_fp_;
       fprintf(fp, "%s\n", (*iter).c_str());

       usleep(sleep_ms_ * 1000);
     }
   }

 private:

   /*
    * get file info from every replica
    * return value don't needed, we just need finfos
    */
   int read_file_info(const VUINT64& replicas,
       const uint64_t block_id, const uint64_t file_id, map<uint64_t, FileInfo>& finfos)
   {
     finfos.clear();
     int ret = TFS_SUCCESS;
     VUINT64::const_iterator iter = replicas.begin();
     for ( ; iter != replicas.end(); iter++)
     {
       int32_t retry = 2;
       while (retry--)
       {
         FileInfo info;
         ret = ToolUtil::read_file_info(*iter, block_id, file_id, FORCE_STAT, info);
         if (TFS_SUCCESS == ret)
         {
           finfos.insert(make_pair(*iter, info));
           break;
         }
         else if (EXIT_META_NOT_FOUND_ERROR == ret) // file not exist, just ignore
         {
           break;
         }
       }
     }

     return (finfos.size() > 0) ? TFS_SUCCESS : TFS_ERROR;
   }

   /*
    * if random flag set, randomly choose a replica
    * or we will choose the latest replica
    * return iterator of choosen replica
    */
   map<uint64_t, FileInfo>::iterator choose_replica(map<uint64_t, FileInfo>& finfos, const bool random)
   {
     assert(finfos.size() != 0);
     map<uint64_t, FileInfo>::iterator target = finfos.begin();
     if (random)
     {
       int index = rand() % finfos.size();
       while (index--) target++;
     }
     else
     {
       map<uint64_t, FileInfo>::iterator iter = ++target;
       for ( ; iter != finfos.end(); iter++)
       {
         if (iter->second.modify_time_ > target->second.modify_time_)
         {
           target = iter;
         }
       }
     }
     return target;
   }

   int read_file_data(const uint64_t server_id, const uint64_t block_id, const FileInfo& finfo, char* data)
   {
     int ret = TFS_SUCCESS;
     int32_t retry = 2;
     while (retry--)
     {
       int32_t read_len = 0;
       ret = ToolUtil::read_file_data(server_id, block_id, finfo.id_, finfo.size_, 0,
           READ_DATA_OPTION_FLAG_FORCE, data, read_len);
       if (TFS_SUCCESS == ret)
       {
         if (read_len != finfo.size_)
         {
           ret = TFS_ERROR;
           TBSYS_LOG(WARN, "blockid %"PRI64_PREFIX"u fileid %"PRI64_PREFIX"u cannot read enough data from %s",
               block_id, finfo.id_, tbsys::CNetUtil::addrToString(server_id).c_str());
         }
         break;
       }
     }
     return ret;
   }

   int repair_file(const uint64_t server_id, const uint64_t block_id, const FileInfo& finfo, const string& name)
   {
     int ret = TFS_SUCCESS;
     if (!((finfo.flag_ & FI_DELETED) || (finfo.flag_ & FI_INVALID)))
     {
       char tfs_name[TFS_FILE_LEN];
       char* data = new (std::nothrow) char[finfo.size_];
       assert(NULL != data);
       ret = read_file_data(server_id, block_id, finfo, data);
       if (TFS_SUCCESS == ret)
       {
         int64_t save_size = TfsClientImpl::Instance()->save_buf_ex(tfs_name,
             TFS_FILE_LEN, data, finfo.size_, T_WRITE, name.c_str(), NULL, ns_addr_.c_str());
         ret = (save_size == finfo.size_) ? TFS_SUCCESS : TFS_ERROR;
       }
       tbsys::gDelete(data);

       // if it's concealed, conceal it, ignore return value
       if ((TFS_SUCCESS == ret) && (finfo.flag_ & FI_CONCEAL))
       {
         int64_t file_size = 0;
         TfsClientImpl::Instance()->unlink(file_size, tfs_name, NULL, ns_addr_.c_str(), CONCEAL);
       }
     }
     return ret;
   }

 private:
   vector<string> input_;
   string ns_addr_;
   FILE* success_fp_;
   FILE* fail_fp_;
   int32_t sleep_ms_;
   bool random_;
};
typedef tbutil::Handle<WorkThread> WorkThreadPtr;

static int32_t thread_num = 1;
static WorkThreadPtr* work_threads = NULL;

int main(int argc, char* argv[])
{
  int ret = TFS_SUCCESS;
  string ns_addr;
  string log_level = "info";
  string input_file;
  string output_dir = "./repair_log";
  int32_t sleep_ms = 0;
  bool random = false;
  int flag = 0;

  while ((flag = getopt(argc, argv, "s:f:o:t:l:i:rhv")) != EOF)
  {
    switch (flag)
    {
      case 's':
        ns_addr = optarg;
        break;
      case 'f':
        input_file = optarg;
        break;
      case 'r':
        random = true;
        break;
      case 'i':
        sleep_ms = atoi(optarg);
        break;
      case 'o':
        output_dir = optarg;
        break;
      case 't':
        thread_num = atoi(optarg);
        break;
      case 'l':
        log_level = optarg;
        break;
      case 'v':
        version();
        break;
      case 'h':
      default:
        usage(argv[0]);
        break;
    }
  }

  if ((ns_addr.length() == 0) || (input_file.length() == 0))
  {
    usage(argv[0]);  // will exit
  }

  string log_dir = output_dir + string("/logs");
  string success_file = output_dir + string("/success");
  string fail_file = output_dir + string("/fail");
  DirectoryOp::create_directory(output_dir.c_str());
  DirectoryOp::create_directory(log_dir.c_str());

  string log_file = log_dir + string("/repair_file.log");
  if (log_file.size() != 0)
  {
    TBSYS_LOGGER.rotateLog(log_file.c_str());
  }
  TBSYS_LOGGER.setMaxFileSize(1024 * 1024 * 1024);
  TBSYS_LOGGER.setLogLevel(log_level.c_str());

  vector<string> files;
  FILE* success_fp = fopen(success_file.c_str(), "w");
  FILE* fail_fp = fopen(fail_file.c_str(), "w");
  ret = (NULL == success_fp || NULL == fail_fp) ? EXIT_OPEN_FILE_ERROR : TFS_SUCCESS;
  if (TFS_SUCCESS == ret)
  {
    FILE* fp = fopen(input_file.c_str(), "r");
    ret = (NULL == fp) ? EXIT_OPEN_FILE_ERROR : TFS_SUCCESS;
    if (TFS_SUCCESS == ret)
    {
      char line[256];
      while (NULL != fgets(line, 256, fp))
      {
        int32_t len = strlen(line);
        while (line[len-1] == '\n') len--;
        line[len] = '\0';
        files.push_back(line);
      }
      fclose(fp);
    }
  }

  MessageFactory* factory = new (std::nothrow) MessageFactory();
  assert(NULL != factory);
  BasePacketStreamer* streamer = new (std::nothrow) BasePacketStreamer(factory);
  assert(NULL != streamer);

  ret = NewClientManager::get_instance().initialize(factory, streamer);
  if (TFS_SUCCESS == ret)
  {
    ret = TfsClientImpl::Instance()->initialize(NULL,
        DEFAULT_BLOCK_CACHE_TIME, DEFAULT_BLOCK_CACHE_ITEMS, false);
  }

  if (TFS_SUCCESS == ret)
  {
    work_threads = new WorkThreadPtr[thread_num];
    for (int index = 0; index < thread_num; ++index)
    {
      work_threads[index] = new WorkThread(ns_addr, success_fp, fail_fp, sleep_ms, random);
    }

    // dispatch input file to threads
    for (uint32_t index = 0; index < files.size(); index++)
    {
      work_threads[index%thread_num]->add(files[index]);
    }

    for (int index = 0; index < thread_num; index++)
    {
      work_threads[index]->start();
    }

    for (int index = 0; index < thread_num; ++index)
    {
      work_threads[index]->join();
    }

    tbsys::gDeleteA(work_threads);
  }

  if (TFS_SUCCESS == ret)
  {
    fclose(success_fp);
    fclose(fail_fp);
  }

  NewClientManager::get_instance().destroy();

  tbsys::gDelete(streamer);
  tbsys::gDelete(factory);

  return ret;
}
