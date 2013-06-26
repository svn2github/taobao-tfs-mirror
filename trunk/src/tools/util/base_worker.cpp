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
#include "new_client/tfs_client_impl.h"
#include "new_client/fsname.h"

#include "base_worker.h"

using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::client;
using namespace std;

namespace tfs
{
  namespace tools
  {
    static int32_t g_thread_count = 1;
    static BaseWorkerPtr* g_workers = NULL;

    BaseWorkerManager::BaseWorkerManager():
      cluster_id_(0), timestamp_(0), interval_ms_(0),
      succ_fp_(NULL), fail_fp_(NULL)
    {
    }

    BaseWorkerManager::~BaseWorkerManager()
    {
    }

    void BaseWorkerManager::usage(const char* app_name)
    {
      char *options =
        "-s           source server ip:port\n"
        "-d           dest server ip:port, optional\n"
        "-f           input file name\n"
        "-m           timestamp eg: 20130610, optional, default 0\n"
        "-c           cluster id eg: 1, default 0\n"
        "-i           sleep interval (ms), optional, default 0\n"
        "-o           output directory, optional, default ./output\n"
        "-x           extend arg, optional, default empty\n"
        "-t           thread count, optional, defaul 1\n"
        "-l           log level, optional, default info\n"
        "-v           print version information\n"
        "-h           print help information\n"
        "signal       SIGUSR1 inc sleep interval 1000ms\n"
        "             SIGUSR2 dec sleep interval 1000ms\n";
      fprintf(stderr, "%s usage:\n%s", app_name, options);
      exit(-1);
    }

    void BaseWorkerManager::version(const char* app_name)
    {
      fprintf(stderr, "%s %s\n", app_name, Version::get_build_description());
      exit(0);
    }

    void BaseWorkerManager::handle_signal(int signal)
    {
      TBSYS_LOG(INFO, "receive signal: %d", signal);
      bool destory_flag = false;
      bool reload_flag = 0;
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

      if (g_workers != NULL)
      {
        for (int32_t i = 0; i < g_thread_count; ++i)
        {
          if (g_workers[i] != 0)
          {
            if (destory_flag)
            {
              g_workers[i]->destroy();
            }
            else if (0 != reload_flag)
            {
              g_workers[i]->reload(reload_flag);
            }
          }
        }
      }
    }

    int BaseWorkerManager::main(int argc, char* argv[])
    {
      int ret = TFS_SUCCESS;
      string timestamp;
      output_dir_ = "./output";  // default output directory
      int flag = 0;

      while ((flag = getopt(argc, argv, "s:d:m:f:o:c:x:t:l:i:hv")) != EOF)
      {
        switch (flag)
        {
          case 's':
            src_addr_ = optarg;
            break;
          case 'd':
            dest_addr_ = optarg;
            break;
          case 'f':
            input_file_ = optarg;
            break;
          case 'm':
            timestamp = string(optarg) + string("000000");
            break;
          case 'i':
            interval_ms_ = atoi(optarg);
            break;
          case 'c':
            cluster_id_ = atoi(optarg);
            break;
          case 'o':
            output_dir_ = optarg;
            break;
          case 'x':
            extra_arg_ = optarg;
            break;
          case 'l':
            log_level_ = optarg;
            break;
          case 't':
            g_thread_count = atoi(optarg);
            break;
          case 'v':
            version(argv[0]);
            break;
          case 'h':
          default:
            usage(argv[0]);
            break;
        }
      }

      // source addr & input file must not be empty
      if ((src_addr_.length() == 0) || (input_file_.length() == 0))
      {
        usage(argv[0]);  // will exit
      }
      timestamp_ = tbsys::CTimeUtil::strToTime(const_cast<char*>(timestamp.c_str()));

      string log_dir = output_dir_ + string("/logs/");
      DirectoryOp::create_directory(output_dir_.c_str());
      DirectoryOp::create_directory(log_dir.c_str());
      string log_file = log_dir + string(basename(argv[0])) + string(".log");

      if (log_file.size() != 0)
      {
        TBSYS_LOGGER.rotateLog(log_file.c_str());
      }
      TBSYS_LOGGER.setMaxFileSize(1024 * 1024 * 1024);
      TBSYS_LOGGER.setLogLevel(log_level_.c_str());

      begin(); // callback begin function

      string succ_path = output_dir_ + "/success";
      string fail_path = output_dir_ + "/fail";

      succ_fp_ = fopen(succ_path.c_str(), "w");
      fail_fp_ = fopen(fail_path.c_str(), "w");
      ret = ((NULL != succ_fp_) && (NULL != fail_fp_)) ? TFS_SUCCESS : TFS_ERROR;

      vector<string> files;
      if (TFS_SUCCESS == ret)
      {
        FILE* fp = fopen(input_file_.c_str(), "r");
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

      signal(SIGPIPE, SIG_IGN);
      signal(SIGHUP, SIG_IGN);
      signal(SIGINT, handle_signal);
      signal(SIGTERM, handle_signal);
      signal(SIGUSR1, handle_signal);
      signal(SIGUSR2, handle_signal);

      MessageFactory* factory = new (std::nothrow) MessageFactory();
      assert(NULL != factory);
      BasePacketStreamer* streamer = new (std::nothrow) BasePacketStreamer(factory);
      assert(NULL != streamer);

      ret = NewClientManager::get_instance().initialize(factory, streamer);
      if (TFS_SUCCESS == ret)
      {
        g_workers = new BaseWorkerPtr[g_thread_count];
        for (int index = 0; index < g_thread_count; ++index)
        {
          g_workers[index] = create_worker();
          g_workers[index]->set_src_addr(src_addr_);
          g_workers[index]->set_dest_addr(dest_addr_);
          g_workers[index]->set_extra_arg(extra_arg_);
          g_workers[index]->set_cluster_id(cluster_id_);
          g_workers[index]->set_timestamp(timestamp_);
          g_workers[index]->set_interval_ms(interval_ms_);
          g_workers[index]->set_succ_fp(succ_fp_);
          g_workers[index]->set_fail_fp(fail_fp_);
        }

        // dispatch input file to threads
        for (uint32_t index = 0; index < files.size(); index++)
        {
          g_workers[index%g_thread_count]->add(files[index]);
        }

        for (int index = 0; index < g_thread_count; index++)
        {
          g_workers[index]->start();
        }

        for (int index = 0; index < g_thread_count; ++index)
        {
          g_workers[index]->join();
        }

        tbsys::gDeleteA(g_workers);
      }

      if (NULL != succ_fp_)
      {
        fclose(succ_fp_);
      }

      if (NULL != fail_fp_)
      {
        fclose(fail_fp_);
      }

      end(); // callback begin function

      NewClientManager::get_instance().destroy();

      tbsys::gDelete(streamer);
      tbsys::gDelete(factory);

      return ret;
    }
  }
}
