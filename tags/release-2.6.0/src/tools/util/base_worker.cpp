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
      retry_count_(3), timestamp_(0), interval_ms_(0), type_(-1), force_(false),
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
        "-i           sleep interval (ms), optional, default 0\n"
        "-p           base work directory, optional, default ./\n"
        "-c           retry count when process fail\n"
        "-x           extend arg, optional, default empty\n"
        "-t           thread count, optional, defaul 1\n"
        "-l           log level, optional, default info\n"
        "-u           type, 0: transfer block, 1: sync block, 2: sync file, 3: compare block, optional\n"
        "-k           dest server addr path\n"
        "-e           force flag, need strong consistency(crc), optional\n"
        "-n           deamon, default false\n"
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

    void BaseWorkerManager::handle_signal(int signal)
    {
      TBSYS_LOG(INFO, "receive signal: %d", signal);
      bool destory_flag = false;
      int32_t reload_flag = 0;
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
      bool daemon = false;
      output_dir_ = "./";  // default output directory
      string timestamp = get_day(1, true);//第二天0点
      int flag = 0;

      while ((flag = getopt(argc, argv, "s:d:m:f:p:c:x:t:l:i:u:k:enhv")) != EOF)
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
            timestamp = string(optarg);
            break;
          case 'i':
            interval_ms_ = atoi(optarg);
            break;
          case 'p':
            output_dir_ = optarg;
            break;
          case 'c':
            retry_count_ = atoi(optarg);
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
          case 'u':
            type_ = atoi(optarg);
            break;
          case 'k':
            dest_addr_path_ = optarg;
            break;
          case 'e':
            force_ = true;
            break;
          case 'n':
            daemon = true;
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
      timestamp += "000000";
      timestamp_ = tbsys::CTimeUtil::strToTime(const_cast<char*>(timestamp.c_str()));
      if (0 == timestamp_)
      {
        TBSYS_LOG(ERROR, "timestamp param: %s error", timestamp.c_str());
        return TFS_ERROR;
      }

      string log_dir = output_dir_ + "logs/";
      string data_dir = output_dir_ + "data/";
      DirectoryOp::create_directory(output_dir_.c_str());
      DirectoryOp::create_directory(log_dir.c_str());
      DirectoryOp::create_directory(data_dir.c_str());
      string log_file = log_dir + basename(argv[0]) + ".log";
      string pid_file = log_dir + basename(argv[0]) + ".pid";
      TBSYS_LOGGER.rotateLog(log_file.c_str());
      TBSYS_LOGGER.setMaxFileSize(1024 * 1024 * 1024);
      TBSYS_LOGGER.setLogLevel(log_level_.c_str());

      int pid = 0;
      if (daemon)
      {
        pid = Func::start_daemon(pid_file.c_str(), log_file.c_str());
      }

      if (0 == pid)
      {
        ret = begin(); // callback begin function
        if(TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "some initialization fail in begin stage, ret:%d", ret);
          return ret;
        }

        string succ_path = data_dir + "/success";
        string fail_path = data_dir + "/fail";

        succ_fp_ = fopen(succ_path.c_str(), "a+");
        fail_fp_ = fopen(fail_path.c_str(), "a");
        ret = ((NULL != succ_fp_) && (NULL != fail_fp_)) ? TFS_SUCCESS : TFS_ERROR;

        set<string> done;
        if (TFS_SUCCESS == ret)
        {
          char line[256];
          while (NULL != fgets(line, 256, succ_fp_))
          {
            int32_t len = strlen(line);
            while (line[len-1] == '\n' || line[len-1] == ' ') len--;
            line[len] = '\0';
            done.insert(line);
          }
        }

        vector<string> todo;
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
              while (line[len-1] == '\n' || line[len-1] == ' ') len--;
              line[len] = '\0';
              // only process element not in success list
              if (done.find(line) == done.end())
              {
                todo.push_back(line);
              }
              else
              {
                TBSYS_LOG(INFO, "line %s already done, won't process", line);
              }
            }
            fclose(fp);
          }
          else
          {
            TBSYS_LOG(ERROR, "open input file: %s fail, ret: %d", input_file_.c_str(), ret);
          }
        }

        if (TFS_SUCCESS == ret)
        {
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
              g_workers[index]->set_retry_count(retry_count_);
              g_workers[index]->set_extra_arg(extra_arg_);
              g_workers[index]->set_timestamp(timestamp_);
              g_workers[index]->set_interval_ms(interval_ms_);
              g_workers[index]->set_succ_fp(succ_fp_);
              g_workers[index]->set_fail_fp(fail_fp_);
            }

            // dispatch input file to threads
            for (uint32_t index = 0; index < todo.size(); index++)
            {
              g_workers[index%g_thread_count]->add(todo[index]);
            }

            for (int index = 0; index < g_thread_count; index++)
            {
              g_workers[index]->start();
            }

            for (int index = 0; index < g_thread_count; index++)
            {
              g_workers[index]->join();
            }

            tbsys::gDeleteA(g_workers);
          }

          end(); // callback begin function

          NewClientManager::get_instance().destroy();

          tbsys::gDelete(streamer);
          tbsys::gDelete(factory);
        }

        if (NULL != succ_fp_)
        {
          fclose(succ_fp_);
        }

        if (NULL != fail_fp_)
        {
          fclose(fail_fp_);
        }
      }
      return ret;
    }
  }
}
