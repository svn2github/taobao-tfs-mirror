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
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   zongdai <zongdai@taobao.com>
 *      - modify 2010-04-23
 *
 */
#include <iostream>
#include <string>
#include "dataserver.h"
#include "version.h"
#include "common/directory_op.h"
#include "common/error_msg.h"
#include <Memory.hpp>

using namespace std;
using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::dataserver;

static void data_server_signal_handler(int sig);
static DataServer* s_data_server = NULL;

int main(int argc, char *argv[])
{
  std::string file_name, server_index;
  int32_t is_daemon = 0, help = 0;
  int i;

  while ((i = getopt(argc, argv, "f:i:dvh")) != EOF)
  {
    switch (i)
    {
    case 'f':
      file_name = optarg;
      break;
    case 'i':
      server_index = optarg;
      break;
    case 'd':
      is_daemon = 1;
      break;
    case 'v':
      cout << "dataserver: " << Version::get_build_description() << endl;
      return TFS_SUCCESS;
    case 'h':
    default:
      help = 1;
    break;
    }
  }

  if (file_name.size() == 0 || server_index.size() == 0 || help)
  {
    cout << "Usage:" << argv[0] << " -f conf_file -d -h -v" << endl;
    cout << "-f conf_file configurefile" << endl;
    cout << "-i server_index  dataserver index number" << endl;
    cout << "-d run background" << endl;
    cout << "-v verison" << endl;
    cout << "-h help" << endl;
    return TFS_SUCCESS;
  }

  int ret = TFS_ERROR;
  if ((ret = SysParam::instance().load_data_server(file_name, server_index)) != TFS_SUCCESS)
  {
    cerr << "SysParam::load data server failed:" << file_name << endl;
    return ret;
  }

  //set log level
  TBSYS_LOGGER.setLogLevel(CONFIG.get_string_value(CONFIG_PUBLIC, CONF_LOG_LEVEL));
  TBSYS_LOGGER.setMaxFileSize(CONFIG.get_int_value(CONFIG_PUBLIC, CONF_LOG_SIZE, 1024 * 1024 * 1024));
  TBSYS_LOGGER.setMaxFileIndex(CONFIG.get_int_value(CONFIG_PUBLIC, CONF_LOG_NUM, 10));

  //check directory
  string work_dir = SYSPARAM_DATASERVER.work_dir_;
  if (work_dir.size() == 0)
  {
    TBSYS_LOG(ERROR, "Directory %s.%s is undefined\n", CONFIG_PUBLIC, CONF_WORK_DIR);
    return EXIT_CONFIG_ERROR;
  }

  string pid_file = SYSPARAM_DATASERVER.pid_file_;
  //check pid
  int pid = 0;
  if ((pid = tbsys::CProcess::existPid(pid_file.c_str())))
  {
    TBSYS_LOG(ERROR, "dataserver %s has already run. Pid: %d", server_index.c_str(), pid);
    return EXIT_SYSTEM_ERROR;
  }
  if (!DirectoryOp::create_full_path(pid_file.c_str(), true))
  {
    TBSYS_LOG(ERROR, "create file(%s)'s directory failed", pid_file.c_str());
    return EXIT_GENERAL_ERROR;
  }

  string log_file = SYSPARAM_DATASERVER.log_file_;
  if (log_file.size() != 0 && access(log_file.c_str(), R_OK) == 0)
  {
    TBSYS_LOGGER.rotateLog(log_file.c_str());
  }
  else if (!DirectoryOp::create_full_path(log_file.c_str(), true))
  {
    TBSYS_LOG(ERROR, "create file(%s)'s directory failed", log_file.c_str());
    return EXIT_GENERAL_ERROR;
  }

  string storage_dir;
  storage_dir.assign(work_dir);
  storage_dir.append("/storage");
  if (!DirectoryOp::create_full_path(storage_dir.c_str()))
  {
    TBSYS_LOG(ERROR, "create directory(%s) failed", storage_dir.c_str());
    return EXIT_GENERAL_ERROR;
  }

  storage_dir.assign(work_dir);
  storage_dir.append("/tmp");

  if (!DirectoryOp::create_full_path(storage_dir.c_str()))
  {
    TBSYS_LOG(ERROR, "create directory(%s) failed", storage_dir.c_str());
    return EXIT_GENERAL_ERROR;
  }

  storage_dir.assign(work_dir);
  storage_dir.append("/mirror");
  if (!DirectoryOp::create_full_path(storage_dir.c_str()))
  {
    TBSYS_LOG(ERROR, "create directory(%s) failed", storage_dir.c_str());
    return EXIT_GENERAL_ERROR;
  }

  //start daemon
  pid = 0;
  if (is_daemon)
  {
    pid = tbsys::CProcess::startDaemon(pid_file.c_str(), log_file.c_str());
  }

  //child
  if (pid == 0)
  {
    signal(SIGPIPE, SIG_IGN);
    signal(SIGHUP, SIG_IGN);
    signal(SIGINT, data_server_signal_handler);
    signal(SIGTERM, data_server_signal_handler);
    signal(40, data_server_signal_handler);

    s_data_server = new DataServer(server_index);
    ret = s_data_server->start();
    unlink(pid_file.c_str());
    tbsys::gDelete(s_data_server);
    return ret;
  }

  return TFS_SUCCESS;
}

static void data_server_signal_handler(int sig)
{
  switch (sig)
  {
    case SIGHUP:
    case SIGTERM:
    case SIGINT:
      s_data_server->stop();
      break;
    case SIGSEGV:
    case SIGABRT:
      TBSYS_LOG(ERROR, "error msg: %s", (sig == SIGSEGV ? "*SIGSEGV*" : "*SIGABRT*"));
      break;
    case 40:
      TBSYS_LOGGER.checkFile();
      break;
  }
}

namespace tfs
{
  namespace dataserver
  {
    // DataServer
    DataServer::DataServer() :
      server_index_(0)
    {
    }

    DataServer::DataServer(string server_index) :
      server_index_(server_index)
    {
    }

    DataServer::~DataServer()
    {
    }

    string DataServer::get_server_index()
    {
      return server_index_;
    }

    void DataServer::stop()
    {
      tran_sport_.stop();
      data_service_.stop();
    }

    int DataServer::start()
    {
      packet_streamer_.set_packet_factory(&msg_factory_);
      CLIENT_POOL.init_with_transport(&tran_sport_);

      data_service_.init(server_index_);
      VINT pids;
      //init data service
      if (data_service_.start(&pids) != TFS_SUCCESS)
      {
        return TFS_ERROR;
      }

      //start transport
      int32_t server_port = SYSPARAM_DATASERVER.local_ds_port_;
      char spec[SPEC_LEN];
      bool ret = true;

      sprintf(spec, "tcp::%d", server_port);
      if (tran_sport_.listen(spec, &packet_streamer_, &data_service_) == NULL)
      {
        TBSYS_LOG(ERROR, "Failed to listen port: %d", server_port);
        ret = false;
      }

      ++server_port;
      sprintf(spec, "tcp::%d", server_port);
      if (ret && tran_sport_.listen(spec, &packet_streamer_, &data_service_) == NULL)
      {
        TBSYS_LOG(ERROR, "Failed to listen port: %d", server_port);
        ret = false;
      }
      if (ret == false)
      {
        data_service_.stop();
        return TFS_ERROR;
      }

      tran_sport_.start();
      TBSYS_LOG(INFO, "========== DataServer Start Run ========== PID: %d, Listen Port: %d %d", getpid(), server_port - 1,
          server_port);

      //wait
      tran_sport_.wait();
      for (uint32_t i = 0; i < pids.size(); ++i)
      {
        pthread_join(pids[i], NULL);
      }
      data_service_.wait();
      TBSYS_LOG(INFO, "Process exit normally.");
      return TFS_SUCCESS;
    }

  }
}
