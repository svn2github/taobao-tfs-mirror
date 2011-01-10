
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
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 *
 */
#include <stdio.h>
#include <pthread.h>
#include <vector>
#include <string>
#include <queue>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <tbsys.h>
#include <Service.h>

#include "common/config.h"
#include "common/config_item.h"
#include "common/directory_op.h"
#include "common/error_msg.h"
#include "mock_data_server_instance.h"
using namespace tfs::mock;
using namespace tfs::message;
using namespace tfs::common;
using namespace tbnet;
using namespace tbsys;
using namespace tbutil;

namespace tfs
{

namespace mock
{
class MockDataService 
{
public:
  MockDataService(int32_t max_write_file_size);
  virtual ~MockDataService();
  virtual int run(int32_t port, int64_t capacity, const std::string& work_index, const std::string& config);
  virtual bool destroy();
  virtual int wait_for_shut_down();
private:
  MockDataServerInstance instance_;
};

MockDataService::MockDataService(int32_t max_write_file_size):
  instance_(max_write_file_size)
{

}

MockDataService::~MockDataService()
{

}

int MockDataService::run(int32_t port, int64_t capacity, const std::string& work_index, const std::string& conf)
{
 return instance_.initialize(port, capacity,work_index, conf);
}

bool MockDataService::destroy()
{
  TBSYS_LOG(INFO, "%s", "Mock DataServer Stoped");
  instance_.destroy();
  return true;
}

int MockDataService::wait_for_shut_down()
{
  return instance_.wait_for_shut_down();
}

}
}

void helper()
{
  std::string options=
    "Options:\n"
    "-i INDEX           Work directory index...\n"
    "-f FILE            Configure files...\n"
    "-c CAPACITY        Disk capacity(GB)...\n"
    "-s FILE SIZE       write file size(byte)...\n"
    "-d                 Run as a daemon...\n"
    "-h                 Show this message...\n"
    "-v                 Show porgram version...\n";
  fprintf(stderr,"Usage:\n%s" ,options.c_str());
}

static tfs::mock::MockDataService* gmock_data_service;

static void interruptcallback(int signal)
{
  TBSYS_LOG(INFO, "application signal[%d]", signal );
  switch( signal )
  {
     case SIGTERM:
     case SIGINT:
     default:
      gmock_data_service->destroy();
     break;
  }
}

int main(int argc, char* argv[])
{
  std::string conf;
  std::string work_index;
  int64_t capacity = 1024;//GB
  int64_t max_write_file_size = 0;
  int32_t index = 0;
  bool daemon = false;
  bool help   = false;
  bool version= false;
  while ((index = getopt(argc, argv, "f:i:c:s:dvh")) != EOF)
  {
    switch (index)
    {
    case 'f':
      conf = optarg;
    break;
    case 'i':
      work_index = optarg;
    break;
    case 'c':
      capacity = atoll(optarg);
    break;
    case 'd':
      daemon = true;
      break;
    case 's':
      max_write_file_size = atoll(optarg);
      break;
    case 'v':
      version = true;
      break;
    case 'h':
    default:
      help = true;
      break;
    }
  }

  help = (conf.empty() || work_index.empty());

  if (help)
  {
    helper();
    return TFS_ERROR;
  }
  int iret = SysParam::instance().load_mock_dataserver(conf);
  if (iret != TFS_SUCCESS)
  {
    fprintf(stderr, "load config file(%s) failed\n", conf.c_str());
    return EXIT_GENERAL_ERROR;
  }
  SYSPARAM_MOCK_DATASERVER.log_file_ = SYSPARAM_MOCK_DATASERVER.work_dir_ + "/logs/mock_dataserver_"+ work_index + ".log";
  TBSYS_LOGGER.setLogLevel(CONFIG.get_string_value(CONFIG_PUBLIC, CONF_LOG_LEVEL, "debug"));       
  TBSYS_LOGGER.setMaxFileSize(CONFIG.get_int_value(CONFIG_PUBLIC, CONF_LOG_SIZE, 0x40000000));
  TBSYS_LOGGER.setMaxFileIndex(CONFIG.get_int_value(CONFIG_PUBLIC, CONF_LOG_NUM, 0x0A));
  const char *log_file = SYSPARAM_MOCK_DATASERVER.log_file_.c_str();
  if (access(log_file, R_OK) == 0)                                                                 
  {
    TBSYS_LOGGER.rotateLog(log_file);
  }
  else if (!DirectoryOp::create_full_path(log_file, true))
  {
    fprintf(stderr, "create file(%s)'s directory failed\n",log_file); 
    return EXIT_GENERAL_ERROR;
  }

  if (!DirectoryOp::create_full_path(SYSPARAM_MOCK_DATASERVER.work_dir_.c_str(), true))
  {
    fprintf(stderr, "create file(%s)'s directory failed\n",SYSPARAM_MOCK_DATASERVER.work_dir_.c_str()); 
    return EXIT_GENERAL_ERROR;
  }

  std::string pid_file = SYSPARAM_MOCK_DATASERVER.work_dir_ +"/logs/mock_dataserver_" + work_index + ".pid";
  int32_t pid = 0;
  if ((pid = tbsys::CProcess::existPid(pid_file.c_str())))
  {
    fprintf(stderr, "mockdataserver%s is running\n", work_index.c_str());
    return EXIT_SYSTEM_ERROR;
  }
  
  pid = 0;
  if (daemon)
  {
    pid = tbsys::CProcess::startDaemon(pid_file.c_str(), log_file);
  }

  index = atoi(work_index.c_str());
  int32_t port = SYSPARAM_MOCK_DATASERVER.port_ + index;
  const int64_t BASE_CAPACITY_GB = 1024 * 1024 * 1024;//GB
  capacity = capacity * BASE_CAPACITY_GB;

  if (pid == 0)
  {
    signal(SIGPIPE, SIG_IGN);
    signal(SIGHUP, SIG_IGN);
    signal(SIGINT, SIG_IGN);                                                                   
    signal(SIGTERM, SIG_IGN);
    signal(SIGUSR1, SIG_IGN);
    try
    {
      gmock_data_service = new MockDataService(max_write_file_size);
      iret = gmock_data_service->run(port, capacity, work_index, conf);
      if (iret != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "%s", "mockdataserver initialize fail, must be exit...");
        gmock_data_service->destroy();
        gmock_data_service->wait_for_shut_down();
        return iret;
      }
      signal(SIGHUP, interruptcallback);
      signal(SIGINT, interruptcallback);                                                                   
      signal(SIGTERM, interruptcallback);
      signal(SIGUSR1, interruptcallback);
      gmock_data_service->wait_for_shut_down();
      unlink(pid_file.c_str());
    }
    catch(std::exception& ex)
    {
      TBSYS_LOG(ERROR, "catch exception(%s),must be exit...", ex.what());
      gmock_data_service->destroy();
    }
    catch(...)
    {
      TBSYS_LOG(ERROR, "%s", "catch exception(unknow),must be exit...");
      gmock_data_service->destroy();
    }
    tbsys::gDelete(gmock_data_service);
    return iret;
  }
  return 0;
}
