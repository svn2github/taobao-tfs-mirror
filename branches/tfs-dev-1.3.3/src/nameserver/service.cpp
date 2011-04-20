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
 *   qushan<qushan@taobao.com> 
 *      - modify 2009-03-27
 *   duanfei <duanfei@taobao.com> 
 *      - modify 2010-04-23
 *
 */
#include <exception>
#include <tbsys.h>
#include <Memory.hpp>
#include "service.h"
#include "common/config_item.h"
#include "common/directory_op.h"
#include "common/error_msg.h"
#include "message/new_client.h"

using namespace tbsys;
using namespace tfs::common;
using namespace tfs::nameserver;
using namespace tfs::message;

static void signal_handler(int32_t sig);
static Service* g_tfs_ns_service_ = NULL;
static int in_ip_list(uint64_t local_ip)
{
  const char *ip_list = CONFIG.get_string_value(CONFIG_NAMESERVER, CONF_IP_ADDR_LIST);
  if (ip_list == NULL)
  {
    return 0;
  }
  char buffer[1024];
  strcpy(buffer, ip_list);
  char *token = NULL;
  char *str = buffer;
  while ((token = strsep(&str, "|")) != NULL)
  {
    uint64_t ip = Func::get_addr(token);
    if (ip == local_ip)
    {
      return 1;
    }
  }
  return 0;
}

int main(int argc, char *argv[])
{
  int32_t i;
  bool is_deamon = false;
  bool is_help = false;
  std::string conf_file_path;

  while ((i = getopt(argc, argv, "f:dh")) != EOF)
  {
    switch (i)
    {
    case 'f':
      conf_file_path = optarg;
      break;
    case 'd':
      is_deamon = true;
      break;
    case 'h':
    default:
      is_help = true;
      break;
    }
  }

  if (conf_file_path.empty() || conf_file_path.c_str() == " " || is_help)
  {
    fprintf(stderr, "\nUsage: %s -f conf_file -d -h\n", argv[0]);
    fprintf(stderr, "  -f conf_file   config file path\n");
    fprintf(stderr, "  -d             run deamon\n");
    fprintf(stderr, "  -h             help\n");
    fprintf(stderr, "\n");
    return EXIT_GENERAL_ERROR;
  }

  int ret = TFS_SUCCESS;
  if ((ret = SysParam::instance().load(conf_file_path.c_str())) != TFS_SUCCESS)
  {
    fprintf(stderr, "load confiure file(%s) fail\n", conf_file_path.c_str());
    return ret;
  }
  TBSYS_LOGGER.setLogLevel(CONFIG.get_string_value(CONFIG_PUBLIC, CONF_LOG_LEVEL, "debug"));
	TBSYS_LOGGER.setMaxFileSize(CONFIG.get_int_value(CONFIG_PUBLIC, CONF_LOG_SIZE, 0x40000000));
	TBSYS_LOGGER.setMaxFileIndex(CONFIG.get_int_value(CONFIG_PUBLIC, CONF_LOG_NUM, 0x0A));
  const char *ip = CONFIG.get_string_value(CONFIG_NAMESERVER, CONF_IP_ADDR);
  const char *dev_name = CONFIG.get_string_value(CONFIG_NAMESERVER, CONF_DEV_NAME);
  uint64_t local_ip = Func::get_local_addr(dev_name);
  if (Func::get_addr(ip) != local_ip && in_ip_list(local_ip) == 0)
  {
    TBSYS_LOG(ERROR, "ip '%s' is not local ip, local ip: %s", ip, tbsys::CNetUtil::addrToString(local_ip).c_str());
    return EXIT_GENERAL_ERROR;
  }

  const char *work_dir = CONFIG.get_string_value(CONFIG_NAMESERVER, CONF_WORK_DIR);
  if (work_dir == NULL)
  {
    TBSYS_LOG(ERROR, "%s", "directory not found");
    return EXIT_CONFIG_ERROR;
  }
  if (!DirectoryOp::create_full_path(work_dir))
  {
    TBSYS_LOG(ERROR, "create directory(%s) failed", work_dir);
    return ret;
  }

  char *pid_file_path = CONFIG.get_string_value(CONFIG_NAMESERVER, CONF_LOCK_FILE);
  int32_t pid = 0;
  if ((pid = tbsys::CProcess::existPid(pid_file_path)))
  {
    TBSYS_LOG(ERROR, "program has been running: pid(%d)", pid);
    return EXIT_SYSTEM_ERROR;
  }

  const char *log_file_path = CONFIG.get_string_value(CONFIG_NAMESERVER, CONF_LOG_FILE);
  if (access(log_file_path, R_OK) == 0)
  {
    char dest_log_file_path[256];
    sprintf(dest_log_file_path, "%s.%s", log_file_path, Func::time_to_str(time(NULL), 1).c_str());
    rename(log_file_path, dest_log_file_path);
  }

  pid = 0;
  if (is_deamon)
  {
    pid = tbsys::CProcess::startDaemon(pid_file_path, log_file_path);
  }

  if (pid == 0)
  {
    signal(SIGPIPE, SIG_IGN);
    signal(SIGHUP, SIG_IGN);
    signal(SIGINT, SIG_IGN);
    signal(SIGTERM, SIG_IGN);
    signal(SIGUSR1, SIG_IGN);
    signal(40, SIG_IGN);

    try
    {
      ARG_NEW(g_tfs_ns_service_, Service);
      ret = g_tfs_ns_service_->start();
    }
    catch (std::exception& ex)
    {
      TBSYS_LOG(WARN, "Catch execption (%s), must be exit...", ex.what());
      g_tfs_ns_service_->stop();
    }
    catch (...)
    {
      TBSYS_LOG(WARN, "%s", "Catch unknow execption, must be exit...");
      g_tfs_ns_service_->stop();
    }
    tbsys::gDelete(g_tfs_ns_service_);
  }
  return ret;
}

static void signal_handler(int32_t sig)
{
  switch (sig)
  {
  case SIGTERM:
  case SIGINT:
    g_tfs_ns_service_->stop();
    break;
  case SIGSEGV:
  case SIGABRT:
    //TBSYS_LOG(DEBUG,sig == SIGSEGV ? "*SIGSEGV*" : "*SIGABRT*");
    //TBSYS_LOG(ERROR, const_cast<char*>(sig == SIGSEGV ? "*SIGSEGV*" : "*SIGABRT*"));
    break;
  case 40:
    TBSYS_LOGGER.checkFile();
    break;
  }
}

namespace tfs
{

  namespace nameserver
  {

    int global_callback_func(tfs::message::NewClient* client)
    {
      int32_t iret = NULL != g_tfs_ns_service_ ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        return g_tfs_ns_service_->callback(client); 
      }
      return iret;
    }


    Service::Service()
    {
    }

    Service::~Service()
    {
    }

    int Service::start()
    {
      TBSYS_LOGGER.setMaxFileSize();
      TBSYS_LOGGER.setMaxFileIndex();
      if (fs_name_system_.start() == TFS_SUCCESS)
      {
        signal(SIGINT, signal_handler);
        signal(SIGTERM, signal_handler);
        signal(SIGUSR1, signal_handler);
        signal(40, signal_handler);
        fs_name_system_.wait();
      }
      const char *pid_file_path = CONFIG.get_string_value(CONFIG_NAMESERVER, CONF_LOCK_FILE);
      unlink(pid_file_path);
      TBSYS_LOG(INFO, "%s", "nameserver exit");
      return TFS_SUCCESS;
    }

    void Service::stop()
    {
      fs_name_system_.stop();
    }
    
    int Service::callback(message::NewClient* client)
    {
      return fs_name_system_.callback(client);
    }
  }
}

