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
 *   nayan<nayan@taobao.com>
 *      - modify 2009-03-27
 *
 */
#include "adminserver.h"
#include "common/directory_op.h"
#include "message/client.h"
#include "message/client_pool.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::adminserver;

int usage(const char *name)
{
  fprintf(stderr, "Usage: %s -f conf_file -d -s service_name\n", name);
  fprintf(stderr, "-f conf_file    confiure file\n");
  fprintf(stderr, "-d              is daemon\n");
  fprintf(stderr, "-s service_name {ns|ds}\n");
  fprintf(stderr, "\n");
  exit(1);
}

int main(int argc, char *argv[])
{
  int i;
  int32_t service_name = 0;
  int32_t is_daemon = 0;
  char* conf_file = NULL;

  while ((i = getopt(argc, argv, "f:s:i:d")) != EOF)
  {
    switch (i)
    {
    case 'f':
      conf_file = optarg;
      break;
    case 's':
      if (strncmp(optarg, "ns", 2) == 0)
      {
        service_name |= 1;
      }
      else if (strncmp(optarg, "ds", 2) == 0)
      {
        service_name |= 2;
      }
      break;
    case 'd':
      is_daemon = 1;
      break;
    }
  }

  if (conf_file == NULL || service_name == 0)
  {
    usage(argv[0]);
  }

  if (CONFIG.load(conf_file) != TFS_SUCCESS)
  {
    fprintf(stderr, "load config file fail.");
    return TFS_ERROR;
  }

  // set log level
  TBSYS_LOGGER.setLogLevel(CONFIG.get_string_value(CONFIG_PUBLIC, CONF_LOG_LEVEL));

  AdminServer* adminserver = new AdminServer();
  int ret = adminserver->main(conf_file, service_name, is_daemon);
  delete adminserver;
  adminserver = NULL;

  return ret;
}

namespace tfs
{
  namespace adminserver
  {
    // therer is only one AdminServer instance
    int32_t AdminServer::stop_;
    AdminServer::AdminServer()
    {
      stop_ = 0;
      check_interval_ = CONFIG.get_int_value(CONFIG_ADMINSERVER, CONF_CHECK_INTERVAL, 1);
      check_count_ = CONFIG.get_int_value(CONFIG_ADMINSERVER, CONF_CHECK_COUNT, 5);
    }

    AdminServer::~AdminServer()
    {
    }

    void AdminServer::signal_handler(int sig)
    {
      switch (sig)
      {
      case SIGTERM:
      case SIGINT:
        stop();
        break;
      }
    }

    void AdminServer::stop()
    {
      stop_ = 1;
    }

    void AdminServer::set_ds_list(char* index_range, vector<string>& ds_index)
    {
      if (index_range != NULL)
      {
        char buf[256];
        strncpy(buf, index_range, 256);
        char* index = NULL;
        char* buf_p = buf;

        while ((index = strsep(&buf_p, ", ")) != NULL)
        {
          if (strlen(index) != 0)
          {
            ds_index.push_back(index);
          }
        }
      }
    }

    int AdminServer::main(const char* conf_file, const int32_t service_name, const int32_t is_daemon)
    {
      int i;
      void *retp;
      pthread_t tid_ns = 0, tid_ds = 0;

      vector<MonitorParam*> param_ns_list;
      vector<MonitorParam*> param_ds_list;

      char* top_work_dir = CONFIG.get_string_value(CONFIG_PUBLIC, CONF_WORK_DIR);
      if (NULL == top_work_dir)
      {
        TBSYS_LOG(ERROR, "work directory config not found");
        return TFS_ERROR;
      }

      char default_pid_file[MAX_PATH_LENGTH];
      snprintf(default_pid_file, MAX_PATH_LENGTH, "%s/logs/adminserver.pid", top_work_dir);
      char *pid_file = CONFIG.get_string_value(CONFIG_ADMINSERVER, CONF_LOCK_FILE, default_pid_file);
      if ((i = tbsys::CProcess::existPid(pid_file)) > 0)
      {
        TBSYS_LOG(ERROR, "adminserver has already run. Pid: %d", i);
        return TFS_ERROR;
      }
      if (!DirectoryOp::create_full_path(pid_file, true))
      {
        TBSYS_LOG(ERROR, "create file(%s)'s directory failed", pid_file);
        return TFS_ERROR;
      }

      char default_log_file[MAX_PATH_LENGTH];
      snprintf(default_log_file, MAX_PATH_LENGTH, "%s/logs/adminserver.log", top_work_dir);
      const char *log_file = CONFIG.get_string_value(CONFIG_ADMINSERVER, CONF_LOG_FILE, default_log_file);
      if (access(log_file, R_OK) == 0)
      {
        TBSYS_LOGGER.rotateLog(log_file);
      }
      else if (!DirectoryOp::create_full_path(log_file, true))
      {
        TBSYS_LOG(ERROR, "create file(%s)'s directory failed", log_file);
        return TFS_ERROR;
      }

      if (is_daemon)
      {
        if (tbsys::CProcess::startDaemon(pid_file, log_file) > 0)
        {
          return TFS_SUCCESS;
        }
      }

      signal(SIGPIPE, SIG_IGN);
      signal(SIGHUP, SIG_IGN);
      signal(SIGINT, signal_handler);
      signal(SIGTERM, signal_handler);

      if (service_name & 1)
      {
        MonitorParam *param_ns = new MonitorParam();
        param_ns->server_ = this;
        param_ns->lock_file_ = CONFIG.get_string_value(CONFIG_NAMESERVER, CONF_LOCK_FILE);
        param_ns->port_ = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_PORT);
        param_ns->script_ = CONFIG.get_string_value(CONFIG_ADMINSERVER, CONF_NS_SCRIPT);
        param_ns->fkill_waittime_ = CONFIG.get_int_value(CONFIG_ADMINSERVER, CONF_NS_FKILL_WAITTIME);
        param_ns->description_ = "nameserver";
        param_ns->isds_ = 0;
        param_ns_list.push_back(param_ns);
        pthread_create(&tid_ns, NULL, AdminServer::do_monitor, &param_ns_list);
      }
      if (service_name & 2)
      {
        char *index_range = CONFIG.get_string_value(CONFIG_ADMINSERVER, CONF_DS_INDEX_LIST, NULL);
        vector < string > ds_index;
        set_ds_list(index_range, ds_index);
        int32_t size = ds_index.size();

        for (int32_t i = 0; i < size; ++i)
        {
          MonitorParam* param_ds = new MonitorParam();
          int ret = TFS_ERROR;
          // maybe load once then handle the trail suffix
          if ((ret = SysParam::instance().load_data_server(conf_file, ds_index[i])) != TFS_SUCCESS)
          {
            cerr << "SysParam::load_data_server failed:" << conf_file << endl;
            return ret;
          }

          param_ds->server_ = this;
          string& tmp_pid = SYSPARAM_DATASERVER.pid_file_;
          param_ds->lock_file_ = new char[tmp_pid.size() + 1];
          memset(param_ds->lock_file_, 0, tmp_pid.size() + 1);
          memcpy(param_ds->lock_file_, tmp_pid.c_str(), tmp_pid.size());

          param_ds->port_ = SYSPARAM_DATASERVER.local_ds_port_;

          string ds_cmd = CONFIG.get_string_value(CONFIG_ADMINSERVER, CONF_DS_SCRIPT);
          ds_cmd.append(" -i " + ds_index[i]);
          param_ds->script_ = new char[ds_cmd.size() + 1];
          memset(param_ds->script_, 0, ds_cmd.size() + 1);
          memcpy(param_ds->script_, ds_cmd.c_str(), ds_cmd.size());

          param_ds->fkill_waittime_ = CONFIG.get_int_value(CONFIG_ADMINSERVER, CONF_DS_FKILL_WAITTIME);

          string& tmp_desc = SYSPARAM_FILESYSPARAM.mount_name_;
          param_ds->description_ = new char[tmp_desc.size() + 1];
          memset(param_ds->description_, 0, tmp_desc.size() + 1);
          memcpy(param_ds->description_, tmp_desc.c_str(), tmp_desc.size());

          param_ds->isds_ = 1;
          fprintf(stderr, "load dataserver %s, desc : %s, lock_file : %s, port : %d, script : %s, waittime: %d\n",
                  ds_index[i].c_str(), param_ds->description_, param_ds->lock_file_, param_ds->port_, param_ds->script_,
                  param_ds->fkill_waittime_);

          param_ds_list.push_back(param_ds);
        }

        pthread_create(&tid_ds, NULL, AdminServer::do_monitor, &param_ds_list);
      }
      if (tid_ns != 0)
      {
        pthread_join(tid_ns, &retp);
      }
      if (tid_ds != 0)
      {
        pthread_join(tid_ds, &retp);
      }
      TBSYS_LOG(INFO, "adminserver normal exit.\n");

      int32_t ns_size = param_ns_list.size();
      for (int32_t i = 0; i < ns_size; ++i)
      {
        delete param_ns_list[i];
      }

      int32_t ds_size = param_ds_list.size();
      for (int32_t i = 0; i < ds_size; ++i)
      {
        if (param_ds_list[i]->lock_file_)
        {
          delete[] param_ds_list[i]->lock_file_;
          param_ds_list[i]->lock_file_ = NULL;
        }
        if (param_ds_list[i]->script_)
        {
          delete[] param_ds_list[i]->script_;
          param_ds_list[i]->script_ = NULL;
        }
        if (param_ds_list[i]->description_)
        {
          delete[] param_ds_list[i]->description_;
          param_ds_list[i]->description_ = NULL;
        }
        delete param_ds_list[i];
      }
      return TFS_SUCCESS;
    }

    void* AdminServer::do_monitor(void* args)
    {
      vector<MonitorParam*> *as = reinterpret_cast<vector<MonitorParam*> *> (args);
      if (as->size() == 0)
        return NULL;

      (*as)[0]->server_->run_monitor(as);
      return NULL;
    }

    int AdminServer::ping(Client* client, const uint64_t ip)
    {
      if (client->connect() == TFS_ERROR)
      {
        TBSYS_LOG(ERROR, "connect fail: %s", tbsys::CNetUtil::addrToString(ip).c_str());
        return TFS_ERROR;
      }
      int status = TFS_ERROR;
      StatusMessage ping_msg(STATUS_MESSAGE_PING);
      Message *message = client->call(&ping_msg);
      if (message != NULL)
      {
        if (message->get_message_type() == STATUS_MESSAGE && dynamic_cast<StatusMessage*> (message)->get_status()
            == STATUS_MESSAGE_PING)
        {
          status = TFS_SUCCESS;
        }
        delete message;
      }
      return status;
    }

    int AdminServer::ping(const uint64_t ip)
    {
      Client *client = CLIENT_POOL.get_client(ip);
      if (client->connect() == TFS_ERROR)
      {
        TBSYS_LOG(ERROR, "connect fail: %s", tbsys::CNetUtil::addrToString(ip).c_str());
        CLIENT_POOL.release_client(client);
        return TFS_ERROR;
      }

      int32_t status = TFS_ERROR;
      StatusMessage ping_msg(STATUS_MESSAGE_PING);
      Message* message = client->call(&ping_msg);
      if (message != NULL)
      {
        if (message->get_message_type() == STATUS_MESSAGE && dynamic_cast<StatusMessage*> (message)->get_status()
            == STATUS_MESSAGE_PING)
        {
          status = TFS_SUCCESS;
        }
        delete message;
      }

      CLIENT_POOL.release_client(client);
      return status;
    }

    int AdminServer::ping_nameserver(const int estatus)
    {
      uint64_t nsip;
      IpAddr* adr = reinterpret_cast<IpAddr*> (&nsip);
      uint32_t ip = Func::get_addr(CONFIG.get_string_value(CONFIG_NAMESERVER, CONF_IP_ADDR));
      if (ip > 0)
      {
        adr->ip_ = ip;
        adr->port_ = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_PORT);
      }

      Client* client = CLIENT_POOL.get_client(nsip);
      int32_t count = 0;
      while (!stop_)
      {
        if (ping(client, nsip) == estatus)
          break;
        Func::sleep(check_interval_, &stop_);
        ++count;
      }
      CLIENT_POOL.release_client(client);

      return count;
    }

    int AdminServer::run_monitor(vector<MonitorParam*>* vmp)
    {
      int32_t size = vmp->size();
      vector < MonitorStatus > vms;
      for (int32_t i = 0; i < size; ++i)
      {
        MonitorStatus ms;
        memset(&ms, 0, sizeof(MonitorStatus));
        uint32_t ip = Func::get_addr("127.0.0.1");
        if (ip > 0)
        {
          ms.adr_.ip_ = ip;
          ms.adr_.port_ = (*vmp)[i]->port_;
        }

        // check process
        if ((ms.pid_ = tbsys::CProcess::existPid((*vmp)[i]->lock_file_)) == 0)
        {
          if ((*vmp)[i]->isds_)
          {
            ping_nameserver( TFS_SUCCESS); // wait for ns
            if (stop_)
              return TFS_SUCCESS;
          }
          TBSYS_LOG(INFO, "start %s: cmd: %s", (*vmp)[i]->description_, (*vmp)[i]->script_);
          if (system((*vmp)[i]->script_) == -1)
          {
            TBSYS_LOG(ERROR, "start %s fail.", (*vmp)[i]->description_);
          }
          else
          {
            Func::sleep(2, &stop_);
            ms.restarting_ = 1;
            ms.failure_ = 0;
          }
        }

        vms.push_back(ms);
      }

      while (!stop_)
      {
        // check process
        for (int i = 0; i < size; ++i)
        {
          if (vms[i].pid_ == 0 || kill(vms[i].pid_, 0) != 0)
          {
            if ((vms[i].pid_ = tbsys::CProcess::existPid((*vmp)[i]->lock_file_)) == 0)
            {
              TBSYS_LOG(ERROR, "start %s", (*vmp)[i]->description_);
              if (system((*vmp)[i]->script_) == -1)
              {
                TBSYS_LOG(ERROR, "start %s fail.", (*vmp)[i]->description_);
              }
              else
              {
                vms[i].restarting_ = 1;
                vms[i].failure_ = 0;
              }
            }
          }
          if (stop_)
            break;

          // process exist. ping
          if (vms[i].pid_ > 0)
          {
            uint64_t ip_address = *(uint64_t*) &(vms[i].adr_);
            int32_t status = ping(ip_address);
            if (status == TFS_ERROR)
            {
              if (vms[i].restarting_ == 0)
              {
                TBSYS_LOG(ERROR, "ping %s fail, ip: %s, failure: %d", (*vmp)[i]->description_,
                    tbsys::CNetUtil::addrToString(ip_address).c_str(), vms[i].failure_);
                vms[i].failure_++;
              }
              else // do not ad failure num if the process is restarting status
              {
                TBSYS_LOG(ERROR, "restarting, desc : %s ip: %s", (*vmp)[i]->description_,
                    tbsys::CNetUtil::addrToString(ip_address).c_str());
                vms[i].failure_ = 0;
              }
            }
            else
            {
              TBSYS_LOG(DEBUG, "ping %s success, ip: %s", (*vmp)[i]->description_, tbsys::CNetUtil::addrToString(
                  ip_address).c_str());
              vms[i].failure_ = 0;
              vms[i].restarting_ = 0;
            }

            // restart
            if (vms[i].failure_ > check_count_)
            {
              TBSYS_LOG(WARN, "close, pid: %u", vms[i].pid_);
              kill(vms[i].pid_, SIGTERM);

              vms[i].retry_ = (*vmp)[i]->fkill_waittime_ * 10;
              // close it
              while (kill(vms[i].pid_, 0) == 0 && vms[i].retry_ > 0)
              {
                vms[i].retry_--;
                usleep(100000);
                if (stop_)
                  break;
              }

              if (vms[i].retry_ <= 0) // still not closed
              {
                kill(vms[i].pid_, SIGKILL); // force to close
                TBSYS_LOG(WARN, "force to close, pid: %u", vms[i].pid_);
              }
              vms[i].pid_ = 0;
              vms[i].failure_ = 0;
              vms[i].retry_ = 0;
            }
          }
        }
        // sleep
        Func::sleep(check_interval_, &stop_);
      }

      return TFS_SUCCESS;
    }

  }
}
