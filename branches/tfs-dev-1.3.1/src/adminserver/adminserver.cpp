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
#include "message/client_pool.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::adminserver;

AdminServer* g_adminserver = NULL;

void signal_handler(int sig)
{
  switch (sig)
  {
  case SIGTERM:
  case SIGINT:
    if (g_adminserver)
    {
      g_adminserver->stop();
    }
    else
    {
      exit(TFS_ERROR);
    }
    break;
  }
}

int usage(const char *name)
{
  fprintf(stderr, "Usage: %s -f conf_file -d -s service_name\n", name); //
  fprintf(stderr, "-f conf_file    confiure file\n");
  fprintf(stderr, "-s service_name {ns|ds}\n");
  fprintf(stderr, "-d              is daemon\n");
  fprintf(stderr, "-q              not run monitor now\n");
  fprintf(stderr, "-o              is old version\n");
  fprintf(stderr, "\n");
  exit(TFS_ERROR);
}

int main(int argc, char *argv[])
{
  int i = 0;
  ServiceName service_name = SERVICE_NONE;
  bool is_daemon = false, run_now = true, is_old = false;
  char* conf_file = NULL;

  while ((i = getopt(argc, argv, "f:s:i:qdo")) != EOF)
  {
    switch (i)
    {
    case 'f':
      conf_file = optarg;
      break;
    case 's':
      if (strncmp(optarg, "ns", 2) == 0)
      {
        service_name = SERVICE_NS;
      }
      else if (strncmp(optarg, "ds", 2) == 0)
      {
        service_name = SERVICE_DS;
      }
      break;
    case 'd':
      is_daemon = true;
      break;
    case 'q':
      run_now = false;
      break;
    case 'o':
      is_old = true;
      break;
    default:
      usage(argv[0]);
    }
  }

  if (NULL == conf_file || SERVICE_NONE == service_name)
  {
    usage(argv[0]);
  }

  if (CONFIG.load(conf_file) != TFS_SUCCESS)
  {
    fprintf(stderr, "fail to load config file %s : %s\n.", conf_file, strerror(errno));
    return TFS_ERROR;
  }

  // set log level
  TBSYS_LOGGER.setLogLevel(CONFIG.get_string_value(CONFIG_PUBLIC, CONF_LOG_LEVEL));

  char* top_work_dir = CONFIG.get_string_value(CONFIG_PUBLIC, CONF_WORK_DIR);
  if (NULL == top_work_dir)
  {
    TBSYS_LOG(ERROR, "work directory config not found");
    return TFS_ERROR;
  }

  // pid file
  char default_pid_file[MAX_PATH_LENGTH+1];
  default_pid_file[MAX_PATH_LENGTH] = '\0';
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

  // log file
  char default_log_file[MAX_PATH_LENGTH+1];
  default_log_file[MAX_PATH_LENGTH] = '\0';
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

  // daemon child process
  signal(SIGPIPE, SIG_IGN);
  signal(SIGHUP, SIG_IGN);
  signal(SIGINT, signal_handler);
  signal(SIGTERM, signal_handler);

  g_adminserver = new AdminServer(conf_file, service_name, is_daemon, is_old);
  int ret = g_adminserver->start(run_now);
  delete g_adminserver;
  g_adminserver = NULL;

  return ret;
}


////////////////////////////////
namespace tfs
{
  namespace adminserver
  {
    // therer is only one AdminServer instance
    AdminServer::AdminServer() :
      is_daemon_(false), service_name_(SERVICE_NONE), stop_(0), running_(false),
      check_interval_(0), check_count_(0), warn_dead_count_(0)
    {
      conf_file_[0] = '\0';
    }

    AdminServer::AdminServer(const char* conf_file, ServiceName service_name, bool is_daemon, bool is_old) :
      is_old_(is_old), is_daemon_(is_daemon), service_name_(service_name), stop_(0), running_(false)
    {
      strncpy(conf_file_, conf_file, strlen(conf_file)+1);
      check_interval_ = CONFIG.get_int_value(CONFIG_ADMINSERVER, CONF_CHECK_INTERVAL, 1);
      check_count_ = CONFIG.get_int_value(CONFIG_ADMINSERVER, CONF_CHECK_COUNT, 5);
      warn_dead_count_ = CONFIG.get_int_value(CONFIG_ADMINSERVER, CONF_WARN_DEAD_COUNT, ADMIN_WARN_DEAD_COUNT);
    }

    AdminServer::~AdminServer()
    {
      destruct();
    }

    template<typename T> static void delet_map(T& m)
    {
      for (typename T::iterator it = m.begin(); it != m.end(); it++)
      {
        tbsys::gDelete(it->second);
      }
      m.clear();
    }

    void AdminServer::destruct()
    {
      delet_map(monitor_param_);
      delet_map(monitor_status_);
    }

    void AdminServer::wait()
    {
      transport_.wait();
      task_queue_thread_.wait();
    }

    int AdminServer::stop()
    {
      stop_monitor();
      transport_.stop();
      task_queue_thread_.stop();
      return TFS_SUCCESS;
    }

    void AdminServer::set_ds_list(char* index_range, vector<string>& ds_index)
    {
      if (index_range != NULL)
      {
        char buf[MAX_PATH_LENGTH+1];
        buf[MAX_PATH_LENGTH] = '\0';
        strncpy(buf, index_range, MAX_PATH_LENGTH);
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

    void AdminServer::modify_conf(string& index, int32_t type)
    {
      char cmd[MAX_PATH_LENGTH+1];
      cmd[MAX_PATH_LENGTH] = '\0';
      // use sed directly,
      if (1 == type)            // insert, strip trailing ", "
      {
        snprintf(cmd, MAX_PATH_LENGTH, "sed -i 's/\\(%s.*[^ ,]\\)[, ]*$/\\1,%s/g' %s",
                 CONF_DS_INDEX_LIST, index.c_str(), conf_file_);
      }
      else                      // delete
      {
        snprintf(cmd, MAX_PATH_LENGTH, "sed -i 's/\\(%s.*[, ]*\\)\\b%s\\b[, ]*\\(.*$\\)/\\1\\2/g' %s",
                 CONF_DS_INDEX_LIST, index.c_str(), conf_file_);
      }
      TBSYS_LOG(INFO, "cmd %s", cmd);
      int ret = system(cmd);
      TBSYS_LOG(INFO, "%s ds index %s %s", (1 == type) ? "add" : "delete", index.c_str(), (-1 == ret) ? "fail" : "success");
    }

    int AdminServer::get_param(string& index)
    {
      int ret = TFS_SUCCESS;
      MonitorParam* param = new MonitorParam();

      if (index.size())         // dataserver
      {
        // for old compatibility, require different load strategy
        if (is_old_)
        {
          ret = CONFIG.load(conf_file_);
        }
        else
        {
          ret = SysParam::instance().load_data_server(conf_file_, index);
        }

        if (ret != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "load config file %s fail : %s", conf_file_, strerror(errno));
          return TFS_ERROR;
        }

        // common whether old or not
        param->index_ = index;
        param->active_ = 1;
        param->fkill_waittime_ = CONFIG.get_int_value(CONFIG_ADMINSERVER, CONF_DS_FKILL_WAITTIME);
        param->adr_.ip_ = Func::get_addr("127.0.0.1"); // just monitor local stuff
        param->script_ = CONFIG.get_string_value(CONFIG_ADMINSERVER, CONF_DS_SCRIPT);

        if(is_old_)
        {
          string suffix = ".conf";
          size_t pos = param->script_.find(suffix);
          if (string::npos == pos)
          {
            TBSYS_LOG(ERROR, "adminserver conf ds script invalid: %s", param->script_.c_str());
            return TFS_ERROR;
          }
          param->script_.replace(pos, suffix.size() , "." + index + suffix);

          size_t conf_start = param->script_.find("-f");
          if (string::npos == conf_start)
          {
            TBSYS_LOG(ERROR, "adminserver conf ds script invalid: %s", param->script_.c_str());
            return TFS_ERROR;
          }
          conf_start += 2;        // skip -f
          conf_start = param->script_.find_first_not_of(" ", conf_start);
          size_t conf_end = param->script_.find_first_of(" ", conf_start);
          string conf_file = param->script_.substr(conf_start, (string::npos == conf_end) ? conf_end : conf_end-conf_start);

          // load specified conf file
          if (CONFIG.load(conf_file) != TFS_SUCCESS)
          {
            TBSYS_LOG(ERROR, "load config file %s fail: %s", conf_file.c_str(), strerror(errno));
            return TFS_ERROR;
          }
          param->adr_.port_ = CONFIG.get_int_value(CONFIG_DATASERVER, CONF_PORT);
          param->description_ = CONFIG.get_string_value(CONFIG_DATASERVER, CONF_WORK_DIR);
          param->lock_file_ = CONFIG.get_string_value(CONFIG_DATASERVER, CONF_LOCK_FILE);
        }
        else
        {
          param->script_ += " -i " + index;
          param->description_ = SYSPARAM_FILESYSPARAM.mount_name_;
          param->adr_.port_ = SYSPARAM_DATASERVER.local_ds_port_;
          param->lock_file_ = SYSPARAM_DATASERVER.pid_file_;
        }

        TBSYS_LOG(INFO, "load dataserver %s, desc : %s, lock_file : %s, port : %d, script : %s, waittime: %d\n",
                  index.c_str(), param->description_.c_str(), param->lock_file_.c_str(), param->adr_.port_, param->script_.c_str(),
                  param->fkill_waittime_);
      }
      else                      // ns
      {
        param->lock_file_ = CONFIG.get_string_value(CONFIG_NAMESERVER, CONF_LOCK_FILE);
        param->adr_.ip_ = Func::get_addr("127.0.0.1");
        param->adr_.port_ = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_PORT);
        param->script_ = CONFIG.get_string_value(CONFIG_ADMINSERVER, CONF_NS_SCRIPT);
        param->fkill_waittime_ = CONFIG.get_int_value(CONFIG_ADMINSERVER, CONF_NS_FKILL_WAITTIME);
        param->description_ = "nameserver";
        param->index_ = index;
        param->active_ = 1;
      }

      monitor_param_.insert(MSTR_PARA::value_type(index, param));
      TBSYS_LOG(DEBUG, "get index %s paramter", index.empty() ? "ns" : index.c_str());
      return ret;
    }

    void AdminServer::add_index(string& index, bool add_conf)
    {
      TBSYS_LOG(DEBUG, "add new index %s to monitor", index.c_str());
      // paramter
      get_param(index);

      // status
      MSTR_STAT_ITER it;
      if ((it = monitor_status_.find(index)) == monitor_status_.end())
      {
        monitor_status_.insert(MSTR_STAT::value_type(index, new MonitorStatus(index)));
      }
      // conf
      if (add_conf)
        modify_conf(index, 1);
    }

    void AdminServer::clear_index(string& index, bool del_conf)
    {
      // paramter
      tbsys::gDelete(monitor_param_[index]);
      monitor_param_.erase(index);
      // conf
      if (del_conf)
        modify_conf(index, 0);
    }

    int AdminServer::kill_process(MonitorStatus* status, int32_t wait_time, bool clear)
    {
      if (status->pid_ != 0)
      {
        TBSYS_LOG(WARN, "close, pid: %u", status->pid_);
        kill(status->pid_, SIGTERM);

        int32_t retry = wait_time * 10;
        // close it
        while (kill(status->pid_, 0) == 0 && retry > 0)
        {
          retry--;
          usleep(100000);
          if (stop_)
            break;
        }

        if (retry <= 0) // still not closed
        {
          kill(status->pid_, SIGKILL); // force to close
          TBSYS_LOG(WARN, "force to close, pid: %u", status->pid_);
        }
      }

      status->pid_ = 0;
      status->failure_ = 0;
      status->dead_time_ = time(NULL);
      status->start_time_ = 0;
      if (clear)
      {
        status->dead_count_ = 0;
      }
      else
      {
        status->dead_count_++;
      }
      return TFS_SUCCESS;
    }

    int AdminServer::start(bool run_now)
    {
      // start
      packet_streamer_.set_packet_factory(&msg_factory_);
      CLIENT_POOL.init_with_transport(&transport_);

      int32_t port = CONFIG.get_int_value(CONFIG_ADMINSERVER, CONF_PORT, 12000);
      char spec[SPEC_LEN];
      sprintf(spec, "tcp::%d", port);
      if (transport_.listen(spec, &packet_streamer_, this) == NULL)
      {
        TBSYS_LOG(ERROR, "Failed to listen port: %d", port);
        return TFS_ERROR;
      }
      transport_.start();

      // start queue thread
      task_queue_thread_.setThreadParameter(CONFIG.get_int_value(CONFIG_ADMINSERVER, CONF_THREAD_COUNT, 1), this, NULL);
      task_queue_thread_.start();

      TBSYS_LOG(INFO, "==== AdminServer start! listen port: %d, pid: %d, service: %s ====", port, getpid(),
                (SERVICE_NS == service_name_) ? "ns" : "ds");
      if (run_now)
      {
        start_monitor();
      }

      // main wait for everything
      wait();

      TBSYS_LOG(INFO, "==== Adminserver exit normally! ====");
      return TFS_SUCCESS;
    }

    int AdminServer::stop_monitor()
    {
      stop_ = 1;
      return TFS_SUCCESS;
    }

    int AdminServer::start_monitor()
    {
      // wait for last monitor stop, there is always only one monitor running...
      while (running_)
      {
      }

      int ret = TFS_SUCCESS;

      // clean old parameters and status, reread config file to construct parameter
      destruct();

      if (CONFIG.load(conf_file_) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "load config file %s fail : %s", conf_file_, strerror(errno));
        return TFS_ERROR;
      }

      if (service_name_ & SERVICE_NS)
      {
        string index = "";
        add_index(index, false);
      }
      else if (service_name_ & SERVICE_DS)
      {
        char *index_range = CONFIG.get_string_value(CONFIG_ADMINSERVER, CONF_DS_INDEX_LIST, NULL);
        if (!index_range)
        {
          TBSYS_LOG(ERROR, "ds index list not found in config file %s .", conf_file_);
          return TFS_ERROR;
        }
        vector<string> ds_index;
        set_ds_list(index_range, ds_index);
        int32_t size = ds_index.size();

        for (int32_t i = 0; i < size; ++i)
        {
          add_index(ds_index[i]);
        }
      }

      pthread_t tid;
      pthread_create(&tid, NULL, AdminServer::do_monitor, this);

      if (0 == tid)
      {
        TBSYS_LOG(ERROR, "start monitor fail");
        ret = TFS_ERROR;
      }
      // detach thread, return now for handle packet
      pthread_detach(tid);

      return ret;
    }

    void* AdminServer::do_monitor(void* args)
    {
      reinterpret_cast<AdminServer*>(args)->run_monitor();
      return NULL;
    }

    int AdminServer::run_monitor()
    {
      if (0 == monitor_param_.size())
      {
        TBSYS_LOG(ERROR, "no monitor index error");
        return TFS_ERROR;
      }

      // now is running, there is always only one monitor running...
      running_ = true;
      stop_ = 0;
      TBSYS_LOG(WARN, "== monitor normally start tid: %lu ==", pthread_self());

      if (SERVICE_DS == service_name_)
      {
        ping_nameserver(TFS_SUCCESS); // wait for ns
        if (stop_)
          return TFS_SUCCESS;
      }

      MonitorParam* m_param = NULL;
      MonitorStatus* m_status = NULL;

      while (!stop_)
      {
        // check process;
        // get pramameter's size every time
        for (MSTR_PARA_ITER it = monitor_param_.begin(); it != monitor_param_.end(); ++it)
        {
          m_param = it->second;
          m_status = monitor_status_[it->first];

          if (!m_param->active_) // ask for stop, not active, kill
          {
            TBSYS_LOG(DEBUG, "ask for stop index %s", it->first.c_str());
            // kill
            kill_process(m_status, m_param->fkill_waittime_, true); // ask for stop, clear dead count
            clear_index(const_cast<string&>(it->first));
            break;
          }

          if ( 0 == m_status->pid_ || kill(m_status->pid_, 0) != 0) // process not run
          {
            if ((m_status->pid_ = tbsys::CProcess::existPid(m_param->lock_file_.c_str())) == 0)
            {
              TBSYS_LOG(ERROR, "start %s", m_param->description_.c_str());
              if (system(m_param->script_.c_str()) == -1)
              {
                TBSYS_LOG(ERROR, "start %s fail.", m_param->script_.c_str());
              }
              else
              {
                m_status->restarting_ = 1;
                m_status->failure_ = 0;
                m_status->start_time_ = time(NULL);
              }
            }
          }
          else if (0 == m_status->start_time_) // startup, ds/ns is already running
          {
            m_status->start_time_ = time(NULL);
          }

          if (stop_)
            break;

          // process exist. ping
          if (m_status->pid_ > 0)
          {
            uint64_t ip_address = *(uint64_t*) &(m_param->adr_);
            int32_t status = ping(ip_address);
            if (status == TFS_ERROR)
            {
              if (m_status->restarting_ == 0)
              {
                TBSYS_LOG(ERROR, "ping %s fail, ip: %s, failure: %d", m_param->description_.c_str(),
                          tbsys::CNetUtil::addrToString(ip_address).c_str(), m_status->failure_);
                m_status->failure_++;
              }
              else // do not ad failure num if the process is restarting status
              {
                TBSYS_LOG(ERROR, "restarting, desc : %s ip: %s", m_param->description_.c_str(),
                          tbsys::CNetUtil::addrToString(ip_address).c_str());
                m_status->failure_ = 0;
              }
            }
            else
            {
              TBSYS_LOG(DEBUG, "ping %s success, ip: %s", m_param->description_.c_str(),
                        tbsys::CNetUtil::addrToString(ip_address).c_str());
              m_status->failure_ = 0;
              m_status->restarting_ = 0;
            }

            // kill
            if (m_status->failure_ > check_count_)
            {
              kill_process(m_status, m_param->fkill_waittime_);
            }
          }
        }
        // sleep
        Func::sleep(check_interval_, &stop_);
      }

      // now not running
      TBSYS_LOG(WARN, "== monitor normally exit tid: %lu ==", pthread_self());
      running_ = false;
      return TFS_SUCCESS;
    }

    int AdminServer::ping(const uint64_t ip, Client *al_client)
    {
      Client *client = al_client ? al_client : CLIENT_POOL.get_client(ip);
      int32_t ret = TFS_ERROR;

      if (client->connect() == TFS_ERROR)
      {
        TBSYS_LOG(ERROR, "connect fail: %s", tbsys::CNetUtil::addrToString(ip).c_str());
      }
      else
      {
        StatusMessage ping_msg(STATUS_MESSAGE_PING);
        Message* message = client->call(&ping_msg);
        if (message != NULL)
        {
          if (message->get_message_type() == STATUS_MESSAGE && dynamic_cast<StatusMessage*> (message)->get_status()
              == STATUS_MESSAGE_PING)
          {
            ret = TFS_SUCCESS;
          }
          delete message;
        }
      }
      if (!al_client)
      {
        CLIENT_POOL.release_client(client);
      }
      return ret;
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

      // reuse client
      Client* client = CLIENT_POOL.get_client(nsip);
      int32_t count = 0;
      while (!stop_)
      {
        if (ping(nsip, client) == estatus)
          break;
        Func::sleep(check_interval_, &stop_);
        ++count;
      }
      CLIENT_POOL.release_client(client);

      return count;
    }

    tbnet::IPacketHandler::HPRetCode AdminServer::handlePacket(tbnet::Connection *connection, tbnet::Packet *packet)
    {
      if (NULL == connection || NULL == packet)
      {
        TBSYS_LOG(ERROR, "connection or packet ptr NULL");
        return tbnet::IPacketHandler::FREE_CHANNEL;
      }

      if (!packet->isRegularPacket() || packet->getPCode() != ADMIN_CMD_MESSAGE) // only ADMIN_CMD_MESSAGE support
      {
        TBSYS_LOG(ERROR, "ControlPacket, cmd: %d", dynamic_cast<tbnet::ControlPacket*>(packet)->getCommand());
        return tbnet::IPacketHandler::FREE_CHANNEL;
      }

      Message* bp = dynamic_cast<Message*>(packet);
      bp->set_connection(connection);
      bp->set_direction(DIRECTION_RECEIVE);
      task_queue_thread_.push(bp);

      return tbnet::IPacketHandler::KEEP_CHANNEL;
    }

    bool AdminServer::handlePacketQueue(tbnet::Packet* packet, void* args)
    {
      AdminCmdMessage* message = dynamic_cast<AdminCmdMessage*>(packet); // only AdminCmdMessage enqueue
      if (NULL == message)
      {
        TBSYS_LOG(ERROR, "process packet NULL can not convert to message");
        return true;
      }

      int32_t cmd_type = message->get_cmd_type();
      // only start_monitor and kill_adminserver can handle when monitor is not running
      if (!running_ && cmd_type != ADMIN_CMD_START_MONITOR && cmd_type != ADMIN_CMD_KILL_ADMINSERVER)
      {
        message->reply_message(new StatusMessage(TFS_ERROR, "monitor is not running"));
        return TFS_SUCCESS;
      }

      int ret = TFS_SUCCESS;
      // check cmd type
      switch (cmd_type)
      {
      case ADMIN_CMD_GET_STATUS:
        ret = cmd_reply_status(message);
        break;
      case ADMIN_CMD_CHECK:
        ret = cmd_check(message);
        break;
      case ADMIN_CMD_START_MONITOR:
        ret = cmd_start_monitor(message);
        break;
      case ADMIN_CMD_RESTART_MONITOR:
        ret = cmd_restart_monitor(message);
        break;
      case ADMIN_CMD_STOP_MONITOR:
        ret = cmd_stop_monitor(message);
        break;
      case ADMIN_CMD_START_INDEX:
        ret = cmd_start_monitor_index(message);
        break;
      case ADMIN_CMD_STOP_INDEX:
        ret = cmd_stop_monitor_index(message);
        break;
      case ADMIN_CMD_KILL_ADMINSERVER:
        ret = cmd_exit(message);
        break;
      default:
        ret = TFS_ERROR;
      }

      if (ret != TFS_SUCCESS)
      {
        MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), 12,
                                           "execute message fail, type: %d. ret: %d\n", message->get_message_type(), ret);
      }
      return true;
    }

    int AdminServer::cmd_check(AdminCmdMessage* message)
    {
      AdminCmdMessage* resp_msg = new AdminCmdMessage(ADMIN_CMD_RESP);
      for (MSTR_STAT_ITER it = monitor_status_.begin(); it != monitor_status_.end(); it++)
      {
        if (0 == it->second->pid_ || it->second->dead_count_ > warn_dead_count_) // add to confile later
        {
          resp_msg->set_status(it->second);
        }
      }

      message->reply_message(resp_msg);
      return TFS_SUCCESS;
    }

    int AdminServer::cmd_reply_status(AdminCmdMessage* message)
    {
      AdminCmdMessage* resp_msg = new AdminCmdMessage(ADMIN_CMD_RESP);
      for (MSTR_STAT_ITER it = monitor_status_.begin(); it != monitor_status_.end(); it++)
      {
        resp_msg->set_status(it->second);
      }

      message->reply_message(resp_msg);
      return TFS_SUCCESS;
    }

    int AdminServer::cmd_start_monitor(message::AdminCmdMessage* message)
    {
      const char* err_msg;
      int ret = TFS_ERROR;

      if (running_ && !stop_)     // is running and not will be stoped
      {
        err_msg = "monitor is already running";
      }
      else
      {
        ret = start_monitor();
        err_msg = (TFS_SUCCESS == ret) ? "start monitor SUCCESS" : "start monitor FAIL";
      }

      message->reply_message(new StatusMessage(ret, const_cast<char*>(err_msg)));
      return TFS_SUCCESS;
    }

    int AdminServer::cmd_restart_monitor(message::AdminCmdMessage* message)
    {
      stop_monitor();
      return cmd_start_monitor(message);
    }

    int AdminServer::cmd_stop_monitor(message::AdminCmdMessage* message)
    {
      stop_monitor();
      message->reply_message(new StatusMessage(TFS_SUCCESS, const_cast<char*>("stop monitor success")));
      return TFS_SUCCESS;
    }

    int AdminServer::cmd_start_monitor_index(AdminCmdMessage* message)
    {
      VSTRING* index = message->get_index();
      string success = "", fail = "";
      MSTR_PARA_ITER it;

      for (size_t i = 0; i < index->size(); i++)
      {
        string& cur_index = (*index)[i];
        if ((it = monitor_param_.find(cur_index)) != monitor_param_.end()) // found
        {
          if (it->second->active_)
          {
            fail += cur_index + " ";
          }
          else
          {
            it->second->active_ = 1;                    // mark dead
            success += cur_index + " ";
          }
        }
        else
        {
          add_index(cur_index, true);
          success += cur_index + " ";
        }
      }

      string err_msg = success.empty() ? "" : "start index " + success + "success\n";
      err_msg += fail.empty() ? "" : "index " + fail + "already running\n";
      message->reply_message(new StatusMessage(fail.empty() ? TFS_SUCCESS : TFS_ERROR,
                                               const_cast<char*>(err_msg.c_str())));
      return TFS_SUCCESS;
    }

    int AdminServer::cmd_stop_monitor_index(AdminCmdMessage* message)
    {
      VSTRING* index = message->get_index();
      string success = "", fail = "";
      MSTR_PARA_ITER it;

      for (size_t i = 0; i < index->size(); i++)
      {
        string& cur_index = (*index)[i];
        if ((it = monitor_param_.find(cur_index)) != monitor_param_.end()) // found
        {
          if (!it->second->active_)
          {
            fail += cur_index + " ";
          }
          else
          {
            it->second->active_ = 0;
            success += cur_index + " ";
          }
        }
        else
        {
          fail += cur_index + " ";
        }
      }

      string err_msg = success.empty() ? "" : "stop index " + success + "success\n";
      err_msg += fail.empty() ? "" : "index " + fail + "not running\n";
      message->reply_message(new StatusMessage(fail.empty() ? TFS_SUCCESS : TFS_ERROR,
                                               const_cast<char*>(err_msg.c_str())));
      return TFS_SUCCESS;
    }

    int AdminServer::cmd_exit(message::AdminCmdMessage* message)
    {
      message->reply_message(new StatusMessage(TFS_SUCCESS, "adminserver exit"));
      return stop();
    }

  }
}
////////////////////////////////
