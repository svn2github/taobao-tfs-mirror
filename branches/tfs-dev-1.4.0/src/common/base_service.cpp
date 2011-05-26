/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: base_service.cpp 185 2011-04-21 14:44:43Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 *
 */
#include <tblog.h>
#include <libgen.h>
#include "error_msg.h"
#include "config_item.h"
#include "base_service.h"
#include "directory_op.h"
#include "local_packet.h"

namespace tfs
{
  namespace common
  {
    int BaseService::golbal_async_callback_func(NewClient* client, void* args)
    {
      BaseService* service = dynamic_cast<BaseService*>(BaseService::instance());
      if (NULL != service)
      {
        return service->async_callback(client, args);
      }
      return TFS_ERROR;
    }

    BaseService::BaseService():
      Service(),
      packet_factory_(NULL),
      streamer_(NULL),
      timer_(0),
      work_queue_size_(10240)
    {

    }
    
    BaseService::~BaseService()
    {

    }

    bool BaseService::destroy()
    {
      NewClientManager::get_instance().destroy();
      transport_.stop();
      destroy_service();
      main_workers_.stop();
      if (0 != timer_)
      {
        timer_->destroy();
      }

      transport_.wait();
      main_workers_.wait();

      destroy_packet_factory(packet_factory_);
      destroy_packet_streamer(streamer_);
      return true;
    }

    int BaseService::async_callback(NewClient* client, void* args)
    {
      assert(NULL != client);
      LocalPacket* packet = dynamic_cast<LocalPacket*>(packet_factory_->createPacket(LOCAL_PACKET));
      assert(NULL != packet);
      packet->set_new_client(client);
      bool bret = main_workers_.push(packet, 0/*no limit*/, false/*no block*/);
      assert(true == bret);
      return TFS_SUCCESS;
    }

    bool BaseService::push(BasePacket* packet)
    {
      return main_workers_.push(packet, work_queue_size_);
    }

    tbnet::IPacketHandler::HPRetCode BaseService::handlePacket(tbnet::Connection *connection, tbnet::Packet *packet)
    {
      TBSYS_LOG(DEBUG, "packet code : %d", packet->getPCode());
      bool bret = NULL != connection && NULL != packet;
      if (bret)
      {
        if (!packet->isRegularPacket())
        {
          bret = false;
          TBSYS_LOG(WARN, "control packet, pcode: %d", dynamic_cast<tbnet::ControlPacket*>(packet)->getCommand());
        }
        if (bret)
        {
          BasePacket* bpacket = dynamic_cast<BasePacket*>(packet);
          bpacket->set_connection(connection);
          bpacket->setExpireTime(MAX_RESPONSE_TIME);
          bpacket->set_direction(static_cast<DirectionStatus>(bpacket->get_direction()|DIRECTION_RECEIVE));

          if (bpacket->is_enable_dump())
          {
            bpacket->dump(); 
          }
          if (!main_workers_.push(bpacket, work_queue_size_))
          {
            bpacket->reply_error_packet(TBSYS_LOG_LEVEL(ERROR),STATUS_MESSAGE_ERROR, "%s, task message beyond max queue size, discard", get_ip_addr());
          }
        }
      }
      return tbnet::IPacketHandler::FREE_CHANNEL;
    }

    /** Note if return true, PacketQueueThread will delete this packet*/
    bool BaseService::handlePacketQueue(tbnet::Packet *packet, void *args)
    {
      bool bret = true;
      if (NULL == packet)
      {
        bret = false;
        TBSYS_LOG(ERROR, "%s", "invalid packet, packet is null");
      }
      else
      {
        if (LOCAL_PACKET == packet->getPCode())
        {
          LocalPacket* local_packet = dynamic_cast<LocalPacket*>(packet);          
          int32_t iret = local_packet->execute();
          if (TFS_SUCCESS != iret)
          {
            TBSYS_LOG(ERROR, " LocalPacket execute error, iret: %d", iret);
          }
        }
      }
      return bret;
    }

    const char* BaseService::get_work_dir() const
    {
      return TBSYS_CONFIG.getString(CONF_SN_PUBLIC, CONF_WORK_DIR, NULL);
    }

    int32_t BaseService::get_port() const
    {
      int32_t port = -1;
      port = TBSYS_CONFIG.getInt(CONF_SN_PUBLIC, CONF_PORT, -1);
      if (port < 1024 || port > 65535)
      {
        port = -1;
      }
      return port;
    }

    const char* BaseService::get_log_file_level() const
    {
      const char* level = TBSYS_CONFIG.getString(CONF_SN_PUBLIC, CONF_LOG_LEVEL, "debug");
      return level;
    }

    const char* BaseService::get_log_path() const
    {
      return log_file_path_.empty() ? NULL : log_file_path_.c_str();
    }

    int64_t BaseService::get_log_file_size() const
    {
      return TBSYS_CONFIG.getInt(CONF_SN_PUBLIC, CONF_LOG_SIZE, 0x40000000);
    }

    int32_t BaseService::get_log_file_count() const
    {
      return TBSYS_CONFIG.getInt(CONF_SN_PUBLIC, CONF_LOG_NUM, 16);
    }

    const char* const BaseService::get_dev() const
    {
      return TBSYS_CONFIG.getString(CONF_SN_PUBLIC, CONF_DEV_NAME, NULL);
    }

    int32_t BaseService::get_work_thread_count() const
    {
      return TBSYS_CONFIG.getInt(CONF_SN_PUBLIC, CONF_THREAD_COUNT, 8);
    }

    int32_t BaseService::get_work_queue_size() const
    {
      return work_queue_size_;
    }

    const char* BaseService::get_ip_addr() const
    {
      return TBSYS_CONFIG.getString(CONF_SN_PUBLIC, CONF_IP_ADDR, NULL);
    }

    int BaseService::run(int argc , char*argv[], const std::string& config, std::string& error_msg)
    {
      char buf[256];
      //load config file
      int32_t iret = TBSYS_CONFIG.load(config.c_str());
      iret = EXIT_SUCCESS == iret ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS != iret)
      {
        snprintf(buf, 256, "load config file '%s' fail", config.c_str());
        error_msg = buf;
      }

      if (TFS_SUCCESS == iret)
      {
        iret = parse_common_line_args(argc, argv);
        if (TFS_SUCCESS != iret)
        {
          snprintf(buf, 256, "%s parse common line args fail", argv[0]);
          error_msg = buf;
        }
      }

      //create workdir & create logdir && create log file
      if (TFS_SUCCESS == iret)
      {
        iret = initialize_work_dir(argv[0], error_msg);
        if (TFS_SUCCESS != iret)
        {
          snprintf(buf, 256, "%s initialize work directory fail", argv[0]);
          error_msg = buf;
        }
      }

      //check ip, port, starrt tbnet
      if (TFS_SUCCESS == iret)
      {
        iret = initialize_network(argv[0], error_msg);
        if (TFS_SUCCESS != iret)
        {
          snprintf(buf, 256, "%s initialize network fail", argv[0]);
          error_msg = buf;
        }
      }

      //start workthread
      if (TFS_SUCCESS == iret)
      {
        int32_t thread_count = get_work_thread_count();
        main_workers_.setThreadParameter(thread_count, this, NULL);        
        main_workers_.start();

        work_queue_size_ = TBSYS_CONFIG.getInt(CONF_SN_PUBLIC, CONF_TASK_MAX_QUEUE_SIZE, 10240);
        work_queue_size_ = std::max(work_queue_size_, 10240);
        work_queue_size_ = std::min(work_queue_size_, 40960);
        timer_ = new tbutil::Timer();
      }

      if (TFS_SUCCESS == iret)
      {
        iret = initialize(argc, argv);
        if (TFS_SUCCESS != iret)
        {
          snprintf(buf, 256, "%s initialize user data fail", argv[0]);
          error_msg = buf;
        }
      }
      return iret;
    }

    int BaseService::interruptCallback(int sig)
    {
      switch (sig)
      {
       case SIGTERM:
       case SIGINT:
        destroy();
        break;
      }
      return TFS_SUCCESS;
    }
    
    int BaseService::initialize_work_dir(const char* app_name, std::string& error_msg)
    {
      int32_t iret = TFS_SUCCESS;
      char buf[256];
      const char* work_dir = get_work_dir();
      if (NULL == work_dir)
      {
        snprintf(buf, 256, "%s not set workdir", app_name);
        error_msg = buf;
        iret = EXIT_CONFIG_ERROR;
      }

      if (TFS_SUCCESS == iret)
      {
        if (!DirectoryOp::create_full_path(work_dir))
        {
          snprintf(buf, 256, "%s create workdir(%s) fail ", app_name, work_dir);
          error_msg = buf;
          iret = EXIT_MAKEDIR_ERROR;
        }
        else
        {
          const char* const tmp_path = get_log_file_path();
          std::string log_path(NULL == tmp_path ? "" : tmp_path);
          if (log_path.empty()
              || log_path == "")
          {
            log_path = work_dir;
            log_path += "/logs/";
            std::string tmp(app_name);
            std::string::size_type pos = tmp.find_last_of('/');
            std::string name = tmp.substr(pos);
            if (!name.empty() && name.c_str()[0] == '/')
            {
              name = tmp.substr(pos + 1);
            }
            log_path += std::string::npos == pos || name.empty() ? "base_service" : name;
            log_path += ".log";
            log_file_path_ = log_path;
          }

          if (0 == access(log_path.c_str(), R_OK))
          {
            TBSYS_LOGGER.rotateLog(log_path.c_str());
          }
          else
          {
            if (!DirectoryOp::create_full_path(log_path.c_str(), true))
            {
              snprintf(buf, 256, "%s create log directory(%s) fail ", app_name, dirname(const_cast<char*>(log_path.c_str())));
              error_msg = buf;
              iret = EXIT_MAKEDIR_ERROR;
            }
          }
          if (TFS_SUCCESS == iret)
          {
            TBSYS_LOGGER.setFileName(log_path.c_str());
            TBSYS_LOGGER.setLogLevel(get_log_file_level());
            TBSYS_LOGGER.setMaxFileSize(get_log_file_size());
            TBSYS_LOGGER.setMaxFileIndex(get_log_file_count());
          }
        }
      }
      return iret;
    }

    int BaseService::initialize_network(const char* app_name, std::string& error_msg)
    {
      char buf[256];
      int32_t iret = TFS_SUCCESS;
      const char* ip_addr = get_ip_addr();
      if (NULL == ip_addr)//get ip addr
      {
        iret =  EXIT_CONFIG_ERROR;
        snprintf(buf, 256, "%s not set ip_addr", app_name);
        error_msg = buf;
      }

      if (TFS_SUCCESS == iret)
      {
        const char *dev_name = get_dev();
        if (NULL == dev_name)//get dev name
        {
          iret =  EXIT_CONFIG_ERROR;
          snprintf(buf, 256, "%s not set dev_name", app_name);
          error_msg = buf;
        }
        else
        {
          uint64_t local_ip = Func::get_local_addr(dev_name);
          if (Func::get_addr(ip_addr) != local_ip) //check ip
          {
            iret = EXIT_GENERAL_ERROR;
            snprintf(buf, 256, "ip '%s' is not local ip, local ip: %s",ip_addr, tbsys::CNetUtil::addrToString(local_ip).c_str());
            error_msg = buf;
          }
        }
      }

      int32_t port = 0;
      if (TFS_SUCCESS == iret)
      {
        port  = get_listen_port();//check port
        if (port <= 0)
        {
          iret = EXIT_CONFIG_ERROR;
          snprintf(buf, 256, "%s not set port or port is invalid", app_name);
          error_msg = buf;
        }
      }

      //start transport
      if (TFS_SUCCESS == iret)
      {
        char spec[32];
        sprintf(spec, "tcp::%d", port);
        
        packet_factory_ = create_packet_factory();
        if (NULL == packet_factory_)
        {
          iret = EXIT_GENERAL_ERROR;
          snprintf(buf, 256, "%s create packet factory fail", app_name);
          error_msg = buf;
        }
        if (TFS_SUCCESS == iret)
        {
          streamer_ = dynamic_cast<BasePacketStreamer*>(create_packet_streamer());
          if (NULL == streamer_)
          {
            snprintf(buf, 256, "%s create packet streamer fail", app_name);
            error_msg = buf;
            iret = EXIT_GENERAL_ERROR; 
          }
          else
          {
            streamer_->set_packet_factory(packet_factory_);
            tbnet::IOComponent* com = transport_.listen(spec, streamer_, this);
            if (NULL == com)
            {
              snprintf(buf, 256, "%s listen port: %d fail", app_name, port);
              error_msg = buf;
              iret = EXIT_NETWORK_ERROR;
            }
            else
            {
              transport_.start();        
            }
          }
        }
      }

      // start client manager
      if (TFS_SUCCESS == iret)
      {
        iret = NewClientManager::get_instance().initialize(packet_factory_, streamer_, 
                &transport_, &BaseService::golbal_async_callback_func, this);
        if (TFS_SUCCESS != iret)
        {
          snprintf(buf, 256, "%s start client manager fail", app_name);
          error_msg = buf;
          iret = EXIT_NETWORK_ERROR;
        }
      }
      return iret;
    }
  }
}
