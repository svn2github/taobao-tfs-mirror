/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: meta_server_service.cpp 49 2011-08-08 09:58:57Z nayan@taobao.com $
 *
 * Authors:
 *   qixiao <qixiao.zs@alibaba-inc.com>
 *      - initial release
 *
 */

#include "expire_service.h"

#include <time.h>
#include <zlib.h>
#include "common/config_item.h"
#include "common/parameter.h"
#include "common/base_packet.h"
#include "common/mysql_cluster/mysql_engine_helper.h"
#include "common/client_manager.h"

using namespace tfs::common;
using namespace tfs::message;
using namespace std;

namespace tfs
{
  namespace expireserver
  {
    static int32_t FINISH_TASK_MAX_RETRY_COUNT = 3;
    static int32_t MAX_TIMEOUT_MS = 500;

    ExpireService::ExpireService()
      :clean_task_helper_(),heart_manager_(clean_task_helper_)
    {
    }

    ExpireService::~ExpireService()
    {
    }

    tbnet::IPacketStreamer* ExpireService::create_packet_streamer()
    {
      return new BasePacketStreamer();
    }

    void ExpireService::destroy_packet_streamer(tbnet::IPacketStreamer* streamer)
    {
      tbsys::gDelete(streamer);
    }

    BasePacketFactory* ExpireService::create_packet_factory()
    {
      return new message::MessageFactory();
    }

    void ExpireService::destroy_packet_factory(BasePacketFactory* factory)
    {
      tbsys::gDelete(factory);
    }

    const char* ExpireService::get_log_file_path()
    {
      const char* log_file_path = NULL;
      const char* work_dir = get_work_dir();
      if (work_dir != NULL)
      {
        log_file_path_ = work_dir;
        log_file_path_ += "/logs/expireserver";
        log_file_path_ +=  ".log";
        log_file_path = log_file_path_.c_str();
      }
      return log_file_path;
    }

    const char* ExpireService::get_pid_file_path()
    {
      const char* pid_file_path = NULL;
      const char* work_dir = get_work_dir();
      if (work_dir != NULL)
      {
        pid_file_path_ = work_dir;
        pid_file_path_ += "/logs/expireserver";
        pid_file_path_ += ".pid";
        pid_file_path = pid_file_path_.c_str();
      }
      return pid_file_path;
    }

    int ExpireService::initialize(int argc, char* argv[])
    {
      int ret = TFS_SUCCESS;
      UNUSED(argc);
      UNUSED(argv);

      if ((ret = SYSPARAM_EXPIRESERVER.initialize(config_file_)) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "call SYSPARAM_EXPIRESERVER::initialize fail. ret: %d", ret);
      }

      if (TFS_SUCCESS == ret)
      {
        kv_engine_helper_ = new MysqlEngineHelper(SYSPARAM_EXPIRESERVER.conn_str_,
            SYSPARAM_EXPIRESERVER.user_name_,
            SYSPARAM_EXPIRESERVER.pass_wd_);
        ret = kv_engine_helper_->init();
      }

      if (TFS_SUCCESS == ret)
      {
        ret = clean_task_helper_.init(kv_engine_helper_);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "Expire server initial fail: %d", ret);
        }
      }

      //init expire heart
      if (TFS_SUCCESS == ret)
      {
        bool es_ip_same_flag = false;
        es_ip_same_flag = tbsys::CNetUtil::isLocalAddr(SYSPARAM_EXPIRESERVER.es_ip_port_);

        if (true == es_ip_same_flag)
        {
          local_ipport_id_ = SYSPARAM_EXPIRESERVER.es_ip_port_;
          expireroot_ipport_id_ = SYSPARAM_EXPIRESERVER.ers_ip_port_;
          server_start_time_ = time(NULL);
          ret = heart_manager_.initialize(expireroot_ipport_id_, local_ipport_id_,
              server_start_time_, get_work_thread_count() - 1);
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "init expire heart_manager error");
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "is not local ip ret: %d", ret);
          ret = TFS_ERROR;
        }
      }
      return ret;
    }

    int ExpireService::destroy_service()
    {
      heart_manager_.destroy();
      delete kv_engine_helper_;
      kv_engine_helper_ = NULL;
      return TFS_SUCCESS;
    }

    bool ExpireService::handlePacketQueue(tbnet::Packet *packet, void *args)
    {
      int ret = true;
      BasePacket* base_packet = NULL;
      if (!(ret = BaseService::handlePacketQueue(packet, args)))
      {
        TBSYS_LOG(ERROR, "call BaseService::handlePacketQueue fail. ret: %d", ret);
      }
      else
      {
        base_packet = dynamic_cast<BasePacket*>(packet);
        switch (base_packet->getPCode())
        {
          case REQ_EXPIRE_CLEAN_TASK_MESSAGE:
            ret = do_clean_task(dynamic_cast<ReqCleanTaskFromRtsMessage*>(base_packet));
            break;
          default:
            ret = EXIT_UNKNOWN_MSGTYPE;
            TBSYS_LOG(ERROR, "unknown msg type: %d", base_packet->getPCode());
            break;
        }
      }

      if (ret != TFS_SUCCESS && NULL != base_packet)
      {
        base_packet->reply_error_packet(TBSYS_LOG_LEVEL(INFO), ret, "execute message failed");
      }

      // always return true. packet will be freed by caller
      return true;
    }

    int ExpireService::do_clean_task(ReqCleanTaskFromRtsMessage* req_clean_task_msg)
    {
      //we will give a resp to rootserver first,
      //then execute the clean task and report task result to rootserver
      int ret = TFS_SUCCESS;
      int rspret = TFS_SUCCESS;
      if (NULL == req_clean_task_msg)
      {
        ret = EXIT_INVALID_ARGU;
        TBSYS_LOG(ERROR, "ExpireService::req_clean_task fail, ret: %d", ret);
      }

      ExpireTaskInfo::ExpireTaskType task_type;
      int32_t total_es;
      int32_t num_es;
      int32_t note_interval;
      int32_t task_time;
      ExpireTaskInfo task;

      if (TFS_SUCCESS == ret)
      {
        task = req_clean_task_msg->get_task();
        task_type = task.type_;
        total_es = task.alive_total_;
        num_es =  task.assign_no_;
        note_interval = task.note_interval_;
        task_time = task.spec_time_;
        if ((task_type == ExpireTaskInfo::TASK_TYPE_DELETE) &&
            (total_es > 0 && num_es >= 0 && note_interval >= 0) &&
            (num_es < total_es) && task_time > 0)
        {
          TBSYS_LOG(DEBUG, "%d, %d, %d, %d, %d",task_type, total_es, num_es, note_interval, task_time);
          ret = TFS_SUCCESS;
        }
        else
        {
          ret = EXIT_INVALID_ARGU;
          TBSYS_LOG(ERROR, "ExpireService::req_clean_task fail EXIT_INVALID_ARGU, ret: %d", ret);
        }
      }

      if (TFS_SUCCESS != ret)
      {
        rspret = req_clean_task_msg->reply_error_packet(TBSYS_LOG_LEVEL(INFO), ret, "clean is not start");
      }
      else
      {
        rspret = req_clean_task_msg->reply(new StatusMessage(STATUS_MESSAGE_OK));
      }

      if (TFS_SUCCESS == ret)
      {
        //tbutil::Time start = tbutil::Time::now();
        if (ExpireTaskInfo::TASK_TYPE_DELETE == task_type)
        {
          ret = clean_task_helper_.do_clean_task(local_ipport_id_, total_es, num_es, note_interval, task_time);
        }
        //tbutil::Time end = tbutil::Time::now();
        //TBSYS_LOG(INFO, "put_object cost: %"PRI64_PREFIX"d", (int64_t)(end - start).toMilliSeconds());

        // send finish msg
        ReqFinishTaskFromEsMessage req_finish_task_msg;
        //req_finish_task_msg.set_reserve(0);
        req_finish_task_msg.set_es_id(local_ipport_id_);
        req_finish_task_msg.set_task(task);

        NewClient* client = NULL;
        int32_t retry_count = 0;
        int32_t iret = TFS_SUCCESS;
        tbnet::Packet* response = NULL;
        do
        {
          ++retry_count;
          client = NewClientManager::get_instance().create_client();
          iret = send_msg_to_server(expireroot_ipport_id_, client, &req_finish_task_msg, response, MAX_TIMEOUT_MS);
          if (TFS_SUCCESS != iret)
          {
            TBSYS_LOG(ERROR, "EXIT_NETWORK_ERROR ret: %d",iret);
            iret = EXIT_NETWORK_ERROR;
          }
          else if (STATUS_MESSAGE == response->getPCode())
          {
            StatusMessage* resp_status_msg = dynamic_cast<StatusMessage*>(response);
            if ((iret = resp_status_msg->get_status()) != STATUS_MESSAGE_OK)
            {
              TBSYS_LOG(ERROR, "es root return error, ret: %d", ret);
            }
          }
          else
          {
            ret = EXIT_UNKNOWN_MSGTYPE;
            TBSYS_LOG(ERROR, "rsp fail,"
                "server_addr: %s,"
                "ret: %d, msg type: %d",
                tbsys::CNetUtil::addrToString(expireroot_ipport_id_).c_str(), ret, response->getPCode());
          }
          NewClientManager::get_instance().destroy_client(client);
        }while ((retry_count < FINISH_TASK_MAX_RETRY_COUNT)
            && (TFS_SUCCESS != iret));
      }
      return ret;
    }

  }/** expireserver **/
}/** tfs **/
