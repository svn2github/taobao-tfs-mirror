/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id:
 *
 * Authors:
 *   qixiao <qixiao.zs@alibaba-inc.com>
 *      - initial release
 *
 */


#include "handle_task_helper.h"

#include <malloc.h>
#include "common/func.h"
#include "common/tairengine_helper.h"
#include "common/client_manager.h"
#include "exp_server_manager.h"
#include "message/expire_message.h"

using namespace std;
namespace tfs
{
  using namespace common;
  namespace exprootserver
  {
    const int32_t KEY_BUFF_SIZE = 512 + 8 + 8;
    const int32_t VALUE_BUFF_SIZE = 512;
    const int32_t SCAN_LIMIT = 1000;

    const int8_t MAX_RETRY_COUNT = 2;
    const int16_t MAX_TIMEOUT_MS = 500;

    const int64_t INT64_INFI = 0x7FFFFFFFFFFFFFFFL;
    const int32_t INT32_INFI = 0x7FFFFFFF;

    enum
    {
      CMD_RANGE_ALL = 1,
      CMD_RANGE_VALUE_ONLY,
      CMD_RANGE_KEY_ONLY,
    };

    HandleTaskHelper::HandleTaskHelper(ExpServerManager &manager)
      :kv_engine_helper_(NULL), lifecycle_area_(0),
      assign_task_thread_(0), task_period_(0),
      note_interval_(0), destroy_(false), manager_(manager)
    {
    }

    HandleTaskHelper::~HandleTaskHelper()
    {
    }

    int HandleTaskHelper::destroy()
    {
      destroy_ = true;
      if (0 != assign_task_thread_)
      {
        assign_task_thread_->join();
        assign_task_thread_ = 0;
      }
      return TFS_SUCCESS;
    }

    int HandleTaskHelper::init(common::KvEngineHelper* kv_engine_helper)
    {
      int ret = TFS_SUCCESS;

      if (NULL == kv_engine_helper)
      {
        ret = TFS_ERROR;
      }
      else
      {
        kv_engine_helper_ = kv_engine_helper;
      }

      if (TFS_SUCCESS == ret)
      {
        lifecycle_area_ = SYSPARAM_EXPIREROOTSERVER.lifecycle_area_;
        task_period_ = SYSPARAM_EXPIREROOTSERVER.task_period_;
        note_interval_ = SYSPARAM_EXPIREROOTSERVER.note_interval_;
        assign_task_thread_ = new AssignTaskThreadHelper(*this);
      }

      return ret;
    }

    int HandleTaskHelper::handle_finish_task(const uint64_t es_id, const common::ExpireTaskInfo& task)
    {
      mutex_running_.lock();
      map<uint64_t, set<common::ExpireTaskInfo> >::iterator it;
      it = m_s_running_tasks_.find(es_id);
      if (it != m_s_running_tasks_.end())
      {
        if(it->second.erase(task) < 1)
        {
        TBSYS_LOG(ERROR, "can not find task");
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "fatal error, rts has no %s task", tbsys::CNetUtil::addrToString(es_id).c_str());
      }
      mutex_running_.unlock();
      return TFS_SUCCESS;
    }

    int HandleTaskHelper::assign(const uint64_t es_id, const ExpireTaskInfo &del_task)
    {
      NewClient* client = NULL;
      int32_t retry_count = 0;
      int32_t iret = TFS_SUCCESS;
      tbnet::Packet* rsp = NULL;

      message::ReqCleanTaskFromRtsMessage msg;
      msg.set_task(del_task);

      do
      {
        ++retry_count;
        client = NewClientManager::get_instance().create_client();
        tbutil::Time start = tbutil::Time::now();
        iret = send_msg_to_server(es_id, client, &msg, rsp, MAX_TIMEOUT_MS);
        tbutil::Time end = tbutil::Time::now();
        if (TFS_SUCCESS == iret)
        {
          TBSYS_LOG(DEBUG, "task_sucess");
          assert(NULL != rsp);
          iret = rsp->getPCode() == STATUS_MESSAGE ? TFS_SUCCESS : TFS_ERROR;
          if (TFS_SUCCESS == iret)
          {
            StatusMessage* rmsg = dynamic_cast<StatusMessage*>(rsp);
            if (rmsg->get_status() == STATUS_MESSAGE_OK)
            {
              NewClientManager::get_instance().destroy_client(client);
              break;
            }
            else
            {
              iret = TFS_ERROR;
            }
          }
        }
        //TBSYS_LOG(DEBUG, "MAX_TIMEOUT_MS: %d, cost: %"PRI64_PREFIX"d, inter: %"PRI64_PREFIX"d", MAX_TIMEOUT_MS, (int64_t)(end - start).toMilliSeconds(), heart_inter_);
        if (TFS_SUCCESS != iret)
        {
          usleep(500000);
        }
        NewClientManager::get_instance().destroy_client(client);
      }
      while ((retry_count < MAX_RETRY_COUNT)
          && (TFS_SUCCESS != iret));

      return iret;
    }

    void HandleTaskHelper::assign_task()
    {
      int ret = TFS_SUCCESS;
      int32_t sleep_time = SYSPARAM_EXPIREROOTSERVER.es_rts_lease_expired_time_;

      while (!destroy_)
      {
        common::VUINT64 v_avaliable_servers;
        manager_.get_available_expire_server(v_avaliable_servers);
        tbutil::Time now = tbutil::Time::now(tbutil::Time::Realtime);

        vector<ServerExpireTask> out_tasks;
        size_t available_index = 0;
        //deal with waiting tasks
        mutex_waitint_.lock();
        for (; available_index < v_avaliable_servers.size(); available_index++)
        {
          if (dq_waiting_tasks_.empty())
          {
            break;
          }
          ExpireTaskInfo del_task = dq_waiting_tasks_.front();
          dq_waiting_tasks_.pop_front();
          out_tasks.push_back(ServerExpireTask(v_avaliable_servers[available_index], del_task));
        }
        mutex_waitint_.unlock();


        //dela with new tasks
        if (now.toSeconds() % task_period_ < sleep_time)
        {
          int sum_num = v_avaliable_servers.size() - available_index;
          for (int i =0; i + available_index < v_avaliable_servers.size(); i++)
          {
            ExpireTaskInfo::ExpireTaskType type = ExpireTaskInfo::TASK_TYPE_DELETE;
            int32_t note_interval = note_interval_;
            ExpireTaskInfo del_task(sum_num, i, now.toSeconds(), 0, note_interval, type);
            out_tasks.push_back(ServerExpireTask(v_avaliable_servers[i + available_index], del_task));
          }
        }

        for (size_t i=0; i < out_tasks.size(); i++)
        {
          ret = assign(out_tasks[i].server_id_, out_tasks[i].task_);
          if (TFS_SUCCESS == ret)
          {
            mutex_running_.lock();
            m_s_running_tasks_[out_tasks[i].server_id_].insert(out_tasks[i].task_);
            mutex_running_.unlock();
          }
          else
          {
            mutex_waitint_.lock();
            dq_waiting_tasks_.push_back(out_tasks[i].task_);
            mutex_waitint_.unlock();
          }
        }

        sleep(sleep_time);
      }

      return ;
    }

    void HandleTaskHelper::AssignTaskThreadHelper::run()
    {
      handle_task_helper_.assign_task();
    }

    int HandleTaskHelper::handle_fail_servers(const common::VUINT64 &down_servers)
    {
      vector<common::ExpireTaskInfo> retry_tasks;

      mutex_running_.lock();
      std::map<uint64_t, set<common::ExpireTaskInfo> >::iterator it;
      set<common::ExpireTaskInfo>::iterator set_it;
      for (uint32_t i = 0; i < down_servers.size(); i++)
      {
        it = m_s_running_tasks_.find(down_servers[i]);
        if (it != m_s_running_tasks_.end())
        {
          for(set_it= it->second.begin(); set_it != it->second.end(); set_it++)
          {
            retry_tasks.push_back(*set_it);
          }
          m_s_running_tasks_.erase(it);
        }
      }
      mutex_running_.unlock();
      mutex_waitint_.lock();
      for (size_t i = 0; i <retry_tasks.size(); i++)
      {
        dq_waiting_tasks_.push_back(retry_tasks[i]);
      }
      mutex_waitint_.unlock();


      return TFS_SUCCESS;
    }

    int HandleTaskHelper::query_task(const uint64_t es_id, std::vector<common::ServerExpireTask>* p_running_tasks)
    {
      map<uint64_t, set<common::ExpireTaskInfo> >::iterator it;
      mutex_running_.lock();
      if (es_id != 0)
      {
        it = m_s_running_tasks_.find(es_id);
        if (it != m_s_running_tasks_.end())
        {
          set<common::ExpireTaskInfo>::iterator it_set;
          for(it_set = it->second.begin(); it_set != it->second.end(); ++it_set)
          {
            common::ServerExpireTask one_task;
            one_task.server_id_ = es_id;
            one_task.task_ = *it_set;
            p_running_tasks->push_back(one_task);
          }
        }
      }
      else
      {
        for (it = m_s_running_tasks_.begin(); it != m_s_running_tasks_.end(); ++it)
        {
          set<common::ExpireTaskInfo>::iterator it_set;
          for(it_set = it->second.begin(); it_set != it->second.end(); ++it_set)
          {
            common::ServerExpireTask one_task;
            one_task.server_id_ = it->first;
            one_task.task_ = *it_set;
            p_running_tasks->push_back(one_task);
          }
        }
      }
      mutex_running_.unlock();
      return TFS_SUCCESS;
    }

  }// end for exprootserver
}// end for tfs

