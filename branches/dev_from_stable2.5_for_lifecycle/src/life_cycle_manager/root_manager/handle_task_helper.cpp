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
    const int32_t TASK_PERIOD_SECONDS = 60 * 60;

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
      assign_task_thread_(0), manager_(manager)
    {
    }

    HandleTaskHelper::~HandleTaskHelper()
    {
    }

    int HandleTaskHelper::destroy()
    {
      kv_engine_helper_ = NULL;
      if (0 != assign_task_thread_)
      {
        assign_task_thread_->join();
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
        assign_task_thread_ = new AssignTaskThreadHelper(*this);
        lifecycle_area_ = SYSPARAM_EXPIREROOTSERVER.lifecycle_area_;
      }

      return ret;
    }

    int HandleTaskHelper::query_progress(const uint64_t es_id, const int32_t num_es, const int32_t task_time,
        const int32_t hash_bucket_num, const ExpireTaskType type,
        int64_t *sum_file_num, int32_t *current_percent)
    {
      UNUSED(type);

      int ret = TFS_SUCCESS;

      ret = (NULL == sum_file_num || NULL == current_percent) ? TFS_ERROR : TFS_SUCCESS;

      char *start_key_buff = (char*)malloc(KEY_BUFF_SIZE);
      char *end_key_buff = (char*)malloc(KEY_BUFF_SIZE);

      if (NULL == start_key_buff || NULL == end_key_buff)
      {
        ret = TFS_ERROR;
      }

      KvKey start_key;
      KvKey end_key;

      if (TFS_SUCCESS == ret)
      {
        //has assign hash_bucket_num
        ret = ExpireDefine::serialize_es_stat_key(es_id, num_es,
            task_time, hash_bucket_num > 0 ? hash_bucket_num : 0,
            0, &start_key,
            start_key_buff, KEY_BUFF_SIZE);

        if (TFS_SUCCESS == ret)
        {
          ret = ExpireDefine::serialize_es_stat_key(es_id, num_es, task_time,
              hash_bucket_num > 0 ? hash_bucket_num : INT32_INFI,
              INT64_INFI, &end_key,
              end_key_buff, KEY_BUFF_SIZE);
        }
      }

      uint64_t last_id = 0;
      int32_t last_num_es = 0;
      int32_t last_task_time = 0;
      int32_t last_hash_bucket_num = 0;
      int64_t last_sum_file_num = 0;

      //op value
      vector<KvValue*> kv_value_keys;
      vector<KvValue*> kv_value_values;
      int32_t offset = 0;
      int32_t limit = 1000;
      int32_t res_size = 0;

      //total_hash_bucket_num means every es should have xxx hash_bucket_nums
      int32_t total_hash_bucket_num = 0;

      if (TFS_SUCCESS == ret)
      {
        mutex_task_.lock();
        map<uint64_t, ExpireDeleteTask>::iterator iter = m_task_info_.find(es_id);
        if (iter != m_task_info_.end())
        {
          total_hash_bucket_num = ExpireDefine::HASH_BUCKET_NUM/(iter->second).alive_total_;
        }
        else
        {
          ret = TFS_ERROR;
          TBSYS_LOG(ERROR, "es_id: %s's task is complete", tbsys::CNetUtil::addrToString(es_id).c_str());
        }
        mutex_task_.unlock();
      }


      while (TFS_SUCCESS == ret)
      {
        bool loop = true;
        ret = kv_engine_helper_->scan_keys(lifecycle_area_, start_key, end_key, limit, offset, &kv_value_keys, &kv_value_values, &res_size, CMD_RANGE_ALL);

        if (TFS_SUCCESS != ret && EXIT_KV_RETURN_HAS_MORE_DATA != ret)
        {
          TBSYS_LOG(ERROR, "get_range_fail, ret: %d", ret);
        }
        else if (EXIT_KV_RETURN_HAS_MORE_DATA == ret && res_size > 0)
        {
          ret = TFS_SUCCESS;
          start_key.key_ = kv_value_keys[res_size -1]->get_data();
          start_key.key_size_ = kv_value_keys[res_size -1]->get_size();
          offset = 1;
        }
        else if (TFS_SUCCESS == ret)
        {
          if (res_size > 0)
          {
            ExpireDefine::deserialize_es_stat_key(kv_value_keys[res_size -1]->get_data(), kv_value_keys[res_size -1]->get_size(), &last_id, &last_num_es, &last_task_time, &last_hash_bucket_num, &last_sum_file_num);
          }
          if (res_size == limit)
          {
            ret = ExpireDefine::serialize_es_stat_key(es_id, num_es, task_time, last_hash_bucket_num,
                                                     INT64_INFI, &start_key,
                                                     start_key_buff, KEY_BUFF_SIZE);
          }
          else if (res_size < limit)
          {
            //TBSYS_LOG(INFO, "res_size: %d", res_size);
            loop = false;
          }
        }

        for (int i = 0; i < res_size; ++i)//free kv
        {
          kv_value_values[i]->free();
        }
        kv_value_values.clear();

        if (!loop)
        {
          break;
        }
      }

      if (TFS_SUCCESS == ret)
      {
        if (hash_bucket_num == 0 && total_hash_bucket_num > 0)
        {
          *current_percent = (last_hash_bucket_num - total_hash_bucket_num * num_es) * 100 / total_hash_bucket_num;
        }
        else
        {
          *sum_file_num = last_sum_file_num;
        }
      }

      if (NULL != start_key_buff)
      {
        free(start_key_buff);
        start_key_buff = NULL;
      }

      if (NULL != end_key_buff)
      {
        free(end_key_buff);
        end_key_buff = NULL;
      }

      return ret;
    }

    int HandleTaskHelper::handle_finish_task(const uint64_t es_id)
    {
      mutex_task_.lock();
      TASK_INFO_ITER iter = m_task_info_.find(es_id);
      if (iter != m_task_info_.end())
      {
        m_task_info_.erase(iter);
      }
      else
      {
        TBSYS_LOG(ERROR, "fatal error, rts has no %s assign task", tbsys::CNetUtil::addrToString(es_id).c_str());
      }
      mutex_task_.unlock();
      return TFS_SUCCESS;
    }

    int HandleTaskHelper::assign(const uint64_t es_id, const ExpireDeleteTask &del_task)
    {
      NewClient* client = NULL;
      int32_t retry_count = 0;
      int32_t iret = TFS_SUCCESS;
      tbnet::Packet* rsp = NULL;

      message::ReqCleanTaskFromRtsMessage msg;
      msg.set_total_es(del_task.alive_total_);
      msg.set_num_es(del_task.assign_no_);
      msg.set_task_time(del_task.spec_time_);
      msg.set_task_type(del_task.type_);
      msg.set_note_interval(del_task.note_interval_);

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

    int HandleTaskHelper::assign_task()
    {
      int ret = TFS_SUCCESS;

      while (true)
      {
        ExpTable exp_table;
        ret = manager_.get_table(exp_table);

        tbutil::Time now = tbutil::Time::now(tbutil::Time::Realtime);

        //check arrive clock
        if (now.toSeconds() % TASK_PERIOD_SECONDS == 0)
        {
          mutex_task_wait_.lock();
          if (task_wait_.empty())
          {
            mutex_task_wait_.unlock();
            uint32_t num = exp_table.v_exp_table_.size();

            for (uint32_t i = 0; i < num; i++)
            {
              // if es has handled in note_interval second, should record in tair
              ExpireTaskType type = RAW;
              int32_t note_interval = 200;
              ExpireDeleteTask del_task(num, i, now.toSeconds(), 0, note_interval, type);
              mutex_task_wait_.lock();
              task_wait_.push_back(del_task);
              mutex_task_wait_.unlock();
            }
          }
          else
          {
            mutex_task_wait_.unlock();
            TBSYS_LOG(INFO, "still have %lu task wait to assign", task_wait_.size());
          }
        }

        mutex_task_wait_.lock();
        if (!exp_table.v_idle_table_.empty() && !task_wait_.empty())
        {
          mutex_task_wait_.unlock();
          for (uint32_t i = 0; i < exp_table.v_idle_table_.size(); i++)
          {
            mutex_task_wait_.lock();
            ExpireDeleteTask del_task = task_wait_.front();
            task_wait_.pop_front();
            mutex_task_wait_.unlock();

            //TBSYS_LOG(INFO, "begin to assign msg to %s", tbsys::CNetUtil::addrToString(exp_table.v_idle_table_[i]).c_str());
            ret = assign(exp_table.v_idle_table_[i], del_task);

            if (TFS_SUCCESS != ret)
            {
              mutex_task_wait_.lock();
              task_wait_.push_back(del_task);
              mutex_task_wait_.unlock();
            }
            else
            {
              mutex_task_.lock();
              TASK_INFO_ITER iter = m_task_info_.find(exp_table.v_idle_table_[i]);
              if (iter == m_task_info_.end())
              {
                m_task_info_[exp_table.v_idle_table_[i]] = del_task;
              }
              else
              {
                TBSYS_LOG(ERROR, "%s has task, should not assign task",
                    tbsys::CNetUtil::addrToString(iter->first).c_str());
              }
              mutex_task_.unlock();
            }
          }
        }
        else
        {
          mutex_task_wait_.unlock();
        }

        sleep(1);
      }

      return ret;
    }

    //for unit test
    void HandleTaskHelper::get_task_info(map<uint64_t, ExpireDeleteTask> &m_task_info)
    {
      mutex_task_.lock();
      m_task_info = m_task_info_;
      mutex_task_.unlock();
    }

    void HandleTaskHelper::put_task_info(const map<uint64_t, ExpireDeleteTask> &m_task_info)
    {
      mutex_task_.lock();
      m_task_info_ = m_task_info;
      mutex_task_.unlock();
    }

    void HandleTaskHelper::get_task_wait(deque<ExpireDeleteTask> &task_wait)
    {
      mutex_task_wait_.lock();
      task_wait = task_wait_;
      mutex_task_wait_.unlock();
    }

    void HandleTaskHelper::AssignTaskThreadHelper::run()
    {
      handle_task_helper_.assign_task();
    }

    int HandleTaskHelper::handle_fail_servers(const common::VUINT64 &down_servers)
    {
      for (uint32_t i = 0; i < down_servers.size(); i++)
      {
        TASK_INFO_ITER iter;
        mutex_task_.lock();
        if ((iter = m_task_info_.find(down_servers[i])) != m_task_info_.end())
        {
          ExpireDeleteTask del_task = iter->second;
          m_task_info_.erase(iter);
          mutex_task_.unlock();
          mutex_task_wait_.lock();
          task_wait_.push_back(del_task);
          mutex_task_wait_.unlock();
        }
        else
        {
          mutex_task_.unlock();
        }
      }
      return TFS_SUCCESS;
    }
  }// end for exprootserver
}// end for tfs

