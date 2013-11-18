/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: $
 *
 * Authors:
 *   xueya.yy <xueya.yy@taobao.com>
 *      - initial release
 *
 */

#include "exp_server_manager.h"
#include <Time.h>
#include "common/define.h"
#include "common/func.h"
#include "common/lock.h"
#include "common/internal.h"
#include "common/error_msg.h"
#include "common/kv_rts_define.h"
#include "common/atomic.h"
#include "common/parameter.h"
#include "exp_root_server.h"

using namespace tfs::common;

namespace tfs
{
  namespace exprootserver
  {
    static const int32_t max_check_interval = 10;
    ExpServerManager::ExpServerManager(HandleTaskHelper &handle_task_helper):
      check_es_lease_thread_(0),
      initialize_(false),
      destroy_(false),
      handle_task_helper_(handle_task_helper)
    {

    }

    ExpServerManager::~ExpServerManager()
    {

    }

    int ExpServerManager::initialize()
    {
      int32_t iret = !initialize_ ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        need_move_ = false;
        wait_time_check_ = SYSPARAM_EXPIREROOTSERVER.es_rts_check_lease_interval_;
        if (wait_time_check_ < 0 || wait_time_check_ > max_check_interval)
        {
          iret = TFS_ERROR;
          TBSYS_LOG(ERROR, "wait_time_check: %d not invalid", wait_time_check_);
        }
        check_es_lease_thread_ = new CheckExpServerLeaseThreadHelper(*this);
        initialize_ = true;
        tbutil::Time now = tbutil::Time::now(tbutil::Time::Monotonic);
        root_start_time_ = now.toSeconds();
      }
      return iret;
    }

    void ExpServerManager::destroy()
    {
      initialize_ = false;
      destroy_ = true;
      servers_.clear();
      exp_table_.v_exp_table_.clear();
      exp_table_.v_idle_table_.clear();
      if (0 != check_es_lease_thread_ )
      {
        check_es_lease_thread_ ->join();
      }
    }

    uint64_t ExpServerManager::new_lease_id(void)
    {
      uint64_t lease_id = atomic_inc(&lease_id_factory_);
      assert(lease_id <= UINT64_MAX - 1);
      return lease_id;
    }

    int ExpServerManager::check_ms_lease_expired(void)
    {
      tbutil::Time now;
      int64_t check_interval_ = wait_time_check_ * 1000000;
      while (!destroy_)
      {
        now = tbutil::Time::now(tbutil::Time::Monotonic);
        if (now.toSeconds() < root_start_time_ + SYSPARAM_KVRTSERVER.safe_mode_time_)
        {
          sleep(SYSPARAM_KVRTSERVER.safe_mode_time_);
        }
        check_es_lease_expired_helper(now);
        usleep(check_interval_);
      }
      return TFS_SUCCESS;
    }

    void ExpServerManager::check_es_lease_expired_helper(const tbutil::Time& now)
    {
      //TBSYS_LOG(INFO, "check_ms_lease_expired_helper start");
      VUINT64 down_servers;

      mutex_.lock();
      EXP_SERVER_MAPS_ITER iter = servers_.begin();
      for (; iter != servers_.end(); )
      {
        TBSYS_LOG(INFO, "%s now time: %ld, lease_id: %"PRI64_PREFIX"u, lease_expired_time: %"PRI64_PREFIX"d",
            tbsys::CNetUtil::addrToString(iter->first).c_str(), (int64_t)now.toSeconds(),
            iter->second.lease_.lease_id_, iter->second.lease_.lease_expired_time_);
        if (!iter->second.lease_.has_valid_lease(now.toSeconds()))
        {
          TBSYS_LOG(INFO, "%s lease expired, must be delete", tbsys::CNetUtil::addrToString(iter->first).c_str());
          down_servers.push_back(iter->first);
          servers_.erase(iter++);
          need_move_ = true;
        }
        else
        {
          ++iter;
        }
      }
      mutex_.unlock();
      if (need_move_)
      {
        handle_task_helper_.handle_fail_servers(down_servers);
      }

      move_table();
      return;
    }

    int ExpServerManager::keepalive(common::ExpServerBaseInformation& base_info)
    {
      tbutil::Time now = tbutil::Time::now(tbutil::Time::Monotonic);
      int32_t iret = initialize_ ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        tbutil::Mutex::Lock lock(mutex_);

        ExpServer* pserver = NULL;
        EXP_SERVER_MAPS_ITER iter = servers_.find(base_info.id_);
        if (servers_.end() == iter)//no exist
        {
          pserver = new ExpServer();
          pserver->lease_.lease_id_ = new_lease_id();
          pserver->lease_.lease_expired_time_ = now.toSeconds() + SYSPARAM_EXPIREROOTSERVER.es_rts_lease_expired_time_ ;//6s
          pserver->base_info_ = base_info;
          pserver->base_info_.last_update_time_ = now.toSeconds();
          std::pair<EXP_SERVER_MAPS_ITER, bool> res =
            servers_.insert(EXP_SERVER_MAPS::value_type(base_info.id_, *pserver));
          TBSYS_LOG(INFO, "new join %s now time: %ld, lease_id: %"PRI64_PREFIX"u, lease_expired_time: %"PRI64_PREFIX"d",
              tbsys::CNetUtil::addrToString(iter->first).c_str(), (int64_t)now.toSeconds(),
              iter->second.lease_.lease_id_, iter->second.lease_.lease_expired_time_);
          delete pserver;
          need_move_ = true;
        }
        else
        {
          pserver = &iter->second;
          pserver->lease_.lease_expired_time_ = now.toSeconds() + SYSPARAM_EXPIREROOTSERVER.es_rts_lease_expired_time_ ;
          pserver->base_info_.last_update_time_ = now.toSeconds();
          pserver->base_info_.task_status_ = base_info.task_status_;
        }
      }

      return iret;
    }

    void ExpServerManager::move_table()
    {
      mutex_.lock();
      mutex_for_get_.lock();
      exp_table_.v_exp_table_.clear();
      exp_table_.v_idle_table_.clear();
      EXP_SERVER_MAPS::iterator iter = servers_.begin();
      for(; iter != servers_.end(); ++iter)
      {
        exp_table_.v_exp_table_.push_back(iter->first);
        if ((iter->second).base_info_.task_status_ == 0)
        {
          exp_table_.v_idle_table_.push_back(iter->first);
        }
      }
      mutex_for_get_.unlock();
      mutex_.unlock();

      need_move_ = false;
    }

    int ExpServerManager::get_table(ExpTable &exp_table)
    {
      int32_t iret = TFS_SUCCESS;

      if (TFS_SUCCESS == iret)
      {
        tbutil::Mutex::Lock lock(mutex_for_get_);
        exp_table.v_exp_table_ = exp_table_.v_exp_table_;
        exp_table.v_idle_table_ = exp_table_.v_idle_table_;
      }
      return iret;
    }

    void ExpServerManager::CheckExpServerLeaseThreadHelper::run()
    {
      manager_.check_ms_lease_expired();
    }
  } /**exp root server **/
}/** tfs **/
