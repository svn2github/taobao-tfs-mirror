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
 *   qixiao <qixiao.zs@alibaba-inc.com>
 *      - initial release
 *
 */

#include <Time.h>
#include "common/define.h"
#include "common/func.h"
#include "common/lock.h"
#include "common/internal.h"
#include "common/error_msg.h"
#include "common/kv_rts_define.h"
#include "common/atomic.h"
#include "common/parameter.h"
#include "kv_meta_server_manager.h"

using namespace tfs::common;

namespace tfs
{
  namespace kvrootserver
  {
    KvMetaServerManager::KvMetaServerManager():
      check_ms_lease_thread_(0),
      initialize_(false),
      destroy_(false)
    {

    }

    KvMetaServerManager::~KvMetaServerManager()
    {

    }

    int KvMetaServerManager::initialize()
    {
      int32_t iret = !initialize_ ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        need_move_ = false;
        wait_time_check_ = SYSPARAM_KVRTSERVER.kv_rts_check_lease_interval_;
        check_ms_lease_thread_ = new CheckKvMetaServerLeaseThreadHelper(*this);
        initialize_ = true;
        tbutil::Time now = tbutil::Time::now(tbutil::Time::Monotonic);
        root_start_time_ = now.toSeconds();
      }
      return iret;
    }

    void KvMetaServerManager::destroy()
    {
      initialize_ = false;
      destroy_ = true;
      servers_.clear();
      meta_table_.v_meta_table_.clear();
      if (0 != check_ms_lease_thread_ )
      {
        check_ms_lease_thread_ ->join();
      }
    }

    uint64_t KvMetaServerManager::new_lease_id(void)
    {
      uint64_t lease_id = atomic_inc(&lease_id_factory_);
      assert(lease_id <= UINT64_MAX - 1);
      return lease_id;
    }

    int KvMetaServerManager::check_ms_lease_expired(void)
    {
      tbutil::Time now;
      int64_t check_interval_ = wait_time_check_ * 1000000;
      TBSYS_LOG(INFO, "%"PRI64_PREFIX"d", check_interval_);
      while (!destroy_)
      {
        now = tbutil::Time::now(tbutil::Time::Monotonic);
        if (now.toSeconds() < root_start_time_ + SYSPARAM_KVRTSERVER.safe_mode_time_)
        {
          sleep(SYSPARAM_KVRTSERVER.safe_mode_time_);
        }
        check_ms_lease_expired_helper(now);
        usleep(check_interval_);
      }
      return TFS_SUCCESS;
    }

    void KvMetaServerManager::check_ms_lease_expired_helper(const tbutil::Time& now)
    {
      TBSYS_LOG(INFO, "check_ms_lease_expired_helper start");

      tbutil::Mutex::Lock lock(mutex_);

      KV_META_SERVER_MAPS_ITER iter = servers_.begin();
      for (; iter != servers_.end(); )
      {
        TBSYS_LOG(INFO, "%s now time: %ld, lease_id: %"PRI64_PREFIX"u, lease_expired_time: %"PRI64_PREFIX"d",
            tbsys::CNetUtil::addrToString(iter->first).c_str(), (int64_t)now.toSeconds(),
            iter->second.lease_.lease_id_, iter->second.lease_.lease_expired_time_);
        if (!iter->second.lease_.has_valid_lease(now.toSeconds()))
        {
          TBSYS_LOG(INFO, "%s lease expired, must be delete", tbsys::CNetUtil::addrToString(iter->first).c_str());
          servers_.erase(iter++);
          need_move_ = true;
        }
        else
        {
          ++iter;
        }
      }
      if (need_move_)
      {
        move_table();
      }
      return;
    }

    int KvMetaServerManager::keepalive(common::KvMetaServerBaseInformation& base_info)
    {
      tbutil::Time now = tbutil::Time::now(tbutil::Time::Monotonic);
      int32_t iret = initialize_ ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        tbutil::Mutex::Lock lock(mutex_);

        KvMetaServer* pserver = NULL;
        KV_META_SERVER_MAPS_ITER iter = servers_.find(base_info.id_);
        if (servers_.end() == iter)//no exist
        {
          pserver = new KvMetaServer();
          pserver->lease_.lease_id_ = new_lease_id();
          pserver->lease_.lease_expired_time_ = now.toSeconds() + SYSPARAM_KVRTSERVER.kv_mts_rts_lease_expired_time_ ;//6s
          pserver->base_info_ = base_info;
          pserver->base_info_.last_update_time_ = now.toSeconds();
          std::pair<KV_META_SERVER_MAPS_ITER, bool> res =
            servers_.insert(KV_META_SERVER_MAPS::value_type(base_info.id_, *pserver));
          TBSYS_LOG(INFO, "new join %s now time: %ld, lease_id: %"PRI64_PREFIX"u, lease_expired_time: %"PRI64_PREFIX"d",
              tbsys::CNetUtil::addrToString(iter->first).c_str(), (int64_t)now.toSeconds(),
              iter->second.lease_.lease_id_, iter->second.lease_.lease_expired_time_);
          delete pserver;
          need_move_ = true;
        }
        else
        {
          pserver = &iter->second;
          pserver->lease_.lease_expired_time_ = now.toSeconds() + SYSPARAM_KVRTSERVER.kv_mts_rts_lease_expired_time_ ;
          pserver->base_info_.last_update_time_ = now.toSeconds();
        }
      }

      return iret;
    }

    void KvMetaServerManager::move_table()
    {
      tbutil::Mutex::Lock lock(mutex_for_get_);
      meta_table_.v_meta_table_.clear();
      KV_META_SERVER_MAPS::iterator iter = servers_.begin();
      for(; iter != servers_.end(); ++iter)
      {
        meta_table_.v_meta_table_.push_back(iter->first);
      }
      need_move_ = false;
    }

    int KvMetaServerManager::get_table(KvMetaTable &meta_table)
    {
      int32_t iret = TFS_SUCCESS;
      if (TFS_SUCCESS == iret)
      {
        tbutil::Mutex::Lock lock(mutex_for_get_);
        meta_table.v_meta_table_ = meta_table_.v_meta_table_;
      }
      return iret;
    }

    void KvMetaServerManager::CheckKvMetaServerLeaseThreadHelper::run()
    {
      manager_.check_ms_lease_expired();
    }
  } /**kv root server **/
}/** tfs **/
