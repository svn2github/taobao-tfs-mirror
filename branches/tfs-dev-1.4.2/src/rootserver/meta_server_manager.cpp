/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: rootserver.h 590 2011-08-17 16:36:13Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */

#include <Time.h> 
#include "common/define.h"
#include "common/func.h"
#include "common/lock.h"
#include "common/internal.h"
#include "common/error_msg.h"
#include "common/rts_define.h"
#include "common/atomic.h"
#include "common/parameter.h"
#include "meta_server_manager.h"

using namespace tfs::common;

namespace tfs
{
  namespace rootserver
  {
    MetaServerManager::MetaServerManager():
      lease_id_factory_(INVALID_LEASE_ID),
      build_table_thread_(0),
      check_ms_lease_thread_(0),
      initialize_(false),
      destroy_(false),
      interrupt_(INTERRUPT_NONE)
    {

    }
    
    MetaServerManager::~MetaServerManager()
    {

    }
    
    int MetaServerManager::initialize(const std::string& table_file_path)
    {
      int32_t iret = !initialize_ ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        iret = table_file_path.empty() ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == iret)
        {
          iret = build_tables_.intialize(table_file_path);
          if (TFS_SUCCESS != iret)
          {
            TBSYS_LOG(ERROR, "initialize build table manger fail: %d", iret);
          }
        }
        if (TFS_SUCCESS == iret)
        {
          build_table_thread_ = new BuildTableThreadHelper(*this);
          check_ms_lease_thread_ = new CheckMetaServerLeaseThreadHelper(*this);
          initialize_ = true;
        }
      }
      return iret;
    }

    int MetaServerManager::destroy()
    {
      initialize_ = false;
      destroy_ = true;
      servers_.clear();

      {
        tbutil::Monitor<tbutil::Mutex>::Lock lock(build_table_monitor_);
        build_table_monitor_.notifyAll();
      }

      {
        tbutil::Monitor<tbutil::Mutex>::Lock lock(check_ms_lease_monitor_);
        check_ms_lease_monitor_.notifyAll();
      }
      if (0 != build_table_thread_)
      {
        build_table_thread_->join();
      }
      if (0 != check_ms_lease_thread_ )
      {
        check_ms_lease_thread_ ->join();
      }
      return build_tables_.destroy();
    }

    bool MetaServerManager::exist(const uint64_t id)
    {
      RWLock::Lock lock(mutex_, READ_LOCKER);
      META_SERVER_MAPS_ITER iter = servers_.find(id);
      return (initialize_ && (servers_.end() != iter));
    }

    bool MetaServerManager::lease_exist(const uint64_t id)
    {
      tbutil::Time now = tbutil::Time::now(tbutil::Time::Monotonic);
      RWLock::Lock lock(mutex_, READ_LOCKER);
      META_SERVER_MAPS_ITER iter = servers_.find(id);
      return initialize_
              && servers_.end() != iter
              && iter->second.lease_.has_valid_lease(now.toSeconds());
    }

    int MetaServerManager::keepalive(const int8_t type, common::MetaServer& server)
    {
      int32_t iret = TFS_ERROR;
      switch (type)
      {
      case RTS_MS_KEEPALIVE_TYPE_LOGIN:
        iret = register_(server);
        break;
      case RTS_MS_KEEPALIVE_TYPE_RENEW:
        iret = renew(server);
        break;
      case RTS_MS_KEEPALIVE_TYPE_LOGOUT:
        iret = unregister(server.base_info_.id_);
        break;
      default:
        TBSYS_LOG(ERROR, "%s keepalive, type: %hh not found", 
            tbsys::CNetUtil::addrToString(server.base_info_.id_).c_str(), type);
        break;
      }
      return iret;
    }

    int MetaServerManager::register_(common::MetaServer& server)
    {
      tbutil::Time now = tbutil::Time::now(tbutil::Time::Monotonic);
      int32_t iret = initialize_ ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        MetaServer* pserver = NULL;
        RWLock::Lock lock(mutex_, WRITE_LOCKER);
        META_SERVER_MAPS_ITER iter = servers_.find(server.base_info_.id_);
        if (servers_.end() == iter)
        {
          std::pair<META_SERVER_MAPS_ITER, bool> res = 
            servers_.insert(META_SERVER_MAPS::value_type(server.base_info_.id_, server));
          pserver =  &res.first->second;
        }
        else
        {
          iret = !iter->second.lease_.has_valid_lease(now.toSeconds()) ? TFS_SUCCESS : EXIT_REGISTER_EXIST_ERROR;
          if (TFS_SUCCESS == iret)
          {
            pserver = &iter->second;
            memset(pserver, 0, sizeof(MetaServer));
          }
        }
        if (TFS_SUCCESS == iret)
        {
          memcpy(pserver, &server, sizeof(MetaServer));
          pserver->lease_.lease_id_ = new_lease_id();
          pserver->lease_.lease_expired_time_ = now.toSeconds() + SYSPARAM_RTSERVER.mts_rts_lease_expired_time_;
          pserver->base_info_.last_update_time_ = now.toSeconds();
          server.tables_.version_ = build_tables_.get_active_table_version();
          server.lease_.lease_expired_time_ = SYSPARAM_RTSERVER.mts_rts_lease_expired_time_; 
        }
      }
      if (TFS_SUCCESS == iret)
      {
        interrupt();
      }
      return iret;
    }

    int MetaServerManager::unregister(const uint64_t id)
    {
      RWLock::Lock lock(mutex_, WRITE_LOCKER);
      META_SERVER_MAPS_ITER iter = servers_.find(id);
      if (servers_.end() != iter)
      {
        servers_.erase(iter);
      }
      return TFS_SUCCESS;
    }

    int MetaServerManager::renew(common::MetaServer& server)
    {
      int32_t iret = initialize_ ? TFS_SUCCESS : TFS_ERROR ;
      if (TFS_SUCCESS == iret)
      {
        MetaServer* pserver = NULL;
        tbutil::Time now = tbutil::Time::now(tbutil::Time::Monotonic);
        RWLock::Lock lock(mutex_, WRITE_LOCKER);
        META_SERVER_MAPS_ITER iter = servers_.find(server.base_info_.id_);
        if (servers_.end() == iter)
        {
          iret = EXIT_REGISTER_NOT_EXIST_ERROR;
        }
        else
        {
          iret = iter->second.lease_.has_valid_lease(now.toSeconds()) ? TFS_SUCCESS : EXIT_LEASE_EXPIRED;
          if (TFS_SUCCESS == iret)
          {
            pserver = &iter->second;
            iret = pserver->lease_.renew(SYSPARAM_RTSERVER.mts_rts_lease_expired_time_, now.toSeconds()) ? TFS_SUCCESS : EXIT_LEASE_EXPIRED;
            if (TFS_SUCCESS == iret)
            {
              pserver->base_info_.update(server.base_info_, now.toSeconds());
              memcpy(&pserver->throughput_, &server.throughput_, sizeof(MetaServerThroughput));
              memcpy(&pserver->net_work_stat_, &server.net_work_stat_, sizeof(NetWorkStatInformation));
              memcpy(&pserver->capacity_, &server.capacity_, sizeof(MetaServerCapacity));
              server.tables_.version_ = build_tables_.get_active_table_version();
              server.lease_.lease_expired_time_ = SYSPARAM_RTSERVER.mts_rts_lease_expired_time_; 
            }
          }
        }
      }
      return iret;
    }

    int MetaServerManager::dump_meta_server(void)
    {
      return TFS_SUCCESS;
    }

    int MetaServerManager::dump_meta_server(common::Buffer& buffer)
    {
      UNUSED(buffer);
      return TFS_SUCCESS;
    }

    int MetaServerManager::check_ms_lease_expired(void)
    {
      {
        tbutil::Monitor<tbutil::Mutex>::Lock lock(check_ms_lease_monitor_);
        check_ms_lease_monitor_.timedWait(tbutil::Time::seconds(SYSPARAM_RTSERVER.safe_mode_time_));//safe mode time
      }
      std::vector<uint64_t> servers;
      tbutil::Time now;
      while (!destroy_)
      {
        {
          tbutil::Monitor<tbutil::Mutex>::Lock lock(check_ms_lease_monitor_);
          check_ms_lease_monitor_.timedWait(tbutil::Time::seconds(10));
        }
        now = tbutil::Time::now(tbutil::Time::Monotonic);
        check_ms_lease_expired_helper(now, servers);
        if (!servers.empty())
        {
          servers.clear();
          interrupt();
        }
      }
      return TFS_SUCCESS;
    }

    int MetaServerManager::switch_table(NEW_TABLE& tables)
    {
      int32_t iret = initialize_ && !tables.empty() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        iret = build_tables_.switch_table();
      }
      return iret;
    }

    int MetaServerManager::update_tables_item_status(const uint64_t server, const int64_t version,
                                    const int8_t status, const int8_t phase)
    {
      int32_t iret  = initialize_ ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        tbutil::Monitor<tbutil::Mutex>::Lock lock(build_table_monitor_);
        iret = build_tables_.update_tables_item_status(server, version, status, phase, new_tables_);
      }
      return iret;
    }

    int MetaServerManager::get_tables(char* tables, int64_t& length)
    {
      tbutil::Monitor<tbutil::Mutex>::Lock lock(build_table_monitor_);
      int32_t iret = NULL == tables || length < build_tables_.get_active_table_length()
        ? TFS_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == iret)
      {
        memcpy(tables, build_tables_.get_active_table(), build_tables_.get_active_table_length());
      }
      return iret;
    }

    void MetaServerManager::build_table(void)
    {
      {
        tbutil::Monitor<tbutil::Mutex>::Lock lock(build_table_monitor_);
        build_table_monitor_.timedWait(tbutil::Time::seconds(SYSPARAM_RTSERVER.safe_mode_time_));//safe mode time
      }
      std::set<uint64_t> servers;
      bool update_complete = false;
      int8_t phase = UPDATE_TABLE_PHASE_1;
      while (!destroy_)
      {
        tbutil::Monitor<tbutil::Mutex>::Lock lock(build_table_monitor_);
        if(!Func::test_bit(interrupt_, BUILD_TABLE_INTERRUPT_ALL)//no rebuild && no update tables, wait
            && new_tables_.empty())
        {
          build_table_monitor_.wait();
        }

        if (Func::test_bit(interrupt_, BUILD_TABLE_INTERRUPT_ALL))//rebuild
        {
          build_table_helper(phase, new_tables_, update_complete);
        }
        else if (!new_tables_.empty())//check update tables status
        {
          build_table_monitor_.timedWait(tbutil::Time::milliSeconds(100));//100ms
          update_table_helper(phase, new_tables_, update_complete);
        }
      }
      return;
    }

    uint64_t MetaServerManager::new_lease_id(void)
    {
      uint64_t lease_id = atomic_inc(&lease_id_factory_);
      assert(lease_id <= UINT64_MAX - 1);
      return lease_id;
    }

    void MetaServerManager::interrupt(void) 
    {
      tbutil::Monitor<tbutil::Mutex>::Lock lock(build_table_monitor_);
      Func::set_bit(interrupt_, BUILD_TABLE_INTERRUPT_ALL);
      build_table_monitor_.notifyAll();
    }

    void MetaServerManager::check_ms_lease_expired_helper(const tbutil::Time& now, std::vector<uint64_t>& servers)
    {
      RWLock::Lock lock(mutex_, WRITE_LOCKER);
      META_SERVER_MAPS_ITER iter = servers_.begin();
      for (; iter != servers_.end(); )
      {
        if (!iter->second.lease_.has_valid_lease(now.toSeconds()))
        {
          servers.push_back(iter->first);
          servers_.erase(iter++);
        }
        else
        {
          ++iter;
        }
      }
      return;
    }

    int MetaServerManager::build_table_helper(int8_t& phase, NEW_TABLE& tables, bool& update_complete)
    {
      bool change = false;
      tables.clear();
      phase = UPDATE_TABLE_PHASE_1;
      std::set<uint64_t> servers;
      Func::clr_bit(interrupt_, BUILD_TABLE_INTERRUPT_ALL);
      RWLock::Lock lock(mutex_, READ_LOCKER);
      META_SERVER_MAPS_CONST_ITER iter = servers_.begin();
      for (; iter != servers_.end(); ++iter)
      {
        servers.insert(iter->first);
      }
      int32_t iret = build_tables_.build_table(interrupt_, change, tables, servers);
      if (TFS_SUCCESS == iret)
      {
        if (change)
        {
          build_tables_.update_table(interrupt_, phase, tables, update_complete);
        }
      }
      return iret;
    }

    int MetaServerManager::update_table_helper(int8_t& phase, NEW_TABLE& tables, bool& update_complete)
    {
      int32_t iret = tables.empty() ? TFS_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == iret)
      {
        iret = build_tables_.check_update_table_complete(interrupt_, phase, tables, update_complete);//check update status
        if (TFS_SUCCESS == iret)
        {
          if (update_complete)
          {
            if (UPDATE_TABLE_PHASE_1 == phase)
            {
              iret = switch_table(tables);
              if (TFS_SUCCESS == iret)
              {
                TBSYS_LOG(INFO, "update table complete, version: %"PRI64_PREFIX"d, phase: %hh",
                  build_tables_.get_active_table_version(), phase);
                update_complete = false;
                phase = UPDATE_TABLE_PHASE_2;
                build_tables_.update_table(interrupt_, phase, tables, update_complete);
              }
            }
            else
            {
              tables.clear();
              TBSYS_LOG(INFO, "update table complete, version: %"PRI64_PREFIX"d, phase: %hh",
                  build_tables_.get_active_table_version(), phase);
            }
          }
        }
      }
      return iret;
    }

    common::MetaServer* MetaServerManager::get(const uint64_t id)
    {
      RWLock::Lock lock(mutex_, READ_LOCKER);
      META_SERVER_MAPS_ITER iter = servers_.find(id);
      return servers_.end() != iter && initialize_ ? &iter->second : NULL;
    }

    void MetaServerManager::BuildTableThreadHelper::run()
    {
      manager_.build_table();
    }

    void MetaServerManager::CheckMetaServerLeaseThreadHelper::run()
    {
      manager_.check_ms_lease_expired();
    }
  } /** root server **/
}/** tfs **/
