/*
 * (C) 2007-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: migrate_manager.cpp 746 2013-09-02 11:27:59Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 */
#include <string>
#include "common/error_msg.h"
#include "ds_define.h"
#include "migrate_manager.h"
#include "common/array_helper.h"
#include "common/client_manager.h"
#include "message/migrate_ds_heart_message.h"

using namespace tfs::common;
using namespace tfs::message;
namespace tfs
{
  namespace dataserver
  {
    MigrateManager::MigrateManager(const uint64_t dest_addr,const uint64_t self_addr) :
      dest_addr_(dest_addr),
      self_addr_(self_addr)
    {

    }

    MigrateManager::~MigrateManager()
    {

    }

    int MigrateManager::initialize()
    {
      work_thread_ = new (std::nothrow) WorkThreadHelper(*this);
      assert(work_thread_ != 0);
      return TFS_SUCCESS;
    }

    int MigrateManager::destroy()
    {
      if (work_thread_ != 0)
      {
        work_thread_->join();
        work_thread_ = 0;
      }
      return TFS_SUCCESS;
    }

    int MigrateManager::do_migrate_heartbeat_(const int32_t timeout_ms)
    {
      MigrateDsHeartMessage req_msg;
      NewClient* client = NewClientManager::get_instance().create_client();
      int32_t ret = (NULL != client) ? TFS_SUCCESS : EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
      if (TFS_SUCCESS == ret)
      {
        tbnet::Packet* result = NULL;
        ret = send_msg_to_server(dest_addr_, client, &req_msg, result, timeout_ms);
        if (TFS_SUCCESS == ret)
        {
          ret = RSP_MIGRATE_DS_HEARTBEAT_MESSAGE == result->getPCode() ? TFS_SUCCESS : EXIT_MIGRATE_DS_HEARTBEAT_ERROR;
        }
        if (TFS_SUCCESS == ret)
        {
          MigrateDsHeartResponseMessage* rsp = dynamic_cast<MigrateDsHeartResponseMessage*>(result);
          ret = rsp->get_ret_value();
        }
      }
      NewClientManager::get_instance().destroy_client(client);
      return ret;
    }

    void MigrateManager::run_()
    {
      const int32_t MAX_RETRY_TIMES = 3;
      const int32_t MAX_TIMEOUT_MS  = 1000;
      const int32_t MAX_SLEEP_TIME  = 30;//30s
      DsRuntimeGlobalInformation& rgi = DsRuntimeGlobalInformation::instance();
      while (!rgi.is_destroyed())
      {
        int32_t ret = TFS_SUCCESS;
        int32_t index = 0;
        for (index = 0; index < MAX_RETRY_TIMES && TFS_SUCCESS == ret; ++index)
        {
          ret = do_migrate_heartbeat_(MAX_TIMEOUT_MS);
          if (TFS_SUCCESS != ret && EXIT_TIMEOUT_ERROR != ret)
            sleep(1);
        }
        int32_t sleep_time =  MAX_SLEEP_TIME - index;
        for (index = 0; index < sleep_time && !rgi.is_destroyed(); ++index)
        {
          sleep(1);
        }
      }
    }

    void MigrateManager::WorkThreadHelper::run()
    {
      manager_.run_();
    }
  }/** end namespace dataserver **/
}/** end namesapce tfs **/
