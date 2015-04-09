/*
 * (C) 2007-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id
 *
 * Authors:
 *      qixiao.zs(qixiao.zs@alibaba-inc.com)
 *      - initial release
 *
 */

#include "tfs_lifecycle_root_client_impl.h"
#include "common/client_manager.h"
#include "message/message_factory.h"
#include "tfs_client_api.h"
#include "tfs_lifecycle_root_helper.h"


namespace tfs
{
  namespace client
  {
    using namespace tfs::common;
    using namespace std;

    LifeCycleClientImpl::LifeCycleClientImpl()
    :rs_id_(0)
    {
      packet_factory_ = new message::MessageFactory();
      packet_streamer_ = new common::BasePacketStreamer(packet_factory_);
    }

    LifeCycleClientImpl::~LifeCycleClientImpl()
    {
      tbsys::gDelete(packet_factory_);
      tbsys::gDelete(packet_streamer_);
    }

    int LifeCycleClientImpl::initialize(const char *lifecycle_rs_addr)
    {
      int ret = TFS_SUCCESS;
      if (NULL == lifecycle_rs_addr)
      {
        TBSYS_LOG(WARN, "lifecycle_rs_addr is null");
        ret = EXIT_INVALID_ARGU_ERROR;
      }
      else
      {
        ret = initialize(Func::get_host_ip(lifecycle_rs_addr));
      }
      return ret;
    }

    int LifeCycleClientImpl::initialize(const uint64_t lifecycle_rs_addr)
    {
      int ret = TFS_SUCCESS;
      ret = NewClientManager::get_instance().initialize(packet_factory_, packet_streamer_);
      if (TFS_SUCCESS == ret)
      {
        rs_id_ = lifecycle_rs_addr;
      }
      return ret;
    }

    TfsRetType LifeCycleClientImpl::query_task(const uint64_t es_id, std::vector<common::ServerExpireTask>* p_res_task)
    {
       TfsRetType ret = TFS_SUCCESS;

       if (TFS_SUCCESS == ret)
       {
         ret = do_query_task(es_id, p_res_task);
         if (TFS_SUCCESS != ret)
         {
           TBSYS_LOG(ERROR, "query task failed. bucket");
         }
       }

       return ret;
    }

    /* ==========================================================*/

    int LifeCycleClientImpl::do_query_task(const uint64_t es_id, std::vector<common::ServerExpireTask>* p_res_task)
    {
      int ret = TFS_SUCCESS;
      int32_t retry = ClientConfig::meta_retry_count_;
      do
      {
        ret = LifeCycleHelper::do_query_task(rs_id_, es_id, p_res_task);
      }
      while ((EXIT_NETWORK_ERROR == ret) && --retry);

      return ret;
    }


  }
}

