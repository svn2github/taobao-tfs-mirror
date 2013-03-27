/* * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: $
 *
 * Authors:
 *   qixiao.zs<qixiao.zs@alibaba-inc.com>
 *      - initial release

*/
#include <zlib.h>
#include <Time.h>
#include "common/internal.h"
#include "common/parameter.h"
#include "common/base_service.h"
#include "common/client_manager.h"
#include "common/status_message.h"
#include "common/kv_rts_define.h"
#include "message/kv_rts_message.h"
#include "kv_meta_heart_manager.h"


using namespace tfs::common;
using namespace tfs::message;

namespace tfs
{
  namespace kvmetaserver
  {
    const int8_t  HeartManager::MAX_RETRY_COUNT = 2;
    const int16_t HeartManager::MAX_TIMEOUT_MS  = 500;//ms
    HeartManager::HeartManager():
      heart_thread_(0),
      destroy_(false),
      heart_inter_(2)
    {
    }

    HeartManager::~HeartManager()
    {

    }

    int HeartManager::initialize(uint64_t rs_ipport_id, uint64_t ms_ipport_id, int64_t start_time)
    {
      rs_ipport_id_ = rs_ipport_id;
      ms_ipport_id_ = ms_ipport_id;
      start_time_ = start_time;
      heart_thread_ = new MSHeartBeatThreadHelper(*this);
      heart_thread_->start();
      return TFS_SUCCESS;
    }

    void HeartManager::destroy(void)
    {
      destroy_ = true;
      if (0 != heart_thread_)
      {
        heart_thread_->join();
        heart_thread_ = 0;
      }
    }

    void HeartManager::run_heart()
    {
      int32_t iret = TFS_SUCCESS;
      int64_t wait_time_ms;
      do
      {
        wait_time_ms = heart_inter_ * 1000000;
        iret = keepalive();
        usleep(wait_time_ms);// sleep vs
      }
      while (!destroy_);

    }

    int HeartManager::keepalive()
    {
      uint64_t server =  rs_ipport_id_;
      common::KvMetaServerBaseInformation base_info;
      base_info.id_ = ms_ipport_id_;
      base_info.start_time_ = start_time_;
      KvRtsMsHeartMessage msg;
      msg.set_ms(base_info);

      NewClient* client = NULL;
      int32_t retry_count = 0;
      int32_t iret = TFS_SUCCESS;
      tbnet::Packet* response = NULL;
      do
      {
        ++retry_count;
        client = NewClientManager::get_instance().create_client();
        tbutil::Time start = tbutil::Time::now();
        iret = send_msg_to_server(server, client, &msg, response, MAX_TIMEOUT_MS);
        tbutil::Time end = tbutil::Time::now();
        if (TFS_SUCCESS == iret)
        {
          TBSYS_LOG(DEBUG, "heart_sucess");
          assert(NULL != response);
          iret = response->getPCode() == RSP_KV_RT_MS_KEEPALIVE_MESSAGE ? TFS_SUCCESS : TFS_ERROR;
          if (TFS_SUCCESS == iret)
          {
            KvRtsMsHeartResponseMessage* rmsg = dynamic_cast<KvRtsMsHeartResponseMessage*>(response);
            heart_inter_ = rmsg->get_time();
          }
        }
        TBSYS_LOG(DEBUG, "MAX_TIMEOUT_MS: %d, cost: %"PRI64_PREFIX"d, inter: %"PRI64_PREFIX"d", MAX_TIMEOUT_MS, (int64_t)(end - start).toMilliSeconds(), heart_inter_);
        if (TFS_SUCCESS != iret)
        {
          usleep(500000);
        }
        NewClientManager::get_instance().destroy_client(client);
      }
      while ((retry_count < MAX_RETRY_COUNT)
          && (TFS_SUCCESS != iret)
          && (!destroy_));
      return iret;
    }

    void HeartManager::MSHeartBeatThreadHelper::run()
    {
      manager_.run_heart();
    }
  }/** namemetaserver **/
}/** tfs **/


