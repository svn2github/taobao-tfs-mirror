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
#include "message/expire_message.h"
#include "expire_heart_manager.h"


using namespace tfs::common;
using namespace tfs::message;

namespace tfs
{
  namespace expireserver
  {
    const int8_t  ExpireHeartManager::MAX_RETRY_COUNT = 2;
    const int16_t ExpireHeartManager::MAX_TIMEOUT_MS  = 500;//ms
    ExpireHeartManager::ExpireHeartManager(CleanTaskHelper &cleantask):
      heart_thread_(0),
      destroy_(false),
      heart_inter_(2),
      ref_clean_helper_(cleantask)
    {
    }

    ExpireHeartManager::~ExpireHeartManager()
    {

    }

    int ExpireHeartManager::initialize(uint64_t ers_ipport_id, uint64_t es_ipport_id, int64_t start_time)
    {
      ers_ipport_id_ = ers_ipport_id;
      es_ipport_id_ = es_ipport_id;
      start_time_ = start_time;
      heart_thread_ = new ESHeartBeatThreadHelper(*this);
      heart_thread_->start();
      return TFS_SUCCESS;
    }

    void ExpireHeartManager::destroy(void)
    {
      destroy_ = true;
      if (0 != heart_thread_)
      {
        heart_thread_->join();
        heart_thread_ = 0;
      }
    }

    void ExpireHeartManager::run_heart()
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

    int ExpireHeartManager::keepalive()
    {
      uint64_t server =  ers_ipport_id_;
      common::ExpServerBaseInformation base_info;
      base_info.id_ = es_ipport_id_;
      base_info.start_time_ = start_time_;
      base_info.task_status_ = ref_clean_helper_.get_task_state();
      ReqRtsEsHeartMessage msg;
      msg.set_es(base_info);

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
          iret = response->getPCode() == RSP_RT_ES_KEEPALIVE_MESSAGE ? TFS_SUCCESS : TFS_ERROR;
          if (TFS_SUCCESS == iret)
          {
            RspRtsEsHeartMessage* rmsg = dynamic_cast<RspRtsEsHeartMessage*>(response);
            heart_inter_ = rmsg->get_time();
          }
        }
        TBSYS_LOG(DEBUG, "MAX_TIMEOUT_MS: %d, cost: %"PRI64_PREFIX"d, inter: %"PRI64_PREFIX"d",
                  MAX_TIMEOUT_MS, (int64_t)(end - start).toMilliSeconds(), heart_inter_);
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

    void ExpireHeartManager::ESHeartBeatThreadHelper::run()
    {
      manager_.run_heart();
    }
  }/** namemetaserver **/
}/** tfs **/


