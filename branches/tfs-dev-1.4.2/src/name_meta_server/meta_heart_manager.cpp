/* * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: table_manager.cpp 381 2011-09-07 14:07:39Z nayan@taobao.com $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 */
#include <zlib.h>
#include <Time.h>
#include "common/internal.h"
#include "common/parameter.h"
#include "common/base_service.h"
#include "common/client_manager.h"
#include "common/meta_server_define.h"
#include "message/rts_ms_heart_message.h"
#include "message/get_tables_from_rts_message.h"
#include "meta_heart_manager.h"
#include "meta_bucket_manager.h"

using namespace tfs::common;
using namespace tfs::message;

namespace tfs
{
  namespace namemetaserver
  {
    const int8_t  HeartManager::MAX_RETRY_COUNT = 4;
    const int16_t HeartManager::MAX_TIMEOUT_MS  = 500;//ms
    HeartManager::HeartManager(BucketManager& bucket_manager):
      bucket_manager_(bucket_manager),
      heart_thread_(0),
      keepalive_type_(RTS_MS_KEEPALIVE_TYPE_LOGIN),
      has_valid_lease_(false)
    {
      heart_thread_ = new MSHeartBeatThreadHelper(*this);
    }

    HeartManager::~HeartManager()
    {

    }

    int HeartManager::initialize(void)
    {
      int32_t iret = heart_thread_ != 0 ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        heart_thread_->start();
      }
      return iret;
    }

    void HeartManager::destroy(void)
    {
      {
        tbutil::Monitor<tbutil::Mutex>::Lock lock(monitor_);
        if (!destroy_)
        {
          keepalive_type_ = RTS_MS_KEEPALIVE_TYPE_LOGOUT;
          destroy_ = true;
          monitor_.notifyAll();
        }
      }

      if (0 != heart_thread_)
      {
        heart_thread_->join();
        heart_thread_ = 0;
      }
    }

    void HeartManager::run_heart()
    {
      int32_t iret = TFS_SUCCESS;
      int64_t new_version = INVALID_TABLE_VERSION;
      int32_t wait_time_ms = RTS_MS_RENEW_LEASE_INTERVAL_TIME_DEFAULT;
      tbutil::Time lease_expired;
      tbutil::Time now = tbutil::Time::now(tbutil::Time::Monotonic);
      do
      {
        iret = keepalive(keepalive_type_, new_version, wait_time_ms, lease_expired/*, server*/);
        now = tbutil::Time::now(tbutil::Time::Monotonic);
        has_valid_lease_ = ((now < lease_expired));
                            //&& bucket_manager_.bucket_version_valid(new_version));
        if (has_valid_lease_)
        {
          tbutil::Monitor<tbutil::Mutex>::Lock lock(monitor_);
          monitor_.timedWait(tbutil::Time::seconds(wait_time_ms <= 0 ? 
          RTS_MS_RENEW_LEASE_INTERVAL_TIME_DEFAULT : wait_time_ms));
        }
        else
        {
          bucket_manager_.cleanup();
        }
      }
      while (!destroy_);
    }

    int HeartManager::keepalive(RtsMsKeepAliveType& type, int64_t& new_version,
          int32_t& wait_time, tbutil::Time& lease_expired/*, const uint64_t server*/)
    {
      uint64_t server =  SYSPARAM_NAMEMETASERVER.rs_ip_port_; 
      RtsMsHeartMessage msg;
      msg.set_type(type);
      msg.set_ms(MsRuntimeGlobalInformation::instance().server_);
      NewClient* client = NULL;
      int32_t retry_count = 0;
      int32_t iret = TFS_SUCCESS;
      tbnet::Packet* response = NULL;
      do
      {
        ++retry_count;
        client = NewClientManager::get_instance().create_client();
        iret = send_msg_to_server(server, client, &msg, response, MAX_TIMEOUT_MS);
        if (TFS_SUCCESS == iret)
        {
          assert(NULL != response);
          RtsMsHeartResponseMessage* rmsg = dynamic_cast<RtsMsHeartResponseMessage*>(response);
          iret = rmsg->get_ret_value();
          if (TFS_SUCCESS == iret)
          {
            if (RTS_MS_KEEPALIVE_TYPE_LOGIN == type)
            {
              type = RTS_MS_KEEPALIVE_TYPE_RENEW;
              int32_t count = 0;
              do
              {
                ++count;
                iret = get_buckets_from_rs();
              } 
              while ((count < MAX_RETRY_COUNT)
                  && (TFS_SUCCESS != iret));
              if (TFS_SUCCESS != iret)
              {
                lease_expired = 0;
                type = RTS_MS_KEEPALIVE_TYPE_LOGIN;
                TBSYS_LOG(ERROR, "%s get buckets failed", tbsys::CNetUtil::addrToString(server).c_str());
              }
            }
            if (TFS_SUCCESS == iret)
            {
              new_version = rmsg->get_active_table_version();
              lease_expired = tbutil::Time::now(tbutil::Time::Monotonic) + tbutil::Time::seconds(rmsg->get_lease_expired_time());
              wait_time = rmsg->get_renew_lease_interval_time();
            }
          }
          else if (EXIT_REGISTER_EXIST_ERROR == iret)
          {
            lease_expired = 0;
            iret = TFS_SUCCESS;
            type = RTS_MS_KEEPALIVE_TYPE_LOGIN;
            TBSYS_LOG(WARN, "%s register failed because %s is existed", 
              tbsys::CNetUtil::addrToString(server).c_str(), tbsys::CNetUtil::addrToString(server).c_str());
          }
          else if (EXIT_LEASE_EXPIRED == iret)
          {
            lease_expired = 0;
            type = RTS_MS_KEEPALIVE_TYPE_LOGIN;
            TBSYS_LOG(WARN, "%s lease expired", tbsys::CNetUtil::addrToString(server).c_str());
          }
        }
        NewClientManager::get_instance().destroy_client(client);
      }
      while ((retry_count < MAX_RETRY_COUNT)
            && (TFS_SUCCESS != iret)
            && (!destroy_));
      return iret;
    }

    int HeartManager::get_buckets_from_rs(void)
    {
      NewClient* client = NewClientManager::get_instance().create_client();
      uint64_t server =  SYSPARAM_NAMEMETASERVER.rs_ip_port_; 
      GetTableFromRtsMessage msg;
      tbnet::Packet* response = NULL;
      int32_t iret = send_msg_to_server(server, client, &msg, response, MAX_TIMEOUT_MS);
      if (TFS_SUCCESS == iret)
      {
        assert(NULL != response);
        GetTableFromRtsResponseMessage* reply = dynamic_cast<GetTableFromRtsResponseMessage*>(response);
        if (TABLE_VERSION_MAGIC != reply->get_version())
        {
          uint64_t dest_length = common::MAX_BUCKET_DATA_LENGTH;
          unsigned char* dest = new unsigned char[common::MAX_BUCKET_DATA_LENGTH];
          iret = uncompress(dest, &dest_length, (unsigned char*)reply->get_table(), reply->get_table_length()); 
          if (Z_OK != iret)
          {
            TBSYS_LOG(ERROR, "uncompress error: ret : %d, version: %"PRI64_PREFIX"d", iret, reply->get_version());
            iret = TFS_ERROR;
          }
          else
          {
            MsRuntimeGlobalInformation& rgi = MsRuntimeGlobalInformation::instance();
            iret = bucket_manager_.update_table((const char*)dest, dest_length,
                      reply->get_version(), rgi.server_.base_info_.id_);
            if (TFS_SUCCESS == iret)
            {
              iret = bucket_manager_.switch_table(rgi.server_.base_info_.id_, reply->get_version());
             // bucket_manager_.dump(TBSYS_LOG_LEVEL_DEBUG);
            }
          }
          tbsys::gDeleteA(dest);
        }
      }
      NewClientManager::get_instance().destroy_client(client);
      return iret;
    }

    void HeartManager::MSHeartBeatThreadHelper::run()
    {
      manager_.run_heart();
    }
  }/** namemetaserver **/
}/** tfs **/


