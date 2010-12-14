/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   qushan<qushan@taobao.com> 
 *      - modify 2009-03-27
 *   duanfei <duanfei@taobao.com> 
 *      - modify 2010-04-23
 *
 */
#ifndef TFS_NAMESERVER_DEFINE_H_
#define TFS_NAMESERVER_DEFINE_H_ 

#include <Mutex.h>
#include <tbsys.h>
#include "common/interval.h"
#include "common/func.h"

namespace tfs
{
  namespace nameserver
  {
    struct NsGlobalInfo
    {
      NsGlobalInfo() :
        use_capacity_(0), total_capacity_(0), total_block_count_(0), total_load_(0), max_load_(1), max_block_count_(1),
            alive_server_count_(0)
      {

      }

      NsGlobalInfo(uint64_t use_capacity, uint64_t totoal_capacity, uint64_t total_block_count, int32_t total_load,
          int32_t max_load, int32_t max_block_count, int32_t alive_server_count) :
        use_capacity_(use_capacity), total_capacity_(totoal_capacity), total_block_count_(total_block_count),
            total_load_(total_load), max_load_(max_load), max_block_count_(max_block_count), alive_server_count_(
                alive_server_count)
      {

      }

      int64_t use_capacity_;
      int64_t total_capacity_;
      int64_t total_block_count_;
      int32_t total_load_;
      int32_t max_load_;
      int32_t max_block_count_;
      int32_t alive_server_count_;
    };

    enum NsRole
    {
      NS_ROLE_NONE = 0x00,
      NS_ROLE_MASTER,
      NS_ROLE_SLAVE
    };

    enum NsStatus
    {
      NS_STATUS_NONE = -1,
      NS_STATUS_UNINITIALIZE = 0x00,
      NS_STATUS_OTHERSIDEDEAD,
      NS_STATUS_ACCEPT_DS_INFO,
      NS_STATUS_INITIALIZED
    };

    enum NsSyncDataFlag
    {
      NS_SYNC_DATA_FLAG_NONE = 0x00,
      NS_SYNC_DATA_FLAG_NO,
      NS_SYNC_DATA_FLAG_READY,
      NS_SYNC_DATA_FLAG_YES
    };

    enum NsDestroyFlag
    {
      NS_DESTROY_FLAGS_NO = 0x00,
      NS_DESTROY_FLAGS_YES
    };

    enum NsSwitchFlag
    {
      NS_SWITCH_FLAG_NO = 0x00,
      NS_SWITCH_FLAG_YES
    };

    struct NsRuntimeGlobalInformation: public tbutil::Mutex
    {
      uint64_t owner_ip_port_;
      uint64_t other_side_ip_port_;
      time_t switch_time_;
      uint32_t vip_;
      NsDestroyFlag destroy_flag_;
      NsRole owner_role_;
      NsRole other_side_role_;
      NsStatus owner_status_;
      NsStatus other_side_status_;
      NsSyncDataFlag sync_oplog_flag_;
      tbutil::Time last_owner_check_time_;
      tbutil::Time last_push_owner_check_packet_time_;
      void dump(int32_t level, const char* file = __FILE__, const int32_t line = __LINE__, const char* function =
          __FUNCTION__) const
      {
        TBSYS_LOGGER.logMessage(
            level,
            file,
            line,
            function,
            "owner ip port(%s), other side ip port(%s), switch time(%s), vip(%s)\
,destroy flag(%s), owner role(%s), other side role(%s), owner status(%s), other side status(%s)\
,sync oplog flag(%s), last owner check time(%s), last push owner check packet time(%s)",
            tbsys::CNetUtil::addrToString(owner_ip_port_).c_str(),
            tbsys::CNetUtil::addrToString(other_side_ip_port_).c_str(),
            common::Func::time_to_str(switch_time_).c_str(), tbsys::CNetUtil::addrToString(vip_).c_str(), destroy_flag_
                == NS_DESTROY_FLAGS_NO ? "no" : destroy_flag_ == NS_DESTROY_FLAGS_YES ? "yes" : "unknow", owner_role_
                == NS_ROLE_MASTER ? "master" : owner_role_ == NS_ROLE_SLAVE ? "slave" : "unknow", other_side_role_
                == NS_ROLE_MASTER ? "master" : other_side_role_ == NS_ROLE_SLAVE ? "slave" : "unknow", owner_status_
                == NS_STATUS_UNINITIALIZE ? "uninitialize"
                : owner_status_ == NS_STATUS_OTHERSIDEDEAD ? "other side dead" : owner_status_
                    == NS_STATUS_ACCEPT_DS_INFO ? "accepct ds info"
                    : owner_status_ == NS_STATUS_INITIALIZED ? "initialize" : "unknow", other_side_status_
                == NS_STATUS_UNINITIALIZE ? "uninitialize"
                : other_side_status_ == NS_STATUS_OTHERSIDEDEAD ? "other side dead" : other_side_status_
                    == NS_STATUS_ACCEPT_DS_INFO ? "accepct ds info"
                    : other_side_status_ == NS_STATUS_INITIALIZED ? "initialize" : "unknow", sync_oplog_flag_
                == NS_SYNC_DATA_FLAG_NONE ? "none" : sync_oplog_flag_ == NS_SYNC_DATA_FLAG_NO ? "no" : sync_oplog_flag_
                == NS_SYNC_DATA_FLAG_READY ? "ready" : sync_oplog_flag_ == NS_SYNC_DATA_FLAG_YES ? "yes" : "unknow",
            last_owner_check_time_.toDateTime().c_str(), last_push_owner_check_packet_time_.toDateTime().c_str());
      }
    };
  }
}

#endif 
