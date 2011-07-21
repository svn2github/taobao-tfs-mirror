/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: ns_define.h 344 2011-05-26 01:17:38Z duanfei@taobao.com $
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
#include "ns_define.h"
#include "server_collect.h"

namespace tfs
{
  namespace nameserver
  {
    NsGlobalStatisticsInfo::NsGlobalStatisticsInfo() :
      elect_seq_num_(ELECT_SEQ_NO_INITIALIZE),
      use_capacity_(0),
      total_capacity_(0),
      total_block_count_(0),
      total_load_(0),
      max_load_(1),
      max_block_count_(1),
      alive_server_count_(0)
    {

    }

    NsGlobalStatisticsInfo::NsGlobalStatisticsInfo(uint64_t use_capacity, uint64_t totoal_capacity, uint64_t total_block_count, int32_t total_load,
        int32_t max_load, int32_t max_block_count, int32_t alive_server_count) :
      use_capacity_(use_capacity), total_capacity_(totoal_capacity), total_block_count_(total_block_count),
      total_load_(total_load), max_load_(max_load), max_block_count_(max_block_count), alive_server_count_(
          alive_server_count)
    {

    }

    void NsGlobalStatisticsInfo::update(const common::DataServerStatInfo& info, const bool is_new)
    {
      common::RWLock::Lock lock(*this, common::WRITE_LOCKER);
      if (is_new)
      {
        use_capacity_ += info.use_capacity_;
        total_capacity_ += info.total_capacity_;
        total_load_ += info.current_load_;
        total_block_count_ += info.block_count_;
        alive_server_count_ += 1;
      }
      max_load_ = std::max(info.current_load_, max_load_);
      max_block_count_ = std::max(info.block_count_, max_block_count_);
    }

    void NsGlobalStatisticsInfo::update(const NsGlobalStatisticsInfo& info)
    {
      common::RWLock::Lock lock(*this, common::WRITE_LOCKER);
      use_capacity_ = info.use_capacity_;
      total_capacity_ = info.total_capacity_;
      total_block_count_ = info.total_block_count_;
      total_load_ = info.total_load_;
      max_load_ = info.max_load_;
      max_block_count_ = info.max_block_count_;
      alive_server_count_ = info.alive_server_count_;
    }

    NsGlobalStatisticsInfo& NsGlobalStatisticsInfo::instance()
    {
      return instance_;
    }

    void NsGlobalStatisticsInfo::dump()
    {
      TBSYS_LOG(DEBUG, "elect_seq_num: %"PRI64_PREFIX"d, use_capacity: %"PRI64_PREFIX"d, total_capacity: %"PRI64_PREFIX"d, total_block_count: %"PRI64_PREFIX"d, total_load: %d, max_load: %d, max_block_count: %d, alive_server_count: %d",
          elect_seq_num_, use_capacity_, total_capacity_, total_block_count_, total_load_, max_load_, max_block_count_, alive_server_count_); 
    }

    void NsRuntimeGlobalInformation::initialize()
    {
      memset(this, 0, sizeof(*this));
      owner_role_ = NS_ROLE_SLAVE;
      other_side_role_ = NS_ROLE_SLAVE;
      destroy_flag_ = NS_DESTROY_FLAGS_NO;
      owner_status_ = NS_STATUS_NONE;
      other_side_status_ = NS_STATUS_NONE;
      sync_oplog_flag_ = NS_SYNC_DATA_FLAG_NONE;
    }

    void NsRuntimeGlobalInformation::dump(int32_t level, const char* file , const int32_t line , const char* function ) const
    {
      TBSYS_LOGGER.logMessage(
          level,
          file,
          line,
          function,
          "owner ip port: %s, other side ip port: %s, switch time: %s, vip: %s\
          ,destroy flag: %s, owner role: %s, other side role: %s, owner status: %s, other side status: %s\
          ,sync oplog flag: %s, last owner check time: %s, last push owner check packet time: %s",
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
        common::Func::time_to_str(last_owner_check_time_/1000000).c_str(),
        common::Func::time_to_str(last_push_owner_check_packet_time_/1000000).c_str());
    }

    NsRuntimeGlobalInformation& NsRuntimeGlobalInformation::instance()
    {
      return instance_;
    }

    void print_servers(const std::vector<ServerCollect*>& servers, std::string& result)
    {
      std::vector<ServerCollect*>::const_iterator iter = servers.begin();
      for (; iter != servers.end(); ++iter)
      {
        result += "/";
        result += tbsys::CNetUtil::addrToString((*iter)->id());
      }
    }

    void print_servers(const std::vector<uint64_t>& servers, std::string& result)
    {
      std::vector<uint64_t>::const_iterator iter = servers.begin();
      for (; iter != servers.end(); ++iter)
      {
        result += "/";
        result += tbsys::CNetUtil::addrToString((*iter));
      }
    }

    void print_blocks(const std::vector<uint32_t>& blocks, std::string& result)
    {
      char data[32]={'\0'};
      std::vector<uint32_t>::const_iterator iter = blocks.begin();
      for (; iter != blocks.end(); ++iter)
      {
        result += "/";
        snprintf(data, 32, "%d", (*iter));
        result.append(data);
      }
    }

  }/** nameserver **/
}/** tfs **/
