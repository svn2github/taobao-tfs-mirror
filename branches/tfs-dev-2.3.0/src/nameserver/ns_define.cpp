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
#include "common/parameter.h"
#include "server_collect.h"

namespace tfs
{
  namespace nameserver
  {
    NsGlobalStatisticsInfo::NsGlobalStatisticsInfo() :
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

    void NsGlobalStatisticsInfo::dump(int32_t level, const char* file , const int32_t line , const char* function ) const
    {
      TBSYS_LOGGER.logMessage(level, file, line, function,
          "use_capacity: %"PRI64_PREFIX"d, total_capacity: %"PRI64_PREFIX"d, total_block_count: %"PRI64_PREFIX"d, total_load: %d, max_load: %d, max_block_count: %d, alive_server_count: %d",
          use_capacity_, total_capacity_, total_block_count_, total_load_, max_load_, max_block_count_, alive_server_count_);
    }

    NsRuntimeGlobalInformation::NsRuntimeGlobalInformation()
    {
      initialize();
    }

    void NsRuntimeGlobalInformation::initialize()
    {
      memset(this, 0, sizeof(*this));
      owner_role_ = NS_ROLE_SLAVE;
      peer_role_ = NS_ROLE_SLAVE;
      destroy_flag_ = false;
      owner_status_ = NS_STATUS_NONE;
      peer_status_ = NS_STATUS_NONE;
    }

    void NsRuntimeGlobalInformation::destroy()
    {
      memset(this, 0, sizeof(*this));
      owner_role_ = NS_ROLE_SLAVE;
      peer_role_ = NS_ROLE_SLAVE;
      destroy_flag_ = true;
      owner_status_ = NS_STATUS_NONE;
      peer_status_ = NS_STATUS_NONE;
    }

    bool NsRuntimeGlobalInformation::is_destroyed() const
    {
      return destroy_flag_;
    }

    bool NsRuntimeGlobalInformation::is_master() const
    {
      return owner_role_ == NS_ROLE_MASTER;
    }

    bool NsRuntimeGlobalInformation::peer_is_master() const
    {
      return peer_role_ == NS_ROLE_MASTER;
    }

    bool NsRuntimeGlobalInformation::peer_is_initialize_complete() const
    {
      return peer_status_ == NS_STATUS_INITIALIZED;
    }

    bool NsRuntimeGlobalInformation::own_is_initialize_complete() const
    {
      return owner_status_ == NS_STATUS_INITIALIZED;
    }

    void NsRuntimeGlobalInformation::update_peer_info(const uint64_t server, const int8_t role, const int8_t status)
    {
      peer_ip_port_ = server;
      peer_role_ = role;
      peer_status_ = status;
    }

    void NsRuntimeGlobalInformation::switch_role(const int64_t now)
    {
      owner_role_ = owner_role_ == NS_ROLE_MASTER ? NS_ROLE_SLAVE : NS_ROLE_MASTER;
      peer_role_ = owner_role_ == NS_ROLE_MASTER ? NS_ROLE_SLAVE : NS_ROLE_MASTER;
      switch_time_ = now + common::SYSPARAM_NAMESERVER.safe_mode_time_;
      discard_newblk_safe_mode_time_ =  now + common::SYSPARAM_NAMESERVER.discard_newblk_safe_mode_time_;
      last_owner_check_time_ = tbutil::Time::now(tbutil::Time::Monotonic).toMicroSeconds() + common::SYSPARAM_NAMESERVER.safe_mode_time_ * 1000000;
      last_push_owner_check_packet_time_ = last_owner_check_time_;
      lease_id_ = common::INVALID_LEASE_ID;
      lease_expired_time_ = 0;
    }

    bool NsRuntimeGlobalInformation::in_safe_mode_time(const int64_t now) const
    {
      return  now < switch_time_;
    }

    bool NsRuntimeGlobalInformation::in_discard_newblk_safe_mode_time(const int64_t now) const
    {
      return  now < discard_newblk_safe_mode_time_ && now >= common::SYSPARAM_NAMESERVER.discard_newblk_safe_mode_time_;
    }

    bool NsRuntimeGlobalInformation::keepalive(int64_t& lease_id, bool& flag, const uint64_t server,
         const int8_t role, const int8_t status, const time_t now)
    {
      static uint64_t lease_id_factory = 1;
      bool ret = owner_role_ == NS_ROLE_MASTER;
      if (ret)
      {
        if (status <= NS_STATUS_UNINITIALIZE)//login
        {
          lease_id = lease_id_ = common::atomic_inc(&lease_id_factory);
          peer_ip_port_ = server;
          peer_status_  = static_cast<NsStatus>(status);
          lease_expired_time_ = now + common::SYSPARAM_NAMESERVER.heart_interval_;
        }
        else
        {
          peer_role_ = static_cast<NsRole>(role);
          ret = lease_id_ == lease_id;
          if (ret)
          {
            ret = renew(now, common::SYSPARAM_NAMESERVER.heart_interval_);
            if (ret)
            {
              if (NS_STATUS_ACCEPT_DS_INFO == status)
                flag = true;
            }
          }
        }
      }
      return ret;
    }

    bool NsRuntimeGlobalInformation::has_valid_lease(const time_t now) const
    {
      return (lease_id_ != common::INVALID_LEASE_ID && lease_expired_time_ > now);
    }

    bool NsRuntimeGlobalInformation::logout()
    {
      lease_id_ = common::INVALID_LEASE_ID;
      peer_status_ = NS_STATUS_UNINITIALIZE;
      lease_expired_time_ = 0;
      return true;
    }

    bool NsRuntimeGlobalInformation::renew(const int32_t step, const time_t now)
    {
      lease_expired_time_  = now + step;
      return true;
    }

    bool NsRuntimeGlobalInformation::renew(const int64_t lease_id, const int32_t step, const time_t now)
    {
      lease_id_ = lease_id;
      lease_expired_time_ = now + step;
      return true;
    }

    void NsRuntimeGlobalInformation::set_own_status(const int8_t status)
    {
      owner_status_ = status;
    }

    void NsRuntimeGlobalInformation::update_own_status()
    {
      if (owner_status_ < NS_STATUS_INITIALIZED)
      {
        ++owner_status_;
      }
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
, last owner check time: %s, last push owner check packet time: %s",
          tbsys::CNetUtil::addrToString(owner_ip_port_).c_str(),
          tbsys::CNetUtil::addrToString(peer_ip_port_).c_str(),
          common::Func::time_to_str(switch_time_).c_str(), tbsys::CNetUtil::addrToString(vip_).c_str(),
          destroy_flag_ ? "yes" : "no", owner_role_
          == NS_ROLE_MASTER ? "master" : owner_role_ == NS_ROLE_SLAVE ? "slave" : "unknow", peer_role_
          == NS_ROLE_MASTER ? "master" : peer_role_ == NS_ROLE_SLAVE ? "slave" : "unknow", owner_status_
          == NS_STATUS_UNINITIALIZE ? "uninitialize"
          : owner_status_ == NS_STATUS_ACCEPT_DS_INFO ? "accepct ds info"
          : owner_status_ == NS_STATUS_INITIALIZED ? "initialize" : "unknow",
          peer_status_ == NS_STATUS_UNINITIALIZE ? "uninitialize"
          : peer_status_ == NS_STATUS_ACCEPT_DS_INFO ? "accepct ds info"
          : peer_status_ == NS_STATUS_INITIALIZED ? "initialize" : "unknow",
          common::Func::time_to_str(last_owner_check_time_/1000000).c_str(),
          common::Func::time_to_str(last_push_owner_check_packet_time_/1000000).c_str());
    }

    NsRuntimeGlobalInformation& NsRuntimeGlobalInformation::instance()
    {
      return instance_;
    }

    void print_servers(const common::ArrayHelper<uint64_t>& servers, std::string& result)
    {
      for (int32_t i = 0; i < servers.get_array_index(); ++i)
      {
        result += "/";
        result += tbsys::CNetUtil::addrToString(*servers.at(i));
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

    double calc_capacity_percentage(const uint64_t capacity, const uint64_t total_capacity)
    {
      double ret = PERCENTAGE_MIN;
      uint64_t unit = capacity > GB ? GB : MB;
      uint64_t tmp_capacity = capacity / unit;
      uint64_t tmp_total_capacity = total_capacity / unit;
      if (0 == tmp_total_capacity)
        ret = PERCENTAGE_MAX;
      if ((tmp_capacity != 0)
          && (tmp_total_capacity != 0))
      {
        ret = (double)tmp_capacity / (double)tmp_total_capacity;
      }
      return ret;
    }
  }/** nameserver **/
}/** tfs **/
