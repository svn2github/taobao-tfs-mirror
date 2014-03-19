/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */
#include <tbsys.h>
#include "common/parameter.h"
#include "global_factory.h"
#include "block_collect.h"
#include "server_collect.h"
#include "layout_manager.h"

using namespace tfs::common;
using namespace tbsys;

namespace tfs
{
  namespace nameserver
  {
    BlockCollect::BlockCollect(const uint64_t block_id, const time_t now):
      BaseObject<LayoutManager>(now),
      last_leave_time_(0),
      reserve_bit_(0),
      choose_master_(BLOCK_CHOOSE_MASTER_COMPLETE_FLAG_NO),
      create_flag_(BLOCK_CREATE_FLAG_NO),
      in_replicate_queue_(BLOCK_IN_REPLICATE_QUEUE_NO),
      has_lease_(BLOCK_HAS_LEASE_FLAG_NO)
    {
      memset(&info_, 0, sizeof(info_));
      info_.block_id_ = block_id;
    }

    BlockCollect::BlockCollect(const uint64_t block_id):
      BaseObject<LayoutManager>(0),
      last_leave_time_(0),
      reserve_bit_(0),
      choose_master_(BLOCK_CHOOSE_MASTER_COMPLETE_FLAG_NO),
      create_flag_(BLOCK_CREATE_FLAG_NO),
      in_replicate_queue_(BLOCK_IN_REPLICATE_QUEUE_NO),
      has_lease_(BLOCK_HAS_LEASE_FLAG_NO)
    {
      info_.family_id_ = INVALID_FAMILY_ID;
      info_.block_id_ = block_id;
      //for query
    }

    BlockCollect::~BlockCollect()
    {

    }

    int BlockCollect::add(bool& writable, bool& master, const uint64_t server, const BlockInfoV2* info, const time_t now)
    {
      master = false;
      writable  = false;
      int32_t ret = (INVALID_SERVER_ID != server) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        dump(TBSYS_LOG_LEVEL(DEBUG));
        if (NULL != info && info->version_ > info_.version_)
          info_ = *info;
        writable = !is_full();
        const int32_t size = servers_.size();
        SERVER_ITER pos = servers_.end();
        SERVER_ITEM* result = get_(server);
        if (NULL == result)
        {
          ret = size < MAX_REPLICATION_NUM ? TFS_SUCCESS : EXIT_COPIES_OUT_OF_LIMIT;
          if (TFS_SUCCESS == ret)
          {
            if (!has_master_())
            {
              const int32_t random_value = random() % SYSPARAM_NAMESERVER.max_replication_;
              if (0 == random_value)
              {
                choose_master_ = BLOCK_CHOOSE_MASTER_COMPLETE_FLAG_YES;
                pos = servers_.begin();
              }
            }
            servers_.insert(pos, std::make_pair(server, NULL == info ? BLOCK_VERSION_INIT_VALUE : info->version_));
          }
        }
        else
        {
          if (NULL != info && result->second != info->version_ )
            const_cast<SERVER_ITEM*>(result)->second = info->version_;
          if (!has_master_())
          {
            const int32_t random_value = random() % SYSPARAM_NAMESERVER.max_replication_;
            if (0 == random_value)
            {
              choose_master_ = BLOCK_CHOOSE_MASTER_COMPLETE_FLAG_YES;
              SERVER_ITEM new_item = *result;
              remove(new_item.first, now);
              servers_.insert(servers_.begin(), new_item);
            }
          }
        }
        if (TFS_SUCCESS == ret)
        {
          master = is_master(server);
        }
      }
      return ret;
    }

    int BlockCollect::remove(const uint64_t server, const time_t now)
    {
      int32_t ret = (INVALID_SERVER_ID != server) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        last_leave_time_ =  now;
        int32_t index = 0;
        ret = EXIT_DATASERVER_NOT_FOUND;
        SERVER_ITER iter = servers_.begin();
        for (; iter != servers_.end() && TFS_SUCCESS != ret; ++iter, ++index)
        {
          ret = (server == iter->first) ? TFS_SUCCESS : EXIT_DATASERVER_NOT_FOUND;
          if (TFS_SUCCESS == ret)
          {
            servers_.erase(iter);
            if (has_master_() && 0 == index)
              choose_master_ = BLOCK_CHOOSE_MASTER_COMPLETE_FLAG_NO;
          }
        }
      }
      return ret;
    }

    bool BlockCollect::exist(const uint64_t server) const
    {
      bool result = false;
      CONST_SERVER_ITER iter = servers_.begin();
      for (; iter != servers_.end() && !result; ++iter)
      {
        result = server == iter->first;
      }
      return result;
    }

    bool BlockCollect::is_master(const uint64_t server) const
    {
      bool result = (!servers_.empty() && common::INVALID_SERVER_ID != server);
      if (result)
      {
        result = is_in_family() ? exist(server) : has_master_() && server == servers_[0].first;
      }
      return result;
    }

    void BlockCollect::get_servers(ArrayHelper<uint64_t>& servers) const
    {
      CONST_SERVER_ITER iter = servers_.begin();
      for (; iter != servers_.end(); ++iter)
      {
        servers.push_back(iter->first);
      }
    }

    uint64_t BlockCollect::get_server(const int8_t index) const
    {
      uint64_t server = INVALID_SERVER_ID;
      int32_t size    = servers_.size();
      if (!servers_.empty()& index >= 0 && index < size)
      {
        server = servers_[index].first;
      }
      return server;
    }

    /**
     * to check a block if replicate
     * @return: -1: none, 0: normal, 1: emergency
     */
    PlanPriority BlockCollect::check_replicate(const time_t now) const
    {
      PlanPriority priority = PLAN_PRIORITY_NONE;
      if (!is_creating() && !is_in_family() && expire(now) && leave_timeout_(now)
        && !in_replicate_queue() && !has_valid_lease(now))
      {
        const int32_t size = servers_.size();
        if (size <= 0)
        {
          TBSYS_LOG(WARN, "block: %"PRI64_PREFIX"u has been lost, do not replicate", info_.block_id_);
        }
        else
        {
          if (size < SYSPARAM_NAMESERVER.max_replication_)
            priority = PLAN_PRIORITY_NORMAL;
          if (1 == size && SYSPARAM_NAMESERVER.max_replication_ > 1)
            priority = PLAN_PRIORITY_EMERGENCY;
        }
      }
      return priority;
    }

    /**
     * to check a block if compact
     * @return: return true if need compact
     */
    bool BlockCollect::check_compact(const time_t now, const bool check_in_family) const
    {
      bool ret = false;
      bool check = check_in_family ? !is_in_family() : true;
      if (!is_creating() && check && !in_replicate_queue() && expire(now) && is_full()
          && !has_valid_lease(now) && check_copies_complete())
      {
        if ((info_.file_count_ > 0)
            && (info_.size_ > 0)
            && (info_.del_file_count_ > 0)
            && (info_.del_size_ > 0))
        {
          const int32_t delete_file_num_ratio = get_delete_file_num_ratio_();
          const int32_t delete_size_ratio = get_delete_file_size_ratio_();
          const int32_t update_file_num_ratio = get_update_file_num_ratio_();
          const int32_t update_size_ratio = get_update_file_size_ratio_();
          if ((delete_file_num_ratio >  SYSPARAM_NAMESERVER.compact_delete_ratio_)
              || (delete_size_ratio > SYSPARAM_NAMESERVER.compact_delete_ratio_)
              || (update_file_num_ratio >  SYSPARAM_NAMESERVER.compact_update_ratio_)
              || (update_size_ratio > SYSPARAM_NAMESERVER.compact_update_ratio_))
          {
            TBSYS_LOG(DEBUG, "block: %"PRI64_PREFIX"u need compact", info_.block_id_);
            ret = true;
          }
        }
      }
      return ret;
    }

    bool BlockCollect::check_balance(const time_t now) const
    {
      return (!is_creating() && !in_replicate_queue() && !has_valid_lease(now)
              && expire(now) && check_copies_complete());
    }

    bool BlockCollect::check_marshalling(const time_t now) const
    {
      bool ret = (!is_creating() && !is_in_family() && !in_replicate_queue() && !has_valid_lease(now) && is_full()
              && expire(now) && check_copies_complete() && size() <= MAX_MARSHALLING_BLOCK_SIZE_LIMIT);
      if (ret)
      {
        const int32_t server_size = servers_.size();
        ret = (server_size >= SYSPARAM_NAMESERVER.max_replication_  && is_full() && size() <= MAX_MARSHALLING_BLOCK_SIZE_LIMIT);
        if (ret)
        {
          const int32_t delete_file_num_ratio = get_delete_file_num_ratio_();
          const int32_t delete_size_ratio = get_delete_file_size_ratio_();
          const int32_t marshalling_visit_time = (time(NULL) - info_.last_access_time_) / 86400;
          ret = (delete_file_num_ratio <= SYSPARAM_NAMESERVER.marshalling_delete_ratio_
                && delete_size_ratio <= SYSPARAM_NAMESERVER.marshalling_delete_ratio_
                && marshalling_visit_time >= SYSPARAM_NAMESERVER.marshalling_visit_time_);
        }
      }
      return ret;
    }

    bool BlockCollect::check_reinstate(const time_t now) const
    {
      return (!is_creating() && is_in_family() && (servers_.size() <= 0U)
        && expire(now) && leave_timeout_(now));
    }

    bool BlockCollect::check_need_adjust_copies_location(common::ArrayHelper< std::pair<uint64_t, int32_t> >& adjust_copies, const time_t now) const
    {
      bool ret = (!is_creating() && expire(now)
                  && servers_.size() > 1U && adjust_copies.get_array_size() >= MAX_REPLICATION_NUM);
      if (ret)
      {
        uint32_t lan    = 0;
        int32_t  version = -1;
        uint64_t server = INVALID_SERVER_ID;
        uint32_t lans[MAX_REPLICATION_NUM] = {0};
        const int32_t size = servers_.size();
        CONST_SERVER_ITER iter = servers_.begin();
        for (; iter != servers_.end(); ++iter)
        {
          if (iter->second > version)
            version = iter->second;
          server = iter->first;
          lan = Func::get_lan(server, SYSPARAM_NAMESERVER.group_mask_);
          for (int32_t index = 0; index < MAX_REPLICATION_NUM && 0 != lan; ++index)
          {
            if (lans[index] == lan)
            {
              adjust_copies.push_back((*iter));
              break;
            }
            if (0 == lans[index])
            {
              lans[index] = lan;
              break;
            }
          }
        }
        for (iter = servers_.begin(); iter != servers_.end(); ++iter)
        {
          int32_t diff = version - iter->second;
          if (diff <= VERSION_DIFF)
          {
            adjust_copies.push_back((*iter));
          }
        }
        ret = ((size - adjust_copies.get_array_index()) >= 1);
      }
      return ret;
    }

    int BlockCollect::scan(SSMScanParameter& param) const
    {
      int16_t child_type = param.child_type_;
      bool has_dump = (child_type & SSM_CHILD_BLOCK_TYPE_FULL) ? is_full() : true;
      if (has_dump)
      {
        if (child_type & SSM_CHILD_BLOCK_TYPE_INFO)
        {
          int64_t pos = 0;
          param.data_.ensureFree(info_.length());
          int32_t ret = info_.serialize(param.data_.getFree(), param.data_.getFreeLen(), pos);
          if (TFS_SUCCESS == ret)
            param.data_.pourData(info_.length());
        }
        if (child_type & SSM_CHILD_BLOCK_TYPE_SERVER)
        {
          uint8_t count = 0;
          param.data_.writeInt8(count);
          CONST_SERVER_ITER iter = servers_.begin();
          for (; iter != servers_.end(); ++iter)
          {
            ++count;
            param.data_.writeInt64(iter->first);
            param.data_.writeInt32(iter->second);
          }
          // data addr will change when expand, so can't keep absolute addr
          unsigned char* pdata = reinterpret_cast<unsigned char*>(param.data_.getFree() - count * (INT64_SIZE + INT_SIZE) - INT8_SIZE);
          param.data_.fillInt8(pdata, count);
          if (count != servers_.size())
          {
            dump(TBSYS_LOG_LEVEL(WARN));
          }
        }
        if (child_type & SSM_CHILD_BLOCK_TYPE_STATUS)
        {
          param.data_.writeInt64(get());
          param.data_.writeInt8(create_flag_);
          param.data_.writeInt8(in_replicate_queue_);
          param.data_.writeInt8(has_lease_);
        }
      }
      return has_dump ? TFS_SUCCESS : TFS_ERROR;
    }

    void BlockCollect::resolve_version_conflict(common::ArrayHelper<std::pair<uint64_t, int32_t> >& helper)
    {
      int32_t version = -1, count = 0, total = servers_.size();
      SERVER_ITER iter = servers_.begin();
      for (; iter != servers_.end(); ++iter)
      {
        if (iter->second > version)
        {
          count = 0;
          version = iter->second;
        }
        if (iter->second >= version)
          ++count;
      }

      if (total > 0 && total != count)
      {
        info_.version_ = version;
        iter = servers_.begin();
        while (iter != servers_.end())
        {
          int32_t diff = version - iter->second;
          if (diff > VERSION_DIFF)
          {
            helper.push_back((*iter));
            iter = servers_.erase(iter);
          }
          else
          {
            ++iter;
          }
        }
        std::stringstream normal, abnormal;
        print_int64(helper, abnormal);
        print_int64(servers_, normal);
        TBSYS_LOG(INFO, "block: %lu, resolve_version_conflict: normal %s, abnormal %s", id(), normal.str().c_str(), abnormal.str().c_str());
      }
    }

    int BlockCollect::apply_lease(const uint64_t server, const time_t now, const int32_t step, const bool update,
      common::ArrayHelper<std::pair<uint64_t, int32_t> >& helper)
    {
      int32_t ret = (INVALID_SERVER_ID != server) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = IS_VERFIFY_BLOCK(id()) ? EXIT_VERIYF_BLOCK_CANNOT_APPLY_LESAE : TFS_SUCCESS;
      }
      if (TFS_SUCCESS == ret)
      {
        ret = expire(now) ? TFS_SUCCESS : EXIT_APPLY_BLOCK_SAFE_MODE_TIME_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        ret = has_valid_lease(now) ? EXIT_LEASE_EXISTED : TFS_SUCCESS;
      }
      if (TFS_SUCCESS == ret)
      {
        resolve_version_conflict(helper);
        bool check = update ? true : is_writable();
        ret = check && is_master(server) && check_copies_complete() ? TFS_SUCCESS : EXIT_CANNOT_APPLY_LEASE;
      }
      if (TFS_SUCCESS == ret)
      {
        set(now, step);
        set_has_lease(BLOCK_HAS_LEASE_FLAG_YES);
      }
      return ret;
    }

    int BlockCollect::renew_lease(const uint64_t server, const time_t now, const int32_t step, const bool update,
        const common::BlockInfoV2& info,common::ArrayHelper<std::pair<uint64_t, int32_t> >& helper)
    {
      int32_t ret = (INVALID_SERVER_ID != server) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = IS_VERFIFY_BLOCK(id()) ? EXIT_VERIYF_BLOCK_CANNOT_APPLY_LESAE : TFS_SUCCESS;
      }
      if (TFS_SUCCESS == ret)
      {
        ret = has_valid_lease(now) ? TFS_SUCCESS: EXIT_LEASE_EXPIRED;
      }
      if (TFS_SUCCESS == ret)
      {
        if (info.version_ > info_.version_)
        {
          info_ = info;
          update_all_version_(info.version_);
        }
      }
      if (TFS_SUCCESS == ret)
      {
        resolve_version_conflict(helper);
        bool check = update ? true : is_writable();
        ret = check && is_master(server) && check_copies_complete() ? TFS_SUCCESS : EXIT_CANNOT_APPLY_LEASE;
      }
      if (TFS_SUCCESS == ret)
      {
        set(now, step);
      }
      return ret;
    }

    int BlockCollect::giveup_lease(const uint64_t server, const time_t now, const common::BlockInfoV2* info)
    {
      UNUSED(server);
      int32_t ret = (INVALID_SERVER_ID != server) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        set(now, 0);
        set_has_lease(BLOCK_HAS_LEASE_FLAG_NO);
        if (NULL != info && info->version_ > info_.version_)
        {
          info_ = *info;
          update_all_version_(info->version_);
        }
      }
      return ret;
    }

    void BlockCollect::update_version(const common::ArrayHelper<uint64_t>& helper, const int32_t step)
    {
      info_.version_ += step;
      for (int64_t index = 0; index < helper.get_array_index(); ++index)
      {
        SERVER_ITEM* item = get_(*helper.at(index));
        if (NULL != item)
        {
          item->second += step;
        }
      }
    }

    void BlockCollect::dump(int32_t level, const char* file, const int32_t line, const char* function, const pthread_t thid) const
    {
      if (level <= TBSYS_LOGGER._level)
      {
        std::stringstream str;
        CONST_SERVER_ITER iter = servers_.begin();
        for (; iter != servers_.end(); ++iter)
        {
          str << CNetUtil::addrToString(iter->first) << iter->second << "/";
        }
        int64_t now = Func::get_monotonic_time();
        TBSYS_LOGGER.logMessage(level, file, line, function,thid,
            "dump block information: family id: %"PRI64_PREFIX"d, block_id: %"PRI64_PREFIX"u, version: %d, file_count: %d,\
            size: %d, del_file_count: %d, del_size: %d, update_file_count: %d, update_file_size: %d, is_creating: %s,\
            in_replicate_queue: %s, has_lease: %s, expire time: %"PRI64_PREFIX"d, has_valild_lease: %s, servers: %s, server_size: %zd",
            info_.family_id_, info_.block_id_, info_.version_, info_.file_count_,
            info_.size_, info_.del_file_count_, info_.del_size_,info_.update_file_count_,
            info_.update_size_,is_creating() ? "yes" : "no", in_replicate_queue() ? "yes" : "no",
            has_lease() ? "yes" : "no", get(), has_valid_lease(now) ? "yes" : "now", str.str().c_str(), servers_.size());
      }
    }

    void BlockCollect::callback(void* args, LayoutManager& manager)
    {
      UNUSED(args);
      uint64_t servers[32];
      ArrayHelper<uint64_t> helper(32, servers);
      manager.get_block_manager().get_mutex_(id()).wrlock();
      get_servers(helper);
      servers_.clear();
      manager.get_block_manager().get_mutex_(id()).unlock();
      for (int64_t index = 0; index < helper.get_array_index(); ++index)
      {
        uint64_t server = *helper.at(index);
        ServerCollect* pserver = manager.get_server_manager().get(server);
        manager.get_server_manager().relieve_relation(pserver, id());
      }
    }

    BlockCollect::SERVER_ITEM* BlockCollect::get_server_item(const uint64_t server)
    {
      return get_(server);
    }

    BlockCollect::SERVER_ITEM* BlockCollect::get_(const uint64_t server)
    {
      SERVER_ITEM* result = NULL;
      SERVER_ITER iter = servers_.begin();
      for (; iter != servers_.end() && NULL == result; ++iter)
      {
        if (server == iter->first)
        {
          result = &(*iter);
        }
      }
      return result;
    }

    void BlockCollect::update_all_version_(const int32_t version)
    {
      SERVER_ITER iter = servers_.begin();
      for (; iter != servers_.end(); ++iter)
      {
        iter->second = version;
      }
    }
  }/** end namesapce nameserver **/
}/** end namesapce tfs **/
