/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *   linqing <linqing.zyd@taobao.com>
 *      - initial release
 *
 */

#include <unistd.h>
#include <iostream>
#include <string>
#include <algorithm>

#include "common/directory_op.h"
#include "common/config_item.h"
#include "common/parameter.h"
#include "message/message_factory.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::message;
using namespace tbutil;
using namespace tbsys;

#include "default_server_helper.h"
#include "check_manager.h"

namespace tfs
{
  namespace checkserver
  {
    static const int32_t SERVER_SLOT_INIT_SIZE = 1024;
    static const int32_t SERVER_SLOT_EXPAND_DEFAULT = 1024;
    static const float   SERVER_SLOT_EXPAND_RATION_DEFAULT = 0.1;
    static const int32_t MAX_BLOCK_CHUNK_NUMS = 1024;
    static const int32_t DEFAULT_NETWORK_RETRY_TIMES = 2;
    static const int32_t CHECK_TIME_PER_BLOCK = 50; // ms
    static const int32_t CHECK_TIME_RESERVE = 30;   // seconds

    CheckManager::CheckManager(BaseServerHelper* server_helper):
      all_servers_(SERVER_SLOT_INIT_SIZE,
          SERVER_SLOT_EXPAND_DEFAULT,
          SERVER_SLOT_EXPAND_RATION_DEFAULT),
      server_helper_(server_helper),
      group_count_(1),
      group_seq_(0),
      max_dispatch_num_(0),
      seqno_(0),
      stop_(false)
    {
      all_blocks_ = new (std::nothrow) BLOCK_MAP[MAX_BLOCK_CHUNK_NUMS];
      assert(NULL != all_blocks_);
      bmutex_ = new (std::nothrow) Mutex[MAX_BLOCK_CHUNK_NUMS];
      assert(NULL != bmutex_);
    }

    CheckManager::~CheckManager()
    {
      for (int index = 0; index < MAX_BLOCK_CHUNK_NUMS; index++)
      {
        Mutex::Lock lock(bmutex_[index]);
        BLOCK_MAP_ITER bit = all_blocks_[index].begin();
        for ( ; bit != all_blocks_[index].end(); bit++)
        {
          // tbsys::gDelete(*bit);
          delete(*bit);
        }
      }

      SERVER_MAP_ITER sit = all_servers_.begin();
      for ( ; sit != all_servers_.end(); sit++)
      {
        tbsys::gDelete(*sit);
      }

      tbsys::gDeleteA(all_blocks_);
      tbsys::gDeleteA(bmutex_);
      tbsys::gDelete(server_helper_);
    }

    void CheckManager::clear()
    {
      seqno_ = 0;
      all_servers_.clear();
      // keep block, but clear server list
      for (int index = 0; index < MAX_BLOCK_CHUNK_NUMS; index++)
      {
        tbutil::Mutex::Lock lock(bmutex_[index]);
        BLOCK_MAP_ITER iter = all_blocks_[index].begin();
        for ( ; iter != all_blocks_[index].end(); iter++)
        {
          (*iter)->reset();
        }
      }
    }

    void CheckManager::reset_servers()
    {
      SERVER_MAP_ITER iter = all_servers_.begin();
      for ( ; iter != all_servers_.end(); iter++)
      {
        (*iter)->reset();
      }
    }

    int64_t CheckManager::get_seqno() const
    {
      return seqno_;
    }

    const BLOCK_MAP* CheckManager::get_blocks() const
    {
      return all_blocks_;
    }

    const SERVER_MAP* CheckManager::get_servers() const
    {
      return &all_servers_;
    }

    int64_t CheckManager::get_block_size() const
    {
      int64_t total = 0;
      for (int index = 0; index < MAX_BLOCK_CHUNK_NUMS; index++)
      {
        total += all_blocks_[index].size();
      }
      return total;
    }

    int32_t CheckManager::get_server_size() const
    {
      return all_servers_.size();
    }

    int CheckManager::handle(tbnet::Packet* packet)
    {
      int ret = TFS_SUCCESS;
      int pcode = packet->getPCode();
      switch(pcode)
      {
        case REPORT_CHECK_BLOCK_MESSAGE:
          ret = update_task(dynamic_cast<ReportCheckBlockMessage*>(packet));
          break;
        default:
          ret = TFS_ERROR;
          TBSYS_LOG(WARN, "unknown pcode : %d",  pcode);
          break;
      }
      return ret;
    }


    void CheckManager::add_block(const uint64_t block_id, const uint64_t server_id)
    {
      int32_t slot = block_id % MAX_BLOCK_CHUNK_NUMS;
      BlockObject query(block_id);
      tbutil::Mutex::Lock lock(bmutex_[slot]);
      BLOCK_MAP_ITER iter = all_blocks_[slot].find(&query);
      if (iter == all_blocks_[slot].end())
      {
        BlockObject* block = new (std::nothrow) BlockObject(block_id);
        assert(NULL != block);
        block->add_server(server_id);
        all_blocks_[slot].insert(block);
      }
      else
      {
        (*iter)->add_server(server_id);
      }
    }

    void CheckManager::add_server(const uint64_t server_id, const uint64_t block_id)
    {
      ServerObject query(server_id);
      SERVER_MAP_ITER iter = all_servers_.find(&query);
      if (iter == all_servers_.end())
      {
        ServerObject* server = new (std::nothrow) ServerObject(server_id);
        assert(NULL != server);
        server->add_block(block_id);
        all_servers_.insert(server);
      }
      else
      {
        (*iter)->add_block(block_id);
      }
    }

    // assign block to a normal server
    uint64_t CheckManager::assign_block(const BlockObject& block)
    {
      bool assigned = false;
      uint64_t server_id = INVALID_SERVER_ID;
      const int32_t replica_size = block.get_server_size();
      if (replica_size > 0)
      {
        const uint64_t *replicas = block.get_servers();
        int32_t random_index = rand() % replica_size;
        for (int index = 0; index < replica_size; index++)
        {
          server_id = replicas[(random_index + index) % replica_size];
          ServerObject query(server_id);
          SERVER_MAP_ITER sit = all_servers_.find(&query);
          if ((sit != all_servers_.end()) && (SERVER_STATUS_OK == (*sit)->get_status()))
          {
            assigned = true;
            break;
          }
        }
      }

      TBSYS_LOG(DEBUG, "assign block %"PRI64_PREFIX"u to server %s",
        block.get_block_id(), tbsys::CNetUtil::addrToString(server_id).c_str());

      return assigned ? server_id : INVALID_SERVER_ID;
    }

    int CheckManager::get_group_info()
    {
      uint64_t ns_id = SYSPARAM_CHECKSERVER.ns_id_;
      int ret = retry_get_group_info(ns_id, group_count_, group_seq_);
      TBSYS_LOG(INFO, "ns: %s, group count: %d, group seq: %d, ret: %d",
          tbsys::CNetUtil::addrToString(ns_id).c_str(), group_count_, group_seq_, ret);
      return ret;
    }

    int CheckManager::fetch_servers()
    {
      VUINT64 servers;
      uint64_t ns_id = SYSPARAM_CHECKSERVER.ns_id_;
      int ret = retry_get_all_ds(ns_id, servers);
      if (TFS_SUCCESS == ret)
      {
        TBSYS_LOG(INFO, "total %zd dataservers",  servers.size());
        VUINT64::iterator iter = servers.begin();
        for ( ; iter != servers.end(); iter++)
        {
          TBSYS_LOG(INFO, "add server %s", tbsys::CNetUtil::addrToString(*iter).c_str());
          ServerObject* server = new (std::nothrow) ServerObject(*iter);
          assert(NULL != server);
          all_servers_.insert(server);
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "get ds list from ns %s fail.",
            tbsys::CNetUtil::addrToString(ns_id).c_str());
      }
      return ret;
    }

    int CheckManager::fetch_blocks(const common::TimeRange& time_range)
    {
      int32_t thread_count = std::min(all_servers_.size(),  SYSPARAM_CHECKSERVER.thread_count_);
      FetchBlockThreadPtr* workers = new (std::nothrow) FetchBlockThreadPtr[thread_count];
      assert(NULL != workers);
      for (int index = 0; index < thread_count; index++)
      {
        workers[index] = new (std::nothrow) FetchBlockThread(*this, time_range, thread_count, index);
      }

      for (int index = 0; index < thread_count; index++)
      {
        workers[index]->start();
      }

      for (int index = 0; index < thread_count; index++)
      {
        workers[index]->join();
      }

      gDeleteA(workers);

      return TFS_SUCCESS;
    }

    int CheckManager::assign_blocks()
    {
      reset_servers(); // reset server assign information
      int32_t thread_count = std::min(MAX_BLOCK_CHUNK_NUMS,  SYSPARAM_CHECKSERVER.thread_count_);
      AssignBlockThreadPtr* workers = new (std::nothrow) AssignBlockThreadPtr[thread_count];
      assert(NULL != workers);
      for (int index = 0; index < thread_count; index++)
      {
        workers[index] = new (std::nothrow) AssignBlockThread(*this, thread_count, index);
      }

      for (int index = 0; index < thread_count; index++)
      {
        workers[index]->start();
      }

      for (int index = 0; index < thread_count; index++)
      {
        workers[index]->join();
      }

      gDeleteA(workers);

      return TFS_SUCCESS;
    }

    int CheckManager::dispatch_task()
    {
      int32_t thread_count = std::min(all_servers_.size(),  SYSPARAM_CHECKSERVER.thread_count_);
      DispatchBlockThreadPtr* workers = new (std::nothrow) DispatchBlockThreadPtr[thread_count];
      assert(NULL != workers);
      for (int index = 0; index < thread_count; index++)
      {
        workers[index] = new (std::nothrow) DispatchBlockThread(*this, thread_count, index);
      }

      for (int index = 0; index < thread_count; index++)
      {
        workers[index]->start();
      }

      for (int index = 0; index < thread_count; index++)
      {
        workers[index]->join();
      }

      gDeleteA(workers);

      // used to estimate wait time
      max_dispatch_num_ = 0;
      SERVER_MAP_ITER iter = all_servers_.begin();
      for ( ; iter != all_servers_.end(); iter++)
      {
        int64_t current = (*iter)->get_blocks().size();
        TBSYS_LOG(INFO, "server %s dispatched %"PRI64_PREFIX"d blocks",
            tbsys::CNetUtil::addrToString((*iter)->get_server_id()).c_str(), current);
        if (current > max_dispatch_num_)
        {
          max_dispatch_num_ = current;
        }
      }

      return TFS_SUCCESS;

    }

    int CheckManager::update_task(message::ReportCheckBlockMessage* message)
    {
      int64_t seqno = message->get_seqno();
      uint64_t server_id = message->get_server_id();
      VUINT64& blocks = message->get_blocks();
      TBSYS_LOG(INFO, "server %s report seqno %"PRI64_PREFIX"u finish %zd blocks",
            tbsys::CNetUtil::addrToString(server_id).c_str(), seqno, blocks.size());
      if ((0 != seqno_) && (seqno == seqno_))
      {
        VUINT64::iterator iter = blocks.begin();
        for ( ; iter < blocks.end(); iter++)
        {
          int32_t slot = *iter % MAX_BLOCK_CHUNK_NUMS;
          BlockObject query(*iter);
          Mutex::Lock lock(bmutex_[slot]);
          BLOCK_MAP_ITER bit = all_blocks_[slot].find(&query);
          if (bit != all_blocks_[slot].end())  // remove succussfully checked block
          {
            // gDelete(*bit);
            all_blocks_[slot].erase(bit);
            delete(*bit);
            TBSYS_LOG(INFO, "block %"PRI64_PREFIX"u check done", *iter);
          }
        }
      }

      return TFS_SUCCESS;
    }

    void CheckManager::run_check()
    {
      while (!stop_)
      {
        TIMER_START();
        clear();  // clear servers & keep failed block
        int64_t now = Func::curr_time();
        seqno_ = now;
        TimeRange range;
        range.end_ = (now / 1000000) - 60; // ignore recent 1 minutes modify
        range.start_ = range.end_ - SYSPARAM_CHECKSERVER.check_span_;
        range.start_ = std::max(range.start_, 0L);  // if start equals 0, do full check
        check_blocks(range);
        TIMER_END();

        int32_t wait_time = SYSPARAM_CHECKSERVER.check_interval_ - TIMER_DURATION() / 1000000;
        wait_time = std::max(wait_time, 0);
        for (int index = 0; index < wait_time && !stop_; index++)
        {
          sleep(1);  // check if stoped every seconds, may receive stop signal
        }
      }
    }

    int CheckManager::check_blocks(const TimeRange& range)
    {
      int ret = get_group_info();
      if (TFS_SUCCESS == ret && !stop_)
      {
        ret = fetch_servers();
      }

      if (TFS_SUCCESS == ret && !stop_)
      {
        int retry_times = SYSPARAM_CHECKSERVER.check_retry_turns_;
        while (retry_times-- && !stop_)
        {
          if (TFS_SUCCESS == ret && !stop_)
          {
            ret = fetch_blocks(range);
          }

          if (TFS_SUCCESS == ret && !stop_)
          {
            ret = assign_blocks();
          }

          if (TFS_SUCCESS == ret && !stop_)
          {
            ret = dispatch_task();
          }

          // wait dataserver check finish
          int32_t wait_time = max_dispatch_num_ * CHECK_TIME_PER_BLOCK / 1000 + CHECK_TIME_RESERVE;
          TBSYS_LOG(INFO, "seqno %"PRI64_PREFIX"u sleep %d seconds to wait dataserver response",
              seqno_, wait_time);
          sleep(wait_time);

          // if all blocks have been checked
          if (0 == get_block_size())
          {
            break;
          }
        }

        // log all blocks that check failed
        int64_t fail_count = 0;
        for (int32_t index = 0; index < MAX_BLOCK_CHUNK_NUMS; index++)
        {
          BLOCK_MAP_ITER iter = all_blocks_[index].begin();
          for ( ; iter != all_blocks_[index].end(); iter++)
          {
            fail_count++;
            TBSYS_LOG(WARN, "block %"PRI64_PREFIX"u check fail.", (*iter)->get_block_id());
          }
        }

        TBSYS_LOG(INFO, "CHECK RESULT: "
            "seqno %"PRI64_PREFIX"d start at %s finish. fail count: %"PRI64_PREFIX"d",
            seqno_, Func::time_to_str(seqno_/1000000, 0).c_str(), fail_count);
      }

      return ret;
    }

    void CheckManager::stop_check()
    {
      stop_ = true;
    }

    int CheckManager::retry_get_group_info(const uint64_t ns, int32_t& group_count, int32_t& group_seq)
    {
      int ret = TFS_SUCCESS;
      int retry_times = DEFAULT_NETWORK_RETRY_TIMES;
      for (int index = 0; index < retry_times; index++)
      {
        ret = server_helper_->get_group_info(ns, group_count, group_seq);
        if (TFS_SUCCESS == ret)
        {
          break;
        }
      }
      return ret;
    }

    int CheckManager::retry_get_all_ds(const uint64_t ns_id, common::VUINT64& servers)
    {
      int ret = TFS_SUCCESS;
      int retry_times = DEFAULT_NETWORK_RETRY_TIMES;
      for (int index = 0; index < retry_times; index++)
      {
        ret = server_helper_->get_all_ds(ns_id, servers);
        if (TFS_SUCCESS == ret)
        {
          break;
        }
      }
      return ret;
    }

    int CheckManager::retry_get_block_replicas(const uint64_t ns_id,
        const uint64_t block_id, common::VUINT64& servers)
    {
      int ret = TFS_SUCCESS;
      int retry_times = DEFAULT_NETWORK_RETRY_TIMES;
      for (int index = 0; index < retry_times; index++)
      {
        ret = server_helper_->get_block_replicas(ns_id, block_id, servers);
        if (TFS_SUCCESS == ret)
        {
          break;
        }
      }
      return ret;
    }

    int CheckManager::retry_fetch_check_blocks(const uint64_t ds_id,
        const common::TimeRange& range, common::VUINT64& blocks)
    {
      int ret = TFS_SUCCESS;
      int retry_times = DEFAULT_NETWORK_RETRY_TIMES;
      for (int index = 0; index < retry_times; index++)
      {
        ret = server_helper_->fetch_check_blocks(ds_id,
            range, group_count_, group_seq_, blocks);
        if (TFS_SUCCESS == ret)
        {
          break;
        }
      }
      return ret;
    }

    int CheckManager::retry_dispatch_check_blocks(const uint64_t ds_id,
        const int64_t seqno, const common::VUINT64& blocks)
    {
      int ret = TFS_SUCCESS;
      int retry_times = DEFAULT_NETWORK_RETRY_TIMES;
      for (int index = 0; index < retry_times; index++)
      {
        ret = server_helper_->dispatch_check_blocks(ds_id, seqno, blocks);
        if (TFS_SUCCESS == ret)
        {
          break;
        }
      }
      return ret;
    }

    void CheckManager::FetchBlockThread::run()
    {
      const SERVER_MAP* all_servers = check_manager_.get_servers();
      SERVER_MAP_CONST_ITER iter = all_servers->begin() + thread_seq_;
      for ( ; iter < all_servers->end(); iter += thread_count_)
      {
        if (SERVER_STATUS_FAIL == (*iter)->get_status())
        {
          VUINT64 check_blocks;
          int ret = check_manager_.retry_fetch_check_blocks((*iter)->get_server_id(),
              time_range_, check_blocks);
          if (TFS_SUCCESS == ret)
          {
            (*iter)->set_status(SERVER_STATUS_OK);
            VUINT64::iterator bit = check_blocks.begin();
            for ( ; bit != check_blocks.end(); bit++)
            {
              check_manager_.add_block(*bit, (*iter)->get_server_id());
              TBSYS_LOG(DEBUG, "add block %"PRI64_PREFIX"u from %s",
                  *bit, tbsys::CNetUtil::addrToString((*iter)->get_server_id()).c_str());
            }
          }
          else
          {
            TBSYS_LOG(WARN, "fetch blocks from server %s fail, ret: %d",
                tbsys::CNetUtil::addrToString((*iter)->get_server_id()).c_str(), ret);
          }
        }
      }
    }

    void CheckManager::AssignBlockThread::run()
    {
      const BLOCK_MAP* all_blocks = check_manager_.get_blocks();
      for (int index = thread_seq_; index < MAX_BLOCK_CHUNK_NUMS; index += thread_count_)
      {
        BLOCK_MAP_ITER bit = all_blocks[index].begin();
        for ( ; bit != all_blocks[index].end(); bit++)
        {
          uint64_t server_id = check_manager_.assign_block(**bit);
          if (INVALID_SERVER_ID != server_id)
          {
            check_manager_.add_server(server_id, (*bit)->get_block_id()); // assign success
          }
          else  // assign fail, update block replica info from nameserver
          {
            VUINT64 replicas;
            int ret = check_manager_.retry_get_block_replicas(SYSPARAM_CHECKSERVER.ns_id_,
                (*bit)->get_block_id(), replicas);
            TBSYS_LOG(INFO, "update block %"PRI64_PREFIX"u replica info from ns",
                (*bit)->get_block_id());
            if (TFS_SUCCESS == ret)
            {
              (*bit)->reset();
              VUINT64::iterator iter = replicas.begin();
              for ( ; iter != replicas.end(); iter++)
              {
                (*bit)->add_server(*iter);
              }

              // re-assign block
              uint64_t server_id = check_manager_.assign_block(**bit);
              if (INVALID_SERVER_ID != server_id)
              {
                check_manager_.add_server(server_id, (*bit)->get_block_id()); // assign success
              }
            }
          }
        }
      }
    }

    void CheckManager::DispatchBlockThread::run()
    {
      const SERVER_MAP* all_servers = check_manager_.get_servers();
      SERVER_MAP_CONST_ITER iter = all_servers->begin() + thread_seq_;
      for ( ; iter < all_servers->end(); iter += thread_count_)
      {
        if ((*iter)->get_blocks().size() <= 0)
        {
          continue;
        }
        int ret = check_manager_.retry_dispatch_check_blocks((*iter)->get_server_id(),
            check_manager_.get_seqno(), (*iter)->get_blocks());
        TBSYS_LOG(INFO, "dispatch task to server %s, ret: %d",
            tbsys::CNetUtil::addrToString((*iter)->get_server_id()).c_str(), ret);
      }
    }

  }
}


