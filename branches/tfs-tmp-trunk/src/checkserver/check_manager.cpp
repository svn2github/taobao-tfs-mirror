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
    // TODO: add config item
    const char* g_ns_ip = "10.232.36.201:3100";
    const int32_t g_thread_count = 1;

    static const int32_t BLOCK_SLOT_INIT_SIZE = 10240;
    static const int32_t BLOCK_SLOT_EXPAND_DEFAULT = 1024;
    static const float   BLOCK_SLOT_EXPAND_RATION_DEFAULT = 0.1;
    static const int32_t SERVER_SLOT_INIT_SIZE = 1024;
    static const int32_t SERVER_SLOT_EXPAND_DEFAULT = 1024;
    static const float   SERVER_SLOT_EXPAND_RATION_DEFAULT = 0.1;

    CheckManager::CheckManager(BaseServerHelper* server_helper):
      all_blocks_(BLOCK_SLOT_INIT_SIZE,
          BLOCK_SLOT_EXPAND_DEFAULT,
          BLOCK_SLOT_EXPAND_RATION_DEFAULT),
      all_servers_(SERVER_SLOT_INIT_SIZE,
          SERVER_SLOT_EXPAND_DEFAULT,
          SERVER_SLOT_EXPAND_RATION_DEFAULT),
      server_helper_(server_helper),
      seqno_(0),
      stop_(false)
    {
      reset();
    }

    CheckManager::~CheckManager()
    {
      BLOCK_MAP_ITER bit = all_blocks_.begin();
      for ( ; bit != all_blocks_.end(); bit++)
      {
        gDelete(*bit);
      }
      SERVER_MAP_ITER sit = all_servers_.begin();
      for ( ; sit != all_servers_.end(); sit++)
      {
        gDelete(*sit);
      }
      gDelete(server_helper_);
    }

    void CheckManager::reset()
    {
      seqno_ = 0;
      all_servers_.clear();
      all_blocks_.clear();
    }

    void CheckManager::reset_servers()
    {
      SERVER_MAP_ITER iter = all_servers_.begin();
      for ( ; iter < all_servers_.end(); iter++)
      {
        (*iter)->reset();
      }
    }

    void CheckManager::reset_blocks()
    {
      BLOCK_MAP_ITER iter = all_blocks_.begin();
      for ( ; iter < all_blocks_.end(); iter++)
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
      return &all_blocks_;
    }

    const SERVER_MAP* CheckManager::get_servers() const
    {
      return &all_servers_;
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


    void CheckManager::add_server_to_block(const uint64_t server_id, const uint64_t block_id)
    {
      BlockObject query(block_id);
      BLOCK_MAP_ITER iter = all_blocks_.find(&query);
      if (iter != all_blocks_.end())
      {
        (*iter)->add_server(server_id);
      }
      else
      {
        BlockObject* block = new (std::nothrow) BlockObject(block_id);
        assert(NULL != block);
        block->add_server(server_id);
        all_blocks_.insert(block);
      }
    }

    void CheckManager::add_block_to_server(const uint64_t block_id, const uint64_t server_id)
    {
      ServerObject query(server_id);
      SERVER_MAP_ITER iter = all_servers_.find(&query);
      if (iter != all_servers_.end())
      {
        (*iter)->add_block(block_id);
      }
      else
      {
        ServerObject* server = new (std::nothrow) ServerObject(server_id);
        assert(NULL != server);
        server->add_block(block_id);
        all_servers_.insert(server);
      }
    }

    int CheckManager::fetch_servers()
    {
      VUINT64 servers;
      uint64_t ns_id = SYSPARAM_CHECKSERVER.ns_id_;
      int ret = server_helper_->get_all_ds(ns_id, servers);
      if (TFS_SUCCESS == ret)
      {
        VUINT64::iterator iter = servers.begin();
        for ( ; iter != servers.end(); iter++)
        {
          TBSYS_LOG(DEBUG, "add server %s", tbsys::CNetUtil::addrToString(*iter).c_str());
          ServerObject* server = new (std::nothrow) ServerObject(*iter);
          assert(NULL != server);
          all_servers_.insert(server);
        }
      }
      return ret;
    }

    int CheckManager::fetch_blocks(const uint64_t ds_id, const common::TimeRange& time_range, common::VUINT64& blocks)
    {
      return server_helper_->fetch_check_blocks(ds_id, time_range, blocks);
    }

    int CheckManager::dispatch_blocks(const uint64_t ds_id, const int64_t seqno, const common::VUINT64& blocks)
    {
      return server_helper_->dispatch_check_blocks(ds_id, seqno, blocks);
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
      int32_t thread_count = std::min(all_blocks_.size(),  SYSPARAM_CHECKSERVER.thread_count_);
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

      return TFS_SUCCESS;

    }

    int CheckManager::update_task(message::ReportCheckBlockMessage* message)
    {
      int64_t seqno = message->get_seqno();
      if ((0 != seqno_) && (seqno == seqno_))
      {
        VUINT64& blocks = message->get_blocks();
        VUINT64::iterator iter = blocks.begin();
        for ( ; iter < blocks.end(); iter++)
        {
          BlockObject query(*iter);
          BLOCK_MAP_ITER bit = all_blocks_.find(&query);
          if (bit != all_blocks_.end())
          {
            (*bit)->set_status(BLOCK_STATUS_DONE);
            TBSYS_LOG(DEBUG, "block %"PRI64_PREFIX"u check done.", *iter);
          }
        }
      }

      return TFS_SUCCESS;
    }

    void CheckManager::run_check()
    {
      while (!stop_)
      {
        seqno_ = Func::get_monotonic_time_us();
        TimeRange range;  // TODO: calculate check time range
        range.start_ = 0;
        range.end_ = 0x7FFFFFFFFFFFFFFF;
        check_blocks(range);
        sleep(SYSPARAM_CHECKSERVER.check_interval_ * 3600);
      }
    }

    int CheckManager::check_blocks(const TimeRange& range)
    {
      int ret = fetch_servers();
      if (TFS_SUCCESS == ret)
      {
        ret = fetch_blocks(range);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = assign_blocks();
      }

      if (TFS_SUCCESS == ret)
      {
        ret = dispatch_task();
      }

      sleep(180);  // TODO

      if (TFS_SUCCESS == ret)
      {
        BLOCK_MAP_ITER iter = all_blocks_.begin();
        for ( ; iter != all_blocks_.end(); iter++)
        {
          if (BLOCK_STATUS_DONE != (*iter)->get_status())
          {
            TBSYS_LOG(WARN, "block %"PRI64_PREFIX"u check fail.", (*iter)->get_block_id());
          }
        }
      }

      return ret;
    }

    void CheckManager::stop_check()
    {
      stop_ = true;
    }

    void CheckManager::FetchBlockThread::run()
    {
      TBSYS_LOG(DEBUG, "fetch block thread %d start", thread_seq_);
      const SERVER_MAP* all_servers = check_manager_.get_servers();
      SERVER_MAP_CONST_ITER iter = all_servers->begin() + thread_seq_;
      for ( ; iter < all_servers->end(); iter += thread_count_)
      {
        VUINT64 check_blocks;
        int ret = check_manager_.fetch_blocks((*iter)->get_server_id(), time_range_, check_blocks);
        if (TFS_SUCCESS == ret)
        {
          VUINT64::iterator bit = check_blocks.begin();
          for ( ; bit != check_blocks.end(); bit++)
          {
            check_manager_.add_server_to_block((*iter)->get_server_id(), *bit);
            TBSYS_LOG(DEBUG, "add block %"PRI64_PREFIX"u from server %s",
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

    void CheckManager::AssignBlockThread::run()
    {
      TBSYS_LOG(DEBUG, "assign block thread %d start", thread_seq_);
      const BLOCK_MAP* all_blocks = check_manager_.get_blocks();
      BLOCK_MAP_CONST_ITER iter = all_blocks->begin() + thread_seq_;
      for ( ; iter < all_blocks->end(); iter += thread_count_)
      {
        if (BLOCK_STATUS_DONE == (*iter)->get_status())
        {
          continue;
        }
        uint64_t alloc_server = (*iter)->next_server();
        check_manager_.add_block_to_server((*iter)->get_block_id(), alloc_server);
        TBSYS_LOG(DEBUG, "assign block %"PRI64_PREFIX"u to server %s",
            (*iter)->get_block_id(), tbsys::CNetUtil::addrToString(alloc_server).c_str());
      }
    }

    void CheckManager::DispatchBlockThread::run()
    {
      TBSYS_LOG(DEBUG, "dispatch block thread %d start", thread_seq_);
      const SERVER_MAP* all_servers = check_manager_.get_servers();
      SERVER_MAP_CONST_ITER iter = all_servers->begin() + thread_seq_;
      for ( ; iter < all_servers->end(); iter += thread_count_)
      {
        if ((*iter)->get_blocks().size() <= 0)
        {
          continue;
        }
        int ret = check_manager_.dispatch_blocks((*iter)->get_server_id(),
            check_manager_.get_seqno(), (*iter)->get_blocks());
        TBSYS_LOG(DEBUG, "dispatch task to server %s %s",
            tbsys::CNetUtil::addrToString((*iter)->get_server_id()).c_str(),
            (TFS_SUCCESS == ret) ? "success" : "fail");
      }
    }

  }
}


