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

#ifndef TFS_CHECKSERVER_CHECKMANAGER_H
#define TFS_CHECKSERVER_CHECKMANAGER_H

#include <iostream>
#include <string>
#include <vector>

#include "common/tfs_vector.h"
#include "common/internal.h"
#include "message/message_factory.h"
#include "tbsys.h"
#include "TbThread.h"
#include "base_server_helper.h"

namespace tfs
{
  namespace checkserver
  {
    enum ServerStatus
    {
      SERVER_STATUS_OK,
      SERVER_STATUS_FAIL
    };

    enum BlockStatus
    {
      BLOCK_STATUS_INIT,
      BLOCK_STATUS_FAIL,
      BLOCK_STATUS_SUCC
    };

    class BlockObject
    {
      public:
        BlockObject():
        block_id_(common::INVALID_BLOCK_ID), server_size_(0),
        status_(BLOCK_STATUS_INIT)
        {
        }

        explicit BlockObject(const uint64_t block_id):
          block_id_(block_id), server_size_(0),
          status_(BLOCK_STATUS_INIT)
        {
        }

        ~BlockObject()
        {
        }

        void reset()
        {
          tbutil::Mutex::Lock lock(mutex_);
          server_size_ = 0;
        }

        uint64_t get_block_id() const
        {
          return block_id_;
        }

        void set_block_id(const uint64_t block_id)
        {
          block_id_ = block_id;
        }

        void add_server(const uint64_t server_id)
        {
          tbutil::Mutex::Lock lock(mutex_);
          bool found = false;
          for (int index = 0; index < server_size_; index++)
          {
            if (servers_[index] == server_id)
            {
              found = true;
              break;
            }
          }
          if (!found)
          {
            servers_[server_size_++] = server_id;
          }
        }

        void remove_server(const uint64_t server_id)
        {
          tbutil::Mutex::Lock lock(mutex_);
          bool found = false;
          int32_t index = 0;
          for ( ; index < server_size_; index++)
          {
            if (servers_[index] == server_id)
            {
              found = true;
              break;
            }
          }

          if (found)
          {
            for ( ; index < server_size_ - 1; index++)
            {
              servers_[index] = servers_[index+1];
            }
            server_size_--;
          }
        }

        const uint64_t* get_servers() const
        {
          return servers_;
        }

        const int32_t get_server_size() const
        {
          tbutil::Mutex::Lock lock(mutex_);
          return server_size_;
        }

        int8_t get_status() const
        {
          return status_;
        }

        void set_status(const int8_t status)
        {
          status_ = status;
        }

      private:
        uint64_t block_id_;
        uint64_t servers_[common::MAX_REPLICATION_NUM];
        int8_t server_size_;
        int8_t status_;
        tbutil::Mutex mutex_;
    };

    struct BlockIdCompare
    {
      bool operator () (const BlockObject* lhs, const BlockObject* rhs) const
      {
        assert(NULL != lhs);
        assert(NULL != rhs);
        return lhs->get_block_id() < rhs->get_block_id();
      }
    };

    class ServerObject
    {
      public:
        ServerObject():
          server_id_(common::INVALID_SERVER_ID), status_(SERVER_STATUS_FAIL)
        {
        }

        explicit ServerObject(const uint64_t server_id):
          server_id_(server_id), status_(SERVER_STATUS_FAIL)
        {
        }

        ~ServerObject()
        {
        }

        void reset()
        {
          tbutil::Mutex::Lock lock(mutex_);
          blocks_.clear();
        }

        uint64_t get_server_id() const
        {
          return server_id_;
        }

        void set_server_id(const uint64_t server_id)
        {
          server_id_ = server_id;
        }

        void add_block(const uint64_t block_id)
        {
          tbutil::Mutex::Lock lock(mutex_);
          blocks_.push_back(block_id);
        }

        const common::VUINT64& get_blocks() const
        {
          return blocks_;
        }

        ServerStatus get_status() const
        {
          return status_;
        }

        void set_status(const ServerStatus status)
        {
          status_ = status;
        }

      private:
        uint64_t server_id_;
        common::VUINT64 blocks_;
        ServerStatus status_;
        tbutil::Mutex mutex_;
    };

    struct ServerIdCompare
    {
      bool operator ()(const ServerObject* lhs, const ServerObject* rhs) const
      {
        assert(NULL != lhs);
        assert(NULL != rhs);
        return lhs->get_server_id() < rhs->get_server_id();
      }
    };


    typedef std::set<BlockObject*, BlockIdCompare> BLOCK_MAP;
    typedef std::set<BlockObject*, BlockIdCompare>::iterator BLOCK_MAP_ITER;
    typedef std::set<BlockObject*, BlockIdCompare>::const_iterator BLOCK_MAP_CONST_ITER;
    typedef common::TfsSortedVector<ServerObject*, ServerIdCompare> SERVER_MAP;
    typedef common::TfsSortedVector<ServerObject*, ServerIdCompare>::iterator SERVER_MAP_ITER;
    typedef common::TfsSortedVector<ServerObject*, ServerIdCompare>::iterator SERVER_MAP_CONST_ITER;

    struct BlockSlot
    {
      BLOCK_MAP blocks_;
      tbutil::Mutex mutex_;
      volatile int32_t succ_count_;
      volatile int32_t fail_count_;

      BlockSlot()
      {
        succ_count_ = 0;
        fail_count_ = 0;
      }
    };

    class CheckServer;
    class CheckManager
    {
      public:
        CheckManager(CheckServer& server, BaseServerHelper* server_helper);
        ~CheckManager();
        int64_t get_seqno() const;
        const BlockSlot* get_blocks() const;
        const SERVER_MAP* get_servers() const;
        void get_block_size(int64_t& total, int64_t& succ, int64_t& fail) const;
        int32_t get_server_size() const;
        int handle(tbnet::Packet* packet);
        void run_check();
        void stop_check();

        void add_block(const uint64_t block_id, const uint64_t server_id);
        void add_server(const uint64_t server_id, const uint64_t blocks);
        uint64_t assign_block(const BlockObject& block);
        int retry_get_group_info(const uint64_t ns, int32_t& group_count, int32_t& group_seq);
        int retry_get_all_ds(const uint64_t ns_id, common::VUINT64& servers);
        int retry_get_block_replicas(const uint64_t ns_id, const uint64_t block_id, common::VUINT64& servers);
        int retry_fetch_check_blocks(const uint64_t ds_id, const common::TimeRange& range, common::VUINT64& blocks);
        int retry_dispatch_check_blocks(const uint64_t ds_id, const uint64_t peer_id_,
            const int64_t seqno, const int32_t interval, const common::CheckFlag flag, const common::VUINT64& blocks);

      private:
        void clear();
        void reset_servers();
        int get_group_info();
        int fetch_servers();
        int fetch_blocks(const common::TimeRange& time_range);
        int assign_blocks();
        int dispatch_task();
        int update_task(message::ReportCheckBlockResponseMessage* message);
        int check_blocks(const common::TimeRange& range);

      private:
        class FetchBlockThread: public tbutil::Thread
        {
          public:
            FetchBlockThread(CheckManager& check_manager, const common::TimeRange& time_range,
                const int32_t thread_count, const int32_t thread_seq):
                check_manager_(check_manager), time_range_(time_range),
                thread_count_(thread_count), thread_seq_(thread_seq)

            {
            }

            ~FetchBlockThread()
            {
            }

            /*
             * Iterator every server
             * send FetchModifiedBlockMessage to DS
             * wait for request, if fail, retry fetch
             */
            void run();

          private:
            DISALLOW_COPY_AND_ASSIGN(FetchBlockThread);
            CheckManager& check_manager_;
            common::TimeRange time_range_;
            int32_t thread_count_;
            int32_t thread_seq_;
        };
        typedef tbutil::Handle<FetchBlockThread> FetchBlockThreadPtr;

        class AssignBlockThread: public tbutil::Thread
        {
          public:
            AssignBlockThread(CheckManager& check_manager,
                const int32_t thread_count, const int32_t thread_seq):
                check_manager_(check_manager), thread_count_(thread_count), thread_seq_(thread_seq)
            {
            }

            ~AssignBlockThread()
            {
            }

            /**
             * dispatch every sever's block to server
             * if fail, retry, fail
             */
            void run();

          private:
            DISALLOW_COPY_AND_ASSIGN(AssignBlockThread);
            CheckManager& check_manager_;
            int32_t thread_count_;
            int32_t thread_seq_;
        };
        typedef tbutil::Handle<AssignBlockThread> AssignBlockThreadPtr;

        class DispatchBlockThread: public tbutil::Thread
        {
          public:
            DispatchBlockThread(CheckManager& check_manager,
                const int32_t thread_count, const int32_t thread_seq):
                check_manager_(check_manager), thread_count_(thread_count), thread_seq_(thread_seq)
            {
            }

            ~DispatchBlockThread()
            {
            }

            /**
             * dispatch every sever's block to server
             * if fail, retry, fail
             */
            void run();

          private:
            DISALLOW_COPY_AND_ASSIGN(DispatchBlockThread);
            CheckManager& check_manager_;
            int32_t thread_count_;
            int32_t thread_seq_;
        };
        typedef tbutil::Handle<DispatchBlockThread> DispatchBlockThreadPtr;

      private:
        DISALLOW_COPY_AND_ASSIGN(CheckManager);
        CheckServer& server_;
        BlockSlot* all_blocks_;
        SERVER_MAP all_servers_;
        BaseServerHelper* server_helper_;
        FILE* result_fp_;
        int32_t group_count_;
        int32_t group_seq_;
        int64_t max_dispatch_num_;
        int64_t seqno_;  // every check has an unique seqno, use timestamp(us)
        int32_t turn_;
        bool stop_;      // check thread stop flag
    };
  }
}

#endif

