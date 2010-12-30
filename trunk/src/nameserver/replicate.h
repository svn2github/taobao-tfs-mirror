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
#ifndef TFS_NAMESERVER_REPLICATE_H_
#define TFS_NAMESERVER_REPLICATE_H_ 

#include "ns_define.h"
#include "meta_manager.h"
#include "proactor_data_pipe.h"
#include "data_container.h"
#include "message/message_factory.h"
#include "strategy.h"
#include "scanner_manager.h"

namespace tfs
{
  namespace nameserver
  {

    class ReplicateLauncher;
    class ReplicateExecutor
    {
      friend class ReplicateLauncher;
      typedef __gnu_cxx::hash_map<uint32_t, common::ReplBlock*> REPL_BLOCK_MAP;
      typedef REPL_BLOCK_MAP::iterator REPL_BLOCK_MAP_ITER;

    public:
      ReplicateExecutor(MetaManager& m) :
        meta_mgr_(m)
      {
      }
      virtual ~ReplicateExecutor();

    public:
      int execute(const uint32_t block_id, void* args);
      int execute(std::vector<common::ReplBlock*> replPlan, void* args);
      int complete(const common::ReplBlock* cmd, bool success);
      int send_replicate_cmd(const uint64_t source_id, const uint64_t destination_id, const uint32_t block_id,
          const int32_t is_move);

      bool is_replicating_block(const common::ReplBlock* cmd, const int64_t count);
      bool is_replicating_block(const uint32_t block_id);
      int check(const uint32_t block_id, const time_t max_time, const bool complete, const bool timeout);
      void clear_all();

      inline const REPL_BLOCK_MAP & get_replicating_map() const
      {
        return replicating_map_;
      }
      inline REPL_BLOCK_MAP& get_replicating_map()
      {
        return replicating_map_;
      }
      inline const ReplicateStrategy::counter_type& get_src_ds_count() const
      {
        return src_ds_counter_;
      }
      inline const ReplicateStrategy::counter_type& get_dest_ds_count() const
      {
        return dest_ds_counter_;
      }
      void clear_replicating_info();

    private:
      common::RWLock mutex_;
      REPL_BLOCK_MAP replicating_map_;
      ReplicateStrategy::counter_type src_ds_counter_;
      ReplicateStrategy::counter_type dest_ds_counter_;
      MetaManager& meta_mgr_;
    };

    class ReplicateLauncher: public Launcher
    {
      friend class ReplicateExecutor;
      static const int8_t PAUSE_FLAG_RUN = 0x00;
      static const int8_t PAUSE_FLAG_PAUSE = 0x01;
      static const int8_t PAUSE_FLAG_PAUSE_BALANCE = 0x02;
      typedef std::vector<common::ReplBlock*> REPL_BLOCK_LIST;
    public:
      ReplicateLauncher(MetaManager& m);
      virtual ~ReplicateLauncher()
      {
      }
      ReplicateExecutor& get_executor()
      {
        return executor_;
      }
      inline void set_pause_flag(int8_t flag)
      {
        pause_flag_ = flag;
      }
      int emergency_replicate(uint32_t block_id);
      bool check(const BlockCollect* blkcol);
      int build_plan(const common::VUINT32& loseBlocks);
      int balance();
      int handle_replicate_complete(message::ReplicateBlockMessage* message);
      int handle_replicate_info_msg(message::ReplicateInfoMessage* message);

      void initialize();
      void destroy();
      void wait_for_shut_down();

      inline void inc_stop_balance_count()
      {
        ++stop_balance_count_;
      }
      inline void dec_stop_balance_count()
      {
        --stop_balance_count_;
      }
      inline int8_t* get_pause_flag()
      {
        return &pause_flag_;
      }

    private:
      void sort(REPL_BLOCK_LIST &blkList);

    private:
      MetaManager& meta_mgr_;
      ReplicateExecutor executor_;
      ProactorDataPipe<std::deque<uint32_t>, ReplicateExecutor> first_thread_;
      ProactorDataPipe<PipeDataAdaptor<common::ReplBlock*> , ReplicateExecutor> second_thread_;
      int64_t stop_balance_count_;
      int8_t pause_flag_;
      NsDestroyFlag destroy_flag_;
    };

    struct SeqNoSort
    {
      bool operator()(const common::ReplBlock *x, const common::ReplBlock *y) const
      {
        if (x->server_count_ == y->server_count_)
        {
          return (x->destination_id_ < y->destination_id_);
        }
        return (x->server_count_ < y->server_count_);
      }
    };

  }
}
#endif 

