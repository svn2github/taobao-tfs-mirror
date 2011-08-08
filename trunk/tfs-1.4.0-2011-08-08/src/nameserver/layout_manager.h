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

#ifndef TFS_NAMESERVER_LAYOUT_MANAGER_H_
#define TFS_NAMESERVER_LAYOUT_MANAGER_H_

#include <pthread.h>
#include <Timer.h>
#include "common/lock.h"
#include "block_chunk.h"
#include "block_collect.h"
#include "server_collect.h"
#include "lease_clerk.h"
#include "common/base_packet.h"
#include "oplog_sync_manager.h"
#include "client_request_server.h"

namespace tfs
{
namespace nameserver
{
  class LayoutManager
  {
    friend class ClientRequestServer;
  public:
    LayoutManager();
    virtual ~LayoutManager();

    int initialize(const int32_t chunk_num = 32);
    void wait_for_shut_down();
    void destroy();

    int32_t get_alive_server_size() const;
    ClientRequestServer& get_client_request_server();

    BlockChunkPtr get_chunk(const uint32_t block_id) const;
    ServerCollect* get_server(const uint64_t server_id);

    int add_server(const common::DataServerStatInfo& info, const time_t now, bool& isnew);
    int remove_server(const uint64_t id, const time_t now);
    int update_relation(ServerCollect* server, const std::vector<common::BlockInfo>& blocks, EXPIRE_BLOCK_LIST& expires, const time_t now);
    int build_relation(BlockCollect* block, ServerCollect* server, const time_t now, const bool force = false);

    BlockCollect* elect_write_block();

    OpLogSyncManager& get_oplog_sync_mgr();

    int update_block_info(const common::BlockInfo& block_info, const uint64_t server, const time_t now, const bool addnew);

    int repair(const uint32_t block_id, const uint64_t server, const int32_t flag, const time_t now, std::string& error_msg);

    int scan(common::SSMScanParameter& stream);
    int dump_plan(tbnet::DataBuffer& stream);
    int dump_plan(void);

    int handle_task_complete(common::BasePacket* msg);

    void interrupt(const uint8_t interrupt, const time_t now);

    int rm_block_from_ds(const uint64_t server_id, const uint32_t block_id);
    int rm_block_from_ds(const uint64_t server_id, const std::vector<uint32_t>& block_ids);

    inline void get_alive_server(common::VUINT64& servers)
    {
      common::RWLock::Lock lock(server_mutex_, common::READ_LOCKER);
      std::for_each(servers_. begin(), servers_.end(), GetAliveServer(servers));
    }

    bool find_block_in_plan(const uint32_t block_id);
    bool find_server_in_plan(ServerCollect* server);
    bool find_server_in_plan(const std::vector<ServerCollect*> & servers, bool all_find, std::vector<ServerCollect*>& result);

    int touch(const uint64_t server, const time_t now = time(NULL), const bool promote = true);
    int touch(ServerCollect* server, const time_t now, const bool promote = false);

    int open_helper_create_new_block_by_id(const uint32_t block_id);

#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION)
  public:
#else
  private:
#endif
    ServerCollect* get_server_(const uint64_t server_id);

    BlockCollect* add_block(uint32_t& block_id);
    BlockCollect* add_new_block(uint32_t& block_id, ServerCollect* server = NULL, const time_t now = time(NULL));

    BlockCollect* add_new_block_helper_create_by_id(const uint32_t block_id, const time_t now);
    BlockCollect* add_new_block_helper_create_by_system(uint32_t& block_id, ServerCollect* server, const time_t now);

    int add_new_block_helper_send_msg(const uint32_t block_id, const std::vector<ServerCollect*>& servers);
    int add_new_block_helper_write_log(const uint32_t block_id, const std::vector<ServerCollect*>& server);
    int add_new_block_helper_rm_block(const uint32_t block_id, std::vector<ServerCollect*>& servers);
    int add_new_block_helper_build_relation(const uint32_t block_id, const std::vector<ServerCollect*>& server);
    ServerCollect* find_server_in_vec(const std::vector<ServerCollect*>& servers, const uint64_t server_id);

    static bool relieve_relation(BlockCollect* block, ServerCollect* server, const time_t now);
    bool relieve_relation(ServerCollect* server, const time_t now);

    void rotate(const time_t now);

    uint32_t get_alive_block_id();
    int64_t calc_all_block_bytes() const;
    int64_t calc_all_block_count() const;

    void check_server();

    int set_runtime_param(const uint32_t index, const uint32_t value, char *retstr);
    int block_oplog_write_helper(const int32_t cmd, const common::BlockInfo& info, const std::vector<uint32_t>& blocks, const std::vector<uint64_t>& servers);

    template<typename Strategy, typename ElectType>
      friend int32_t elect_ds(Strategy& strategy, ElectType op, LayoutManager& meta, std::vector<ServerCollect*>& except,
          int32_t elect_count, bool check_server_in_plan, std::vector<ServerCollect*> & result);

    template<typename Strategy, typename ElectType>
      friend int32_t elect_ds(Strategy& strategy, ElectType op, LayoutManager& meta, std::vector<ServerCollect*>& source,
          std::vector<ServerCollect*>& except, int32_t elect_count, bool check_server_in_plan, std::vector<ServerCollect*>& result);

    friend int OpLogSyncManager::replay_helper_do_oplog(const int32_t type, const char* const data, const int64_t length , int64_t& pos, time_t now);

    struct AddLoad
    {
      int32_t operator()(const int32_t acc, const ServerCollect* const server);
    };

    struct BlockNumComp
    {
      bool operator()(const common::DataServerStatInfo& x, const common::DataServerStatInfo& y);
    };

    struct GetAliveServer
    {
      explicit GetAliveServer(common::VUINT64& servers);
      bool operator() (const std::pair<uint64_t, ServerCollect*>& node);
      common::VUINT64& servers_;
    };

#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION)
  public:
#else
  private:
#endif
    class Task: public tbutil::TimerTask
    {
      friend class LayoutManager;
      public:
      Task(LayoutManager* manager, const common::PlanType type, 
          const common::PlanPriority priority, const uint32_t block_id, 
          const time_t begin, const time_t end, const std::vector<ServerCollect*>& runer,
          const int64_t seqno);
      virtual ~ Task(){};
      virtual int handle() = 0;
      virtual int handle_complete(common::BasePacket* msg, bool& all_complete_flag) = 0;
      virtual void dump(tbnet::DataBuffer& stream);
      virtual void dump(const int32_t level, const char* const format = NULL);
      virtual void runTimerTask();
      bool operator < (const Task& task) const;
#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION)
      public:
#else
      protected:
#endif
      std::vector<ServerCollect*> runer_;
      time_t begin_time_;
      time_t end_time_;
      uint32_t block_id_;
      common::PlanType type_;
      common::PlanStatus status_;
      common::PlanPriority priority_;
      LayoutManager* manager_;
      int64_t seqno_;
      private:
      DISALLOW_COPY_AND_ASSIGN(Task);
    };
    typedef tbutil::Handle<Task> TaskPtr;

    struct TaskCompare
    {
      bool operator()(const TaskPtr& lhs, const TaskPtr& rhs)
      {
        //return lhs < rhs;
        return (*lhs) < (*rhs);
      }
    };
    class CompactTask: public Task 
    {
#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION)
      public:
#endif
        struct CompactComplete
        {
          uint64_t id_;
          uint32_t block_id_;
          common::PlanStatus status_;
          bool all_success_;
          bool has_success_;
          bool is_complete_;
          bool current_complete_result_;
          common::BlockInfo block_info_;
          CompactComplete(const uint64_t id, const uint32_t block_id, const common::PlanStatus status):
            id_(id), block_id_(block_id), status_(status), all_success_(true),
            has_success_(false), is_complete_(true), current_complete_result_(false){}
        };
      public:
        CompactTask(LayoutManager* manager, const common::PlanPriority priority,
                    const uint32_t block_id, const time_t begin, const time_t end,
                    const std::vector<ServerCollect*>& runer,
                    const int64_t seqno);
        virtual ~CompactTask(){}
        virtual int handle();
        virtual int handle_complete(common::BasePacket* msg, bool& all_complete_flag);
        virtual void runTimerTask();
        virtual void dump(tbnet::DataBuffer& stream);
        virtual void dump(const int32_t level, const char* const format = NULL);
#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION)
      public:
#else
      private:
#endif
        void check_complete(CompactComplete& value, common::VUINT64& ds_list);
        int do_complete(CompactComplete& value, common::VUINT64& ds_list);
        common::CompactStatus status_transform_plan_to_compact(const common::PlanStatus status) const;
        common::PlanStatus status_transform_compact_to_plan(const common::CompactStatus status) const;
#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION)
      public:
#else
      private:
#endif
        static const int8_t INVALID_SERVER_ID;
        static const int8_t INVALID_BLOCK_ID;
        std::vector< std::pair <uint64_t, common::PlanStatus> > complete_status_;
        common::BlockInfo block_info_;
      private:
        DISALLOW_COPY_AND_ASSIGN(CompactTask);
    };
    typedef tbutil::Handle<CompactTask> CompactTaskPtr;

    class ReplicateTask : public Task 
    {
      public:
        ReplicateTask(LayoutManager* manager, common::PlanPriority priority,
                      const uint32_t block_id, const time_t begin, const time_t end,
                      const std::vector<ServerCollect*>& runer,const int64_t seqno);
        virtual ~ReplicateTask(){}
        virtual int handle();
        virtual int handle_complete(common::BasePacket* msg, bool& all_complete_flag);
#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION)
      public:
#else
      protected:
#endif
        common::ReplicateBlockMoveFlag flag_;
      private:
        DISALLOW_COPY_AND_ASSIGN(ReplicateTask);
    };
    typedef tbutil::Handle<ReplicateTask> ReplicateTaskPtr;

    class DeleteBlockTask : public Task 
    {
      public:
        DeleteBlockTask(LayoutManager* manager, const common::PlanPriority priority,
                      const uint32_t block_id, const time_t begin, const time_t end,
                      const std::vector<ServerCollect*>& runer, const int64_t seqno);
        virtual ~DeleteBlockTask(){}
        virtual int handle();
        virtual int handle_complete(common::BasePacket* msg, bool& all_complete_flag);
      private:
        DISALLOW_COPY_AND_ASSIGN(DeleteBlockTask);
    };
    typedef tbutil::Handle<DeleteBlockTask> DeleteBlockTaskPtr;

    class MoveTask : public ReplicateTask
    {
      public:
        MoveTask(LayoutManager* manager, const common::PlanPriority priority,
                 const uint32_t block_id, const time_t begin, const time_t end,
                      const std::vector<ServerCollect*>& runer,const int64_t seqno);
        virtual ~MoveTask(){}
      private:
        DISALLOW_COPY_AND_ASSIGN(MoveTask);
    };
    typedef tbutil::Handle<MoveTask> MoveTaskPtr;

    class ExpireTask : public tbutil::TimerTask
    {
      public:
        explicit ExpireTask(LayoutManager* manager):
          manager_(manager) {}
        virtual ~ExpireTask() {}
        void runTimerTask();
      private:
        DISALLOW_COPY_AND_ASSIGN(ExpireTask);
        LayoutManager* manager_;
    };
    typedef tbutil::Handle<ExpireTask> ExpireTaskPtr;

    class BuildPlanThreadHelper: public tbutil::Thread
    {
      public:
        explicit BuildPlanThreadHelper(LayoutManager& manager):
          manager_(manager)
      {
        start();
      }
        virtual ~BuildPlanThreadHelper(){}
        void run();
      private:
        DISALLOW_COPY_AND_ASSIGN(BuildPlanThreadHelper);
        LayoutManager& manager_;
    };
    typedef tbutil::Handle<BuildPlanThreadHelper> BuildPlanThreadHelperPtr;

    class RunPlanThreadHelper : public tbutil::Thread
    {
      public:
        explicit RunPlanThreadHelper(LayoutManager& manager):
          manager_(manager)
      {
        start();
      }
        virtual ~RunPlanThreadHelper(){}
        void run();
      private:
        DISALLOW_COPY_AND_ASSIGN(RunPlanThreadHelper);
        LayoutManager& manager_;
    };
    typedef tbutil::Handle<RunPlanThreadHelper> RunPlanThreadHelperPtr;

    class CheckDataServerThreadHelper: public tbutil::Thread
    {
      public:
        explicit CheckDataServerThreadHelper(LayoutManager& manager):
          manager_(manager)
      {
        start();
      }
        virtual ~CheckDataServerThreadHelper(){}
        void run();
      private:
        LayoutManager& manager_;
        DISALLOW_COPY_AND_ASSIGN(CheckDataServerThreadHelper);
    };
    typedef tbutil::Handle<CheckDataServerThreadHelper> CheckDataServerThreadHelperPtr;
#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION)
  public:
#else
  private:
#endif
    int build_plan();
    void run_plan();

  public:
    void destroy_plan();

#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION)
  public:
#else
  private:
#endif

#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION)
    bool build_replicate_plan(const int64_t plan_seqno, const time_t now, int64_t& need, int64_t& adjust, int64_t& emergency_replicate_count, std::vector<uint32_t>& blocks);
    bool build_compact_plan(const int64_t plan_seqno, const time_t now, int64_t& need, std::vector<uint32_t>& blocks);
    bool build_balance_plan(const int64_t plan_seqno, const time_t now, int64_t& need, std::vector<uint32_t>& blocks);
    bool build_redundant_plan(const int64_t plan_seqno, const time_t now, int64_t& need, std::vector<uint32_t>& blocks);
#else
    bool build_replicate_plan(const int64_t plan_seqno, const time_t now, int64_t& need, int64_t& adjust, int64_t& emergency_replicate_count);
    bool build_compact_plan(const int64_t plan_seqno, const time_t now, int64_t& need);
    bool build_balance_plan(const int64_t plan_seqno, const time_t now, int64_t& need);
    bool build_redundant_plan(const int64_t plan_seqno, const time_t now, int64_t& need);
#endif
    void find_need_replicate_blocks(const int64_t need,
        const time_t now,
        int64_t& emergency_replicate_count,
        std::multimap<common::PlanPriority, BlockCollect*>& middle);

    int64_t calc_average_block_size();

    void statistic_all_server_info(const int64_t need,
        const int64_t average_block_size,
        double& total_capacity, 
        int64_t& total_block_count,
        int64_t& total_load,
        int64_t& alive_server_size);

    void split_servers(const int64_t need,
        const int64_t average_load,
        const double total_capacity,
        const int64_t total_block_count,
        const int64_t average_block_size,
        std::set<ServerCollect*>& source,
        std::set<ServerCollect*>& target);

    bool add_task(const TaskPtr& task);
    bool remove_task(const TaskPtr& task);
    TaskPtr find_task(const uint32_t block_id);
    void find_server_in_plan_helper(std::vector<ServerCollect*>& servers, std::vector<ServerCollect*>& except);
    bool expire();

    int64_t get_running_plan_size() const
    {
      return running_plan_list_.size();
    }

    int64_t get_pending_plan_size() const
    {
      return pending_plan_list_.size();
    }
#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION)
  public:
#else
  private:
#endif
    static const int8_t ELECT_SEQ_INITIALIE_NUM;
    static const int8_t INTERVAL;
    static const int8_t LOAD_BASE_MULTIPLE;
    BuildPlanThreadHelperPtr build_plan_thread_;
    RunPlanThreadHelperPtr run_plan_thread_;
    CheckDataServerThreadHelperPtr check_dataserver_thread_;
    std::map<ServerCollect*, TaskPtr> server_to_task_; 
    std::map<uint32_t, TaskPtr> block_to_task_;
    std::set<TaskPtr, TaskCompare> pending_plan_list_;
    std::set<TaskPtr, TaskCompare> running_plan_list_;
    std::list<TaskPtr> finish_plan_list_;

    OpLogSyncManager oplog_sync_mgr_;

    SERVER_MAP servers_;
    std::vector<ServerCollect*> servers_index_;
    BlockChunkPtr* block_chunk_;

    int64_t block_chunk_num_;
    int64_t write_index_;
    int64_t write_second_index_;
    time_t  zonesec_;
    time_t  last_rotate_log_time_;
    volatile mutable uint32_t max_block_id_;
    volatile int32_t alive_server_size_;

    volatile uint8_t interrupt_;
    int32_t plan_run_flag_;
    tbutil::Monitor<tbutil::Mutex> build_plan_monitor_;
    tbutil::Monitor<tbutil::Mutex> run_plan_monitor_;
    tbutil::Monitor<tbutil::Mutex> check_server_monitor_;
    common::RWLock maping_mutex_;
    common::RWLock server_mutex_;
    static const std::string dynamic_parameter_str[];
    tbutil::Mutex elect_index_mutex_;
    ClientRequestServer client_request_server_;
  };
}
}

#endif /* LAYOUTMANAGER_H_ */
