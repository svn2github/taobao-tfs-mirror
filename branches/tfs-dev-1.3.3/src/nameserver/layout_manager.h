/*
 * LayoutManager.h
 *
 *  Created on: 2010-11-5
 *      Author: duanfei
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
#include "message/message.h"
#include "oplog_sync_manager.h"

namespace tfs
{
namespace nameserver
{
  struct CloseParameter
  {
    common::BlockInfo block_info_;
    uint64_t id_;
    uint32_t lease_id_;
    common::UnlinkFlag unlink_flag_;
    common::WriteCompleteStatus status_;
    bool need_new_;
    char error_msg_[256];
  };
  class LayoutManager
  {
    template<typename Strategy, typename ElectType>
    friend int32_t elect_ds(Strategy& strategy, ElectType op, LayoutManager& meta, std::vector<ServerCollect*>& except,
          int32_t elect_count, bool check_server_in_plan, std::vector<ServerCollect*> & result);
    template<typename Strategy, typename ElectType>
    friend int32_t elect_ds(Strategy& strategy, ElectType op, LayoutManager& meta, std::vector<ServerCollect*>& source,
          std::vector<ServerCollect*>& except, int32_t elect_count, bool check_server_in_plan, std::vector<ServerCollect*>& result);
    friend int OpLogSyncManager::replay_helper(const char* const data, int64_t& length, int64_t& offset, time_t now = time(NULL));
    struct AddLoad
    {
      int32_t operator()(const int32_t acc, const ServerCollect* const server) const
      {
        assert(server != NULL);
        return acc + server->load();
      }
    };
    struct BlockNumComp
    {
      bool operator()(const common::DataServerStatInfo& x, const common::DataServerStatInfo& y) const
      {
        return x.block_count_ < y.block_count_;
      }
    };
    struct GetAliveServer
    {
      explicit GetAliveServer(common::VUINT64& servers)
        :servers_(servers)
      {

      }
      bool operator() (const std::pair<uint64_t, ServerCollect*>& node)
      {
        if (node.second->is_alive())
        {
          servers_.push_back(node.second->id());
          return true;
        }
        return false;
      }
      common::VUINT64& servers_;
    };
  public:
    LayoutManager();
    virtual ~LayoutManager();
    BlockChunkPtr get_chunk(uint32_t block_id) const;
    BlockCollect*  get_block(uint32_t block_id);
    ServerCollect* get_server(uint64_t server_id);

    int initialize(int32_t chunk_num = 32);
    void wait_for_shut_down();
    void destroy();

    int keepalive(const common::DataServerStatInfo&, common::HasBlockFlag flag,
          common::BLOCK_INFO_LIST& blocks, common::VUINT32& expires, bool& need_sent_block);

    int open(uint32_t& block_id, const int32_t mode, uint32_t& lease_id, int32_t& version, common::VUINT64& ds_list);
    int batch_open(const common::VUINT32& blocks, const int32_t mode, const int32_t block_count, std::map<uint32_t, common::BlockInfoSeg>& out);
    int close(CloseParameter& param);
    int update_block_info(const common::BlockInfo& block_info, const uint64_t server, time_t now, bool addnew);
    int repair(uint32_t block_id, uint64_t server, int32_t flag, time_t now, std::string& error_msg);

    int scan(common::SSMScanParameter& stream);
    int dump_plan(tbnet::DataBuffer& stream);
    int dump_plan(void);

    int handle_task_complete(message::Message* msg);
    #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION)
    message::StatusMessage* handle(message::Message* msg);
    #else
    int handle(message::Message* msg);
    #endif

    void interrupt(uint8_t interrupt, time_t now);

    OpLogSyncManager * get_oplog_sync_mgr() { return &oplog_sync_mgr_;}

    int rm_block_from_ds(const uint64_t server_id, const uint32_t block_id);
    int rm_block_from_ds(const uint64_t server_id, const std::vector<uint32_t>& block_ids);

    uint32_t calc_max_block_id();
    
    inline void get_alive_server(common::VUINT64& servers)
    {
      common::RWLock::Lock lock(server_mutex_, common::READ_LOCKER);
      std::for_each(servers_. begin(), servers_.end(), GetAliveServer(servers));
    }

    int touch(uint64_t server, time_t now = time(NULL), bool promote = true);

#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION)
  public:
#else
  private:
#endif
    int add_server(const common::DataServerStatInfo& info, bool& isnew, time_t now);
    int remove_server(uint64_t id, time_t now);
    BlockCollect* add_block(uint32_t& block_id);
    BlockCollect* add_new_block(uint32_t& block_id, ServerCollect* server = NULL, time_t now = time(NULL));

    int update_relation(ServerCollect* server, const std::vector<common::BlockInfo>& blocks, EXPIRE_BLOCK_LIST& expires, time_t now);
    int build_relation(BlockCollect* block, ServerCollect* server, time_t now, bool force = false);
    bool relieve_relation(BlockCollect* block, ServerCollect* server, time_t now);
    bool relieve_relation(ServerCollect* server, time_t now);

    void rotate(time_t now);

    uint32_t get_alive_block_id() const;
    int64_t calc_all_block_bytes() const;
    int64_t calc_all_block_count() const;
    BlockCollect* elect_write_block();
    int open_read_mode(const uint32_t block_id, common::VUINT64& readable_ds_list);
    int open_write_mode(const int32_t mode,
        uint32_t& block_id,
        uint32_t& lease_id,
        int32_t& version,
        common::VUINT64& ds_list);
    int open_helper_create_new_block_by_id(uint32_t block_id);
    int batch_open_read_mode(const common::VUINT32& blocks, std::map<uint32_t, common::BlockInfoSeg>& out);
    int batch_open_write_mode(const int32_t mode, const int32_t block_count, std::map<uint32_t, common::BlockInfoSeg>& out);
    int touch(ServerCollect* server, time_t now, bool promote = false);
    void check_server();

    int set_runtime_param(uint32_t index, uint32_t value, char *retstr);
#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION)
  public:
#else
  private:
#endif
   class Task: public virtual tbutil::TimerTask
   {
     friend class LayoutManager;
   public:
     Task(LayoutManager* manager, common::PlanType type, common::PlanPriority priority, uint32_t block_id, time_t begin, time_t end, const std::vector<ServerCollect*>& runer);
     virtual ~ Task(){};
     virtual int handle() = 0;
     virtual int handle_complete(message::Message* msg, bool& all_complete_flag) = 0;
     virtual void dump(tbnet::DataBuffer& stream);
     virtual void dump(int32_t level, const char* const format = NULL);
     virtual void runTimerTask();
     bool operator < (const Task& task)
     {
       if (priority_ < task.priority_)
         return true;
       if (priority_ > task.priority_)
         return false;
       if (type_ < task.type_)
         return true;
       if (type_ > task.type_)
         return false;
       if (block_id_ < task.block_id_)
         return true;
       if (block_id_ > task.block_id_)
         return false;
       return (begin_time_ < task.begin_time_);
     }
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
   private:
     DISALLOW_COPY_AND_ASSIGN(Task);
   };
   typedef tbutil::Handle<Task> TaskPtr;

   struct TaskCompare
   {
     bool operator()(const TaskPtr& lhs, const TaskPtr& rhs) const
     {
        //return lhs < rhs;
        return (*lhs) < (*rhs);
     }
   };
   class CompactTask: public virtual Task 
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
       CompactComplete(uint64_t id, uint32_t block_id, common::PlanStatus status):
         id_(id), block_id_(block_id), status_(status), all_success_(true),
         has_success_(false), is_complete_(true), current_complete_result_(false){}
     };
   public:
     CompactTask(LayoutManager* manager, common::PlanPriority priority, uint32_t block_id, time_t begin, time_t end, const std::vector<ServerCollect*>& runer);
     virtual ~CompactTask(){}
     virtual int handle();
     virtual int handle_complete(message::Message* msg, bool& all_complete_flag);
     virtual void runTimerTask();
     virtual void dump(tbnet::DataBuffer& stream);
     virtual void dump(int32_t level, const char* const format = NULL);
  #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION)
    public:
  #else
   private:
  #endif
     void check_complete(CompactComplete& value, common::VUINT64& ds_list);
     int do_complete(CompactComplete& value, common::VUINT64& ds_list);
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

   class ReplicateTask : public virtual Task 
   {
   public:
     ReplicateTask(LayoutManager* manager, common::PlanPriority priority, uint32_t block_id, time_t begin, time_t end, const std::vector<ServerCollect*>& runer);
     virtual ~ReplicateTask(){}
     virtual int handle();
     virtual int handle_complete(message::Message* msg, bool& all_complete_flag);
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

   class RedundantTask : public virtual Task 
   {
   public:
     RedundantTask(LayoutManager* manager, common::PlanPriority priority, uint32_t block_id, time_t begin, time_t end, const std::vector<ServerCollect*>& runer);
     virtual ~RedundantTask(){}
     virtual int handle();
     virtual int handle_complete(message::Message* msg, bool& all_complete_flag);
   private:
    DISALLOW_COPY_AND_ASSIGN(RedundantTask);
   };
   typedef tbutil::Handle<RedundantTask> RedundantTaskPtr;

   class MoveTask : public virtual ReplicateTask
   {
   public:
     MoveTask(LayoutManager* manager, common::PlanPriority priority, uint32_t block_id, time_t begin, time_t end, const std::vector<ServerCollect*>& runer);
     virtual ~MoveTask(){}
   private:
     DISALLOW_COPY_AND_ASSIGN(MoveTask);
   };
   typedef tbutil::Handle<MoveTask> MoveTaskPtr;

   class ExpireTask : public virtual tbutil::TimerTask
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
    bool build_replicate_plan(time_t now, int64_t& need, int64_t& adjust, int64_t& emergency_replicate_count, std::vector<uint32_t>& blocks);
    bool build_compact_plan(time_t now, int64_t& need, std::vector<uint32_t>& blocks);
    bool build_balance_plan(time_t now, int64_t& need, std::vector<uint32_t>& blocks);
    bool build_redundant_plan(time_t now, int64_t& need, std::vector<uint32_t>& blocks);
#else
    bool build_replicate_plan(time_t now, int64_t& need, int64_t& adjust, int64_t& emergency_replicate_count);
    bool build_compact_plan(time_t now, int64_t& need);
    bool build_balance_plan(time_t now, int64_t& need);
    bool build_redundant_plan(time_t now, int64_t& need);
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
    bool find_block_in_plan(const uint32_t block_id);
    bool find_server_in_plan(ServerCollect* server);
    bool find_server_in_plan(const std::vector<ServerCollect*> & servers, bool all_find, std::vector<ServerCollect*>& result);
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
    static const int16_t SKIP_BLOCK;
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
 
    common::SERVER_MAP servers_;
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
  };
}
}

#endif /* LAYOUTMANAGER_H_ */
