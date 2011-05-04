/*
 * ServerCollect.h
 *
 *  Created on: 2010-11-5
 *      Author: duanfei
 */

#ifndef TFS_NAMESERVER_SERVER_COLLECT_H_
#define TFS_NAMESERVER_SERVER_COLLECT_H_

#include <stdint.h>
#include <tbnet.h>
#include "ns_define.h"
#include "common/lock.h"
#include "common/parameter.h"
#include "global_factory.h"

namespace tfs
{
namespace nameserver
{
  class BlockCollect;
  class LayoutManager;
  class ServerCollect : public virtual common::RWLock,
                        public virtual GCObject
  {
    friend class BlockCollect;
    friend class LayoutManager;
 public:
    struct BlockIdComp
    {
      bool operator () (const BlockCollect* lhs, const BlockCollect* rhs) const;
    };
 public:
    ServerCollect(const common::DataServerStatInfo& info, time_t now);
    bool add(BlockCollect* block, bool writable);
    bool remove(BlockCollect* block);
    bool exist(BlockCollect* block);
    void update(const common::DataServerStatInfo& info, time_t now);
    void statistics(NsGlobalInfo& stat, bool is_new);
    bool add_writable(BlockCollect* block);
    bool add_master(BlockCollect* block);
    bool remove_master(BlockCollect* block);
    bool remove_writable(BlockCollect* block);
    bool clear(LayoutManager& manager, time_t now);
    bool is_writable(int64_t average_used_capacity) const;
    bool is_readable(int32_t average_load) const;
    bool touch(LayoutManager& manager, time_t now, uint32_t max_block_id, int32_t alive_server_size, bool promote, int32_t& count);
    BlockCollect* elect_write_block();
    BlockCollect* force_elect_write_block(void);

    int scan(common::SSMScanParameter& param, int8_t scan_flag);
    void dump() const;

    inline int64_t use_capacity() const { return use_capacity_;}
    inline int64_t total_capacity() const { return total_capacity_;}
    inline int32_t load() const { return current_load_;}
    bool can_be_master(int32_t max_write_block_count);
    inline void touch(time_t now) { last_update_time_ = now;} 
    inline uint64_t id() const { return id_;}
    inline bool is_full() const { return use_capacity_ >= total_capacity_ * common::SYSPARAM_NAMESERVER.max_use_capacity_ratio_ / 100;}
    inline bool is_alive(time_t now) const { return ((now < last_update_time_+ DEAD_TIME));}
    inline bool is_alive() const { return (status_ == common::DATASERVER_STATUS_ALIVE);}
    inline void dead() { status_ = common::DATASERVER_STATUS_DEAD;}
    inline int32_t block_count() const { return hold_.size();}
    inline void elect_num_inc()
    {
      RWLock::Lock lock(*this, common::WRITE_LOCKER);
      elect_num_ <  NsGlobalInfo::ELECT_SEQ_NO_INITIALIZE ? elect_num_ = GFactory::get_global_info().calc_elect_seq_num_average() : ++elect_num_;
    }
    #ifdef TFS_NS_DEBUG
    inline void total_elect_num_inc()
    {
      RWLock::Lock lock(*this, common::WRITE_LOCKER);
      ++total_elect_num_;
    }
    #endif
    inline int64_t get_elect_num() const { return elect_num_;}
    inline int32_t get_hold_master_size() const { return hold_master_.size();}

    void callback(LayoutManager* manager);

    static const int8_t MULTIPLE;
    static const int8_t MAX_LOAD_DOUBLE;
    static const int8_t DUMP_FLAG_HOLD;
    static const int8_t DUMP_FLAG_WRITABLE;
    static const int8_t DUMP_FLAG_MASTER;
    static const int8_t DUMP_FLAG_INFO;
    static const int8_t AVERAGE_USED_CAPACITY_MULTIPLE;
    static const uint16_t DUMP_SLOTS_MAX;

#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION)
  public:
#else
  private:
#endif
    const uint8_t DEAD_TIME;
    ServerCollect();
    std::set<BlockCollect*, BlockIdComp> hold_;
    std::set<BlockCollect*, BlockIdComp> writable_;
    std::vector<BlockCollect*> hold_master_;
    #ifdef TFS_NS_DEBUG
    int64_t total_elect_num_;
    #endif
    uint64_t id_;
    int64_t write_byte_;
    int64_t read_byte_;
    int64_t write_count_;
    int64_t read_count_;
    int64_t unlink_count_;
    int64_t use_capacity_;
    int64_t total_capacity_;
    int64_t elect_num_;
    int64_t elect_seq_;
    time_t  startup_time_;
    time_t  last_update_time_;
    int32_t current_load_;
    int32_t block_count_;
    int32_t write_index_;
    int8_t  status_;
    volatile uint8_t  elect_flag_;
    bool    first_startup_;
  };
}
}

#endif /* SERVERCOLLECT_H_ */
