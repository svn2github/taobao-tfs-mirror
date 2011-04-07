/*
 * BlockCollect.h
 *
 *  Created on: 2010-11-5
 *      Author: duanfei
 */

#ifndef TFS_NAMESERVER_BLOCK_COLLECT_H_
#define TFS_NAMESERVER_BLOCK_COLLECT_H_

#include <stdint.h>
#include <time.h>
#include <vector>
#include "ns_define.h"
#include "common/parameter.h"

namespace tfs
{
namespace nameserver
{
  class ServerCollect;
  class BlockCollect;
  typedef std::map<ServerCollect*, std::vector<BlockCollect*> > EXPIRE_BLOCK_LIST;
  class BlockCollect : public virtual GCObject
  {
 public:
    BlockCollect(uint32_t block_id, time_t now);
    bool add(ServerCollect* server, time_t now, bool force, bool& writable);
    bool remove(ServerCollect* server, time_t now, bool remove = true);
    bool exist(const ServerCollect* const server) const;
    ServerCollect* find_master();
    bool is_master(const ServerCollect* const server) const;
    bool is_need_master();
    bool is_writable() const;

    bool check_version(ServerCollect* server, int32_t alive_server_size, NsRole role, bool is_new,
          const common::BlockInfo& block_info, EXPIRE_BLOCK_LIST& expires, bool& force_be_master, time_t now);

    common::PlanPriority check_replicate(time_t now) const;
    bool check_balance() const;
    bool check_compact() const;
    int check_redundant() const;
    bool is_relieve_writable_relation();
    bool relieve_relation(bool remove = true);

    inline int32_t size() const { return info_.size_;}
    inline std::vector<ServerCollect*>& get_hold() { return hold_;}
    inline int32_t get_hold_size() const { return hold_.size();}
    inline void update(const common::BlockInfo& info) { memcpy(&info_,&info, sizeof(info_));} 
    inline bool is_full() const { return info_.size_ >= common::SYSPARAM_NAMESERVER.max_block_size_; }
    static bool is_full(int64_t size) { return size >= common::SYSPARAM_NAMESERVER.max_block_size_;} 
    inline uint32_t id() const { return info_.block_id_;}
    inline int32_t version() const { return info_.version_;}
    inline void set_create_flag(int8_t flag = BLOCK_CREATE_FLAG_NO) { create_flag_ = flag;}
    inline int8_t get_creating_flag() const { return create_flag_;}

    int scan(common::SSMScanParameter& param);
    void dump() const;

    static const int8_t HOLD_MASTER_FLAG_NO;
    static const int8_t HOLD_MASTER_FLAG_YES;
    static const int8_t BLOCK_CREATE_FLAG_NO;
    static const int8_t BLOCK_CREATE_FLAG_YES;
    static const int8_t VERSION_AGREED_MASK;
  private:
    static uint32_t register_expire_block(EXPIRE_BLOCK_LIST& result, ServerCollect* server, BlockCollect* block);

  private:
    BlockCollect();
    DISALLOW_COPY_AND_ASSIGN(BlockCollect);

#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION)
  public:
#else
  private:
#endif
    std::vector<ServerCollect*> hold_;
    common::BlockInfo info_;
    time_t last_update_time_;
    int8_t hold_master_;
    int8_t create_flag_;
    int8_t reserve[6];
  };
}
}

#endif /* BLOCKCOLLECT_H_ */
