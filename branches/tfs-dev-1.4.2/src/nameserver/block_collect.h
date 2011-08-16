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
    BlockCollect(const uint32_t block_id, const time_t now);
    bool add(ServerCollect* server, const time_t now, const bool force, bool& writable);
    bool remove(ServerCollect* server, const time_t now, const bool remove = true);
    bool exist(const ServerCollect* const server) const;
    ServerCollect* find_master();
    bool is_master(const ServerCollect* const server) const;
    bool is_need_master() const;
    bool is_writable() const;

    bool check_version(ServerCollect* server, const NsRole role, const bool is_new,
          const common::BlockInfo& block_info, EXPIRE_BLOCK_LIST& expires, bool& force_be_master, const time_t now);

    common::PlanPriority check_replicate(const time_t now) const;
    bool check_balance() const;
    bool check_compact() const;
    int check_redundant() const;
    bool is_relieve_writable_relation() const;
    bool relieve_relation(const bool remove = true);

    inline int32_t size() const { return info_.size_;}
    inline std::vector<ServerCollect*>& get_hold() { return hold_;}
    inline int32_t get_hold_size() const { return hold_.size();}
    inline void update(const common::BlockInfo& info) { memcpy(&info_, &info, sizeof(info_));} 
    inline bool is_full() const { return info_.size_ >= common::SYSPARAM_NAMESERVER.max_block_size_; }
    static bool is_full(int64_t size) { return size >= common::SYSPARAM_NAMESERVER.max_block_size_;}
    inline uint32_t id() const { return info_.block_id_;}
    inline int32_t version() const { return info_.version_;}
    inline void set_create_flag(const int8_t flag = BLOCK_CREATE_FLAG_NO) { create_flag_ = flag;}
    inline int8_t get_creating_flag() const { return create_flag_;}
    inline bool in_master_set() const { return BLOCK_IN_MASTER_SET_YES == in_master_set_;}

    int scan(common::SSMScanParameter& param) const;
    void dump() const;

    static const int8_t HOLD_MASTER_FLAG_NO;
    static const int8_t HOLD_MASTER_FLAG_YES;
    static const int8_t BLOCK_IN_MASTER_SET_NO;
    static const int8_t BLOCK_IN_MASTER_SET_YES;
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
    int8_t in_master_set_;
    int8_t reserve[5];
  };
}
}

#endif /* BLOCKCOLLECT_H_ */
