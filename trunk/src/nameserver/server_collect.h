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
#ifndef TFS_NAMESERVER_SERVER_COLLECT_H_ 
#define TFS_NAMESERVER_SERVER_COLLECT_H_ 

#include "ns_define.h"
#include "common/parameter.h"
#include <set>

namespace tfs
{
  namespace nameserver
  {
    class ServerCollect
    {
      typedef std::set<uint32_t> BLOCK_SET;
    public:
      ServerCollect()
      {
        reset();
      }
      virtual ~ServerCollect()
      {
      }

      void reset()
      {
        alive = true;
        memset(&ds_stat_info_, 0, sizeof(common::DataServerStatInfo));
        elect_seq_ = 0x01;
        block_list_.clear();
        writable_block_list_.clear();
        primary_block_list_.clear();
      }

      inline void dead()
      {
        alive = false;
      }
      inline bool is_alive() const
      {
        return alive;
      }
      const common::DataServerStatInfo *get_ds() const
      {
        return &ds_stat_info_;
      }
      common::DataServerStatInfo* get_ds()
      {
        return &ds_stat_info_;
      }
      inline const BLOCK_SET& get_block_list() const
      {
        return block_list_;
      }
      inline const BLOCK_SET* get_writable_block_list() const
      {
        return &writable_block_list_;
      }
      inline const BLOCK_SET* get_primary_writable_block_list() const
      {
        return &primary_block_list_;
      }

      inline void join_block(const uint32_t id)
      {
        block_list_.insert(id);
      }
      inline void leave_block(const uint32_t id)
      {
        block_list_.erase(id);
      }
      inline void join_writable_block(const uint32_t id)
      {
        writable_block_list_.insert(id);
      }
      inline void leave_writable_block(const uint32_t id)
      {
        writable_block_list_.erase(id);
      }
      inline bool join_primary_writable_block(const uint32_t id)
      {
        if (primary_block_list_.find(id) == primary_block_list_.end())
        {
          primary_block_list_.insert(id);
          return true;
        }
        return false;
      }
      inline bool leave_primary_writable_block(const uint32_t id)
      {
        std::set<uint32_t>::iterator it = primary_block_list_.find(id);
        if (it != primary_block_list_.end())
        {
          primary_block_list_.erase(id);
          return true;
        }
        return false;
      }
      inline void release(const uint32_t id)
      {
        leave_block(id);
        leave_writable_block(id);
        leave_primary_writable_block(id);
      }
      inline void clear_all()
      {
        block_list_.clear();
        writable_block_list_.clear();
        primary_block_list_.clear();
      }

      inline bool is_disk_full() const
      {
        int64_t xa = ds_stat_info_.total_capacity_ * common::SYSPARAM_NAMESERVER.max_use_capacity_ratio_ / 100;
        return ds_stat_info_.use_capacity_ >= xa;
      }

      inline int64_t elect(const int64_t seq)
      {
        elect_seq_ = seq;
        return seq;
      }
      inline int64_t get_elect_seq() const
      {
        return elect_seq_;
      }
      inline uint32_t get_max_block_id() const
      {
        if (writable_block_list_.rbegin() != writable_block_list_.rend())
          return (*writable_block_list_.rbegin());
        return 0;
      }

    private:
      DISALLOW_COPY_AND_ASSIGN( ServerCollect);
      common::DataServerStatInfo ds_stat_info_;
      BLOCK_SET block_list_;
      BLOCK_SET writable_block_list_;
      BLOCK_SET primary_block_list_;
      int64_t elect_seq_;
      bool alive;
    };
  }
}

#endif
