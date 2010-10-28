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
#ifndef TFS_NAMESERVER_BLOCK_COLLECT_H_
#define TFS_NAMESERVER_BLOCK_COLLECT_H_

#include "ns_define.h"
#include "common/parameter.h"

namespace tfs
{
  namespace nameserver
  {
    class BlockCollect
    {
    public:
      enum BLOCK_CREATE_FLAG
      {
        BLOCK_CREATE_FLAG_NO = 0x00,
        BLOCK_CREATE_FLAG_YES
      };
    public:
      BlockCollect() :
        master_ds_(0), last_leave_time_(0), last_join_time_(0), load_error_(0), creating_flag_(BLOCK_CREATE_FLAG_NO)
      {
        memset(&block_info_, 0, sizeof(common::BlockInfo));
      }

      virtual ~BlockCollect()
      {

      }

      inline const common::BlockInfo* get_block_info() const
      {
        return &block_info_;
      }

      inline const common::VUINT64* get_ds() const
      {
        return &ds_list_;
      }

      inline uint64_t get_master_ds() const
      {
        return master_ds_;
      }

      inline void set_master_ds(const uint64_t id)
      {
        master_ds_ = id;
        if (id > 0 && ds_list_.size() > 0 && ds_list_[0] != id)
        {
          leave(id);
          ds_list_.insert(ds_list_.begin(), id);
        }
      }

      inline bool join(const uint64_t id, const bool before = false)
      {
        if (find(ds_list_.begin(), ds_list_.end(), id) == ds_list_.end())
        {
          if (before)
          {
            ds_list_.insert(ds_list_.begin(), id);
          }
          else
          {
            ds_list_.push_back(id);
          }
          last_join_time_ = time(NULL);
          return true;
        }
        return false;
      }

      inline bool leave(const uint64_t id)
      {
        common::VUINT64::iterator where = find(ds_list_.begin(), ds_list_.end(), id);
        if (where != ds_list_.end())
        {
          ds_list_.erase(where);
          last_leave_time_ = time(NULL);
          return true;
        }
        else
        {
          return false;
        }
      }

      inline time_t get_last_leave_time() const
      {
        return last_leave_time_;
      }

      inline void set_creating_flag(BLOCK_CREATE_FLAG flag = BLOCK_CREATE_FLAG_NO)
      {
        creating_flag_ = flag;
      }

      inline BLOCK_CREATE_FLAG get_creating_flag() const
      {
        return creating_flag_;
      }

      inline int32_t get_load_error() const
      {
        return load_error_;
      }

      inline void set_load_error(const int32_t error_no)
      {
        load_error_ = error_no;
      }

      inline bool is_full() const
      {
        return is_full(block_info_.size_);
      }

      static bool is_full(const int64_t size)
      {
        return size > common::SYSPARAM_NAMESERVER.max_block_size_;
      }
    private:

      DISALLOW_COPY_AND_ASSIGN( BlockCollect);

      common::VUINT64 ds_list_;
      uint64_t master_ds_;
      time_t last_leave_time_;
      time_t last_join_time_;
      int32_t load_error_;
      BLOCK_CREATE_FLAG creating_flag_;
      common::BlockInfo block_info_;
    };
  }
}

#endif
