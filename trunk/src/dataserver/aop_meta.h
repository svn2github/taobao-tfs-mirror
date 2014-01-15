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
#ifndef TFS_DATASERVER_AOP_META_H_
#define TFS_DATASERVER_AOP_META_H_

#include <Timer.h>
#include <Mutex.h>
#include "common/func.h"
#include "common/atomic.h"
#include "common/internal.h"
#include "common/lock.h"
#include "common/array_helper.h"
#include "data_file.h"
#include "ds_define.h"

namespace tfs
{
  namespace dataserver
  {
    enum OpType
    {
      OP_TYPE_UNLINK = 0,
      OP_TYPE_WRITE
    };

    struct OpMember
    {
      common::BlockInfoV2 info_;
      uint64_t server_;
      int32_t  status_;
    };

    struct OpStat
    {
      int64_t cost_;            // op cost
      int64_t size_;            // op file size
      int32_t status_;          // op status
      std::stringstream error_; // set only error occurs
    };

    struct OpId
    {
      uint64_t block_id_;
      uint64_t file_id_;
      uint64_t op_id_;

      OpId(const uint64_t block_id, const uint64_t file_id, const uint64_t op_id) :
        block_id_(block_id), file_id_(file_id), op_id_(op_id)
      {
      }

      bool operator < (const OpId& oid) const
      {
        bool less = false;
        if (op_id_ < oid.op_id_)
        {
          less = true;
        }
        else if (op_id_ == oid.op_id_)
        {
          if (block_id_ < oid.block_id_)
          {
            less = true;
          }
          else if (block_id_ == oid.block_id_)
          {
            less = (file_id_ < oid.file_id_);
          }
        }

        return less;
      }
    };

    typedef std::pair<uint64_t, common::BlockInfoV2> BlockReplicaInfo;

    // unlink async operation metadata
    class OpMeta
    {
      public:
        OpMeta(const OpId& oid, const common::VUINT64& servers);
        virtual ~OpMeta();

        void update_last_time(const time_t now)
        {
          last_update_time_ = now;
        }

        int32_t inc_ref()
        {
          tbutil::Mutex::Lock lock(mutex_);
          return ++ref_count_;
        }

        int32_t dec_ref()
        {
          tbutil::Mutex::Lock lock(mutex_);
          return --ref_count_;
        }

        int32_t get_ref()
        {
          tbutil::Mutex::Lock lock(mutex_);
          return ref_count_;
        }

        // unlink need return file_size to client for nginx log statistics
        int64_t get_file_size()
        {
          return file_size_;
        }

        void set_file_size(const int64_t file_size)
        {
          file_size_ = file_size;
        }

        bool timeout(const time_t now)
        {
          return now > last_update_time_ +
            common::SYSPARAM_DATASERVER.expire_data_file_time_;
        }

        // member opertion
        void set_members(const common::VUINT64& servers);
        void reset_members();
        void update_member(const uint64_t server,
            const common::BlockInfoV2& info, const int32_t status);
        void update_member();   // update when received a error packet
        int get_members(common::ArrayHelper< BlockReplicaInfo >& helper) const;
        void get_servers(common::VUINT64& servers) const;

        // check op staa
        // if all finish return true
        bool check(OpStat& stat);

      private:
        // get error msg, called by check
        void strerror(std::stringstream& error);

      private:
        OpId oid_;                  // async operation id
        time_t last_update_time_;   // last update time
        int64_t start_time_;         // request start time, us
        int64_t file_size_;          // request file size
        int32_t server_size_;       // servers in this op
        int32_t done_server_size_;  // done servers in this op
        int32_t ref_count_;         // reference count
        tbutil::Mutex mutex_;       // mutex lock
        OpMember members_[common::MAX_REPLICATION_NUM];

      private:
        DISALLOW_COPY_AND_ASSIGN(OpMeta);
    };

    // write, update async operation metadata
    class WriteOpMeta: public OpMeta
    {
      public:
        WriteOpMeta(const OpId& oid, const common::VUINT64& servers);
        virtual ~WriteOpMeta();
        DataFile& get_data_file() { return data_file_;}

      private:
        DataFile data_file_;

      private:
        DISALLOW_COPY_AND_ASSIGN(WriteOpMeta);
    };

  }
}

#endif
