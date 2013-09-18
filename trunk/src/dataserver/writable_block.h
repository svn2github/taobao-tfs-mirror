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
#ifndef TFS_DATASERVER_WRITABLE_BLOCK_H_
#define TFS_DATASERVER_WRITABLE_BLOCK_H_

#include <Timer.h>
#include <Mutex.h>
#include "common/func.h"
#include "common/atomic.h"
#include "common/internal.h"
#include "common/lock.h"
#include "common/array_helper.h"
#include "ds_define.h"

namespace tfs
{
  namespace dataserver
  {
    enum BlockType
    {
      BLOCK_WRITABLE,
      BLOCK_UPDATE,
      BLOCK_EXPIRED
    };

    class WritableBlock : public GCObject
    {
      public:
        explicit WritableBlock(const uint64_t block_id);
        virtual ~WritableBlock();

        void set_block_id(const uint64_t block_id)
        {
          block_id_ = block_id;
        }

        uint64_t get_block_id() const
        {
          return block_id_;
        }

        void set_use_flag(bool use = true)
        {
          use_ = use;
        }

        bool get_use_flag() const
        {
          return use_;
        }

        void set_type(const BlockType type)
        {
          type_ = type;
        }

        BlockType get_type() const
        {
          return type_;
        }

        void set_servers(const common::ArrayHelper<uint64_t>& servers);
        void get_servers(common::ArrayHelper<uint64_t>& servers);
        void get_servers(common::VUINT64& servers);

      private:
        uint64_t block_id_;
        uint64_t servers_[common::MAX_REPLICATION_NUM];
        int32_t server_size_;
        BlockType type_;
        bool use_; // if block using by write, update or unlink ops
    };
  }
}

#endif
