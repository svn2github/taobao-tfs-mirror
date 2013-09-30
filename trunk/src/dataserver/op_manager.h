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
#ifndef TFS_DATASERVER_OP_MANAGER_H_
#define TFS_DATASERVER_OP_MANAGER_H_

#include <Timer.h>
#include <Mutex.h>
#include "common/func.h"
#include "common/atomic.h"
#include "common/internal.h"
#include "common/lock.h"
#include "common/array_helper.h"
#include "data_file.h"
#include "ds_define.h"
#include "op_meta.h"

namespace tfs
{
  namespace dataserver
  {
    class DataService;
    class BlockManager;
    class WritableBlockManager;
    class LeaseManager;

    // write, update, unlink operation manager
    class OpManager
    {
      typedef std::map<OpId, OpMeta*> OPMETA_MAP;
      typedef std::map<OpId, OpMeta*>::iterator OPMETA_MAP_ITER;
      typedef std::map<OpId, OpMeta*>::iterator OPMETA_MAP_CONST_ITER;

    public:
      OpManager(DataService& service);
      virtual ~OpManager();

      BlockManager& get_block_manager();
      LeaseManager& get_lease_manager();

      // remove expired op meta
      void timeout(const time_t now);

    public:
      /*
       * when receive a write/prepare_unlink reqeust
       * ds must do some prepare work to serve this reqeust
       *
       * prepare_op do the following work:
       *    1. alloc a block id for request if needed
       *    2. alloc a file id in block for request if needed
       *    3. alloc a operation id if needed
       */
      int prepare_op(uint64_t& block_id, uint64_t& file_id, uint64_t& op_id,
        const OpType type, const bool is_master, const common::FamilyInfoExt& family_info,
        common::VUINT64& servers);

      /*
       * when receive a close/unlink reqeust
       * ds should reset opertion meta
       */
      int reset_op(const uint64_t block_id, const uint64_t file_id, const uint64_t op_id,
        common::VUINT64& servers);

      /**
       * forward request to slave
       */
      int forward_op(tbnet::Packet* message,
          const uint64_t block_id, const int64_t family_id, const common::VUINT64& servers);

      /*
       * when receive a response from peer server
       * use this interface to update operation status
       */
      int update_op(const uint64_t block_id, const uint64_t file_id, const uint64_t op_id,
        tbnet::Packet* packet);

      /*
       * when ds finish an operatioin local
       * use this interfact to update operation status
       */
      int update_op(const uint64_t block_id, const uint64_t file_id, const uint64_t op_id,
        const int32_t error_code, const common::BlockInfoV2& info);

      /*
       * check operation status
       * return true if all server finish
       */
      bool check_op(const uint64_t block_id, const uint64_t file_id, const uint64_t op_id,
          OpStat& stat);

      /*
       * release operation metadata when operation finished
       */
      void release_op(const uint64_t block_id, const uint64_t file_id, const uint64_t op_id,
          const int32_t status = common::TFS_SUCCESS);

    public:
      int write_file(const uint64_t block_id, const uint64_t attach_block_id,
        const uint64_t file_id, const uint64_t op_id,
        const char* buffer, const int32_t length, const int32_t offset,
        const int32_t remote_version, common::BlockInfoV2& local);

      int close_file(const uint64_t block_id, const uint64_t attach_block_id,
        uint64_t& file_id, const uint64_t op_id, const uint32_t crc,
        const int32_t status, const bool tmp, common::BlockInfoV2& local);

      int prepare_unlink_file(const uint64_t block_id, const uint64_t attach_block_id,
        const uint64_t file_id, const int64_t op_id, const int32_t action,
        const int32_t remote_version, common::BlockInfoV2& local);

      int unlink_file(const uint64_t block_id, const uint64_t attach_block_id,
        const uint64_t file_id, const int64_t op_id, const int32_t action,
        common::BlockInfoV2& local);

      int update_block_info(const common::BlockInfoV2& block_info,
          const common::UpdateBlockInfoType type);

      int resolve_block_version_conflict(const uint64_t block_id,
          const uint64_t file_id, const uint64_t op_id);

    private:
      bool out_of_limit() const;
      uint64_t gen_op_id();
      void add(const OpId& oid, const common::VUINT64& servers, const OpType type);
      void remove(const OpId& oid);
      int get(const OpId& oid, OpMeta*& op_meta);
      void put(OpMeta* op_meta);

    private:
      DataService& service_;
      uint64_t base_op_id_;
      OPMETA_MAP op_metas_;
      mutable common::RWLock rwmutex_;

    private:
      DISALLOW_COPY_AND_ASSIGN(OpManager);
    };
  }
}

#endif
