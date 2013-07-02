/*
 * (C) 2007-2013 Alibaba Group Holding Limited.
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
#ifndef TFS_DATASERVER_LOGIC_BLOCK_MANAGER_H_
#define TFS_DATASERVER_LOGIC_BLOCK_MANAGER_H_
#include "common/array_helper.h"
#include "common/internal.h"
#include "common/tfs_vector.h"

#include "logic_blockv2.h"

namespace tfs
{
  namespace dataserver
  {
    struct LogicBlockIdCompare
    {
      bool operator ()(const BaseLogicBlock* lhs, const BaseLogicBlock* rhs) const
      {
        return lhs->id() < rhs->id();
      }
    };
    class LogicBlockManager
    {
      typedef common::TfsSortedVector<BaseLogicBlock*, LogicBlockIdCompare> LOGIC_BLOCK_MAP;
      typedef LOGIC_BLOCK_MAP::iterator LOGIC_BLOCK_MAP_ITER;
      typedef std::map<uint64_t, BaseLogicBlock*> TMP_LOGIC_BLOCK_MAP;
      typedef TMP_LOGIC_BLOCK_MAP::iterator TMP_LOGIC_BLOCK_MAP_ITER;
      typedef TMP_LOGIC_BLOCK_MAP::const_iterator TMP_LOGIC_BLOCK_MAP_CONST_ITER;
      public:
      explicit LogicBlockManager(BlockManager& manager);
      virtual ~LogicBlockManager();
      int insert(const uint64_t logic_block_id, const std::string& index_path, const bool tmp = false);
      int remove(BaseLogicBlock*& object, const uint64_t logic_block_id, const bool tmp = false);
      BaseLogicBlock* get(const uint64_t logic_block_id, const bool tmp = false) const;
      int switch_logic_block(const uint64_t logic_block_id, const bool tmp = false);
      bool exist(const uint64_t logic_block_id, const bool tmp = false) const;
      int get_all_block_ids(std::vector<uint64_t>& blocks) const;
      int get_blocks_in_time_range(const common::TimeRange& range, std::vector<uint64_t>& blocks, const int32_t group_count, const int32_t group_seq) const;
      int get_all_block_info(std::set<common::BlockInfo>& blocks) const;
      int get_all_block_info(std::vector<common::BlockInfoV2>& blocks) const;
      int get_all_block_info(common::ArrayHelper<common::BlockInfoV2>& blocks) const;
      int get_all_logic_block_to_physical_block(std::map<uint64_t, std::vector<int32_t> >& blocks) const;
      int32_t size() const;
      int timeout(std::vector<uint64_t>& expired_blocks, const time_t now);
      private:
      int insert_(const uint64_t logic_block_id, const std::string& index_path, const bool tmp = false);
      int remove_(BaseLogicBlock*& object, const uint64_t logic_block_id, const bool tmp = false);
      BaseLogicBlock* get_(const uint64_t logic_block_id, const bool tmp = false) const;
      int switch_logic_block_(const uint64_t logic_block_id, const bool tmp = false);
      int change_create_block_complete_flag_(const uint64_t logic_block_id, const int8_t status,
        const bool tmp = false,const bool flush = false);
      private:
      BlockManager& block_manager_;
      LOGIC_BLOCK_MAP logic_blocks_;
      TMP_LOGIC_BLOCK_MAP tmp_logic_blocks_;
    };
  }/** end namespace dataserver**/
}/** end namespace tfs **/
#endif /* LOGIC_BLOCK_MANAGER_H_ */
