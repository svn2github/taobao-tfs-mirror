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
namespace tfs
{
  namespace dataserver
  {
    class LogicBlockManager
    {
      //typedef common::TfsSorteVector<LogicBlock*> LOGIC_BLOCK_MAP;
      //typedef LOGIC_BLOCK_MAP::iterator LOGIC_BLOCK_MAP_ITER;
      public:
      LogicBlockManager();
      virtual ~LogicBlockManager();
      int insert(const uint64_t logic_block_id, const std::string& index_path);
      int remove(const uint64_t logic_block_id);
      LogicBlock* get(const uint64_t logic_block_id) const;
      int add_physical_block(const uint64_t logic_block_id, const PhysicalBlock* physical_block);
      int del_physical_block(const uint64_t logic_block_id, const uint32_t physical_block_id);
      int switch_block_from_tmp(const uint64_t logic_block_id);
      int del_logic_block_from_table(const uint64_t logic_block_id);
      int del_logic_block_from_tmp(const uint64_t logic_block_id);
      private:
      static const int8_t MAX_TMP_LOGIC_BLOCKS = 16;
      LOGIC_BLOCK_MAP logic_blocks_;
      LogicBlock* tmp_logic_blocks_[MAX_TMP_LOGIC_BLOCKS];
      common::RWLock rwmutex_;
    };
  }/** end namespace dataserver**/
}/** end namespace tfs **/
#endif /* LOGIC_BLOCK_MANAGER_H_ */
