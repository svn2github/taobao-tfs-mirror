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
#include "logic_block_manager.h"

namespace tfs
{
  namespace dataserver
  {
    LogicBlockManager::LogicBlockManager()
    {

    }
    LogicBlockManager::~LogicBlockManager()
    {

    }
    int LogicBlockManager::insert(const uint64_t logic_block_id, const std::string& index_path)
    {
      int32_t ret = TFS_SUCCESS;
      return ret;
    }
    int LogicBlockManager::remove(const uint64_t logic_block_id)
    {
      int32_t ret = TFS_SUCCESS;
      return ret;
    }
    LogicBlock* LogicBlockManager::get(const uint64_t logic_block_id) const
    {
      int32_t ret = TFS_SUCCESS;
      return ret;
    }
    int LogicBlockManager::add_physical_block(const uint64_t logic_block_id, const PhysicalBlock* physical_block)
    {
      int32_t ret = TFS_SUCCESS;
      return ret;
    }
    int LogicBlockManager::del_physical_block(const uint64_t logic_block_id, const uint32_t physical_block_id)
    {
      int32_t ret = TFS_SUCCESS;
      return ret;
    }
    int LogicBlockManager::switch_block_from_tmp(const uint64_t logic_block_id)
    {
      int32_t ret = TFS_SUCCESS;
      return ret;
    }
    int LogicBlockManager::del_logic_block_from_table(const uint64_t logic_block_id)
    {
      int32_t ret = TFS_SUCCESS;
      return ret;
    }
    int LogicBlockManager::del_logic_block_from_tmp(const uint64_t logic_block_id)
    {
      int32_t ret = TFS_SUCCESS;
      return ret;
    }
  }/** end namespace dataserver**/
}/** end namespace tfs **/
#endif /* LOGIC_BLOCK_MANAGER_H_ */
