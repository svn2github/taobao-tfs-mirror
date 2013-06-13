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
#include "common/error_msg.h"
#include "logic_block_manager.h"
#include "block_manager.h"

using namespace tfs::common;
namespace tfs
{
  namespace dataserver
  {
    static const int32_t MAX_LOGIC_BLOCK_SLOT_DEFALUT = 1024;
    static const int32_t MAX_LOGIC_BLOCK_SLOT_EXPAND_DEFAULT = 128;
    static const float   MAX_LOGIC_BLOCK_SLOT_EXPAND_RATION_DEFAULT = 0.1;
    LogicBlockManager::LogicBlockManager(BlockManager& manager):
      block_manager_(manager),
      logic_blocks_(MAX_LOGIC_BLOCK_SLOT_DEFALUT, MAX_LOGIC_BLOCK_SLOT_EXPAND_DEFAULT, MAX_LOGIC_BLOCK_SLOT_EXPAND_RATION_DEFAULT)
    {

    }

    LogicBlockManager::~LogicBlockManager()
    {
      LOGIC_BLOCK_MAP_ITER iter = logic_blocks_.begin();
      for (; iter != logic_blocks_.end(); ++iter)
      {
        tbsys::gDelete((*iter));
      }

      TMP_LOGIC_BLOCK_MAP_ITER it = tmp_logic_blocks_.begin();
      for (; it != tmp_logic_blocks_.end(); ++it)
      {
        tbsys::gDelete(it->second);
      }
    }

    int LogicBlockManager::insert(const uint64_t logic_block_id, const std::string& index_path, const bool tmp)
    {
      return insert_(logic_block_id, index_path, tmp);
    }

    int LogicBlockManager::remove(BaseLogicBlock*& object, const uint64_t logic_block_id, const bool tmp)
    {
      object = NULL;
      return remove_(object, logic_block_id, tmp);
    }

    BaseLogicBlock* LogicBlockManager::get(const uint64_t logic_block_id, const bool tmp) const
    {
      return get_(logic_block_id, tmp);
    }

    int LogicBlockManager::switch_logic_block(const uint64_t logic_block_id, const bool tmp)
    {
      return switch_logic_block_(logic_block_id, tmp);
    }

    int LogicBlockManager::insert_(const uint64_t logic_block_id, const std::string& index_path, const bool tmp)
    {
      int32_t ret = (INVALID_BLOCK_ID != logic_block_id && !index_path.empty()) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BaseLogicBlock* logic_block = NULL;
        if (IS_VERFIFY_BLOCK(logic_block_id))
          logic_block = new (std::nothrow)VerifyLogicBlock(&block_manager_, logic_block_id, index_path);
        else
          logic_block = new (std::nothrow)LogicBlock(&block_manager_, logic_block_id, index_path);
        if (tmp)
        {
          std::pair<TMP_LOGIC_BLOCK_MAP_ITER, bool> res =
            tmp_logic_blocks_.insert(TMP_LOGIC_BLOCK_MAP::value_type(logic_block_id, logic_block));
          ret = res.second ? TFS_SUCCESS : EXIT_BLOCK_EXIST_ERROR;
        }
        else
        {
          BaseLogicBlock* result = NULL;
          ret = logic_blocks_.insert_unique(result, logic_block);
        }
        if (TFS_SUCCESS != ret)
          block_manager_.get_gc_manager().add(logic_block);
      }
      return ret;
    }

    int LogicBlockManager::remove_(BaseLogicBlock*& object, const uint64_t logic_block_id, const bool tmp)
    {
      object = NULL;
      int32_t ret = (INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        if (tmp)
        {
          TMP_LOGIC_BLOCK_MAP_ITER iter = tmp_logic_blocks_.find(logic_block_id);
          if (tmp_logic_blocks_.end() != iter)
          {
            object = iter->second;
            tmp_logic_blocks_.erase(iter);
          }
        }
        else
        {
          BaseLogicBlock query(logic_block_id);
          LOGIC_BLOCK_MAP_ITER iter =  logic_blocks_.find(&query);
          if (logic_blocks_.end() != iter)
          {
            object = logic_blocks_.erase((*iter));
          }
        }
      }
      return ret;
    }

    BaseLogicBlock* LogicBlockManager::get_(const uint64_t logic_block_id, const bool tmp) const
    {
      BaseLogicBlock* result = NULL;
      int32_t ret = (INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        if (tmp)
        {
          TMP_LOGIC_BLOCK_MAP_CONST_ITER iter = tmp_logic_blocks_.find(logic_block_id);
          if (tmp_logic_blocks_.end() != iter)
            result = iter->second;
        }
        else
        {
          BaseLogicBlock query(logic_block_id);
          LOGIC_BLOCK_MAP_ITER iter =  logic_blocks_.find(&query);
          if (logic_blocks_.end() != iter)
            result = (*iter);
        }
      }
      return result;
    }

    int LogicBlockManager::switch_logic_block_(const uint64_t logic_block_id, const bool tmp)
    {
      int32_t ret = (INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        if (tmp)
        {
          TMP_LOGIC_BLOCK_MAP_ITER iter = tmp_logic_blocks_.find(logic_block_id);
          ret = (tmp_logic_blocks_.end() != iter) ? TFS_SUCCESS : EXIT_LOGIC_BLOCK_NOT_EXIST_ERROR;
          if (TFS_SUCCESS == ret)
          {
            ret = change_create_block_complete_flag_(logic_block_id, BLOCK_CREATE_COMPLETE_STATUS_COMPLETE, true, false);
          }
          if (TFS_SUCCESS == ret)
          {
            BaseLogicBlock query(logic_block_id);
            LOGIC_BLOCK_MAP_ITER it =  logic_blocks_.find(&query);
            if (logic_blocks_.end() != it)
            {
              // exchange logic block in two map
              BaseLogicBlock* result = NULL;
              BaseLogicBlock* tmp_logic_block = *it;
              logic_blocks_.erase(*it);
              ret = logic_blocks_.insert_unique(result, iter->second);
              if (TFS_SUCCESS == ret)
              {
                tmp_logic_blocks_.erase(iter);
                tmp_logic_blocks_.insert(TMP_LOGIC_BLOCK_MAP::value_type(logic_block_id, tmp_logic_block));
                change_create_block_complete_flag_(logic_block_id, BLOCK_CREATE_COMPLETE_STATUS_UNCOMPLETE, true, false);
              }
              else
              {
                ret = logic_blocks_.insert_unique(result, tmp_logic_block);
                assert(TFS_SUCCESS == ret);
              }
            }
            else
            {
              BaseLogicBlock* result = NULL;
              logic_blocks_.insert_unique(result, iter->second);
              tmp_logic_blocks_.erase(iter);
            }
          }
          if (TFS_SUCCESS == ret)
          {
            ret = block_manager_.get_super_block_manager().flush();
          }
        }
        /*  won't happen ??
        else
        {
          BaseLogicBlock query(logic_block_id);
          LOGIC_BLOCK_MAP_ITER iter =  logic_blocks_.find(&query);
          ret = (logic_blocks_.end() != iter) ? TFS_SUCCESS : EXIT_LOGIC_BLOCK_NOT_EXIST_ERROR;
          if (TFS_SUCCESS == ret)
          {
            TMP_LOGIC_BLOCK_MAP_ITER it = tmp_logic_blocks_.find(logic_block_id);
            ret = (tmp_logic_blocks_.end() == it) ? TFS_SUCCESS : EXIT_BLOCK_EXIST_ERROR;
            if (TFS_SUCCESS == ret)
            {
              BaseLogicBlock* result = logic_blocks_.erase((*iter));
              tmp_logic_blocks_.insert(TMP_LOGIC_BLOCK_MAP::value_type(logic_block_id, result));
            }
          }
        }
        */
      }
      return ret;
    }

    bool LogicBlockManager::exist(const uint64_t logic_block_id, const bool tmp) const
    {
      BaseLogicBlock* logic_block = get(logic_block_id, tmp);
      return (NULL != logic_block);
    }

    int LogicBlockManager::get_all_block_ids(std::vector<uint64_t>& blocks) const
    {
      blocks.clear();
      int32_t ret = TFS_SUCCESS;
      BlockInfoV2 block_info;
      LOGIC_BLOCK_MAP_ITER iter = logic_blocks_.begin();
      for (; iter != logic_blocks_.end() && TFS_SUCCESS == ret; ++iter)
      {
        ret = (*iter)->get_block_info(block_info);
        if (TFS_SUCCESS == ret)
        {
          blocks.push_back(block_info.block_id_);
        }
      }
      return TFS_SUCCESS;
    }

    int LogicBlockManager::get_blocks_in_time_range(const TimeRange& range, std::vector<uint64_t>& blocks) const
    {
      blocks.clear();
      int32_t ret = TFS_SUCCESS;
      BlockInfoV2 block_info;
      LOGIC_BLOCK_MAP_ITER iter = logic_blocks_.begin();
      for (; iter != logic_blocks_.end() && TFS_SUCCESS == ret; ++iter)
      {
        ret = (*iter)->get_block_info(block_info);
        if ((TFS_SUCCESS == ret) && !IS_VERFIFY_BLOCK(block_info.block_id_)
            && range.include(block_info.last_update_time_)
            && block_info.file_count_ > 0)
        {
          blocks.push_back(block_info.block_id_);
        }
      }
      return TFS_SUCCESS;
    }

    int LogicBlockManager::get_all_block_info(std::set<common::BlockInfo>& blocks) const
    {
      blocks.clear();
      BlockInfoV2 info;
      int32_t ret = TFS_SUCCESS;
      LOGIC_BLOCK_MAP_ITER iter = logic_blocks_.begin();
      for (; iter != logic_blocks_.end() && TFS_SUCCESS == ret; ++iter)
      {
        ret = (*iter)->get_block_info(info);
        if (TFS_SUCCESS == ret)
        {
          BlockInfo old_info;
          old_info.block_id_ = info.block_id_;
          old_info.version_  = info.version_;
          old_info.file_count_ = info.file_count_;
          old_info.size_     = info.size_;
          old_info.del_file_count_ = info.del_file_count_;
          old_info.del_size_ = info.del_size_;
          old_info.seq_no_   = 0;
          std::pair<std::set<common::BlockInfo>::iterator, bool> res =
            blocks.insert(old_info);
        }
      }
      return TFS_SUCCESS;
    }

    int LogicBlockManager::get_all_block_info(std::vector<common::BlockInfoV2>& blocks) const
    {
      blocks.clear();
      BlockInfoV2 info;
      int32_t ret = TFS_SUCCESS;
      LOGIC_BLOCK_MAP_ITER iter = logic_blocks_.begin();
      for (; iter != logic_blocks_.end() && TFS_SUCCESS == ret; ++iter)
      {
        ret = (*iter)->get_block_info(info);
        if (TFS_SUCCESS == ret)
        {
          blocks.push_back(info);
        }
      }
      return TFS_SUCCESS;
    }

    int LogicBlockManager::get_all_block_info(common::ArrayHelper<common::BlockInfoV2>& blocks) const
    {
      blocks.clear();
      int32_t ret = TFS_SUCCESS, index = 0;
      int32_t array_size = blocks.get_array_size();
      LOGIC_BLOCK_MAP_ITER iter = logic_blocks_.begin();
      for (; iter != logic_blocks_.end() && index < array_size && TFS_SUCCESS == ret; ++iter, ++index)
      {
        common::BlockInfoV2& info = *(blocks.get_base_address() + index);
        ret = (*iter)->get_block_info(info);
      }
      return TFS_SUCCESS;
    }

    int LogicBlockManager::get_all_logic_block_to_physical_block(std::map<uint64_t, std::vector<int32_t> >& blocks) const
    {
      blocks.clear();
      LOGIC_BLOCK_MAP_ITER iter = logic_blocks_.begin();
      for (; iter != logic_blocks_.end(); ++iter)
      {
        std::pair<std::map<uint64_t, std::vector<int32_t> >::iterator, bool> res =
          blocks.insert(std::map<uint64_t, std::vector<int32_t> >::value_type((*iter)->id(), std::vector<int32_t>()));
        (*iter)->get_all_physical_blocks(res.first->second);
      }
      return TFS_SUCCESS;
    }

    int32_t LogicBlockManager::size() const
    {
      return logic_blocks_.size();
    }

    int LogicBlockManager::timeout(std::vector<uint64_t>& expired_blocks, const time_t now)
    {
      expired_blocks.clear();
      TMP_LOGIC_BLOCK_MAP_ITER iter = tmp_logic_blocks_.begin();
      for (; iter != tmp_logic_blocks_.end(); ++iter)
      {
       if ((iter->second->get_last_update_time() + iter->second->get_expire_time()) <= now)
          expired_blocks.push_back(iter->second->id());
      }
      return TFS_SUCCESS;
    }

    int LogicBlockManager::change_create_block_complete_flag_(const uint64_t logic_block_id, const int8_t status,
        const bool tmp,const bool flush)
    {
      int32_t ret = (INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        std::vector<int32_t> physical_block_ids;
        BaseLogicBlock* logic_block = NULL;
        if (tmp)
        {
          TMP_LOGIC_BLOCK_MAP_ITER iter = tmp_logic_blocks_.find(logic_block_id);
          ret = (tmp_logic_blocks_.end() != iter) ? TFS_SUCCESS : EXIT_LOGIC_BLOCK_NOT_EXIST_ERROR;
          if (TFS_SUCCESS == ret)
          {
            logic_block = iter->second;
            ret = logic_block->get_all_physical_blocks(physical_block_ids);
          }
        }
        else
        {
          BaseLogicBlock query(logic_block_id);
          LOGIC_BLOCK_MAP_ITER it =  logic_blocks_.find(&query);
          ret = (logic_blocks_.end() != it) ? TFS_SUCCESS : EXIT_LOGIC_BLOCK_NOT_EXIST_ERROR;
          if (TFS_SUCCESS == ret)
          {
            logic_block = (*it);
            ret = logic_block->get_all_physical_blocks(physical_block_ids);
          }
        }

        BlockIndex index;
        index.index_ = -1;
        index.logic_block_id_ = INVALID_BLOCK_ID;
        int32_t main_physical_block_id = INVALID_PHYSICAL_BLOCK_ID;
        ret = physical_block_ids.empty() ? EXIT_PHYSICAL_BLOCK_EXIST_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          main_physical_block_id = (*physical_block_ids.begin());
          assert(main_physical_block_id != INVALID_PHYSICAL_BLOCK_ID);
          ret = block_manager_.get_super_block_manager().get_block_index(index, main_physical_block_id);
        }
        if (TFS_SUCCESS == ret)
        {
          assert(index.logic_block_id_ == logic_block_id);
          assert(index.index_ == 0);
          index.status_ = status;
          ret = block_manager_.get_super_block_manager().update_block_index(index, main_physical_block_id);
        }

        if (TFS_SUCCESS == ret && flush)
        {
          ret = block_manager_.get_super_block_manager().flush();
        }
      }
      return ret;
    }
  }/** end namespace dataserver**/
}/** end namespace tfs **/
