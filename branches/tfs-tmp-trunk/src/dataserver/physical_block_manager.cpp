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
#include "ds_define.h"
#include "super_block_manager.h"
#include "physical_block_manager.h"
#include "block_manager.h"

using namespace tfs::common;

namespace tfs
{
  namespace dataserver
  {
    static const int32_t MAX_PHYSICAL_BLOCK_SLOT_DEFALUT = 1024;
    static const int32_t MAX_PHYSICAL_BLOCK_SLOT_EXPAND_DEFAULT = 128;
    static const float   MAX_PHYSICAL_BLOCK_SLOT_EXPAND_RATION_DEFAULT = 0.1;
    static const int32_t MAX_ALLOC_PHYSICAL_BLOCK_SLOT_DEFALUT = 16;
    static const int32_t MAX_ALLOC_PHYSICAL_BLOCK_SLOT_EXPAND_DEFAULT = 1;
    static const float   MAX_ALLOC_PHYSICAL_BLOCK_SLOT_EXPAND_RATION_DEFAULT = 0.1;
    PhysicalBlockManager::PhysicalBlockManager(BlockManager& manager):
      manager_(manager),
      physical_blocks_(MAX_PHYSICAL_BLOCK_SLOT_DEFALUT, MAX_PHYSICAL_BLOCK_SLOT_EXPAND_DEFAULT, MAX_PHYSICAL_BLOCK_SLOT_EXPAND_RATION_DEFAULT),
      alloc_physical_blocks_(MAX_ALLOC_PHYSICAL_BLOCK_SLOT_DEFALUT, MAX_ALLOC_PHYSICAL_BLOCK_SLOT_EXPAND_DEFAULT, MAX_ALLOC_PHYSICAL_BLOCK_SLOT_EXPAND_RATION_DEFAULT)
    {

    }

    PhysicalBlockManager::~PhysicalBlockManager()
    {
      alloc_physical_blocks_.clear();
      PHYSICAL_BLOCK_MAP_ITER iter = physical_blocks_.begin();
      for (; iter != physical_blocks_.end(); ++iter)
      {
        tbsys::gDelete((*iter));
      }
    }

    int PhysicalBlockManager::insert(const BlockIndex& index, const int32_t physical_block_id, const std::string& path,
        const int32_t start, const int32_t end)
    {
      int32_t ret = (INVALID_PHYSICAL_BLOCK_ID != physical_block_id && !path.empty() && start >= 0 && end > 0 && start < end) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = insert_(index, physical_block_id, path, start, end);
      }
      return ret;
    }

    int PhysicalBlockManager::insert_(const BlockIndex& index, const int32_t physical_block_id, const std::string& path,
        const int32_t start, const int32_t end)
    {
      int32_t ret = (INVALID_PHYSICAL_BLOCK_ID != physical_block_id && !path.empty() && start >= 0 && end > 0 && start < end) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BasePhysicalBlock* physical_block = NULL;
        ret = exist(physical_block_id) ? EXIT_ELEMENT_EXIST : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          BasePhysicalBlock* result = NULL;
          if (index.split_flag_ == BLOCK_SPLIT_FLAG_YES)
          {
            AllocPhysicalBlock* palloc_physical_block =  new (std::nothrow)AllocPhysicalBlock(physical_block_id, path, start, end);
            assert(NULL != palloc_physical_block);
            physical_block = palloc_physical_block;
            ret = palloc_physical_block->load_alloc_bit_map_();
            if (TFS_SUCCESS != ret)
              manager_.get_gc_manager().add(physical_block);
          }
          else
          {
            physical_block = new (std::nothrow)PhysicalBlock(physical_block_id, path, start, end);
          }
          if (TFS_SUCCESS == ret && NULL != physical_block)
          {
            ret = physical_blocks_.insert_unique(result, physical_block);
            if (TFS_SUCCESS != ret)
              manager_.get_gc_manager().add(physical_block);
            if (EXIT_ELEMENT_EXIST == ret)
              assert(NULL != result);
          }
        }
        if ((BLOCK_SPLIT_FLAG_YES == index.split_flag_)
            && (BLOCK_SPLIT_STATUS_UNCOMPLETE == index.split_status_)
            && (TFS_SUCCESS == ret || EXIT_ELEMENT_EXIST == ret))
        {
          physical_block = get_(physical_block_id);
          PHYSICAL_BLOCK_MAP_ITER iter = alloc_physical_blocks_.find(physical_block);
          if (alloc_physical_blocks_.end() == iter)
          {
            BasePhysicalBlock* result = NULL;
            ret =  alloc_physical_blocks_.insert_unique(result, physical_block);
            assert(NULL != result);
          }
        }
      }
      return ret;
    }

    int PhysicalBlockManager::remove(BasePhysicalBlock*& physical_block, const int32_t physcical_block_id)
    {
      BlockIndex index;
      physical_block = NULL;
      SuperBlockInfo* info = NULL;
      int32_t ret = (INVALID_PHYSICAL_BLOCK_ID != physcical_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        SuperBlockManager& supber_block_manager = get_block_manager().get_super_block_manager();
        ret = supber_block_manager.get_super_block_info(info);
        if (TFS_SUCCESS == ret)
        {
          ret = supber_block_manager.get_block_index(index, physcical_block_id);
        }
        if (TFS_SUCCESS == ret)
        {
          if (BLOCK_SPLIT_FLAG_YES != index.split_flag_)
          {
            BasePhysicalBlock query(physcical_block_id);
            physical_block = physical_blocks_.erase(&query);
            ret = supber_block_manager.cleanup_block_index(physcical_block_id);
          }
          if (TFS_SUCCESS == ret && 0 != index.index_)
          {
            ret = free_ext_block(index, false);
          }
        }
        if (TFS_SUCCESS == ret && 0 == index.index_)
        {
          --info->used_main_block_count_;
        }
        if (TFS_SUCCESS == ret)
        {
          ret = supber_block_manager.flush();
        }
      }
      TBSYS_LOG(INFO, "free physical block: %d %s %d, logic block id: %"PRI64_PREFIX"u",
          index.physical_block_id_, TFS_SUCCESS == ret ? "successful" : "failed", ret, index.logic_block_id_);
      return ret;
    }

    int PhysicalBlockManager::alloc_block(BlockIndex& index, const int8_t split_flag, const bool flush, const bool tmp)
    {
      SuperBlockInfo* info = NULL;
      SuperBlockManager& supber_block_manager = get_block_manager().get_super_block_manager();
      int32_t ret = supber_block_manager.get_super_block_info(info);
      if (TFS_SUCCESS == ret)
      {
        int32_t retry_times = info->total_main_block_count_;
        do
        {
          int32_t physical_block_id = INVALID_PHYSICAL_BLOCK_ID;
          ret = supber_block_manager.get_legal_physical_block_id(physical_block_id);
          if (TFS_SUCCESS == ret)
          {
            ret = (INVALID_PHYSICAL_BLOCK_ID == physical_block_id) ? EXIT_PHYSICAL_ID_INVALID : TFS_SUCCESS;
          }
          if (TFS_SUCCESS == ret)
          {
            ret = exist(physical_block_id) ? EXIT_PHYSICAL_BLOCK_EXIST_ERROR : TFS_SUCCESS;
          }
          if (TFS_SUCCESS == ret)
          {
            index.physical_block_id_ = physical_block_id;
            index.physical_file_name_id_ = index.physical_block_id_;
            index.index_      = 0;
            index.next_index_ = 0;
            index.prev_index_ = 0;
            index.split_flag_ = split_flag;
            index.split_status_= BLOCK_SPLIT_STATUS_UNCOMPLETE;
            index.status_     =  tmp ? BLOCK_CREATE_COMPLETE_STATUS_UNCOMPLETE : BLOCK_CREATE_COMPLETE_STATUS_COMPLETE;
            std::stringstream path;
            path << info->mount_point_ << MAINBLOCK_DIR_PREFIX << index.physical_file_name_id_;
            const int32_t start = BLOCK_SPLIT_FLAG_YES != split_flag ? BLOCK_RESERVER_LENGTH : 0;
            const int32_t end   = info->max_main_block_size_;
            ret = insert_(index, index.physical_block_id_, path.str(), start, end);
          }
          if (TFS_SUCCESS == ret)
          {
            ret = supber_block_manager.update_block_index(index, index.physical_block_id_);
          }
          if (TFS_SUCCESS == ret && flush)
          {
            ret = supber_block_manager.flush();
          }
          if (TFS_SUCCESS == ret)
          {
            ++info->used_main_block_count_;  // a main block used here
          }
        }
        while (TFS_SUCCESS != ret && retry_times-- > 0);
      }
      TBSYS_LOG(INFO, "alloc physical block: %d %s %d, logic block id: %"PRI64_PREFIX"u",
          index.physical_block_id_, TFS_SUCCESS == ret ? "successful" : "failed", ret, index.logic_block_id_);
      return ret;
    }

    bool PhysicalBlockManager::exist(const int32_t physical_block_id) const
    {
      int32_t ret = (INVALID_PHYSICAL_BLOCK_ID != physical_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BasePhysicalBlock* result = get_(physical_block_id);
        ret = (NULL == result) ? EXIT_PHYSICAL_BLOCK_NOT_FOUND : TFS_SUCCESS;
      }
      return (TFS_SUCCESS == ret);
    }

    int PhysicalBlockManager::alloc_ext_block(BlockIndex& index, BlockIndex& ext_index, const bool tmp)
    {
      SuperBlockInfo* info = NULL;
      SuperBlockManager& supber_block_manager = get_block_manager().get_super_block_manager();
      int32_t ret = supber_block_manager.get_super_block_info(info);
      if (TFS_SUCCESS == ret)
      {
        int32_t retry_times = info->max_block_index_element_count_ - info->total_main_block_count_;
        do
        {
          std::vector<AllocPhysicalBlock*> alloc_complete_array;
          AllocPhysicalBlock* physical_block = NULL;
          PHYSICAL_BLOCK_MAP_ITER iter = alloc_physical_blocks_.begin();
          for (;iter != alloc_physical_blocks_.end() && NULL == physical_block; ++iter)
          {
            assert(NULL != (*iter));
            physical_block = dynamic_cast<AllocPhysicalBlock*>((*iter));
            if (physical_block->full(info->max_main_block_size_,info->max_extend_block_size_))
            {
              alloc_complete_array.push_back(physical_block);
              physical_block = NULL;
            }
          }
          BlockIndex alloc_block_index;
          std::vector<AllocPhysicalBlock*>::const_iterator it = alloc_complete_array.begin();
          for (; it != alloc_complete_array.end() && TFS_SUCCESS == ret; ++it)
          {
            ret = supber_block_manager.get_block_index(alloc_block_index, (*it)->id());
            if (TFS_SUCCESS == ret)
            {
              alloc_block_index.split_status_ = BLOCK_SPLIT_STATUS_COMPLETE;
              ret = supber_block_manager.update_block_index(alloc_block_index, (*it)->id());
            }
            if (TFS_SUCCESS == ret)
            {
              alloc_physical_blocks_.erase((*it));
              ret = get_block_manager().get_super_block_manager().flush();
            }
          }
          if (TFS_SUCCESS == ret)
          {
            ret = (NULL == physical_block) ? EXIT_PHYSICAL_BLOCK_NOT_FOUND : TFS_SUCCESS;
            if (TFS_SUCCESS != ret)
            {
              ret = alloc_block(alloc_block_index, BLOCK_SPLIT_FLAG_YES, true, false);
              if (TFS_SUCCESS == ret)
              {
                BasePhysicalBlock* tmp = get_(alloc_block_index.physical_block_id_);
                physical_block = (NULL != tmp) ? dynamic_cast<AllocPhysicalBlock*>(tmp) : NULL;
              }
              ret = (NULL == physical_block) ? EXIT_PHYSICAL_BLOCK_NOT_FOUND : TFS_SUCCESS;
            }
          }
          if (TFS_SUCCESS == ret)
          {
            int8_t  pos = 0;
            int32_t start = 0, end = 0;
            ret = physical_block->alloc(pos, start, end, info->max_main_block_size_, info->max_extend_block_size_);
            if (TFS_SUCCESS == ret)
            {
              int32_t physical_block_id = INVALID_PHYSICAL_BLOCK_ID;
              ret = supber_block_manager.get_legal_physical_block_id(physical_block_id, true);
              if (TFS_SUCCESS == ret)
              {
                ret = (INVALID_PHYSICAL_BLOCK_ID == physical_block_id) ? EXIT_PHYSICAL_ID_INVALID : TFS_SUCCESS;
              }
              if (TFS_SUCCESS == ret)
              {
                ret = exist(physical_block_id) ? EXIT_PHYSICAL_BLOCK_EXIST_ERROR : TFS_SUCCESS;
              }
              if (TFS_SUCCESS == ret)
              {
                ext_index.index_ = pos;
                ext_index.physical_block_id_ = physical_block_id;
                ext_index.logic_block_id_ = index.logic_block_id_;
                ext_index.physical_file_name_id_ = physical_block->id();
                ext_index.next_index_ = 0;
                ext_index.prev_index_ = index.physical_block_id_;
                ext_index.split_flag_ = BLOCK_SPLIT_FLAG_NO;
                ext_index.split_status_= BLOCK_SPLIT_STATUS_UNCOMPLETE;
                ext_index.status_     = tmp ? BLOCK_CREATE_COMPLETE_STATUS_UNCOMPLETE : BLOCK_CREATE_COMPLETE_STATUS_COMPLETE;
                index.next_index_     = ext_index.physical_block_id_;
                std::stringstream path;
                path << info->mount_point_ << MAINBLOCK_DIR_PREFIX << ext_index.physical_file_name_id_;
                ret = insert_(ext_index, ext_index.physical_block_id_, path.str(), start, end);
                if (TFS_SUCCESS == ret)
                {
                  ret = supber_block_manager.update_block_index(ext_index, ext_index.physical_block_id_);
                }
                if (TFS_SUCCESS == ret)
                {
                  ret = supber_block_manager.update_block_index(index, index.physical_block_id_);
                }
                if (TFS_SUCCESS == ret)
                {
                  ret = get_block_manager().get_super_block_manager().flush();
                }
                else
                {
                  BasePhysicalBlock* result = NULL;
                  ret = remove(result, ext_index.physical_block_id_);
                  manager_.get_gc_manager().add(result);
                }
              }
            }
          }
        }
        while (TFS_SUCCESS != ret && retry_times-- > 0);
      }
      TBSYS_LOG(INFO, "alloc physical extend block: %d %s %d, physical block file name id: %d, logic block id: %"PRI64_PREFIX"u",
          ext_index.physical_block_id_, TFS_SUCCESS == ret ? "successful" : "failed", ret, ext_index.physical_file_name_id_, ext_index.logic_block_id_);
      return ret;
    }

    int PhysicalBlockManager::free_ext_block(const BlockIndex& index, bool flush)
    {
      int32_t ret = (0 == index.index_ &&  BLOCK_SPLIT_FLAG_YES != index.split_flag_) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        SuperBlockInfo* info = NULL;
        SuperBlockManager& supber_block_manager = get_block_manager().get_super_block_manager();
        ret = supber_block_manager.get_super_block_info(info);
        if (TFS_SUCCESS == ret)
        {
          bool cleanup = false;
          AllocPhysicalBlock* physical_block = NULL;
          BasePhysicalBlock query(index.physical_file_name_id_);
          PHYSICAL_BLOCK_MAP_ITER iter = physical_blocks_.find(&query);
          ret = (physical_blocks_.end() != iter) ? TFS_SUCCESS : EXIT_PHYSICAL_BLOCK_NOT_FOUND;
          if (TFS_SUCCESS == ret)
          {
            physical_block = dynamic_cast<AllocPhysicalBlock*>((*iter));
            assert(NULL != physical_block);
          }
          if (TFS_SUCCESS == ret && 0 != index.index_)
          {
            ret = physical_block->free(index.index_, info->max_main_block_size_, info->max_extend_block_size_);
          }
          if (TFS_SUCCESS == ret)
          {
            PHYSICAL_BLOCK_MAP_ITER iter = alloc_physical_blocks_.find(&query);
            cleanup = physical_block->empty(info->max_main_block_size_, info->max_extend_block_size_);
            if (cleanup)
            {
              BasePhysicalBlock* result = physical_blocks_.erase(&query);
              ret = supber_block_manager.cleanup_block_index(index.physical_file_name_id_);
              if (TFS_SUCCESS == ret)
              {
                --info->used_main_block_count_;
                if (alloc_physical_blocks_.end() != iter)
                  alloc_physical_blocks_.erase(physical_block);
                if (flush)
                  ret = supber_block_manager.flush();
              }
              manager_.get_gc_manager().add(result);
            }
            else
            {
              BlockIndex alloc_block_index;
              ret = get_block_manager().get_super_block_manager().get_block_index(alloc_block_index, index.physical_file_name_id_);
              if (TFS_SUCCESS == ret && alloc_block_index.split_status_ == BLOCK_SPLIT_STATUS_COMPLETE)
              {
                alloc_block_index.split_status_ = BLOCK_SPLIT_STATUS_UNCOMPLETE;
                ret = get_block_manager().get_super_block_manager().update_block_index(alloc_block_index, alloc_block_index.physical_block_id_);
                if (TFS_SUCCESS == ret && flush)
                {
                  ret = get_block_manager().get_super_block_manager().flush();
                }
              }
              if (TFS_SUCCESS == ret)
              {
                BasePhysicalBlock* result = NULL;
                if (alloc_physical_blocks_.end() == iter)
                  ret = alloc_physical_blocks_.insert_unique(result, physical_block);
              }
            }
          }
        }
      }
      TBSYS_LOG(INFO, "free physical extend block: %d %s %d, physical block file name id: %d, logic block id: %"PRI64_PREFIX"u",
          index.physical_block_id_, TFS_SUCCESS == ret ? "successful" : "failed", ret, index.physical_file_name_id_, index.logic_block_id_);
      return ret;
    }

    BasePhysicalBlock* PhysicalBlockManager::get(const int32_t physical_block_id) const
    {
      BasePhysicalBlock* result = NULL;
      int32_t ret = (INVALID_PHYSICAL_BLOCK_ID != physical_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        result = get_(physical_block_id);
      }
      return result;
    }

    BasePhysicalBlock* PhysicalBlockManager::get_(const int32_t physical_block_id) const
    {
      BasePhysicalBlock* result = NULL;
      int32_t ret = (INVALID_PHYSICAL_BLOCK_ID != physical_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BasePhysicalBlock query(physical_block_id);
        PHYSICAL_BLOCK_MAP_ITER iter = physical_blocks_.find(&query);
        if (physical_blocks_.end() != iter)
          result = (*iter);
      }
      return result;
    }

    /*bool PhysicalBlockManager::exist_(const int32_t physical_block_id) const
    {
      BasePhysicalBlock* result = NULL;
      int32_t ret = (INVALID_PHYSICAL_BLOCK_ID != physical_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        result = get_(physical_block_id);
      }
      return (NULL != result);
    }*/
  }/** end namespace dataserver**/
}/** end namespace tfs **/
