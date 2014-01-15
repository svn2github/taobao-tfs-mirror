/* * (C) 2007-2013 Alibaba Group Holding Limited.
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

#include <bitset>
#include <dirent.h>
#include "common/internal.h"
#include "common/error_msg.h"
#include "common/directory_op.h"

#include "block_manager.h"
#include "blockfile_format.h"

using namespace tfs::common;

namespace tfs
{
  namespace dataserver
  {
    BlockManager::BlockManager(const std::string& super_block_path):
      super_block_manager_(super_block_path),
      logic_block_manager_(*this),
      physical_block_manager_(*this)
    {

    }

    BlockManager::~BlockManager()
    {

    }

    int BlockManager::format(const common::FileSystemParameter& parameter)
    {
      //1.initialize super block
      int32_t ret = create_file_system_superblock_(parameter);

      //2.create mount directory
      if (TFS_SUCCESS == ret)
      {
        ret = create_file_system_dir_(parameter);
      }

      //3.pre-allocate create main block
      if (TFS_SUCCESS == ret)
      {
        ret = fallocate_block_(parameter);
      }
      return ret;
    }

    int BlockManager::cleanup(const FileSystemParameter& parameter)
    {
      return DirectoryOp::delete_directory_recursively(parameter.mount_name_.c_str()) ? TFS_SUCCESS : EXIT_RM_DIR_ERROR;
    }

    int BlockManager::bootstrap(const common::FileSystemParameter& parameter)
    {
      //1. load super block
      int32_t ret = load_super_block_(parameter);

      //2. cleanup dirty index files
      if (TFS_SUCCESS == ret)
      {
        ret = cleanup_dirty_index_(parameter);
      }

      //3. load all block
      if (TFS_SUCCESS == ret)
      {
        ret = load_index_(parameter);
      }
      return ret;
    }

    int BlockManager::new_block(const uint64_t logic_block_id, const bool tmp, const int64_t family_id, const int8_t index_num, const int32_t expire_time)
    {
      RWLock::Lock lock(mutex_, WRITE_LOCKER);
      SuperBlockInfo* info = NULL;
      int32_t ret = (INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = get_logic_block_manager().exist(logic_block_id, tmp) ? EXIT_BLOCK_EXIST_ERROR : TFS_SUCCESS;
      }
      if (TFS_SUCCESS == ret)
      {
        ret = get_super_block_manager().get_super_block_info(info);
      }
      if (TFS_SUCCESS == ret)
      {
        BlockIndex index;
        index.logic_block_id_ = logic_block_id;
        BaseLogicBlock*    logic_block = NULL;
        BasePhysicalBlock* physical_block = NULL;
        ret = get_physical_block_manager().alloc_block(index, BLOCK_SPLIT_FLAG_NO, true, tmp);
        if (TFS_SUCCESS == ret)
        {
          physical_block = get_physical_block_manager().get(index.physical_block_id_);
          ret = (NULL != physical_block) ? TFS_SUCCESS : EXIT_ADD_PHYSICAL_BLOCK_ERROR;
        }
        if (TFS_SUCCESS == ret)
        {
          std::stringstream index_path;
          index_path << info->mount_point_ << INDEX_DIR_PREFIX << index.physical_block_id_;
          logic_block = insert_logic_block_(index.logic_block_id_, index_path.str(), tmp, expire_time);
          ret = (NULL != logic_block) ? TFS_SUCCESS : EXIT_ADD_LOGIC_BLOCK_ERROR;
          if (TFS_SUCCESS != ret)
          {
            get_physical_block_manager().remove(physical_block, index.physical_block_id_);
            get_gc_manager().add(physical_block);
          }
        }
        if (TFS_SUCCESS == ret)
        {
          assert(NULL != logic_block);
          if (IS_VERFIFY_BLOCK(logic_block_id))
          {
            VerifyLogicBlock* verify_logic_block = dynamic_cast<VerifyLogicBlock*>(logic_block);
            ret = verify_logic_block->create_index(logic_block_id, family_id, index_num);
          }
          else
          {
            LogicBlock* data_logic_block = dynamic_cast<LogicBlock*>(logic_block);
            ret = data_logic_block->create_index(info->hash_bucket_count_, info->mmap_option_);
          }
          if (TFS_SUCCESS == ret)
          {
            logic_block->add_physical_block(dynamic_cast<PhysicalBlock*>(physical_block));
            ret = get_super_block_manager().flush();
          }
          if (TFS_SUCCESS != ret)
          {
            get_logic_block_manager().remove(logic_block, index.logic_block_id_, tmp);
            get_physical_block_manager().remove(physical_block, index.physical_block_id_);
            get_gc_manager().add(physical_block);
            get_gc_manager().add(logic_block);
          }
        }
      }
      TBSYS_LOG(INFO, "new block : %"PRI64_PREFIX"u, %s, ret: %d, tmp: %s, family id: %"PRI64_PREFIX"d, index_num: %d, expire_time: %d",
          logic_block_id, TFS_SUCCESS == ret ? "successful" : "failed", ret, tmp ? "true" : "false", family_id, index_num, expire_time);
      return ret;
    }

    int BlockManager::del_block(const uint64_t logic_block_id, const bool tmp)
    {
      int32_t ret = (INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        RWLock::Lock lock(mutex_, WRITE_LOCKER);
        ret = del_block_(logic_block_id, tmp);
      }
      TBSYS_LOG(INFO, "del block : %"PRI64_PREFIX"u, %s, ret: %d, tmp: %s",
          logic_block_id, TFS_SUCCESS == ret ? "successful" : "failed", ret, tmp ? "true" : "false");
      return ret;
    }

    BaseLogicBlock* BlockManager::get(const uint64_t logic_block_id, const bool tmp) const
    {
      RWLock::Lock lock(mutex_, READ_LOCKER);
      return logic_block_manager_.get(logic_block_id, tmp);
    }

    int BlockManager::get_blocks_in_time_range(const common::TimeRange& range, std::vector<uint64_t>& blocks, const int32_t group_count, const int32_t group_seq) const
    {
      blocks.clear();
      RWLock::Lock lock(mutex_, READ_LOCKER);
      return logic_block_manager_.get_blocks_in_time_range(range, blocks, group_count, group_seq);
    }

    int BlockManager::get_all_block_ids(std::vector<uint64_t>& blocks) const
    {
      blocks.clear();
      RWLock::Lock lock(mutex_, READ_LOCKER);
      return logic_block_manager_.get_all_block_ids(blocks);
    }

    int BlockManager::get_all_block_info(std::set<BlockInfo>& blocks) const
    {
      blocks.clear();
      RWLock::Lock lock(mutex_, READ_LOCKER);
      return logic_block_manager_.get_all_block_info(blocks);
    }

    int BlockManager::get_all_block_info(std::vector<BlockInfoV2>& blocks) const
    {
      blocks.clear();
      RWLock::Lock lock(mutex_, READ_LOCKER);
      return logic_block_manager_.get_all_block_info(blocks);
    }

    int BlockManager::get_all_block_info(common::BlockInfoV2*& blocks, int32_t& block_count) const
    {
      RWLock::Lock lock(mutex_, READ_LOCKER);
      return logic_block_manager_.get_all_block_info(blocks, block_count);
    }

    int BlockManager::get_all_block_header(std::vector<common::IndexHeaderV2>& headers) const
    {
      headers.clear();
      RWLock::Lock lock(mutex_, READ_LOCKER);
      return logic_block_manager_.get_all_block_header(headers);
    }

    int BlockManager::get_all_block_statistic_visit_info(std::map<uint64_t, common::ThroughputV2> & infos, const bool reset) const
    {
      infos.clear();
      RWLock::Lock lock(mutex_, READ_LOCKER);
      return logic_block_manager_.get_all_block_statistic_visit_info(infos, reset);
    }

    int BlockManager::get_all_logic_block_to_physical_block(std::map<uint64_t, std::vector<int32_t> >& blocks) const
    {
      blocks.clear();
      RWLock::Lock lock(mutex_, READ_LOCKER);
      return logic_block_manager_.get_all_logic_block_to_physical_block(blocks);
    }

    int32_t BlockManager::get_all_logic_block_count() const
    {
      //RWLock::Lock lock(mutex_, READ_LOCKER);
      return logic_block_manager_.size();
    }

    int BlockManager::get_space(int64_t& total_space, int64_t& used_space) const
    {
      total_space = 0;
      used_space  = 0;
      SuperBlockInfo* info = NULL;
      int32_t ret = super_block_manager_.get_super_block_info(info);
      if (TFS_SUCCESS == ret)
      {
        total_space =  info->total_main_block_count_;
        total_space *= info->max_main_block_size_;
        used_space  = info->used_main_block_count_;
        used_space  *= info->max_main_block_size_;
      }
      return ret;
    }

    int BlockManager::switch_logic_block(const uint64_t logic_block_id, const bool tmp)
    {
      int32_t ret = (INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        RWLock::Lock lock(mutex_, WRITE_LOCKER);
        ret = get_logic_block_manager().switch_logic_block(logic_block_id, tmp);
      }
      return ret;
    }

    int BlockManager::timeout(const time_t now)
    {
      RWLock::Lock lock(mutex_, WRITE_LOCKER);
      std::vector<uint64_t> expired_blocks;
      int32_t ret = get_logic_block_manager().timeout(expired_blocks, now);
      if (TFS_SUCCESS == ret)
      {
        std::vector<uint64_t>::const_iterator iter = expired_blocks.begin();
        for (; iter != expired_blocks.end(); ++iter)
        {
          TBSYS_LOG(INFO, "timeout logic block %"PRI64_PREFIX"u", *iter);
          ret = del_block_((*iter), true);
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "delete logic block : %"PRI64_PREFIX"u from tmp logic block map failed, ret: %d",(*iter), ret);
          }
        }
      }

      get_gc_manager().gc(now);
      return ret;
    }

    int BlockManager::get_used_offset(int32_t& size, const uint64_t logic_block_id) const
    {
      int32_t ret = (INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BaseLogicBlock* logic_block = get(logic_block_id);
        ret = (NULL != logic_block) ? TFS_SUCCESS  : EXIT_NO_LOGICBLOCK_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = logic_block->get_used_offset(size);
        }
      }
      return ret;
    }

    int BlockManager::set_used_offset(const int32_t size, const uint64_t logic_block_id)
    {
      int32_t ret = (INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BaseLogicBlock* logic_block = get(logic_block_id);
        ret = (NULL != logic_block) ? TFS_SUCCESS  : EXIT_NO_LOGICBLOCK_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = logic_block->set_used_offset(size);
        }
      }
      return ret;
    }

    int BlockManager::get_family_id(int64_t& family_id, const uint64_t logic_block_id) const
    {
      int64_t ret = (INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BaseLogicBlock* logic_block = get(logic_block_id);
        ret = (NULL != logic_block) ? TFS_SUCCESS  : EXIT_NO_LOGICBLOCK_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = logic_block->get_family_id(family_id);
        }
      }
      return ret;
    }

    int BlockManager::set_family_id(const int64_t family_id, const uint64_t logic_block_id)
    {
      int64_t ret = (INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BaseLogicBlock* logic_block = get(logic_block_id);
        ret = (NULL != logic_block) ? TFS_SUCCESS  : EXIT_NO_LOGICBLOCK_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = logic_block->set_family_id(family_id);
        }
      }
      return ret;
    }

    int BlockManager::get_marshalling_offset(int32_t& size, const uint64_t logic_block_id) const
    {
      int32_t ret = (INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BaseLogicBlock* logic_block = get(logic_block_id);
        ret = (NULL != logic_block) ? TFS_SUCCESS  : EXIT_NO_LOGICBLOCK_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = logic_block->get_marshalling_offset(size);
        }
      }
      return ret;
    }

    int BlockManager::set_marshalling_offset(const int32_t size, const uint64_t logic_block_id)
    {
      int32_t ret = (INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BaseLogicBlock* logic_block = get(logic_block_id);
        ret = (NULL != logic_block) ? TFS_SUCCESS  : EXIT_NO_LOGICBLOCK_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = logic_block->set_marshalling_offset(size);
        }
      }
      return ret;
    }

    int BlockManager::get_index_header(common::IndexHeaderV2& header, const uint64_t logic_block_id) const
    {
      int32_t ret = (INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BaseLogicBlock* logic_block = get(logic_block_id);
        ret = (NULL != logic_block) ? TFS_SUCCESS  : EXIT_NO_LOGICBLOCK_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = logic_block->get_index_header(header);
        }
      }
      return ret;
    }

    int BlockManager::set_index_header(const common::IndexHeaderV2& header, const uint64_t logic_block_id, const bool tmp)
    {
      int32_t ret = (INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BaseLogicBlock* logic_block = get(logic_block_id, tmp);
        ret = (NULL != logic_block) ? TFS_SUCCESS  : EXIT_NO_LOGICBLOCK_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = logic_block->set_index_header(header);
        }
      }
      return ret;
    }

    int BlockManager::flush(const uint64_t logic_block_id, const bool tmp)
    {
      int32_t ret = (INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BaseLogicBlock* logic_block = get(logic_block_id, tmp);
        ret = (NULL != logic_block) ? TFS_SUCCESS  : EXIT_NO_LOGICBLOCK_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = logic_block->flush();
        }
      }
      return ret;
    }

    int BlockManager::check_block_version(common::BlockInfoV2& info, const int32_t remote_version,
        const uint64_t logic_block_id, const uint64_t attach_logic_block_id) const
    {
      int32_t ret = (remote_version >= 0 && INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BaseLogicBlock* logic_block = get(logic_block_id);
        ret = (NULL != logic_block) ? TFS_SUCCESS  : EXIT_NO_LOGICBLOCK_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = logic_block->check_block_version(info, remote_version, attach_logic_block_id);
        }
      }
      return ret;
    }

    int BlockManager::update_block_info(const common::BlockInfoV2& info, const uint64_t logic_block_id) const
    {
      int32_t ret = (INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BaseLogicBlock* logic_block = get(logic_block_id);
        ret = (NULL != logic_block) ? TFS_SUCCESS  : EXIT_NO_LOGICBLOCK_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = logic_block->update_block_info(info);
        }
      }
      return ret;
    }

    int BlockManager::update_block_version(const int8_t step, const uint64_t logic_block_id, const bool tmp)
    {
      int32_t ret = (INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BaseLogicBlock* logic_block = get(logic_block_id, tmp);
        ret = (NULL != logic_block) ? TFS_SUCCESS  : EXIT_NO_LOGICBLOCK_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = logic_block->update_block_version(step);
        }
      }
      return ret;
    }

    int BlockManager::get_block_info(common::BlockInfoV2& info, const uint64_t logic_block_id, const bool tmp) const
    {
      int32_t ret = (INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BaseLogicBlock* logic_block = get(logic_block_id, tmp);
        ret = (NULL != logic_block) ? TFS_SUCCESS  : EXIT_NO_LOGICBLOCK_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = logic_block->get_block_info(info);
        }
      }
      return ret;
    }

    int BlockManager::generation_file_id(uint64_t& fileid, const uint64_t logic_block_id)
    {
      int32_t ret = (INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        LogicBlock* logic_block = dynamic_cast<LogicBlock*>(get(logic_block_id));
        ret = (NULL != logic_block) ? TFS_SUCCESS  : EXIT_NO_LOGICBLOCK_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = logic_block->generation_file_id(fileid);
        }
      }
      return ret;
    }

    int BlockManager::pwrite(const char* buf, const int32_t nbytes, const int32_t offset, const uint64_t logic_block_id, const bool tmp)
    {
      int32_t ret = (NULL != buf && nbytes > 0 && INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BaseLogicBlock* logic_block = get(logic_block_id, tmp);
        ret = (NULL != logic_block) ? TFS_SUCCESS  : EXIT_NO_LOGICBLOCK_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = logic_block->pwrite(buf, nbytes, offset, tmp);
        }
      }
      return ret;
    }

    int BlockManager::pread(char* buf, int32_t& nbytes, const int32_t offset, const uint64_t logic_block_id)
    {
      int32_t ret = (NULL != buf && nbytes > 0 && INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BaseLogicBlock* logic_block = get(logic_block_id);
        ret = (NULL != logic_block) ? TFS_SUCCESS  : EXIT_NO_LOGICBLOCK_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = logic_block->pread(buf, nbytes, offset);
        }
      }
      return ret;
    }

    int BlockManager::write(uint64_t& fileid, DataFile& datafile, const uint64_t logic_block_id,
        const uint64_t attach_logic_block_id, const bool tmp)
    {
      int32_t ret = (datafile.length() > 0 && INVALID_BLOCK_ID != logic_block_id
          && INVALID_BLOCK_ID != attach_logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BaseLogicBlock* logic_block = get(logic_block_id, tmp);
        ret = (NULL != logic_block) ? TFS_SUCCESS  : EXIT_NO_LOGICBLOCK_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = logic_block->write(fileid, datafile, attach_logic_block_id, tmp);
        }
      }
      return ret;
    }

    int BlockManager::read(char* buf, int32_t& nbytes, const int32_t offset,
        const uint64_t fileid, const int8_t flag, const uint64_t logic_block_id, const uint64_t attach_logic_block_id)
    {
      int32_t ret = (NULL != buf && nbytes > 0 && offset >= 0 && INVALID_FILE_ID != fileid && flag >= 0
          && INVALID_BLOCK_ID != logic_block_id && INVALID_BLOCK_ID != attach_logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BaseLogicBlock* logic_block = get(logic_block_id);
        ret = (NULL != logic_block) ? TFS_SUCCESS  : EXIT_NO_LOGICBLOCK_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = logic_block->read(buf, nbytes, offset, fileid, flag, attach_logic_block_id);
        }
      }
      return ret;
    }

    int BlockManager::stat(FileInfoV2& info, const int8_t flag, const uint64_t logic_block_id, const uint64_t attach_logic_block_id) const
    {
      int32_t ret = (INVALID_FILE_ID != info.id_ && INVALID_BLOCK_ID != logic_block_id
          && INVALID_BLOCK_ID != attach_logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BaseLogicBlock* logic_block = get(logic_block_id);
        ret = (NULL != logic_block) ? TFS_SUCCESS  : EXIT_NO_LOGICBLOCK_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = logic_block->stat(info, flag, attach_logic_block_id);
        }
      }
      return ret;
    }

    int BlockManager::unlink(int64_t& size, const uint64_t fileid, const int32_t action,
        const uint64_t logic_block_id, const uint64_t attach_logic_block_id)
    {
      size = 0;
      int32_t ret = (INVALID_FILE_ID != fileid && INVALID_BLOCK_ID != logic_block_id
          && INVALID_BLOCK_ID != attach_logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BaseLogicBlock* logic_block = get(logic_block_id);
        ret = (NULL != logic_block) ? TFS_SUCCESS  : EXIT_NO_LOGICBLOCK_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = logic_block->unlink(size, fileid, action, attach_logic_block_id);
        }
      }
      return ret;
    }

    int BlockManager::write_file_infos(common::IndexHeaderV2& header, std::vector<common::FileInfoV2>& infos, const uint64_t logic_block_id, const uint64_t attach_logic_block_id, const bool tmp, const bool partial)
    {
      int32_t ret = (INVALID_BLOCK_ID != logic_block_id && INVALID_BLOCK_ID != attach_logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BaseLogicBlock* logic_block = get(logic_block_id, tmp);
        ret = (NULL != logic_block) ? TFS_SUCCESS  : EXIT_NO_LOGICBLOCK_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = logic_block->write_file_infos(header, infos, attach_logic_block_id, partial);
        }
      }
      return ret;
    }

    int BlockManager::traverse(common::IndexHeaderV2& header, std::vector<common::FileInfoV2>& finfos, const uint64_t logic_block_id, uint64_t attach_logic_block_id) const
    {
      int32_t ret = (INVALID_BLOCK_ID != logic_block_id && INVALID_BLOCK_ID != attach_logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BaseLogicBlock* logic_block = get(logic_block_id);
        ret = (NULL != logic_block) ? TFS_SUCCESS  : EXIT_NO_LOGICBLOCK_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = logic_block->traverse(header, finfos, attach_logic_block_id);
        }
      }
      return ret;
    }

    int BlockManager::traverse(std::vector<common::FileInfo>& finfos, const uint64_t logic_block_id, uint64_t attach_logic_block_id) const
    {
      int32_t ret = (INVALID_BLOCK_ID != logic_block_id && INVALID_BLOCK_ID != attach_logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BaseLogicBlock* logic_block = get(logic_block_id);
        ret = (NULL != logic_block) ? TFS_SUCCESS  : EXIT_NO_LOGICBLOCK_ERROR;
        if (TFS_SUCCESS == ret)
        {
          LogicBlock* block = dynamic_cast<LogicBlock*>(logic_block);
          ret = block->traverse(finfos, attach_logic_block_id);
        }
      }
      return ret;
    }

    int BlockManager::get_attach_blocks(common::ArrayHelper<uint64_t>& blocks, const uint64_t logic_block_id) const
    {
      int32_t ret = (INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BaseLogicBlock* logic_block = get(logic_block_id);
        ret = (NULL != logic_block) ? TFS_SUCCESS  : EXIT_NO_LOGICBLOCK_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = logic_block->get_attach_blocks(blocks);
        }
      }
      return ret;
    }

    int BlockManager::get_index_num(int32_t& index_num, const uint64_t logic_block_id) const
    {
      int32_t ret = (INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BaseLogicBlock* logic_block = get(logic_block_id);
        ret = (NULL != logic_block) ? TFS_SUCCESS  : EXIT_NO_LOGICBLOCK_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = logic_block->get_index_num(index_num);
        }
      }
      return ret;
    }

    bool BlockManager::exist(const uint64_t logic_block_id, const bool tmp) const
    {
      return logic_block_manager_.exist(logic_block_id, tmp);
    }

    int BlockManager::load_super_block_(const common::FileSystemParameter& parameter)
    {
      SuperBlockInfo* info = NULL;
      int32_t ret =  get_super_block_manager().load();
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "load super block error, ret: %d, errors: %d, %s", ret, errno, strerror(errno));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = get_super_block_manager().get_super_block_info(info);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "read super block information error, ret: %d", ret);
        }
      }
      if (TFS_SUCCESS == ret)
      {
        ret = 0 == memcmp(DEV_TAG, info->mount_tag_, strlen(DEV_TAG) + 1) ? TFS_SUCCESS : EXIT_FS_NOTINIT_ERROR;
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "file system not initialized. please format it first, ret: %d", ret);
        }
      }
      if (TFS_SUCCESS == ret)
      {
        ret = 0 == parameter.mount_name_.compare(info->mount_point_) ? TFS_SUCCESS : EXIT_MOUNT_POINT_ERROR;
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "file system mount point conflict, please check mount point!, point: %s != %s, ret: %d",
              parameter.mount_name_.c_str(), info->mount_point_, ret);
        }
      }
      return ret;
    }

    int BlockManager::load_index_(const common::FileSystemParameter& parameter)
    {
      UNUSED(parameter);
      SuperBlockInfo* info = NULL;
      int32_t ret = get_super_block_manager().get_super_block_info(info);
      if (TFS_SUCCESS == ret)
      {
        BlockIndex index;
        for (int32_t id = 1; id <= info->total_main_block_count_ && TFS_SUCCESS == ret; ++id)
        {
          ret = get_super_block_manager().get_block_index(index, id);
          if (TFS_SUCCESS == ret)
          {
            if (INVALID_PHYSICAL_BLOCK_ID != index.physical_block_id_
                && INVALID_BLOCK_ID == index.logic_block_id_
                && BLOCK_SPLIT_FLAG_NO == index.split_flag_)
            {
              TBSYS_LOG(WARN, "physical block %d maybe reuse, physical block file name id : %d", index.physical_block_id_, index.physical_file_name_id_);
              ret = get_super_block_manager().cleanup_block_index(index.physical_block_id_);
              if (TFS_SUCCESS == ret)
                ret = get_super_block_manager().flush();
              continue;
            }

            if (INVALID_PHYSICAL_BLOCK_ID == index.physical_block_id_)
              continue;

            if (get_physical_block_manager().exist(index.physical_block_id_))
            {
              TBSYS_LOG(WARN, "physical block id %d conflict, physical block existed, logic block id : %"PRI64_PREFIX"u",
                  index.physical_block_id_, index.logic_block_id_);
              continue;
            }
            BasePhysicalBlock* physical_block  = NULL;
            std::stringstream physical_block_path;
            bool complete = (BLOCK_CREATE_COMPLETE_STATUS_COMPLETE == index.status_);
            physical_block_path << info->mount_point_ << MAINBLOCK_DIR_PREFIX << index.physical_file_name_id_;
            TBSYS_LOG(INFO, "load physical block, physical block id: %d, logic block id: %"PRI64_PREFIX"u, complete : %s, alloc: %s",
                index.physical_block_id_, index.logic_block_id_, complete ? "true" : "false", BLOCK_SPLIT_FLAG_YES == index.split_flag_ ? "yes" : "no");
            if (complete)
            {
              if (BLOCK_SPLIT_FLAG_YES == index.split_flag_)
              {
                physical_block = insert_physical_block_(*info, index, index.physical_block_id_, physical_block_path.str());
                assert(NULL != physical_block);
              }
              else
              {
                if (get_logic_block_manager().exist(index.logic_block_id_, false))
                {
                  ret = cleanup_dirty_index_single_logic_block_(index);
                  TBSYS_LOG(INFO, "load block, logic block: %"PRI64_PREFIX"u existed, must be free current block, main physical block id: %d, ret: %d", index.logic_block_id_, index.physical_block_id_, ret);
                }
                else
                {
                  physical_block = insert_physical_block_(*info, index, index.physical_block_id_, physical_block_path.str());
                  assert(NULL != physical_block);
                  std::stringstream index_path;
                  index_path << info->mount_point_ << INDEX_DIR_PREFIX << index.physical_block_id_;
                  BaseLogicBlock* logic_block = insert_logic_block_(index.logic_block_id_, index_path.str(), !complete);
                  assert(NULL != logic_block);
                  ret = logic_block->load_index(info->mmap_option_);
                  if (TFS_SUCCESS == ret)
                  {
                    ret = logic_block->add_physical_block(dynamic_cast<PhysicalBlock*>(physical_block));
                  }
                  if (TFS_SUCCESS == ret)
                  {
                    int32_t next_physical_block_id = index.next_index_;
                    while (INVALID_PHYSICAL_BLOCK_ID != next_physical_block_id && TFS_SUCCESS == ret)
                    {
                      BlockIndex next_block_index;
                      ret = get_super_block_manager().get_block_index(next_block_index, next_physical_block_id);
                      if (TFS_SUCCESS == ret)
                      {
                        next_physical_block_id = next_block_index.next_index_;
                        std::stringstream physical_block_path;
                        physical_block_path << info->mount_point_ << MAINBLOCK_DIR_PREFIX << next_block_index.physical_file_name_id_;
                        physical_block = insert_physical_block_(*info, next_block_index,
                            next_block_index.physical_block_id_, physical_block_path.str());
                        assert(NULL != physical_block);
                        ret = logic_block->add_physical_block(dynamic_cast<PhysicalBlock*>(physical_block));
                      }
                    }
                  }

                  TBSYS_LOG(INFO, "load logic block: %"PRI64_PREFIX"u,ret: %d", index.logic_block_id_, ret);
                }//end if (get_logic_block_manager().exist(logic_block_id, false))
              }// end if (BLOCK_SPLIT_FLAG_YES == index.split_flag_)
            }
            else//end if (complete)
            {
              ret = cleanup_dirty_index_single_logic_block_(index);
              TBSYS_LOG(INFO, "load block, logic block: %"PRI64_PREFIX"u is invalid, must be free current block, main physical block id: %d, ret: %d", index.logic_block_id_, index.physical_block_id_, ret);
            }// end if (complete)
            if (TFS_SUCCESS == ret)
            {
              ret = get_super_block_manager().flush();
            }
          }//end if (TFS_SUCCESS == ret)
        }//end for
      }//end if
      return ret;
    }

    static int index_filter(const struct dirent* entry)
    {
      return ::index(entry->d_name, '.') ? 1 : 0;
    }

    int BlockManager::cleanup_dirty_index_(const common::FileSystemParameter& parameter)
    {
      int32_t index = 0;
      struct dirent** names= NULL;
      std::string path(parameter.mount_name_ + INDEX_DIR_PREFIX);
      int32_t num = scandir(path.c_str(), &names, index_filter, NULL);
      for (index = 0; index < num; ++index)
      {
        std::string file_path = path + names[index]->d_name;
        ::unlink(file_path.c_str());
        free(names[index]);
      }
      free(names);
      return TFS_SUCCESS;
    }

    int BlockManager::cleanup_dirty_index_single_logic_block_(const BlockIndex& index)
    {
      SuperBlockInfo* info = NULL;
      int32_t ret = (index.logic_block_id_ != INVALID_BLOCK_ID && index.physical_block_id_ != INVALID_PHYSICAL_BLOCK_ID) ? TFS_SUCCESS :  EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = get_super_block_manager().get_super_block_info(info);
      }
      if (TFS_SUCCESS == ret)
      {
        std::stringstream index_path;
        index_path << info->mount_point_ << INDEX_DIR_PREFIX << index.physical_block_id_;
        ::unlink(index_path.str().c_str());
        int32_t next_physical_block_id = index.next_index_;
        ret = get_super_block_manager().cleanup_block_index(index.physical_block_id_);
        if (TFS_SUCCESS == ret)
        {
          --info->used_main_block_count_;
          while (INVALID_PHYSICAL_BLOCK_ID != next_physical_block_id && TFS_SUCCESS == ret)
          {
            BlockIndex next_block_index;
            ret = get_super_block_manager().get_block_index(next_block_index, next_physical_block_id);
            if (TFS_SUCCESS == ret)
            {
              next_physical_block_id = next_block_index.next_index_;
              ret = get_super_block_manager().cleanup_block_index(next_block_index.physical_block_id_);
            }
          }
        }
      }
      return ret;
    }

#define CHECK_VALUE_RANGE(current, max, min) (current >= min && current <= max)
    int BlockManager::create_file_system_superblock_(const FileSystemParameter& parameter)
    {
      SuperBlockInfo info;
      memset(&info, 0, sizeof(info));
      info.version_ = parameter.file_system_version_;
      memcpy(info.mount_tag_, DEV_TAG, sizeof(info.mount_tag_));
      strncpy(info.mount_point_, parameter.mount_name_.c_str(), MAX_DEV_NAME_LEN - 1);
      info.mount_time_ = time(NULL);
      info.mount_point_use_space_ = parameter.max_mount_size_ * 1024;
      info.mount_fs_type_ = parameter.base_fs_type_;
      info.max_main_block_size_   = parameter.main_block_size_;
      info.max_extend_block_size_ = parameter.extend_block_size_;
      info.max_use_hash_bucket_ratio_ = parameter.hash_slot_ratio_;
      info.max_use_block_ratio_   = parameter.block_type_ratio_;
      info.superblock_reserve_offset_ = parameter.super_block_reserve_offset_;

      int ret = (EXT4 != info.mount_fs_type_ && EXT3_FULL != info.mount_fs_type_ && EXT3_FTRUN != info.mount_fs_type_)
        ? EXIT_FS_TYPE_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        ret = (CHECK_VALUE_RANGE(info.max_main_block_size_, MAX_BLOCK_SIZE, MIN_BLOCK_SIZE)
            && CHECK_VALUE_RANGE(info.max_extend_block_size_, MAX_EXT_BLOCK_SIZE, MIN_EXT_BLOCK_SIZE)
            && info.max_extend_block_size_ <= info.max_main_block_size_
            && info.max_main_block_size_ > 0 && info.max_extend_block_size_> 0
            && 0 == info.max_main_block_size_ % 2  && 0 == info.max_extend_block_size_ % 2) ? TFS_SUCCESS : EXIT_BLOCK_SIZE_INVALID;

        if (TFS_SUCCESS == ret)
        {
          ret = parameter.avg_segment_size_ > 0 ? TFS_SUCCESS : EXIT_ARG_SEGMENT_SIZE_INVALID;
        }

        if (TFS_SUCCESS == ret)
        {
          ret = info.mount_point_use_space_ > 0 ? TFS_SUCCESS : EXIT_MOUNT_SPACE_SIZE_ERROR;
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "format filesystem superblock fail, mount_point_use_space: %"PRI64_PREFIX"d",info.mount_point_use_space_);
          }
        }

        if (TFS_SUCCESS == ret)
        {
          const int32_t pagesize = getpagesize();
          const int32_t avg_file_count = info.max_main_block_size_ / parameter.avg_segment_size_;
          info.hash_bucket_count_      = std::min(parameter.max_init_index_element_nums_, avg_file_count);
          const int32_t mmap_size = INDEX_HEADER_V2_LENGTH + (info.hash_bucket_count_ + 1) * FILE_INFO_V2_LENGTH;
          const int32_t count     = mmap_size / pagesize;
          const int32_t remainder = mmap_size % pagesize;
          const int32_t max_mmap_size = MAX_MMAP_SIZE/*INDEX_HEADER_V2_LENGTH + (avg_file_count + 1) * FILE_INFO_V2_LENGTH*/;
          const int32_t max_count     = max_mmap_size / pagesize;
          const int32_t max_remainder = max_mmap_size % pagesize;
          const int32_t max_per_mmap_count = (parameter.max_extend_index_element_nums_ * FILE_INFO_V2_LENGTH) / pagesize;
          info.mmap_option_.first_mmap_size_=  remainder ? (count + 1) * pagesize : count * pagesize;
          info.mmap_option_.per_mmap_size_  =  max_per_mmap_count > 0 ? max_per_mmap_count * pagesize : pagesize;
          if (max_remainder)
            info.mmap_option_.max_mmap_size_ = (max_count + 1) * pagesize;
          else
            info.mmap_option_.max_mmap_size_ = max_count * pagesize;
          info.max_hash_bucket_count_ = MAX_INDEX_ELEMENT_NUM;
          //info.mmap_option_.max_mmap_size_ = std::max(MAX_MMAP_SIZE, info.mmap_option_.max_mmap_size_);
          const int32_t INDEXFILE_SAFE_MULT = 4;
          const int32_t avg_index_file_size = (avg_file_count + 1) * INDEXFILE_SAFE_MULT * FILE_INFO_V2_LENGTH + INDEX_HEADER_V2_LENGTH;

          info.used_main_block_count_  = 0;
          info.total_main_block_count_ = (info.mount_point_use_space_ / (info.max_main_block_size_ + avg_index_file_size)) - 2;
          info.max_block_index_element_count_ = static_cast<int32_t>(info.total_main_block_count_ / parameter.block_type_ratio_) +
            info.total_main_block_count_ + 1;
          ret = info.max_block_index_element_count_ > SuperBlockManager::MAX_BLOCK_INDEX_SIZE ? EXIT_MAX_BLOCK_INDEX_COUNT_INVALID : TFS_SUCCESS;

          info.main_block_id_seq_ = PHYSICAL_BLOCK_ID_INIT_VALUE;
          info.ext_block_id_seq_ = info.total_main_block_count_ + 1;
        }

        if (TFS_SUCCESS == ret)
        {
          ret = get_super_block_manager().format(info);
        }
      }
      return ret;
    }

    int BlockManager::create_file_system_dir_(const FileSystemParameter& parameter)
    {
      UNUSED(parameter);
      SuperBlockInfo* info = NULL;
      int32_t ret = get_super_block_manager().get_super_block_info(info);
      //main directory
      if (TFS_SUCCESS == ret)
      {
        ret = DirectoryOp::create_full_path(info->mount_point_, false, DIR_MODE) ? TFS_SUCCESS : EXIT_CREATE_DIR_ERROR;
        if (TFS_SUCCESS != ret)
          TBSYS_LOG(ERROR, "create file system directory: %s error, errno: %d,%s", info->mount_point_, errno, strerror(errno));
      }

      //index directroy
      if (TFS_SUCCESS == ret)
      {
        std::string index_dir(info->mount_point_ + INDEX_DIR_PREFIX);
        ret = DirectoryOp::create_full_path(index_dir.c_str(), false, DIR_MODE) ? TFS_SUCCESS : EXIT_CREATE_DIR_ERROR;
        if (TFS_SUCCESS != ret)
          TBSYS_LOG(ERROR, "create file system index directory: %s error, errno: %d,%s", index_dir.c_str(), errno, strerror(errno));
      }
      return ret;
    }

    int BlockManager::fallocate_block_(const FileSystemParameter& parameter)
    {
      UNUSED(parameter);
      SuperBlockInfo* info = NULL;
      FileFormater* file_formater = NULL;
      int32_t ret = get_super_block_manager().get_super_block_info(info);
      if (TFS_SUCCESS == ret)
      {
        if (EXT4 == info->mount_fs_type_)
          file_formater = new (std::nothrow)Ext4FileFormater();
        else if (EXT3_FULL == info->mount_fs_type_)
          file_formater = new (std::nothrow)Ext3FullFileFormater();
        else if (EXT3_FTRUN == info->mount_fs_type_)
          file_formater = new (std::nothrow)Ext3SimpleFileFormater();
        ret = NULL == file_formater ? EXIT_FS_TYPE_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS != ret)
          TBSYS_LOG(ERROR, "base filesystem type: %d not supported", info->mount_fs_type_);
      }

      if (TFS_SUCCESS == ret)
      {
        std::stringstream str;
        info->dump(str);
        TBSYS_LOG(INFO, "SUPER BLOCK INFORMATION: %s", str.str().c_str());
        for (int32_t index = 1; index <= info->total_main_block_count_ && TFS_SUCCESS == ret; ++index)
        {
          std::stringstream path;
          path << info->mount_point_ << MAINBLOCK_DIR_PREFIX << index;
          FileOperation file_op(path.str(), O_RDWR | O_CREAT);
          ret = file_op.open() < 0 ? EXIT_OPEN_FILE_ERROR : TFS_SUCCESS;
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "allocate space error, open file failed, errno: %d, %s", errno, strerror(errno));
          }
          if (TFS_SUCCESS == ret)
          {
            ret = file_formater->block_file_format(file_op.get_fd(), info->max_main_block_size_);
            if (TFS_SUCCESS != ret)
            {
              TBSYS_LOG(ERROR, "allocate space error, allocate failed, errno: %d, %s", errno, strerror(errno));
            }
          }
        }
      }
      tbsys::gDelete(file_formater);
      return ret;
    }

    BasePhysicalBlock* BlockManager::insert_physical_block_(const SuperBlockInfo& info, const BlockIndex& index, const int32_t physical_block_id, const std::string& path)
    {
      BasePhysicalBlock* physical_block = NULL;
      int32_t ret = (INVALID_PHYSICAL_BLOCK_ID != physical_block_id && !path.empty()) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        //0 == index.index_完整的大数据块(主块，分割的块)
        int32_t start = (0 == index.index_) ? BLOCK_SPLIT_FLAG_YES == index.split_flag_ ? 0 : BLOCK_RESERVER_LENGTH : (index.index_ - 1) * info.max_extend_block_size_;
        if (1 == index.index_)//第一个扩展块要比其他的少8个字节，这8个字节主要用于存储bitmap
          start += AllocPhysicalBlock::STORE_ALLOC_BIT_MAP_SIZE;
        const int32_t end   = (0 == index.index_) ? info.max_main_block_size_ : index.index_ * info.max_extend_block_size_;
        ret = get_physical_block_manager().insert(index, physical_block_id, path, start, end);
      }
      if (TFS_SUCCESS == ret)
      {
        physical_block = get_physical_block_manager().get(physical_block_id);
      }
      return physical_block;
    }

    BaseLogicBlock* BlockManager::insert_logic_block_(const uint64_t logic_block_id, const std::string& index_path, const bool tmp, const int32_t expire_time)
    {
      BaseLogicBlock* logic_block = NULL;
      int32_t ret = (INVALID_BLOCK_ID != logic_block_id && !index_path.empty()) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret && !get_logic_block_manager().exist(logic_block_id, tmp))
      {
        ret = get_logic_block_manager().insert(logic_block_id, index_path, tmp);
      }
      if (TFS_SUCCESS == ret)
      {
        logic_block = get_logic_block_manager().get(logic_block_id, tmp);
        if ((NULL != logic_block) && tmp)
        {
          logic_block->set_expire_time(expire_time);
        }
      }
      return logic_block;
    }

    BaseLogicBlock* BlockManager::get_(const uint64_t logic_block_id, const bool tmp) const
    {
      return logic_block_manager_.get(logic_block_id, tmp);
    }

    bool BlockManager::exist_(const uint64_t logic_block_id, const bool tmp) const
    {
      return logic_block_manager_.exist(logic_block_id, tmp);
    }

    int BlockManager::del_block_(const uint64_t logic_block_id, const bool tmp)
    {
      SuperBlockInfo* info = NULL;
      BaseLogicBlock* logic_block = NULL;
      int32_t ret = (INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = get_super_block_manager().get_super_block_info(info);
      }
      if (TFS_SUCCESS == ret)
      {
        logic_block = get_(logic_block_id, tmp);
        ret = (NULL != logic_block) ? TFS_SUCCESS : EXIT_NO_LOGICBLOCK_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        ret = get_logic_block_manager().remove(logic_block, logic_block_id, tmp);//remove logic block form logic block map, but not free pointer
      }
      if (TFS_SUCCESS == ret)
      {
        BasePhysicalBlock* physical_block = NULL;
        std::vector<int32_t> physical_blocks;
        ret = logic_block->rename_index_filename();//rename index file name
        if (TFS_SUCCESS == ret)
        {
          logic_block->get_all_physical_blocks(physical_blocks);
          std::vector<int32_t>::const_iterator iter = physical_blocks.begin();
          for (; iter != physical_blocks.end(); ++iter)
          {
            ret = get_physical_block_manager().remove(physical_block, (*iter));//remove physical block form physical block map, but not free pointer
            assert(TFS_SUCCESS == ret);
            ret = get_super_block_manager().cleanup_block_index((*iter));//cleanup block index
            assert(TFS_SUCCESS == ret);
            get_gc_manager().add(physical_block);
          }
          get_gc_manager().add(logic_block);
        }

        if (TFS_SUCCESS == ret)
        {
          ret = get_super_block_manager().flush();
        }
      }
      TBSYS_LOG(INFO, "del logic block : %"PRI64_PREFIX"u, %s, ret: %d, tmp: %s",
          logic_block_id, TFS_SUCCESS == ret ? "successful" : "failed", ret, tmp ? "true" : "false");
      return ret;
    }
  }/** end namespace dataserver **/
}/** end namespace tfs **/
