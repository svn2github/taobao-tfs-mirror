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

    int BlockManager::new_block(const uint64_t logic_block_id, const bool tmp)
    {
      SuperBlockInfo* info = NULL;
      int32_t ret = (INVALID_BLOCK_ID == logic_block_id) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
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
        BasePhysicalBlock* physical_block = NULL;
        BaseLogicBlock*    logic_block = NULL;
        ret = get_physical_block_manager().alloc_block(index, BLOCK_SPLIT_FLAG_NO);
        if (TFS_SUCCESS == ret)
        {
          physical_block = get_physical_block_manager().get(index.physical_block_id_);
          ret = (NULL != physical_block) ? TFS_SUCCESS : EXIT_ADD_PHYSICAL_BLOCK_ERROR;
        }
        if (TFS_SUCCESS == ret)
        {
          std::stringstream index_path;
          index_path << info->mount_point_ << INDEX_DIR_PREFIX << index.physical_block_id_;
          logic_block = insert_logic_block_(index.logic_block_id_, index_path.str(), tmp);
          ret = (NULL != logic_block) ? TFS_SUCCESS : EXIT_ADD_LOGIC_BLOCK_ERROR;
          if (TFS_SUCCESS != ret)
            get_physical_block_manager().remove(physical_block, index.physical_block_id_);
        }
        if (TFS_SUCCESS == ret)
        {
          assert(NULL != logic_block);
          logic_block->add_physical_block(dynamic_cast<PhysicalBlock*>(physical_block));
          ret = dynamic_cast<LogicBlock*>(logic_block)->create_index(info->hash_bucket_count_, info->mmap_option_);
          if (TFS_SUCCESS != ret)
          {
            get_logic_block_manager().remove(logic_block, index.logic_block_id_, tmp);
            get_physical_block_manager().remove(physical_block, index.physical_block_id_);
          }
        }
      }
      return ret;
    }

    int BlockManager::del_block(const uint64_t logic_block_id, const bool tmp)
    {
      SuperBlockInfo* info = NULL;
      BaseLogicBlock* logic_block = NULL;
      int32_t ret = (INVALID_BLOCK_ID == logic_block_id) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        ret = get_super_block_manager().get_super_block_info(info);
      }
      if (TFS_SUCCESS == ret)
      {
        logic_block = get(logic_block_id, tmp);
        ret = (NULL != logic_block) ? TFS_SUCCESS : EXIT_NO_LOGICBLOCK_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        logic_block = NULL;
        ret = get_logic_block_manager().remove(logic_block, logic_block_id);//remove logic block form logic block map, but not free pointer
      }
      if (TFS_SUCCESS == ret)
      {
        assert(logic_block != NULL);
        ret = logic_block->rename_index_filename();//rename index file name
      }
      if (TFS_SUCCESS == ret)
      {
        BasePhysicalBlock* physical_block = NULL;
        std::vector<int32_t> physical_blocks;
        logic_block->get_all_physical_blocks(physical_blocks);
        std::vector<int32_t>::const_iterator iter = physical_blocks.begin();
        for (; iter != physical_blocks.end(); ++iter)
        {
          ret = get_physical_block_manager().remove(physical_block, (*iter));//remove physical block form physical block map, but not free pointer
          assert(TFS_SUCCESS == ret);
          ret = get_super_block_manager().cleanup_block_index((*iter));//cleanup block index
          assert(TFS_SUCCESS == ret);
          //TODO, add physical block object to gc object map
        }
        //TODO, add logic block object to gc object map

        if (TFS_SUCCESS == ret)
        {
          info->used_main_block_count_ -= 1;
          info->used_extend_block_count_ -= (physical_blocks.size() - 1);
          ret = get_super_block_manager().flush();
        }
      }
      return ret;
    }

    BaseLogicBlock* BlockManager::get(const uint64_t logic_block_id, const bool tmp) const
    {
      return logic_block_manager_.get(logic_block_id, tmp);
    }

    int BlockManager::get_all_block_info(std::set<BlockInfo>& blocks) const
    {
      blocks.clear();
      return logic_block_manager_.get_all_block_info(blocks);
    }

    int BlockManager::get_all_block_info(std::vector<BlockInfoV2>& blocks) const
    {
      blocks.clear();
      return logic_block_manager_.get_all_block_info(blocks);
    }

    int BlockManager::get_all_block_info(std::set<common::BlockInfoV2>& blocks) const
    {
      blocks.clear();
      return logic_block_manager_.get_all_block_info(blocks);
    }

    int BlockManager::get_all_logic_block_to_physical_block(std::map<uint64_t, std::vector<int32_t> >& blocks) const
    {
      blocks.clear();
      return logic_block_manager_.get_all_logic_block_to_physical_block(blocks);
    }

    int32_t BlockManager::get_all_logic_block_count() const
    {
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
        total_space = info->total_main_block_count_ * info->max_main_block_size_;
        used_space  = info->used_main_block_count_  * info->max_main_block_size_;
      }
      return ret;
    }

    int BlockManager::switch_logic_block(const uint64_t logic_block_id, const bool tmp)
    {
      int32_t ret = (INVALID_BLOCK_ID == logic_block_id) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        ret = get_logic_block_manager().switch_logic_block(logic_block_id, tmp);
      }
      return ret;
    }

    int BlockManager::timeout(const time_t now)
    {
      std::vector<uint64_t> expired_blocks;
      int32_t ret = get_logic_block_manager().timeout(expired_blocks, now);
      if (TFS_SUCCESS == ret)
      {
        std::vector<uint64_t>::const_iterator iter = expired_blocks.begin();
        for (; iter != expired_blocks.end(); ++iter)
        {
          ret = del_block((*iter), true);
          if (TFS_SUCCESS != ret)
            TBSYS_LOG(WARN, "delete logic block : %"PRI64_PREFIX"u from tmp logic block map failed, ret: %d",(*iter), ret);
        }
      }
      return ret;
    }

    int BlockManager::check_block_version(common::BlockInfoV2& info, const int32_t remote_version, const uint64_t logic_block_id) const
    {
      int32_t ret = (remote_version >= 0 && INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        int32_t ret = (INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
        if (TFS_SUCCESS == ret)
        {
          BaseLogicBlock* logic_block = get(logic_block_id);
          ret = (NULL != logic_block) ? TFS_SUCCESS  : EXIT_NO_LOGICBLOCK_ERROR;
          if (TFS_SUCCESS == ret)
          {
            ret = logic_block->check_block_version(info, remote_version);
          }
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

    int BlockManager::update_block_version(const int8_t step, const uint64_t logic_block_id)
    {
      int32_t ret = (INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BaseLogicBlock* logic_block = get(logic_block_id);
        ret = (NULL != logic_block) ? TFS_SUCCESS  : EXIT_NO_LOGICBLOCK_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = logic_block->update_block_version(step);
        }
      }
      return ret;
    }

    int BlockManager::get_block_info(common::BlockInfoV2& info, const uint64_t logic_block_id) const
    {
      int32_t ret = (INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BaseLogicBlock* logic_block = get(logic_block_id);
        ret = (NULL != logic_block) ? TFS_SUCCESS  : EXIT_NO_LOGICBLOCK_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = logic_block->get_block_info(info);
        }
      }
      return ret;
    }

    int BlockManager::pwrite(char* buf, const int32_t nbytes, const int32_t offset, const uint64_t logic_block_id)
    {
      int32_t ret = (NULL != buf && nbytes > 0 && INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BaseLogicBlock* logic_block = get(logic_block_id);
        ret = (NULL != logic_block) ? TFS_SUCCESS  : EXIT_NO_LOGICBLOCK_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = logic_block->pwrite(buf, nbytes, offset);
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
        const uint64_t attach_logic_block_id)
    {
      int32_t ret = (datafile.length() > 0 && INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BaseLogicBlock* logic_block = get(logic_block_id);
        ret = (NULL != logic_block) ? TFS_SUCCESS  : EXIT_NO_LOGICBLOCK_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = logic_block->write(fileid, datafile, attach_logic_block_id);
        }
      }
      return ret;
    }

    int BlockManager::read(char* buf, int32_t& nbytes, const int32_t offset,
        const uint64_t fileid, const int8_t flag, const uint64_t logic_block_id, const uint64_t attach_logic_block_id)
    {
      int32_t ret = (NULL != buf && nbytes >= 0 && offset >= 0 && INVALID_FILE_ID != fileid && flag >= 0 && INVALID_BLOCK_ID != logic_block_id)
                    ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
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

    int BlockManager::stat(FileInfoV2& info,const uint64_t logic_block_id, const uint64_t attach_logic_block_id) const
    {
      int32_t ret = (INVALID_FILE_ID != info.id_) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BaseLogicBlock* logic_block = get(logic_block_id);
        ret = (NULL != logic_block) ? TFS_SUCCESS  : EXIT_NO_LOGICBLOCK_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = logic_block->stat(info, attach_logic_block_id);
        }
      }
      return ret;
    }

    int BlockManager::unlink(int64_t& size, const uint64_t fileid, const int32_t action,
        const uint64_t logic_block_id, const uint64_t attach_logic_block_id)
    {
      size = 0;
      int32_t ret = (INVALID_FILE_ID != fileid && INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
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

    int BlockManager::load_super_block_(const common::FileSystemParameter& parameter)
    {
      SuperBlockInfo* info = NULL;
      int32_t ret =  get_super_block_manager().load();
      if (TFS_SUCCESS != ret)
        TBSYS_LOG(ERROR, "load super block error, ret: %d, errors: %d, %s", ret, errno, strerror(errno));
      if (TFS_SUCCESS == ret)
      {
        ret = get_super_block_manager().get_super_block_info(info);
        if (TFS_SUCCESS != ret)
          TBSYS_LOG(ERROR, "read super block information error, ret: %d", ret);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = 0 == memcmp(DEV_TAG, info->mount_tag_, strlen(DEV_TAG) + 1) ? TFS_SUCCESS : EXIT_FS_NOTINIT_ERROR;
        if (TFS_SUCCESS != ret)
          TBSYS_LOG(ERROR, "file system not initialized. please format it first, ret: %d", ret);
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
        for (int32_t id = 0; id < info->used_main_block_count_ && TFS_SUCCESS == ret; ++id)
        {
          ret = get_super_block_manager().get_block_index(index, id);
          if (TFS_SUCCESS == ret)
          {
            if (INVALID_BLOCK_ID == index.logic_block_id_)
            {
              memset(&index, 0, sizeof(index));
              ret = get_super_block_manager().update_block_index(index,index.physical_block_id_);
              if (TFS_SUCCESS == ret)
                ret = get_super_block_manager().flush();
              continue;
            }

            if ((index.physical_block_id_ == index.physical_file_name_id_ )
                && (BLOCK_SPLIT_FLAG_NO == index.split_flag_))
            {
              bool complete = (BLOCK_CREATE_COMPLETE_STATUS_COMPLETE == index.status_);

              BasePhysicalBlock* physical_block = NULL;
              std::stringstream index_path;
              index_path << info->mount_point_ << INDEX_DIR_PREFIX << index.physical_block_id_;
              BaseLogicBlock* logic_block = insert_logic_block_(index.logic_block_id_, index_path.str());
              ret = (NULL != logic_block) ? TFS_SUCCESS : EXIT_ADD_LOGIC_BLOCK_ERROR;
              if (TFS_SUCCESS == ret)
              {
                std::stringstream physical_block_path;
                physical_block_path << info->mount_point_ << MAINBLOCK_DIR_PREFIX << index.physical_file_name_id_;
                physical_block = insert_physical_block_(*info, index, index.physical_block_id_, physical_block_path.str());
                ret = (NULL != physical_block) ? TFS_SUCCESS : EXIT_ADD_PHYSICAL_BLOCK_ERROR;
              }

              if (TFS_SUCCESS == ret)
              {
                if (BLOCK_SPLIT_FLAG_NO == index.split_flag_)
                  ret = logic_block->add_physical_block(dynamic_cast<PhysicalBlock*>(physical_block));
              }

              if (TFS_SUCCESS == ret)
              {
                BlockIndex ext_index = index;
                while (0 != ext_index.next_index_ && TFS_SUCCESS == ret)
                {
                  ret = get_super_block_manager().get_block_index(ext_index, ext_index.next_index_);
                  if (TFS_SUCCESS == ret)
                  {
                    if (!complete)
                      complete = (BLOCK_CREATE_COMPLETE_STATUS_COMPLETE == ext_index.status_);
                    if (INVALID_BLOCK_ID == ext_index.logic_block_id_
                        || index.logic_block_id_ != ext_index.logic_block_id_
                        || id != ext_index.prev_index_)
                    {
                      memset(&ext_index, 0, sizeof(ext_index));
                      ret = get_super_block_manager().update_block_index(ext_index, ext_index.physical_block_id_);
                      if (TFS_SUCCESS == ret)
                        ret = get_super_block_manager().flush();
                      continue;
                    }

                    std::stringstream ext_physical_block_path;
                    ext_physical_block_path << info->mount_point_ << MAINBLOCK_DIR_PREFIX << ext_index.physical_file_name_id_;
                    physical_block = insert_physical_block_(*info, ext_index, ext_index.physical_block_id_, ext_physical_block_path.str());
                    ret = (NULL != physical_block) ? TFS_SUCCESS : EXIT_ADD_PHYSICAL_BLOCK_ERROR;
                    if (TFS_SUCCESS == ret)
                    {
                      if (BLOCK_SPLIT_FLAG_NO == index.split_flag_)
                        ret = logic_block->add_physical_block(dynamic_cast<PhysicalBlock*>(physical_block));
                    }
                  }
                }//end while
              }//end if (TFS_SUCCESS == ret)

              if (TFS_SUCCESS == ret)
              {
                if (!complete)
                {
                  ret = del_block(index.logic_block_id_);
                }
                if (complete && TFS_SUCCESS == ret)
                {
                  ret = logic_block->load_index(info->mmap_option_);
                }
                if (complete && TFS_SUCCESS == ret)
                {
                  //检测LOGICBLOCK是否完整
                  ret = logic_block->check_block_intact();
                  if (TFS_SUCCESS != ret)
                  {
                    //TODO,不完整的BLOCK是否需要直接删除？？？
                    ret = del_block(index.logic_block_id_);
                  }
                }
              }
            }
          }
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

        int64_t avail_data_space = 0;
        if (TFS_SUCCESS == ret)
        {
          info.max_index_size_ = ((info.max_main_block_size_ / parameter.avg_segment_size_) + 1) * FILE_INFO_V2_LENGTH;
          double ratio = (FILE_INFO_V2_LENGTH * INDEXFILE_SAFE_MULT) / parameter.avg_segment_size_;
          avail_data_space = static_cast<int64_t>(info.mount_point_use_space_ * (1.00000000 - ratio));
          ret = avail_data_space > 0 ? TFS_SUCCESS : EXIT_MOUNT_SPACE_SIZE_ERROR;
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "format filesystem superblock fail, avail_data_space <= 0, mount_point_use_space: %"PRI64_PREFIX"d, index use space ratio : %e",
                info.mount_point_use_space_, ratio);
          }
        }

        if (TFS_SUCCESS == ret)
        {
          const int32_t pagesize = getpagesize();
          const int32_t avg_file_count = info.max_main_block_size_ / parameter.avg_segment_size_;
          info.used_main_block_count_  = 0;
          info.used_extend_block_count_ = 0;
          info.total_main_block_count_ = info.mount_point_use_space_ / info.max_main_block_size_;
          info.max_block_index_element_count_ = static_cast<int32_t>(info.total_main_block_count_ / parameter.block_type_ratio_) +
            info.total_main_block_count_ + 1;
          ret = info.max_block_index_element_count_ > SuperBlockManager::MAX_BLOCK_INDEX_SIZE ? EXIT_MAX_BLOCK_INDEX_COUNT_INVALID : TFS_SUCCESS;
          if (TFS_SUCCESS == ret)
          {
            info.hash_bucket_count_      = std::min(MAX_INITIALIZE_INDEX_SIZE, avg_file_count);
            const int32_t mmap_size = INDEX_HEADER_V2_LENGTH + (info.hash_bucket_count_ + 1) * FILE_INFO_V2_LENGTH;
            const int32_t count     = mmap_size / pagesize;
            const int32_t remainder = mmap_size % pagesize;
            const int32_t max_mmap_size = INDEX_HEADER_V2_LENGTH + (avg_file_count + 1) * FILE_INFO_V2_LENGTH;
            const int32_t max_count     = max_mmap_size / pagesize;
            const int32_t max_remainder = max_mmap_size % pagesize;
            info.mmap_option_.first_mmap_size_=  remainder ? (count + 1) * pagesize : count * pagesize;
            info.mmap_option_.per_mmap_size_  =  pagesize;
            if (max_remainder)
              info.mmap_option_.max_mmap_size_ = (max_count + 1) * pagesize * INNERFILE_MAX_MULTIPE;
            else
              info.mmap_option_.max_mmap_size_ = max_count * pagesize * INNERFILE_MAX_MULTIPE;
          }
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
      //main block directory
      if (TFS_SUCCESS == ret)
      {
        ret = DirectoryOp::create_full_path(info->mount_point_, false, DIR_MODE) ? TFS_SUCCESS : EXIT_CREATE_DIR_ERROR;
        if (TFS_SUCCESS != ret)
          TBSYS_LOG(ERROR, "mkdir main block directory error, errno: %d,%s", errno, strerror(errno));
      }

      //index directroy
      if (TFS_SUCCESS == ret)
      {
        std::string index_dir(info->mount_point_ + INDEX_DIR_PREFIX);
        ret = DirectoryOp::create_full_path(index_dir.c_str(), false, DIR_MODE) ? TFS_SUCCESS : EXIT_CREATE_DIR_ERROR;
        if (TFS_SUCCESS != ret)
          TBSYS_LOG(ERROR, "mkdir index block directory error, errno: %d,%s", errno, strerror(errno));
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
        for (int32_t index = 0; index < info->total_main_block_count_ && TFS_SUCCESS == ret; ++index)
        {
          std::stringstream path;
          path << info->mount_point_ << MAINBLOCK_DIR_PREFIX << index;
          FileOperation file_op(path.str(), O_RDWR | O_CREAT);
          ret = file_op.open() < 0 ? EXIT_OPEN_FILE_ERROR : TFS_SUCCESS;
          if (TFS_SUCCESS != ret)
            TBSYS_LOG(ERROR, "allocate space error, open file failed, errno: %d, %s", errno, strerror(errno));
          if (TFS_SUCCESS == ret)
          {
            ret = file_formater->block_file_format(file_op.get_fd(), info->max_main_block_size_);
            if (TFS_SUCCESS != ret)
              TBSYS_LOG(ERROR, "allocate space error, allocate failed, errno: %d, %s", errno, strerror(errno));
          }
        }
        if (TFS_SUCCESS == ret)
        {
          for (int32_t index = 0; index < info->total_main_block_count_ && TFS_SUCCESS == ret; ++index)
          {
            std::stringstream index_path;
            index_path << info->mount_point_ << INDEX_DIR_PREFIX << index;
            FileOperation index_file_op(index_path.str(), O_RDWR | O_CREAT);
            ret = index_file_op.open() < 0 ? EXIT_OPEN_FILE_ERROR : TFS_SUCCESS;
            if (TFS_SUCCESS != ret)
              TBSYS_LOG(ERROR, "allocate space error, open file failed, errno: %d, %s", errno, strerror(errno));
            if (TFS_SUCCESS == ret)
            {
              ret = file_formater->block_file_format(index_file_op.get_fd(), info->mmap_option_.max_mmap_size_);
              if (TFS_SUCCESS != ret)
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
      int32_t ret = (INVALID_PHYSICAL_BLOCK_ID == physical_block_id || path.empty()) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        //0 == index.index_完整的大数据块(主块，分割的块)
        const int32_t start = (0 == index.index_) ? BLOCK_RESERVER_LENGTH : (index.index_ - 1) * info.max_extend_block_size_;
        const int32_t end   = (0 == index.index_) ? (start + info.max_main_block_size_) : (start + info.max_extend_block_size_);
        ret = get_physical_block_manager().insert(index, physical_block_id, path, start, end);
      }
      if (TFS_SUCCESS == ret)
      {
        physical_block = get_physical_block_manager().get(physical_block_id);
      }
      return physical_block;
    }

    BaseLogicBlock* BlockManager::insert_logic_block_(const uint64_t logic_block_id, const std::string& index_path, const bool tmp)
    {
      BaseLogicBlock* logic_block = NULL;
      int32_t ret = (INVALID_BLOCK_ID == logic_block_id || index_path.empty()) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret && !get_logic_block_manager().exist(logic_block_id))
      {
        ret = get_logic_block_manager().insert(logic_block_id, index_path, tmp);
      }
      if (TFS_SUCCESS == ret)
      {
        logic_block = get_logic_block_manager().get(logic_block_id);
      }
      return logic_block;
    }
  }/** end namespace dataserver **/
}/** end namespace tfs **/
