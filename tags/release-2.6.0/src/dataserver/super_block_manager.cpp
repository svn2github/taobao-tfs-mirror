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
#include "super_block_manager.h"

using namespace tfs::common;
namespace tfs
{
  namespace dataserver
  {
    // super block file implementation, inner format:
    // ------------------------------------------------------------
    // | reserve         | SuperBlock|         BlockIndex         |
    // ------------------------------------------------------------
    // | reserve         | SuperBlock| {BlockIndex|...|BlockIndex}|
    // ------------------------------------------------------------
    const int32_t SuperBlockManager::SUPERBLOCK_RESERVER_LENGTH = 512;
    const int32_t SuperBlockManager::MAX_BLOCK_INDEX_SIZE = 65535 * (1 + 3);
    SuperBlockManager::SuperBlockManager(const std::string& path):
      file_op_(path, O_RDWR | O_SYNC | O_LARGEFILE | O_CREAT),
      index_(PHYSICAL_BLOCK_ID_INIT_VALUE),
      ext_index_(0)
    {

    }

    SuperBlockManager::~SuperBlockManager()
    {

    }

    int SuperBlockManager::format(SuperBlockInfo& info)
    {
      MMapOption opt;
      int32_t pagesize = getpagesize();
      opt.max_mmap_size_ = pagesize + info.max_block_index_element_count_ * sizeof(BlockIndex);
      opt.per_mmap_size_ = pagesize;
      opt.first_mmap_size_ = opt.max_mmap_size_;
      int32_t ret = file_op_.mmap(opt);
      if (TFS_SUCCESS != ret)
        TBSYS_LOG(ERROR, "format super block error: %d, %s", errno, strerror(errno));
      if (TFS_SUCCESS == ret)
      {
        ret = update_super_block_info(info);
      }
      return ret;
    }

    int SuperBlockManager::SuperBlockManager::load()
    {
      uint32_t file_size = file_op_.size();
      int32_t ret = file_size <= sizeof(SuperBlockInfo) ? EXIT_SUPERBLOCK_INVALID_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        int32_t pagesize = getpagesize();
        MMapOption opt;
        opt.first_mmap_size_ = file_size;
        opt.max_mmap_size_ = pagesize + MAX_BLOCK_INDEX_SIZE * sizeof(BlockIndex);
        opt.per_mmap_size_ = pagesize;
        ret = file_op_.mmap(opt);
      }
      return ret;
    }

    int SuperBlockManager::get_super_block_info(SuperBlockInfo*& info) const
    {
      RWLock::Lock lock(mutex_, READ_LOCKER);
      char* data = file_op_.get_data();
      int32_t ret = (NULL == data) ? EXIT_MMAP_DATA_INVALID : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        info = (reinterpret_cast<SuperBlockInfo*>(data+ SUPERBLOCK_RESERVER_LENGTH));
      }
      return ret;
    }

    int SuperBlockManager::update_super_block_info(const SuperBlockInfo& info)
    {
      RWLock::Lock lock(mutex_, WRITE_LOCKER);
      char* data = file_op_.get_data();
      int32_t ret = (NULL == data) ? EXIT_MMAP_DATA_INVALID : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        memset(data, 0, file_op_.size());
        memcpy((data+ SUPERBLOCK_RESERVER_LENGTH), &info, sizeof(info));
      }
      return ret;
    }

    int SuperBlockManager::get_block_index(BlockIndex& index, const int32_t physical_block_id) const
    {
      int32_t ret = (physical_block_id > 0 && physical_block_id < MAX_PHYSICAL_BLOCK_ID) ? TFS_SUCCESS : EXIT_PHYSICAL_ID_INVALID;
      if (TFS_SUCCESS == ret)
      {
        RWLock::Lock lock(mutex_, READ_LOCKER);
        char* data = file_op_.get_data();
        ret = (NULL == data) ? EXIT_MMAP_DATA_INVALID : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          BlockIndex* pstart = reinterpret_cast<BlockIndex*>(data+ SUPERBLOCK_RESERVER_LENGTH + sizeof(SuperBlockInfo));
          index = *(pstart + physical_block_id);
        }
      }
      return ret;
    }

    int SuperBlockManager::update_block_index(const BlockIndex& index, const int32_t physical_block_id)
    {
      int32_t ret = (physical_block_id > 0 && physical_block_id < MAX_PHYSICAL_BLOCK_ID) ? TFS_SUCCESS : EXIT_PHYSICAL_ID_INVALID;
      if (TFS_SUCCESS == ret)
      {
        RWLock::Lock lock(mutex_, WRITE_LOCKER);
        char* data = file_op_.get_data();
        ret = (NULL == data) ? EXIT_MMAP_DATA_INVALID : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          BlockIndex* pstart = reinterpret_cast<BlockIndex*>(data+ SUPERBLOCK_RESERVER_LENGTH + sizeof(SuperBlockInfo));
          memcpy(pstart + physical_block_id, &index, sizeof(index));
        }
      }
      return ret;
    }

    int SuperBlockManager::cleanup_block_index(const int32_t physical_block_id)
    {
      int32_t ret = (physical_block_id > 0 && physical_block_id < MAX_PHYSICAL_BLOCK_ID) ? TFS_SUCCESS : EXIT_PHYSICAL_ID_INVALID;
      if (TFS_SUCCESS == ret)
      {
        RWLock::Lock lock(mutex_, WRITE_LOCKER);
        char* data = file_op_.get_data();
        ret = (NULL == data) ? EXIT_MMAP_DATA_INVALID : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          BlockIndex* pstart = reinterpret_cast<BlockIndex*>(data+ SUPERBLOCK_RESERVER_LENGTH + sizeof(SuperBlockInfo));
          memset((pstart + physical_block_id), 0, sizeof(BlockIndex));
        }
      }
      return ret;
    }

    int SuperBlockManager::get_legal_physical_block_id(int32_t& physical_block_id, const bool extend) const
    {
      RWLock::Lock lock(mutex_, WRITE_LOCKER);
      physical_block_id = INVALID_PHYSICAL_BLOCK_ID;
      char* data = file_op_.get_data();
      int32_t ret = (NULL == data) ? EXIT_MMAP_DATA_INVALID : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        SuperBlockInfo* info = reinterpret_cast<SuperBlockInfo*>(data+ SUPERBLOCK_RESERVER_LENGTH);
        const int32_t MAX_COUNT = extend ? info->max_block_index_element_count_ : info->total_main_block_count_;

        int32_t retry_times = 2;
        BlockIndex* pstart = reinterpret_cast<BlockIndex*>(data+ SUPERBLOCK_RESERVER_LENGTH + sizeof(SuperBlockInfo));
        if (extend)
        {
          const int32_t EXT_PHYSICAL_BLOCK_INIT_VALUE = info->total_main_block_count_ + 1;
          while (INVALID_PHYSICAL_BLOCK_ID == physical_block_id && retry_times-- > 0)
          {
            if (info->ext_block_id_seq_ < EXT_PHYSICAL_BLOCK_INIT_VALUE || info->ext_block_id_seq_ >= MAX_COUNT)
                info->ext_block_id_seq_ = EXT_PHYSICAL_BLOCK_INIT_VALUE;
            for (; info->ext_block_id_seq_ < MAX_COUNT && INVALID_PHYSICAL_BLOCK_ID == physical_block_id; ++info->ext_block_id_seq_)
            {
              BlockIndex* current = (pstart + info->ext_block_id_seq_);
              if (INVALID_PHYSICAL_BLOCK_ID == current->physical_block_id_)
                physical_block_id = info->ext_block_id_seq_;
            }
          }
        }
        else
        {
          while (INVALID_PHYSICAL_BLOCK_ID == physical_block_id && retry_times-- > 0)
          {
            if (info->main_block_id_seq_< PHYSICAL_BLOCK_ID_INIT_VALUE || info->main_block_id_seq_> MAX_COUNT)
                info->main_block_id_seq_ = PHYSICAL_BLOCK_ID_INIT_VALUE;
            for (; info->main_block_id_seq_ <= MAX_COUNT && INVALID_PHYSICAL_BLOCK_ID == physical_block_id; ++info->main_block_id_seq_)
            {
              BlockIndex* current = (pstart + info->main_block_id_seq_);
              if (INVALID_PHYSICAL_BLOCK_ID == current->physical_block_id_)
                physical_block_id = info->main_block_id_seq_;
            }
          }
        }
      }
      return INVALID_PHYSICAL_BLOCK_ID != physical_block_id ? TFS_SUCCESS : EXIT_PHYSICAL_ID_INVALID;
    }

    int SuperBlockManager::dump(tbnet::DataBuffer& buf) const
    {
      UNUSED(buf);
      return TFS_SUCCESS;
    }

    int SuperBlockManager::dump(const int32_t level, const char* file, const int32_t line,
        const char* function, const char* format, ...)
    {
      UNUSED(level);
      UNUSED(file);
      UNUSED(line);
      UNUSED(function);
      UNUSED(format);
      return TFS_SUCCESS;
    }

    int SuperBlockManager::flush()
    {
      // must flush to disk, flush use msync MS_ASYNC flag
      // return file_op_.flush();
      return file_op_.fsync();
    }
  }/** end namespace dataserver **/
}/** end namespace tfs **/
