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
 *
 */
#include <Memory.hpp>
#include "data_manager.h"
#include "visit_stat.h"

namespace tfs
{
  namespace dataserver
  {
    using namespace common;
    using namespace std;

    DataManager::DataManager()
    {
    }

    DataManager::~DataManager()
    {
      tbsys::gDelete(block_manager_);
    }

    int DataManager::initialize(const string& super_block_path)
    {
      block_manager_ = new (std::nothrow) BlockManager(super_block_path);
      assert(NULL != block_manager_);
      return TFS_SUCCESS;
    }

    int DataManager::init_block_files(const FileSystemParameter& fs_param)
    {
      TIMER_START();
      int ret = block_manager_->bootstrap(fs_param);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "init block files fail. ret: %d", ret);
      }
      else
      {
        TIMER_END();
        TBSYS_LOG(INFO, "init block files success. cost: %"PRI64_PREFIX"d", TIMER_DURATION());
      }
      return ret;
    }

    void DataManager::get_ds_filesystem_info(int32_t& block_count, int64_t& use_capacity, int64_t& total_capacity)
    {
      block_count = block_manager_->get_all_logic_block_count();
      block_manager_->get_space(total_capacity, use_capacity);
    }

    int DataManager::get_all_block_info(std::set<common::BlockInfoV2>& blocks)
    {
      return block_manager_->get_all_block_info(blocks);
    }

    int DataManager::get_all_block_info(std::set<common::BlockInfo>& blocks)
    {
      return block_manager_->get_all_block_info(blocks);
    }

    int64_t DataManager::get_all_logic_block_size()
    {
      return block_manager_->get_all_logic_block_count();
    }

    int DataManager::create_file_id(uint64_t block_id, uint64_t &file_id)
    {
      int ret = (INVALID_BLOCK_ID == block_id) ? EXIT_PARAMETER_ERROR: TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        double threshold = 0.8; // add a config item later
        ret = block_manager_->generation_file_id(file_id, threshold, block_id);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "create file id fail. blockid: %"PRI64_PREFIX"u, ret: %d",
              block_id, ret);
        }
      }

      return ret;
    }

    /**
     * @brief
     *
     * flag is not used right not
     * so stat_file will always return fileinfo even though it's deleted or hiden
     */
    int DataManager::stat_file(const uint64_t block_id, const uint64_t file_id, const int32_t flag,
        common::FileInfoV2& file_info)
    {
      int ret = ((INVALID_BLOCK_ID == block_id) || (INVALID_FILE_ID == file_id)) ?
        EXIT_PARAMETER_ERROR: TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        file_info.id_ = file_id;
        ret = block_manager_->stat(file_info, block_id, block_id);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "stat file fail. blockid: %"PRI64_PREFIX"u, "
              "fileid: %"PRI64_PREFIX"u, flag: %d, ret: %d",
              block_id, file_id, flag, ret);
        }
      }

      return ret;
    }

    int DataManager::read_file(const uint64_t block_id, const uint64_t file_id,
        char* buffer, int32_t &length, const int32_t offset, const int8_t flag)
    {
      int ret = ((INVALID_BLOCK_ID == block_id) || (INVALID_FILE_ID == file_id) ||
          (NULL == buffer) || (offset < 0) || (length <= 0)) ?
        EXIT_PARAMETER_ERROR: TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        ret = block_manager_->read(buffer, length, offset, file_id, flag, block_id, block_id);
        if (ret < 0)
        {
          TBSYS_LOG(ERROR, "read file fail. "
              "blockid: %"PRI64_PREFIX"u fileid: %"PRI64_PREFIX"u offset: %d, length: %d, flag: %d",
              block_id, file_id, offset, length, flag);
        }
      }

      return (ret < 0) ? ret: TFS_SUCCESS;
    }

    int DataManager::write_data(WriteLease* lease, const char* buffer,
        const int32_t length, const int32_t offset, const int32_t remote_version, BlockInfoV2& local)
    {
      int ret = ((NULL == lease) || (NULL == buffer) || (offset < 0) || (length <= 0)) ?
        EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        DataFile& data_file = lease->get_data_file();
        uint64_t block_id = lease->lease_id_.block_;
        uint64_t file_id = lease->lease_id_.file_id_;
        int ret = block_manager_->check_block_version(local, remote_version, block_id);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "write check block version conflict. blockid: %"PRI64_PREFIX"u, "
              "remote version: %d, local version: %d",
              block_id, remote_version, local.version_);
        }
        else
        {
          FileInfoInDiskExt none;
          none.version_ = FILE_INFO_EXT_INIT_VERSION;
          ret = data_file.pwrite(none, buffer, length, offset);
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "write datafile fail. blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u",
              block_id, file_id);
          }
        }
      }

      return ret;
    }

    int DataManager::close_file(const uint64_t block_id, uint64_t& file_id, DataFile& data_file)
    {
      int ret = ((INVALID_BLOCK_ID == block_id) || (INVALID_FILE_ID == file_id)) ?
        EXIT_PARAMETER_ERROR: TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        ret = block_manager_->write(file_id, data_file, block_id, block_id);
        if (ret < 0)
        {
          TBSYS_LOG(ERROR, "close file fail. blockid: %"PRI64_PREFIX"u fileid: %"PRI64_PREFIX"u, ret: %d",
              block_id, file_id, ret);
        }
      }

      return (ret < 0) ? ret: TFS_SUCCESS;
    }

    int DataManager::unlink_file(const uint64_t block_id, const uint64_t file_id, const int32_t action,
        const int32_t remote_version, int64_t& size, BlockInfoV2& local)
    {
      int ret = ((INVALID_BLOCK_ID == block_id) || (INVALID_FILE_ID == file_id)) ?
        EXIT_PARAMETER_ERROR: TFS_SUCCESS;

      // ignore negative value for compatible, old version don't check version
      // in unlink operation, it just pass a negative value here
      if (TFS_SUCCESS == ret && remote_version >= 0)
      {
        ret = block_manager_->check_block_version(local, remote_version, block_id);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "unlink check block version conflict. blockid: %"PRI64_PREFIX"u, "
              "remote version: %d, local version: %d, ret: %d",
              block_id, remote_version, local.version_, ret);
        }
      }

      if (TFS_SUCCESS == ret)
      {
        ret = block_manager_->unlink(size, file_id, action, block_id, block_id);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "close file fail. blockid: %"PRI64_PREFIX"u fileid: %"PRI64_PREFIX"u, ret:%d",
              block_id, file_id, ret);
        }
      }

      return ret;
    }

    int DataManager::get_block_info(const uint64_t block_id, BlockInfoV2& block_info)
    {
      int ret = (INVALID_BLOCK_ID == block_id) ? EXIT_PARAMETER_ERROR: TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        ret = block_manager_->get_block_info(block_info, block_id);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "get block info fail. blockid: %"PRI64_PREFIX"u, ret: %d", block_id, ret);
        }
      }
      return ret;
    }

    int DataManager::read_raw_data(char* buffer, const uint32_t block_id,
        int32_t& length, const int32_t offset)
    {
      int ret = ((NULL == buffer) || (offset < 0) || (length < 0)) ?
        EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      if (TFS_SUCCESS == ret);
      {
        ret = block_manager_->pread(buffer, length, offset, block_id);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "read raw data fail. blockid: %"PRI64_PREFIX"u, "
              "length: %d, offset: %d, ret: %d",
              block_id, length, offset, ret);
        }
      }
      return ret;
    }

    int DataManager::write_raw_data(const char* buffer, const uint32_t block_id,
        const int32_t length, const int32_t offset)
    {
      int ret = ((NULL == buffer) || (offset < 0) || (length <= 0)) ?
        EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        ret = block_manager_->pwrite(buffer, length, offset, block_id);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "write raw data fail. blockid: %"PRI64_PREFIX"u, "
              "length: %d, offset: %d, ret: %d",
              block_id, length, offset, ret);
        }
      }
      return ret;
    }

    int DataManager::check_block_version(const uint64_t block_id, const int32_t remote_version,
        common::BlockInfoV2& block_info)
    {
      int ret = (INVALID_BLOCK_ID == block_id) ? EXIT_PARAMETER_ERROR: TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        ret = block_manager_->check_block_version(block_info, remote_version, block_id);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "block version conflict. blockid: %"PRI64_PREFIX"u, "
              "remote_version: %d, local version: %d, ret: %d",
              block_id, remote_version, block_info.version_, ret);
        }
      }
      return ret;
    }

    int DataManager::new_block(const uint64_t block_id, const bool tmp,
        const int64_t family_id, const int8_t index_num)
    {
      int ret = (INVALID_BLOCK_ID == block_id) ? EXIT_PARAMETER_ERROR: TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        ret = block_manager_->new_block(block_id, tmp, family_id, index_num);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "new block %"PRI64_PREFIX"u fail. flag: %d, "
              "family_id: %"PRI64_PREFIX"u, index_num: %d, ret: %d",
              block_id, tmp, family_id, index_num, ret);
        }
      }
      return ret;
    }

    int DataManager::remove_block(const uint64_t block_id, const bool tmp)
    {
      int ret = (INVALID_BLOCK_ID == block_id) ? EXIT_PARAMETER_ERROR: TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        ret = block_manager_->del_block(block_id, tmp);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "remove block %"PRI64_PREFIX"u fail. flag: %d, ret: %d",
              block_id, tmp, ret);
        }
      }
      return ret;
    }

  }
}
