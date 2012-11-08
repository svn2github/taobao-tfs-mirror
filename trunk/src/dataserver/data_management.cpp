/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: data_management.cpp 746 2011-09-06 07:27:59Z daoan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   qushan<qushan@taobao.com>
 *      - modify 2009-03-27
 *   zongdai <zongdai@taobao.com>
 *      - modify 2010-04-23
 *
 */
#include "data_management.h"
#include "blockfile_manager.h"
#include "dataserver_define.h"
#include "visit_stat.h"
#include "erasure_code.h"
#include "task.h"
#include <Memory.hpp>

namespace tfs
{
  namespace dataserver
  {

    using namespace common;
    using namespace std;

    DataManagement::DataManagement()
    {

    }

    DataManagement::~DataManagement()
    {
    }

    /*void DataManagement::set_file_number(const uint64_t file_number)
    {
      file_number_ = file_number;
      TBSYS_LOG(INFO, "set file number. file number: %" PRI64_PREFIX "u\n", file_number_);
    }*/

    int DataManagement::init_block_files(const FileSystemParameter& fs_param)
    {
      int64_t time_start = tbsys::CTimeUtil::getTime();
      TBSYS_LOG(INFO, "block file load blocks begin. start time: %" PRI64_PREFIX "d\n", time_start);
      // just start up
      int ret = BlockFileManager::get_instance()->bootstrap(fs_param);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "blockfile manager boot fail! ret: %d\n", ret);
        return ret;
      }
      int64_t time_end = tbsys::CTimeUtil::getTime();
      TBSYS_LOG(INFO, "block file load blocks end. end time: %" PRI64_PREFIX "d. cost time: %" PRI64_PREFIX "d.",
          time_end, time_end - time_start);
      return TFS_SUCCESS;
    }

    void DataManagement::get_ds_filesystem_info(int32_t& block_count, int64_t& use_capacity, int64_t& total_capacity)
    {
      BlockFileManager::get_instance()->query_approx_block_count(block_count);
      BlockFileManager::get_instance()->query_space(use_capacity, total_capacity);
      return;
    }

    int DataManagement::get_all_logic_block(std::list<LogicBlock*>& logic_block_list)
    {
      return BlockFileManager::get_instance()->get_all_logic_block(logic_block_list);
    }

    int DataManagement::get_all_block_info(std::set<common::BlockInfo>& blocks)
    {
      return BlockFileManager::get_instance()->get_all_block_info(blocks);
    }

    int DataManagement::get_all_block_info(std::set<common::BlockInfoExt>& blocks_ext)
    {
      return BlockFileManager::get_instance()->get_all_block_info(blocks_ext);
    }

    int64_t DataManagement::get_all_logic_block_size()
    {
      return BlockFileManager::get_instance()->get_all_logic_block_size();
    }

    int DataManagement::create_file(const uint32_t block_id, uint64_t& file_id, uint64_t& file_number)
    {
      UNUSED(file_number);
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        TBSYS_LOG(ERROR, "blockid: %u is not exist.", block_id);
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      int ret = TFS_SUCCESS;
      if (0 == file_id)
      {
        ret = logic_block->open_write_file(file_id);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "try getfileid failed. blockid: %u, fileid: %" PRI64_PREFIX "u, ret: %d.", block_id,
              file_id, ret);
          return ret;
        }
      }
      else                      // update seq no over current one to avoid overwrite
      {
        logic_block->reset_seq_id(file_id);
      }

      /*data_file_mutex_.lock();
      file_number = ++file_number_;
      data_file_mutex_.unlock();*/

      //TBSYS_LOG(DEBUG, "open write file. blockid: %u, fileid: %" PRI64_PREFIX "u, filenumber: %" PRI64_PREFIX "u",
      //    block_id, file_id, file_number_);
      return TFS_SUCCESS;
    }


    int DataManagement::write_data(common::BlockInfo& block_info, WriteLease* lease, const int32_t remote_version, const common::WriteDataInfo& info,
            const char* data_buffer)
    {
      int32_t ret = (NULL != lease && INVALID_VERSION != remote_version && NULL != data_buffer) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        lease->update_last_time(Func::get_monotonic_time());
        TBSYS_LOG(DEBUG, "write data: block: %u, fileid: %"PRI64_PREFIX"u, lease_id: %"PRI64_PREFIX"u, remote_version: %d, offset: %d",
          info.block_id_, info.file_id_, info.file_number_, remote_version, info.offset_);
        //if the first fragment, check version
        if (0 == info.offset_)
        {
          LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(info.block_id_);
          ret = NULL == logic_block ? EXIT_NO_LOGICBLOCK_ERROR: TFS_SUCCESS;
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(INFO, "block: %u is not exist.", info.block_id_);
          }
          else
          {
            ret = logic_block->check_block_version(block_info, remote_version);
            if (TFS_SUCCESS != ret)
            {
              TBSYS_LOG(INFO, "block: %u version error. remote_version: %d, local_version: %d, ret: %d",
                info.block_id_, remote_version, block_info.version_, ret);
            }
          }
        }
        if (TFS_SUCCESS == ret)
        {
          int32_t write_len = lease->get_data_file().set_data(data_buffer, info.length_, info.offset_);
          ret = write_len != info.length_ ? EXIT_DATA_FILE_ERROR : TFS_SUCCESS;
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(INFO, "write data to datafile error, ret: %d, block: %u, fileid: %"PRI64_PREFIX"u, lease id: %"PRI64_PREFIX"u, length: %d:%d",
              ret, info.block_id_, info.file_id_, info.file_number_, write_len, info.length_);
          }
        }
      }
      return ret;
    }

    int DataManagement::close_write_file(const CloseFileInfo& colse_file_info, int32_t& write_file_size)
    {
      TIMER_START();
      uint32_t block_id = colse_file_info.block_id_;
      uint64_t file_id = colse_file_info.file_id_;
      uint64_t file_number = colse_file_info.file_number_;
      uint32_t crc = colse_file_info.crc_;
      TBSYS_LOG(DEBUG,
          "close write file, blockid: %u, fileid: %" PRI64_PREFIX "u, lease id: %" PRI64_PREFIX "u, crc: %u",
          block_id, file_id, file_number, crc);

      LeaseId lease_id(file_number, file_id, block_id);
      time_t now = Func::get_monotonic_time();
      Lease* lease = get_lease_manager().get(lease_id, now);
      lease->update_last_time(now);
      int32_t ret = NULL == lease ? EXIT_DATAFILE_EXPIRE_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(INFO, "lease is null mybe timeout or not found. block: %u, fileid: %" PRI64_PREFIX "u, lease id: %" PRI64_PREFIX "u",
            block_id, file_id, file_number);
      }
      if (TFS_SUCCESS == ret)
      {
        WriteLease* write_lease = dynamic_cast<WriteLease*>(lease);
        uint32_t datafile_crc = write_lease->get_data_file().get_crc();
        ret = crc != datafile_crc ? EXIT_DATA_FILE_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(INFO, "close file failed, crc error, block: %u, fileid: %"PRI64_PREFIX"u, lease id: %"PRI64_PREFIX"u, crc: %u:%u",
            block_id, file_id, file_number, datafile_crc, crc);
        }
        LogicBlock* logic_block = NULL;
        if (TFS_SUCCESS == ret)
        {
          write_file_size = write_lease->get_data_file().get_length();
          logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
          ret = NULL != logic_block ? TFS_SUCCESS : EXIT_NO_LOGICBLOCK_ERROR;
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(INFO, "close file failed, block: %u is not exist.", block_id);
          }
        }
        if (TFS_SUCCESS == ret)
        {
          ret = logic_block->close_write_file(file_id, write_lease->get_data_file(), datafile_crc);
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(INFO, "close file failed, flush data to disk error, ret: %d, block: %u", ret, block_id);
          }
        }
        if (TFS_SUCCESS == ret)
        {
          TIMER_END();
          if (TIMER_DURATION() > SYSPARAM_DATASERVER.max_io_warn_time_)
          {
            TBSYS_LOG(INFO, "write file cost time: blockid: %u, fileid: %" PRI64_PREFIX "u, cost time: %" PRI64_PREFIX "d",
                block_id, file_id, TIMER_DURATION());
          }
        }
      }
      get_lease_manager().put(lease);
      get_lease_manager().remove(lease_id);
      return ret;
    }

    int DataManagement::read_data(const uint32_t block_id, const uint64_t file_id, const int32_t read_offset, const int8_t flag,
        int32_t& real_read_len, char* tmp_data_buffer)
    {
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        TBSYS_LOG(ERROR, "block not exist, blockid: %u", block_id);
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      int64_t start = tbsys::CTimeUtil::getTime();
      int ret = logic_block->read_file(file_id, tmp_data_buffer, real_read_len, read_offset, flag);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "blockid: %u read data error, fileid: %" PRI64_PREFIX "u, size: %d, offset: %d, ret: %d",
            block_id, file_id, real_read_len, read_offset, ret);
        return ret;
      }

      TBSYS_LOG(DEBUG, "blockid: %u read data, fileid: %" PRI64_PREFIX "u, read size: %d, offset: %d", block_id,
          file_id, real_read_len, read_offset);

      int64_t end = tbsys::CTimeUtil::getTime();
      if (end - start > SYSPARAM_DATASERVER.max_io_warn_time_)
      {
        TBSYS_LOG(WARN, "read file cost time: blockid: %u, fileid: %" PRI64_PREFIX "u, cost time: %" PRI64_PREFIX "d",
            block_id, file_id, end - start);
      }

      return TFS_SUCCESS;
    }


    /**
     * @brief
     *
     * this interface implements readV2 function
     *
     * if offset == 0; returned data willbe "FileInfo + realdata"
     * else returned data willbe just realdata
     *
     * real data length is returned back by reference
     *
     * @return TFS_SUCCESS on success
     */
    int DataManagement::read_data_degrade(const uint32_t block_id, const uint64_t file_id,
        const int32_t read_offset, const int8_t flag, int32_t& real_read_len,
        char* tmp_data_buffer, const FamilyMemberInfoExt& family_info)
    {
      int32_t family_aid_info = family_info.family_aid_info_;
      const std::vector<std::pair<uint32_t, uint64_t> >& family_members = family_info.members_;
      const int32_t data_num = GET_DATA_MEMBER_NUM(family_aid_info);
      const int32_t check_num = GET_CHECK_MEMBER_NUM(family_aid_info);
      const int32_t member_num = data_num + check_num;

      ErasureCode decoder;
      char* data[member_num];
      int erased[member_num];
      memset(data, 0, member_num * sizeof(char*));
      memset(erased, 0, member_num * sizeof(int));

      int ret = Task::check_reinstate(family_info, erased);
      if (TFS_SUCCESS != ret)
      {
        return ret;
      }

      // get target block index
      int32_t target_block_idx = -1;
      for (int32_t i = 0; i < member_num; i++)
      {
        if (family_members[i].first == block_id)
        {
          target_block_idx = i;
          break;
        }
      }

      if (target_block_idx < 0)
      {
        ret = EXIT_BLOCK_NOT_PRESENT;
        return ret;
      }

      for (int32_t i = 0; i < member_num; i++)
      {
        TBSYS_LOG(DEBUG, "block: %u, server: %s", family_info.members_[i].first,
            tbsys::CNetUtil::addrToString(family_info.members_[i].second).c_str());
      }

      // config encoder parameter, alloc buffer
      ret = decoder.config(data_num, check_num, erased);
      if (TFS_SUCCESS == ret)
      {
        for (int32_t i = 0; i < member_num; i++)
        {
          data[i] = new (std::nothrow) char[MAX_READ_SIZE];
          assert(NULL != data[i]);
        }
        decoder.bind(data, member_num, MAX_READ_SIZE);
      }

      // read index from parity block
      char* target_index = NULL;
      int32_t length = 0;
      if (TFS_SUCCESS == ret)
      {
        for (int32_t i = data_num; i < member_num; i++)
        {
          if (NODE_ALIVE != erased[i])
          {
            continue;
          }
          uint32_t blockid = family_members[i].first;
          uint64_t serverid = family_members[i].second;
          ret = Task::read_raw_index(serverid, blockid, READ_PARITY_INDEX, block_id, target_index, length);
          if (TFS_SUCCESS == ret)
          {
            break;
          }
        }
      }

      // find file in block
      int32_t file_pos = 0;
      int32_t file_size = 0;
      int64_t file_offset = 0;
      RawMeta* meta_info = NULL;
      if (TFS_SUCCESS == ret)
      {
        ret = IndexHandle::hash_find(target_index, length, file_id, file_pos);
        if (TFS_SUCCESS == ret)
        {
          meta_info = reinterpret_cast<RawMeta*>(target_index + file_pos);
          file_size = meta_info->get_size();
          file_offset = meta_info->get_offset();
          if (file_size - read_offset < real_read_len)
          {
            real_read_len = file_size - read_offset;
          }
        }
      }
      tbsys::gDeleteA(target_index);

      // calculate decode offset length, real data offset and length
      int32_t decode_offset = 0;
      int32_t decode_size = 0;
      int32_t offset_in_buffer = 0;
      int32_t size_in_buffer = real_read_len;
      if (TFS_SUCCESS == ret && 0 != real_read_len)
      {
        int32_t decode_unit = ErasureCode::ws_ * ErasureCode::ps_;
        int32_t start = file_offset + read_offset;
        int32_t end = start + real_read_len;

        offset_in_buffer = start % decode_unit;
        if (start % decode_unit != 0)
        {
          start = (start / decode_unit) * decode_unit;
        }

        if (end % decode_unit != 0)
        {
          end = (end / decode_unit + 1) * decode_unit;
        }

        decode_offset = start;
        decode_size = end - start;

        TBSYS_LOG(DEBUG, "degrade read start: %d, end: %d, size: %d", start, end, end-start);
      }

      if (TFS_SUCCESS == ret && 0 != real_read_len)
      {
        char* data_buffer = new (std::nothrow) char[decode_size];
        assert(NULL != data_buffer);
        int32_t decode_idx = 0;
        int32_t decode_len = 0;
        do
        {
          decode_len = MAX_READ_SIZE;
          if (decode_size - decode_idx < MAX_READ_SIZE)
          {
            decode_len = decode_size - decode_idx;
          }

          for (int32_t i = 0; i < member_num; i++)
          {
            if (NODE_ALIVE != erased[i])
            {
              continue;
            }

            memset(data[i], 0, decode_len);
            uint32_t blockid = family_members[i].first;
            uint64_t serverid = family_members[i].second;
            int32_t data_file_size = 0;
            ret = Task::read_raw_data(serverid, blockid, data[i],
                decode_len, decode_offset + decode_idx, data_file_size);
            if (TFS_SUCCESS != ret)
            {
              break;
            }
          }

          if (TFS_SUCCESS == ret)
          {
            ret = decoder.decode(decode_len);
            if (TFS_SUCCESS == ret)
            {
              memcpy(data_buffer + decode_idx, data[target_block_idx], decode_len);
              decode_idx += decode_len;
            }
          }

          if (TFS_SUCCESS != ret)
          {
            break;
          }
        } while (decode_idx < decode_size);

        // extra data from data_buffer
        if (TFS_SUCCESS == ret)
        {
          memcpy(tmp_data_buffer, data_buffer + offset_in_buffer, size_in_buffer);
        }

        tbsys::gDeleteA(data_buffer);
      }

      for (int32_t i = 0; i < member_num; i++)
      {
        tbsys::gDeleteA(data[i]);
      }

      // check file status
      if (TFS_SUCCESS == ret)
      {
        if (0 == read_offset)      // first fragment
        {
          FileInfo* finfo = reinterpret_cast<FileInfo*> (tmp_data_buffer);
          if (FILEINFO_SIZE == real_read_len)  // degrade stat
          {
            if ((0 == finfo->id_) || (finfo->id_ != file_id ) ||
                ((finfo->flag_ & (FI_DELETED | FI_INVALID | FI_CONCEAL) != 0) && (NORMAL_STAT == flag)))
            {
              ret = EXIT_FILE_STATUS_ERROR;
            }
            else
            {
              // minus the header(FileInfo)
              finfo->size_ -= sizeof(FileInfo);
              finfo->flag_ = LogicBlock::get_real_flag(*meta_info, finfo->flag_);
            }
          }
          else                     // degrade read
          {
            if ((finfo->id_ != file_id) ||
                ((finfo->flag_ & (FI_DELETED | FI_INVALID | FI_CONCEAL) != 0) &&
                 (READ_DATA_OPTION_FLAG_NORMAL == flag)))
            {
              ret = EXIT_FILE_INFO_ERROR;
            }
          }
        }
      }

      return ret;
    }

    int DataManagement::read_raw_data(const uint32_t block_id, const int32_t read_offset, int32_t& real_read_len,
        char* tmp_data_buffer, int32_t& data_file_size)
    {
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        TBSYS_LOG(ERROR, "block not exist, blockid: %u", block_id);
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      int ret = logic_block->read_raw_data(tmp_data_buffer, real_read_len, read_offset);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "blockid: %u read data batch error, offset: %d, rlen: %d, ret: %d", block_id, read_offset,
            real_read_len, ret);
        return ret;
      }
      data_file_size = logic_block->get_data_file_size();

      return TFS_SUCCESS;
    }

    int DataManagement::read_file_info(const uint32_t block_id, const uint64_t file_id, const int32_t mode,
        FileInfo& finfo)
    {
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        TBSYS_LOG(ERROR, "blockid: %u is not exist.", block_id);
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      int ret = logic_block->read_file_info(file_id, finfo, mode);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "read file info, blockid: %u, fileid: %"PRI64_PREFIX"u, mode: %d",
            block_id, file_id, mode);
        return ret;
      }

      // minus the header(FileInfo)
      finfo.size_ -= sizeof(FileInfo);
      return TFS_SUCCESS;
    }

    int DataManagement::rename_file(const uint32_t block_id, const uint64_t file_id, const uint64_t new_file_id)
    {
      // return if fileid is same
      if (file_id == new_file_id)
      {
        TBSYS_LOG(WARN, "rename file fail. blockid:%u, fileid: %" PRI64_PREFIX "u, newfileid: %" PRI64_PREFIX "u",
            block_id, file_id, new_file_id);
        return EXIT_RENAME_FILEID_SAME_ERROR;
      }

      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        TBSYS_LOG(ERROR, "blockid: %u is not exist.", block_id);
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      int ret = logic_block->rename_file(file_id, new_file_id);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR,
            "modfileid fail, blockid: %u, fileid: %" PRI64_PREFIX "u, newfileid: %" PRI64_PREFIX "u, ret: %d",
            block_id, file_id, new_file_id, ret);
        return ret;
      }

      return TFS_SUCCESS;
    }

    int DataManagement::unlink_file(common::BlockInfo& info, int64_t& file_size, const uint32_t block_id, const uint64_t file_id, const int32_t action, const int32_t remote_version, int32_t& unlink_flag)
    {
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      int32_t ret = NULL == logic_block ? EXIT_NO_LOGICBLOCK_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(INFO, "block: %u is not exist.", block_id);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = logic_block->check_block_version(info, remote_version);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(INFO, "block: %u version error. remote_version: %d, local_version: %d, ret: %d",
             block_id, remote_version, info.version_, ret);
        }
      }
      if (TFS_SUCCESS == ret)
      {
        ret = logic_block->unlink_file(file_id, action, file_size, unlink_flag);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "del file fail, blockid: %u, fileid: %" PRI64_PREFIX "u, ret: %d", block_id, file_id, ret);
        }
      }
      return ret;
    }

    int DataManagement::unlink_file_parity(const uint32_t block_id, const uint32_t index_id, const uint64_t file_id, const int32_t action, int64_t& file_size)
    {
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      int32_t ret = NULL == logic_block ? EXIT_NO_LOGICBLOCK_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(INFO, "block: %u is not exist.", block_id);
      }
      else
      {
        ret = logic_block->unlink_file_parity(index_id, file_id, action, file_size);
      }

      TBSYS_LOG(DEBUG, "unlink file parity, blockid: %u, indexid: %u, %"PRI64_PREFIX"u, action: %d",
          block_id, index_id, file_id, action);

      return ret;
    }

    int DataManagement::batch_new_block(const VUINT32* new_blocks)
    {
      int ret = TFS_SUCCESS;
      if (NULL != new_blocks)
      {
        for (uint32_t i = 0; i < new_blocks->size(); ++i)
        {
          TBSYS_LOG(INFO, "new block: %u\n", new_blocks->at(i));
          uint32_t physic_block_id = 0;
          ret = BlockFileManager::get_instance()->new_block(new_blocks->at(i), physic_block_id);
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "new block fail, blockid: %u, ret: %d", new_blocks->at(i), ret);
            return ret;
          }
          else
          {
            TBSYS_LOG(INFO, "new block successful, blockid: %u, phyical blockid: %u", new_blocks->at(i),
                physic_block_id);
          }
        }
      }

      return ret;
    }

    int DataManagement::batch_remove_block(const VUINT32* remove_blocks)
    {
      int ret = TFS_SUCCESS;
      if (NULL != remove_blocks)
      {
        for (uint32_t i = 0; i < remove_blocks->size(); ++i)
        {
          TBSYS_LOG(INFO, "remove block: %u\n", remove_blocks->at(i));
          ret = BlockFileManager::get_instance()->del_block(remove_blocks->at(i));
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "remove block error, blockid: %u, ret: %d", remove_blocks->at(i), ret);
            return ret;
          }
          else
          {
            TBSYS_LOG(INFO, "remove block successful, blockid: %u", remove_blocks->at(i));
          }
        }
      }

      return ret;
    }

    int DataManagement::query_bit_map(const int32_t query_type, char** tmp_data_buffer, int32_t& bit_map_len,
        int32_t& set_count)
    {
      // the caller should release the tmp_data_buffer memory
      if (NORMAL_BIT_MAP == query_type)
      {
        BlockFileManager::get_instance()->query_bit_map(tmp_data_buffer, bit_map_len, set_count, C_ALLOCATE_BLOCK);
      }
      else
      {
        BlockFileManager::get_instance()->query_bit_map(tmp_data_buffer, bit_map_len, set_count, C_ERROR_BLOCK);
      }

      return TFS_SUCCESS;
    }

    int DataManagement::query_block_status(const int32_t query_type, VUINT& block_ids, std::map<uint32_t, std::vector<
        uint32_t> >& logic_2_physic_blocks, std::map<uint32_t, BlockInfo*>& block_2_info)
    {
      std::list<LogicBlock*> logic_blocks;
      std::list<LogicBlock*>::iterator lit;

      BlockFileManager::get_instance()->get_logic_block_ids(block_ids);
      BlockFileManager::get_instance()->get_all_logic_block(logic_blocks);

      if (query_type & LB_PAIRS) // logick block ==> physic block list
      {
        std::list<PhysicalBlock*>* phy_blocks;
        std::list<PhysicalBlock*>::iterator pit;

        for (lit = logic_blocks.begin(); lit != logic_blocks.end(); ++lit)
        {
          if (*lit)
          {
            TBSYS_LOG(DEBUG, "query block status, query type: %d, blockid: %u\n", query_type,
                (*lit)->get_logic_block_id());
            phy_blocks = (*lit)->get_physic_block_list();
            std::vector < uint32_t > phy_block_ids;
            for (pit = phy_blocks->begin(); pit != phy_blocks->end(); ++pit)
            {
              phy_block_ids.push_back((*pit)->get_physic_block_id());
            }

            logic_2_physic_blocks.insert(std::map<uint32_t, std::vector<uint32_t> >::value_type(
                (*lit)->get_logic_block_id(), phy_block_ids));
          }
        }
      }

      if (query_type & LB_INFOS) // logic block info
      {
        for (lit = logic_blocks.begin(); lit != logic_blocks.end(); ++lit)
        {
          if (*lit)
          {
            block_2_info.insert(std::map<uint32_t, BlockInfo*>::value_type((*lit)->get_logic_block_id(),
                (*lit)->get_block_info()));
          }
        }
      }

      return TFS_SUCCESS;
    }

    int DataManagement::get_block_info(const uint32_t block_id, BlockInfo*& blk, int32_t& visit_count)
    {
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        TBSYS_LOG(ERROR, "blockid: %u is not exist.", block_id);
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      blk = logic_block->get_block_info();
      visit_count = logic_block->get_visit_count();
      return TFS_SUCCESS;
    }

    int DataManagement::get_visit_sorted_blockids(std::vector<LogicBlock*>& block_ptrs)
    {
      std::list<LogicBlock*> logic_blocks;
      BlockFileManager::get_instance()->get_all_logic_block(logic_blocks);

      for (std::list<LogicBlock*>::iterator lit = logic_blocks.begin(); lit != logic_blocks.end(); ++lit)
      {
        block_ptrs.push_back(*lit);
      }

      sort(block_ptrs.begin(), block_ptrs.end(), visit_count_sort());
      return TFS_SUCCESS;
    }

    int DataManagement::get_block_file_list(const uint32_t block_id, std::vector<FileInfo>& fileinfos)
    {
      TBSYS_LOG(INFO, "getfilelist. blockid: %u\n", block_id);
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        TBSYS_LOG(ERROR, "blockid: %u is not exist.", block_id);
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      logic_block->get_file_infos(fileinfos);
      TBSYS_LOG(INFO, "getfilelist. blockid: %u, filenum: %zd\n", block_id, fileinfos.size());
      return TFS_SUCCESS;
    }

    int DataManagement::get_block_meta_info(const uint32_t block_id, RawMetaVec& meta_list)
    {
      TBSYS_LOG(INFO, "get raw meta list. blockid: %u\n", block_id);
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        TBSYS_LOG(ERROR, "blockid: %u is not exist.", block_id);
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      logic_block->get_meta_infos(meta_list);
      TBSYS_LOG(INFO, "get meta list. blockid: %u, filenum: %zd\n", block_id, meta_list.size());
      return TFS_SUCCESS;
    }

    int DataManagement::reset_block_version(const uint32_t block_id)
    {
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        TBSYS_LOG(ERROR, "blockid: %u is not exist.", block_id);
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      logic_block->reset_block_version();
      TBSYS_LOG(INFO, "reset block version: %u\n", block_id);
      return TFS_SUCCESS;
    }

    int DataManagement::new_single_block(const uint32_t block_id, const BlockType type)
    {
      int ret = TFS_SUCCESS;
      // delete if exist
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL != logic_block)
      {
        TBSYS_LOG(INFO, "block already exist, blockid: %u. first del it", block_id);
        ret = BlockFileManager::get_instance()->del_block(block_id);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "block already exist, blockid: %u. block delete fail. ret: %d", block_id, ret);
          return ret;
        }
      }

      uint32_t physic_block_id = 0;
      ret = BlockFileManager::get_instance()->new_block(block_id, physic_block_id, type);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "block create error, blockid: %u, ret: %d", block_id, ret);
        return ret;
      }
      return TFS_SUCCESS;
    }

    int DataManagement::del_single_block(const uint32_t block_id)
    {
      TBSYS_LOG(INFO, "remove single block, blockid: %u", block_id);
      return BlockFileManager::get_instance()->del_block(block_id);
    }

    int DataManagement::get_block_curr_size(const uint32_t block_id, int32_t& size)
    {
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        TBSYS_LOG(ERROR, "blockid: %u is not exist.", block_id);
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      size = logic_block->get_data_file_size();
      TBSYS_LOG(DEBUG, "blockid: %u data file size: %d\n", block_id, size);
      return TFS_SUCCESS;
    }

    int DataManagement::write_raw_data(const uint32_t block_id, const int32_t data_offset, const int32_t msg_len,
        const char* data_buffer)
    {
      //zero length
      if (0 == msg_len)
      {
        return TFS_SUCCESS;
      }
      int ret = TFS_SUCCESS;
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        TBSYS_LOG(ERROR, "blockid: %u is not exist.", block_id);
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      ret = logic_block->write_raw_data(data_buffer, msg_len, data_offset);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "write data batch error. blockid: %u, ret: %d", block_id, ret);
        return ret;
      }

      return TFS_SUCCESS;
    }

    int DataManagement::batch_write_meta(const uint32_t block_id, const BlockInfo* blk, const RawMetaVec* meta_list, const int32_t remove_flag)
    {
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        TBSYS_LOG(ERROR, "blockid: %u is not exist.", block_id);
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      VersionStep step = remove_flag? VERSION_INC_STEP_NONE: VERSION_INC_STEP_REPLICATE;
      int ret = logic_block->batch_write_meta(blk, meta_list, step);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "blockid: %u batch write meta error.", block_id);
        return ret;
      }

      return TFS_SUCCESS;
    }

    int DataManagement::write_raw_index(const uint32_t block_id, const int64_t family_id,
        const RawIndexOp index_op, const RawIndexVec* index_vec)
    {
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        TBSYS_LOG(ERROR, "blockid: %u is not exist.", block_id);
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      int ret = logic_block->write_raw_index(family_id, index_op, index_vec);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "blockid: %u write raw index error, ret: %d", block_id, ret);
      }

      return ret;
    }

    int DataManagement::read_raw_index(const uint32_t block_id, const common::RawIndexOp index_op,
        const uint32_t index_id, char* & buf, int32_t& size)
    {
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        TBSYS_LOG(ERROR, "blockid: %u is not exist.", block_id);
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      int ret = logic_block->read_raw_index(index_op, index_id, buf, size);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "blockid: %u read raw index error, ret: %d", block_id, ret);
      }

      return ret;
    }

    int DataManagement::expire_blocks(const VUINT32& expire_blocks,
        set<BlockInfoExt>& clear_blocks, set<BlockInfoExt>& delete_blocks)
    {
      TBSYS_LOG(INFO, "expire block list size: %u\n", static_cast<uint32_t>(expire_blocks.size()));
      for (uint32_t i = 0; i < expire_blocks.size(); i++)
      {
        TBSYS_LOG(DEBUG, "expiring block %u", expire_blocks[i]);
        LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(expire_blocks[i]);
        if (NULL != logic_block)
        {
          if (0 == logic_block->get_prefix_flag())
          {
            logic_block->set_family_id(0);
            BlockInfoExt block_ext;
            block_ext.block_info_ = *(logic_block->get_block_info());
            block_ext.family_id_ = 0;
            clear_blocks.insert(block_ext);
          }
          else
          {
            int ret = BlockFileManager::get_instance()->del_block(expire_blocks[i]);
            if (TFS_SUCCESS == ret)
            {
              BlockInfoExt block_ext;
              block_ext.block_info_.block_id_ = expire_blocks[i];
              delete_blocks.insert(block_ext);
            }
          }
        }
        else
        {
          BlockInfoExt block_ext;
          block_ext.block_info_.block_id_ = expire_blocks[i];
          delete_blocks.insert(block_ext);
        }
      }
      return TFS_SUCCESS;
    }

    int DataManagement::add_new_expire_block(const VUINT32* expire_block_ids, const VUINT32* remove_block_ids, const VUINT32* new_block_ids)
    {
      // delete expire block
      if (NULL != expire_block_ids)
      {
        TBSYS_LOG(INFO, "expire block list size: %u\n", static_cast<uint32_t>(expire_block_ids->size()));
        for (uint32_t i = 0; i < expire_block_ids->size(); ++i)
        {
          TBSYS_LOG(INFO, "expire(delete) block. blockid: %u\n", expire_block_ids->at(i));
          BlockFileManager::get_instance()->del_block(expire_block_ids->at(i));
        }
      }

      // delete remove block
      if (NULL != remove_block_ids)
      {
        TBSYS_LOG(INFO, "remove block list size: %u\n", static_cast<uint32_t>(remove_block_ids->size()));
        for (uint32_t i = 0; i < remove_block_ids->size(); ++i)
        {
          TBSYS_LOG(INFO, "delete block. blockid: %u\n", remove_block_ids->at(i));
          BlockFileManager::get_instance()->del_block(remove_block_ids->at(i));
        }
      }

      // new
      if (NULL != new_block_ids)
      {
        TBSYS_LOG(INFO, "new block list size: %u\n", static_cast<uint32_t>(new_block_ids->size()));
        for (uint32_t i = 0; i < new_block_ids->size(); ++i)
        {
          TBSYS_LOG(INFO, "new block. blockid: %u\n", new_block_ids->at(i));
          uint32_t physical_block_id = 0;
          BlockFileManager::get_instance()->new_block(new_block_ids->at(i), physical_block_id);
        }
      }

      return EXIT_SUCCESS;
    }

    int DataManagement::create_file_id_(uint64_t& file_id, const uint32_t block_id)
    {
      int32_t ret = INVALID_BLOCK_ID == block_id ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        LogicBlock* block = BlockFileManager::get_instance()->get_logic_block(block_id);
        ret = NULL != block ? TFS_SUCCESS : EXIT_NO_LOGICBLOCK_ERROR;
        if (TFS_SUCCESS != ret)
          TBSYS_LOG(INFO, "block: %u is not exist.", block_id);
        if (TFS_SUCCESS == ret)
        {
          if (0 == file_id)
          {
            ret = block->open_write_file(file_id);
            if (TFS_SUCCESS != ret)
              TBSYS_LOG(INFO, "create file id failed. blockid: %u, ret: %d.", block_id, ret);
          }
          else
          {
            //update seq no over current one to avoid overwrite
            block->reset_seq_id(file_id);
          }
        }
      }
      TBSYS_LOG(DEBUG, "create file id %s. blockid: %u, fileid: %" PRI64_PREFIX "u",
          TFS_SUCCESS == ret ? "successful" : "failed",block_id, file_id);
      return ret;
    }
  }
}
