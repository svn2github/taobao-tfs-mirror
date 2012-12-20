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
 *   linqing <linqing.zyd@taobao.com>
 *      - modify 2012-12-12
 *
 */
#include "data_management.h"
#include "ds_define.h"
#include "visit_stat.h"
#include <Memory.hpp>

namespace tfs
{
  namespace dataserver
  {

    using namespace common;
    using namespace std;

    DataManagement::DataManagement() :
      file_number_(0), last_gc_data_file_time_(0)
    {
    }

    DataManagement::~DataManagement()
    {
    }

    void DataManagement::set_file_number(const uint64_t file_number)
    {
      file_number_ = file_number;
      TBSYS_LOG(INFO, "set file number. file number: %" PRI64_PREFIX "u\n", file_number_);
    }

    int DataManagement::init_block_files(const FileSystemParameter& fs_param)
    {
      return DataManager::instance().init_block_files(fs_param);
    }

    void DataManagement::get_ds_filesystem_info(int32_t& block_count, int64_t& use_capacity, int64_t& total_capacity)
    {
      return DataManager::instance().get_ds_filesystem_info(block_count, use_capacity, total_capacity);
    }

    // this interface won't be called
    int DataManagement::get_all_logic_block(std::list<LogicBlock*>& logic_block_list)
    {
      UNUSED(logic_block_list);
      return EXIT_NOT_SUPPORT_ERROR;
    }

    int DataManagement::get_all_block_info(std::set<common::BlockInfo>& blocks)
    {
      return DataManager::instance().get_all_block_info(blocks);
    }

    int64_t DataManagement::get_all_logic_block_size()
    {
      return DataManager::instance().get_all_logic_block_size();
    }

    int DataManagement::create_file(const uint32_t block_id, uint64_t& file_id, uint64_t& file_number)
    {
      int ret = TFS_SUCCESS;
      if (0 == file_id)
      {
        ret = DataManager::instance().create_file_id(block_id, file_id);
      }
      else  // update seq no over current one to avoid overwrite
      {
        // TODO reset seq id
      }

      data_file_mutex_.lock();
      file_number = ++file_number_;
      data_file_mutex_.unlock();

      TBSYS_LOG(DEBUG, "open write file. blockid: %u, fileid: %" PRI64_PREFIX "u, filenumber: %" PRI64_PREFIX "u",
          block_id, file_id, file_number_);
      return TFS_SUCCESS;
    }

    int DataManagement::write_data(const WriteDataInfo& write_info, const int32_t lease_id, int32_t& version,
        const char* data_buffer, UpdateBlockType& repair)
    {
      UNUSED(repair);
      TBSYS_LOG(DEBUG,
          "write data. blockid: %u, fileid: %" PRI64_PREFIX "u, filenumber: %" PRI64_PREFIX "u, lease: %d",
          write_info.block_id_, write_info.file_id_, write_info.file_number_, lease_id);
      //if the first fragment, check version
      int ret = TFS_SUCCESS;
      if (0 == write_info.offset_)
      {
        BlockInfoV2 none;  // not used in old version
        ret = DataManager::instance().check_block_version(write_info.block_id_, version, none);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(DEBUG, "check_block_version error. blockid: %u, ret: %d", write_info.block_id_, ret);
          return ret;
        }
      }

      // write data to DataFile first
      data_file_mutex_.lock();
      DataFileMapIter bit = data_file_map_.find(write_info.file_number_);
      DataFile* datafile = NULL;
      if (bit != data_file_map_.end())
      {
        datafile = bit->second;
      }
      else                      // not found
      {
        // control datafile size
        if (data_file_map_.size() >= static_cast<uint32_t> (SYSPARAM_DATASERVER.max_datafile_nums_))
        {
          TBSYS_LOG(ERROR, "blockid: %u, datafile nums: %zd is large than default.", write_info.block_id_,
              data_file_map_.size());
          data_file_mutex_.unlock();
          return EXIT_DATAFILE_OVERLOAD;
        }

        datafile = new DataFile(write_info.file_number_);
        data_file_map_.insert(DataFileMap::value_type(write_info.file_number_, datafile));
      }

      if (NULL == datafile)
      {
        TBSYS_LOG(ERROR, "datafile is null. blockid: %u, fileid: %" PRI64_PREFIX "u, filenumber: %" PRI64_PREFIX "u",
            write_info.block_id_, write_info.file_id_, write_info.file_number_);
        data_file_mutex_.unlock();
        return EXIT_DATA_FILE_ERROR;
      }
      datafile->set_last_update();
      data_file_mutex_.unlock();

      // write to datafile
      FileInfoInDiskExt none;  // not used in old version
      int32_t write_len = datafile->pwrite(none, data_buffer, write_info.length_, write_info.offset_);
      if (write_len != write_info.length_)
      {
        TBSYS_LOG(
            ERROR,
            "Datafile write error. blockid: %u, fileid: %" PRI64_PREFIX "u, filenumber: %" PRI64_PREFIX "u, req writelen: %d, actual writelen: %d",
            write_info.block_id_, write_info.file_id_, write_info.file_number_, write_info.length_, write_len);
        // clean dirty data
        erase_data_file(write_info.file_number_);
        return EXIT_DATA_FILE_ERROR;
      }

      return TFS_SUCCESS;
    }

    int DataManagement::close_write_file(const CloseFileInfo& colse_file_info, int32_t& write_file_size)
    {
      uint32_t block_id = colse_file_info.block_id_;
      uint64_t file_id = colse_file_info.file_id_;
      uint64_t file_number = colse_file_info.file_number_;
      uint32_t crc = colse_file_info.crc_;
      TBSYS_LOG(DEBUG,
          "close write file, blockid: %u, fileid: %" PRI64_PREFIX "u, filenumber: %" PRI64_PREFIX "u, crc: %u",
          block_id, file_id, file_number, crc);

      //find datafile
      DataFile* datafile = NULL;
      data_file_mutex_.lock();
      DataFileMapIter bit = data_file_map_.find(file_number);
      if (bit != data_file_map_.end())
      {
        datafile = bit->second;
      }

      //lease expire
      if (NULL == datafile)
      {
        TBSYS_LOG(ERROR, "Datafile is null. blockid: %u, fileid: %" PRI64_PREFIX "u, filenumber: %" PRI64_PREFIX "u",
            block_id, file_id, file_number);
        data_file_mutex_.unlock();
        return EXIT_DATAFILE_EXPIRE_ERROR;
      }
      datafile->set_last_update();
      datafile->add_ref();
      data_file_mutex_.unlock();

      //compare crc
      uint32_t datafile_crc = datafile->crc();
      if (crc != datafile_crc)
      {
        TBSYS_LOG(
            ERROR,
            "Datafile crc error. blockid: %u, fileid: %" PRI64_PREFIX "u, filenumber: %" PRI64_PREFIX "u, local crc: %u, msg crc: %u",
            block_id, file_id, file_number, datafile_crc, crc);
        datafile->sub_ref();
        erase_data_file(file_number);
        return EXIT_DATA_FILE_ERROR;
      }

      write_file_size = datafile->length();
      TIMER_START();
      int ret = DataManager::instance().close_file(block_id, file_id, *datafile);
      if (TFS_SUCCESS != ret)
      {
        datafile->sub_ref();
        erase_data_file(file_number);
        return ret;
      }

      TIMER_END();
      if (TIMER_DURATION() > SYSPARAM_DATASERVER.max_io_warn_time_)
      {
        TBSYS_LOG(WARN, "write file cost time: blockid: %u, fileid: %" PRI64_PREFIX "u, cost time: %" PRI64_PREFIX "d",
            block_id, file_id, TIMER_DURATION());
      }

      // success, gc datafile
      // close tmp file, release opened file handle
      // datafile , bit->second point to same thing, once delete
      // bit->second, datafile will be obseleted immediately.
      datafile->sub_ref();
      erase_data_file(file_number);
      return TFS_SUCCESS;
    }

    int DataManagement::erase_data_file(const uint64_t file_number)
    {
      data_file_mutex_.lock();
      DataFileMapIter bit = data_file_map_.find(file_number);
      if (bit != data_file_map_.end() && NULL != bit->second && bit->second->get_ref() <= 0)
      {
        tbsys::gDelete(bit->second);
        data_file_map_.erase(bit);
      }
      data_file_mutex_.unlock();
      return TFS_SUCCESS;
    }

    int DataManagement::read_data(const uint32_t block_id, const uint64_t file_id, const int32_t read_offset, const int8_t flag,
        int32_t& real_read_len, char* tmp_data_buffer)
    {
      int64_t start = tbsys::CTimeUtil::getTime();
      int ret = DataManager::instance().read_file(block_id, file_id, tmp_data_buffer, real_read_len, read_offset, flag);
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

    int DataManagement::read_file_info(const uint32_t block_id, const uint64_t file_id, const int32_t mode,
        FileInfo& finfo)
    {
      FileInfoV2 finfo_v2;
      int ret = DataManager::instance().stat_file(block_id, file_id, mode, finfo_v2);
      if (TFS_SUCCESS == ret)
      {
        // transform FileInfoV2 to FileInfo for compatible
        finfo.id_ = finfo_v2.id_;
        finfo.offset_ = finfo_v2.offset_;
        finfo.size_ = finfo_v2.size_ - FILEINFO_EXT_SIZE;
        finfo.usize_ = finfo_v2.size_ - FILEINFO_EXT_SIZE;
        finfo.modify_time_ = finfo_v2.modify_time_;
        finfo.create_time_ = finfo_v2.create_time_;
        finfo.flag_ = finfo_v2.status_;
     }

      return ret;
    }

    int DataManagement::rename_file(const uint32_t block_id, const uint64_t file_id, const uint64_t new_file_id)
    {
      UNUSED(block_id);
      UNUSED(file_id);
      UNUSED(new_file_id);
      return EXIT_NOT_SUPPORT_ERROR;
    }

    int DataManagement::unlink_file(const uint32_t block_id, const uint64_t file_id, const int32_t action, int64_t& file_size)
    {
      BlockInfoV2 none;
      // -1 denotes not check version
      return DataManager::instance().unlink_file(block_id, file_id, action, -1, file_size, none);
    }

    int DataManagement::batch_new_block(const VUINT32* new_blocks)
    {
      int ret = (NULL == new_blocks) ? EXIT_PARAMETER_ERROR: TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        // if one fail, return error
        VUINT32::const_iterator iter = new_blocks->begin();
        for ( ; (TFS_SUCCESS == ret) && (iter != new_blocks->end()); iter++)
        {
          ret = DataManager::instance().new_block(*iter, false);
        }
      }
      return ret;
    }

    int DataManagement::batch_remove_block(const VUINT32* remove_blocks)
    {
      int ret = (NULL == remove_blocks) ? EXIT_PARAMETER_ERROR: TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        // if one fail, return error
        VUINT32::const_iterator iter = remove_blocks->begin();
        for ( ; (TFS_SUCCESS == ret) && (iter != remove_blocks->end()); iter++)
        {
          ret = DataManager::instance().remove_block(*iter, false);
        }
      }
      return ret;
    }

    int DataManagement::query_bit_map(const int32_t query_type, char** tmp_data_buffer, int32_t& bit_map_len,
        int32_t& set_count)
    {
      UNUSED(query_type);
      UNUSED(tmp_data_buffer);
      UNUSED(bit_map_len);
      UNUSED(set_count);
      return EXIT_NOT_SUPPORT_ERROR;
    }

    int DataManagement::query_block_status(const int32_t query_type, VUINT& block_ids, std::map<uint32_t, std::vector<
        uint32_t> >& logic_2_physic_blocks, std::map<uint32_t, BlockInfo*>& block_2_info)
    {
      UNUSED(query_type);
      UNUSED(block_ids);
      UNUSED(logic_2_physic_blocks);
      UNUSED(block_2_info);
      return EXIT_NOT_SUPPORT_ERROR;

      /*
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
      */

    }

    int DataManagement::get_block_info(const uint32_t block_id, BlockInfo& blk, int32_t& visit_count)
    {
      visit_count = 0;  // TODO
      BlockInfoV2 blk_v2;
      int ret = DataManager::instance().get_block_info(block_id, blk_v2);
      if (TFS_SUCCESS == ret)
      {
        blk.block_id_ = blk_v2.block_id_;
        blk.version_ = blk_v2.version_;
        blk.file_count_ = blk_v2.file_count_;
        blk.size_ = blk_v2.size_;
        blk.del_file_count_ = blk_v2.del_file_count_;
        blk.del_size_ = blk_v2.del_size_;
        // blk.seq_no_ = blk_v2.seq_no_;
      }
      return ret;
    }

    int DataManagement::get_visit_sorted_blockids(std::vector<LogicBlock*>& block_ptrs)
    {
      UNUSED(block_ptrs);
      return EXIT_NOT_SUPPORT_ERROR;

      /*
      std::list<LogicBlock*> logic_blocks;
      BlockFileManager::get_instance()->get_all_logic_block(logic_blocks);

      for (std::list<LogicBlock*>::iterator lit = logic_blocks.begin(); lit != logic_blocks.end(); ++lit)
      {
        block_ptrs.push_back(*lit);
      }

      sort(block_ptrs.begin(), block_ptrs.end(), visit_count_sort());
      return TFS_SUCCESS;

      */

    }

    int DataManagement::get_block_file_list(const uint32_t block_id, std::vector<FileInfo>& fileinfos)
    {
      UNUSED(block_id);
      UNUSED(fileinfos);
      return EXIT_NOT_SUPPORT_ERROR;

      /*
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
      */
    }

    int DataManagement::get_block_meta_info(const uint32_t block_id, RawMetaVec& meta_list)
    {
      UNUSED(block_id);
      UNUSED(meta_list);
      return EXIT_NOT_SUPPORT_ERROR;

      /*
      TBSYS_LOG(INFO, "get raw meta list. blockid: %u\n", block_id);
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        TBSYS_LOG(ERROR, "blockid: %u is not exist.", block_id);
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      logic_block->get_meta_infos(meta_list);
      TBSYS_LOG(INFO, "get meta list. blockid: %u, filenum: %zd\n", block_id, meta_list.size());
      */
    }

    int DataManagement::reset_block_version(const uint32_t block_id)
    {
      UNUSED(block_id);
      return EXIT_NOT_SUPPORT_ERROR;

      /*
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        TBSYS_LOG(ERROR, "blockid: %u is not exist.", block_id);
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      logic_block->reset_block_version();
      TBSYS_LOG(INFO, "reset block version: %u\n", block_id);
      return TFS_SUCCESS;
      */
    }

    int DataManagement::new_single_block(const uint32_t block_id, const bool tmp)
    {
      TBSYS_LOG(INFO, "new single block, blockid: %u", block_id);
      return DataManager::instance().new_block(block_id, tmp);
    }

    int DataManagement::del_single_block(const uint32_t block_id, const bool tmp)
    {
      TBSYS_LOG(INFO, "remove single block, blockid: %u", block_id);
      return DataManager::instance().remove_block(block_id, tmp);
    }

    int DataManagement::get_block_curr_size(const uint32_t block_id, int32_t& size)
    {
      UNUSED(block_id);
      UNUSED(size);
      return EXIT_NOT_SUPPORT_ERROR;

      /*
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        TBSYS_LOG(ERROR, "blockid: %u is not exist.", block_id);
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      size = logic_block->get_data_file_size();
      TBSYS_LOG(DEBUG, "blockid: %u data file size: %d\n", block_id, size);
      return TFS_SUCCESS;
      */
    }

    int DataManagement::read_raw_data(const uint32_t block_id, const int32_t read_offset, int32_t& real_read_len,
        char* tmp_data_buffer)
    {
      return DataManager::instance().read_raw_data(tmp_data_buffer, block_id, real_read_len, read_offset);
    }

    int DataManagement::write_raw_data(const uint32_t block_id, const int32_t data_offset, const int32_t msg_len,
        const char* data_buffer)
    {
      //zero length
      if (0 == msg_len)
      {
        return TFS_SUCCESS;
      }

      return DataManager::instance().write_raw_data(data_buffer, block_id, msg_len, data_offset);
    }

    int DataManagement::batch_write_meta(const uint32_t block_id, const BlockInfo* blk, const RawMetaVec* meta_list)
    {
      UNUSED(block_id);
      UNUSED(blk);
      UNUSED(meta_list);
      return EXIT_NOT_SUPPORT_ERROR;

      /*
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        TBSYS_LOG(ERROR, "blockid: %u is not exist.", block_id);
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      int ret = logic_block->batch_write_meta(blk, meta_list);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "blockid: %u batch write meta error.", block_id);
        return ret;
      }

      return TFS_SUCCESS;
      */
    }

    int DataManagement::add_new_expire_block(const VUINT32* expire_block_ids, const VUINT32* remove_block_ids, const VUINT32* new_block_ids)
    {
      // delete expire block
      if (NULL != expire_block_ids)
      {
        TBSYS_LOG(INFO, "expire block list size: %u\n", static_cast<uint32_t>(expire_block_ids->size()));
        batch_remove_block(expire_block_ids);
      }

      // delete remove block
      if (NULL != remove_block_ids)
      {
        TBSYS_LOG(INFO, "remove block list size: %u\n", static_cast<uint32_t>(remove_block_ids->size()));
        batch_remove_block(remove_block_ids);
      }

      // new block
      if (NULL != new_block_ids)
      {
        TBSYS_LOG(INFO, "new block list size: %u\n", static_cast<uint32_t>(new_block_ids->size()));
        batch_remove_block(new_block_ids);
      }

      return TFS_SUCCESS;
    }

    // gc expired and no referenced datafile
    int DataManagement::gc_data_file()
    {
      int32_t current_time = time(NULL);
      int32_t diff_time = current_time - SYSPARAM_DATASERVER.expire_data_file_time_;

      if (last_gc_data_file_time_ < diff_time)
      {
        data_file_mutex_.lock();
        int32_t old_data_file_size = data_file_map_.size();
        for (DataFileMapIter it = data_file_map_.begin(); it != data_file_map_.end();)
        {
          // no reference and expire
          if (it->second && it->second->get_ref() <= 0 && it->second->get_last_update() < diff_time)
          {
            tbsys::gDelete(it->second);
            data_file_map_.erase(it++);
          }
          else
          {
            ++it;
          }
        }

        int32_t new_data_file_size = data_file_map_.size();

        last_gc_data_file_time_ = current_time;
        data_file_mutex_.unlock();
        TBSYS_LOG(INFO, "datafilemap size. old: %d, new: %d", old_data_file_size, new_data_file_size);
      }

      return TFS_SUCCESS;
    }

    // remove all datafile
    int DataManagement::remove_data_file()
    {
      data_file_mutex_.lock();
      for (DataFileMapIter it = data_file_map_.begin(); it != data_file_map_.end(); ++it)
      {
        tbsys::gDelete(it->second);
      }
      data_file_map_.clear();
      data_file_mutex_.unlock();
      return TFS_SUCCESS;
    }

  }
}
