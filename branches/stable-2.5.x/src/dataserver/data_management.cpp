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
#include <Memory.hpp>
#include "dataservice.h"

namespace tfs
{
  namespace dataserver
  {
    using namespace common;
    using namespace std;

    DataManagement::DataManagement(DataService& service):
      service_(service), file_number_(0), last_gc_data_file_time_(0)
    {
    }

    DataManagement::~DataManagement()
    {
      for (DataFileMapIter it = data_file_map_.begin(); it != data_file_map_.end(); ++it)
      {
        tbsys::gDelete(it->second);
      }
    }

    inline BlockManager& DataManagement::get_block_manager()
    {
      return service_.get_block_manager();
    }

    void DataManagement::set_file_number(const uint64_t file_number)
    {
      file_number_ = file_number;
      TBSYS_LOG(INFO, "set file number. file number: %" PRI64_PREFIX "u\n", file_number_);
    }

    int DataManagement::create_file(const uint32_t block_id, uint64_t& file_id, uint64_t& file_number)
    {
      int ret = TFS_SUCCESS;
      if (0 == file_id)
      {
        ret = get_block_manager().generation_file_id(file_id, block_id);
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

      UNUSED(version);
      //if the first fragment, check version
      // int ret = TFS_SUCCESS;
      /*
      if (0 == write_info.offset_)
      {
        BlockInfoV2 none;  // not used in old version
        ret = get_block_manager().check_block_version(none, version, write_info.block_id_, write_info.block_id_);//TODO
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(DEBUG, "check_block_version error. blockid: %u, ret: %d", write_info.block_id_, ret);
          return ret;
        }
      }
      */

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

        datafile = new DataFile(write_info.file_number_,
            dynamic_cast<DataService*>(DataService::instance())->get_real_work_dir());
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
      int ret = get_block_manager().write(file_id, *datafile, block_id, block_id);
      ret = (ret < 0) ? ret : TFS_SUCCESS;
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
      int ret = get_block_manager().read(tmp_data_buffer, real_read_len, read_offset, file_id, flag, block_id, block_id);
      ret = (ret < 0) ? ret : TFS_SUCCESS;
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
      finfo_v2.id_ = file_id;
      int ret = get_block_manager().stat(finfo_v2, mode, block_id, block_id);
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
        finfo.crc_ = finfo_v2.crc_;
     }

      return ret;
    }

    int DataManagement::unlink_file(const uint32_t block_id, const uint64_t file_id, const int32_t action, int64_t& file_size)
    {
      return get_block_manager().unlink(file_size, file_id, action, block_id, block_id);
    }

    int DataManagement::del_single_block(const uint32_t block_id, const bool tmp)
    {
      return get_block_manager().del_block(block_id, tmp);
    }

    int DataManagement::get_block_info(const uint32_t block_id, BlockInfo& blk, int32_t& visit_count)
    {
      visit_count = 0;
      BlockInfoV2 blk_v2;
      int ret = get_block_manager().get_block_info(blk_v2, block_id);
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
        TBSYS_LOG(DEBUG, "datafilemap size. old: %d, new: %d", old_data_file_size, new_data_file_size);
      }

      return TFS_SUCCESS;
    }
  }
}
