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
#include "common/atomic.h"

#include "ds_define.h"
#include "logic_blockv2.h"
#include "block_manager.h"
#include "super_block_manager.h"
#include "physical_block_manager.h"

using namespace tfs::common;

namespace tfs
{
  namespace dataserver
  {
    BaseLogicBlock::BaseLogicBlock(BlockManager* manager, const uint64_t logic_block_id, const std::string& index_path):
      GCObject(Func::get_monotonic_time()),
      manager_(manager),
      logic_block_id_(logic_block_id),
      data_handle_(*this),
      index_handle_(NULL),
      expire_time_(0)
    {
      if (IS_VERFIFY_BLOCK(logic_block_id))
        index_handle_ = new (std::nothrow)VerifyIndexHandle(index_path);
      else
        index_handle_ = new (std::nothrow)IndexHandle(index_path);
    }

    BaseLogicBlock::BaseLogicBlock(const uint64_t logic_block_id):
      GCObject(Func::get_monotonic_time()),
      manager_(NULL),
      logic_block_id_(logic_block_id),
      data_handle_(*this),
      index_handle_(NULL),
      expire_time_(0)
    {

    }

    BaseLogicBlock::~BaseLogicBlock()
    {
      tbsys::gDelete(index_handle_);
    }

    void BaseLogicBlock::set_expire_time(const int32_t expire_time)
    {
      expire_time_ = expire_time;
    }
    int32_t BaseLogicBlock::get_expire_time() const
    {
      return expire_time_;
    }

    int BaseLogicBlock::remove_self_file()
    {
      RWLock::Lock lock(mutex_, WRITE_LOCKER);
      int32_t ret = index_handle_->remove_self(id());
      /*if (TFS_SUCCESS == ret)//TODO
      {
        SuperBlockManager& supber_block_manager = get_block_manager_().get_super_block_manager();
        PHYSICAL_BLOCK_LIST_CONST_ITER iter = physical_block_list_.begin();
        for (; iter != physical_block_list_.end() && TFS_SUCCESS == ret; ++iter)
        {
          ret = supber_block_manager.cleanup_block_index((*iter)->id());
        }
      }*/
      return ret;
    }

    int BaseLogicBlock::rename_index_filename()
    {
      RWLock::Lock lock(mutex_, WRITE_LOCKER);
      return index_handle_->rename_filename(id());
    }

    int BaseLogicBlock::add_physical_block(PhysicalBlock* physical_block)
    {
      int32_t ret = (NULL != physical_block) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        physical_block_list_.push_back(physical_block);
        ret = index_handle_->update_avail_offset(physical_block->length());
      }
      return ret;
    }

    int BaseLogicBlock::get_all_physical_blocks(std::vector<int32_t>& physical_blocks) const
    {
      physical_blocks.clear();
      RWLock::Lock lock(mutex_, READ_LOCKER);
      PHYSICAL_BLOCK_LIST_CONST_ITER iter = physical_block_list_.begin();
      for (; iter != physical_block_list_.end(); ++iter)
      {
        physical_blocks.push_back((*iter)->id());
      }
      return TFS_SUCCESS;
    }

    int BaseLogicBlock::choose_physic_block(PhysicalBlock*& block, int32_t& length, int32_t& inner_offset, const int32_t offset) const
    {
      block = NULL, inner_offset = 0;
      int32_t ret = (offset >= 0 && length > 0) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        int32_t total_length = 0;
        PHYSICAL_BLOCK_LIST_CONST_ITER iter = physical_block_list_.begin();
        for (; iter != physical_block_list_.end() && (NULL == block); ++iter)
        {
          total_length += (*iter)->length();
          if (offset < total_length)
          {
            block = (*iter);
            length       = total_length - offset;
            inner_offset = block->length() - length;
          }
        }
        ret = (NULL != block) ? TFS_SUCCESS : EXIT_PHYSIC_BLOCK_OFFSET_ERROR;
      }
      return ret;
    }

    int BaseLogicBlock::update_block_info(const common::BlockInfoV2& info) const
    {
      RWLock::Lock lock(mutex_, WRITE_LOCKER);
      return index_handle_->update_block_info(info);
    }

    int BaseLogicBlock::update_block_version(const int8_t step)
    {
      int32_t ret = (step >= 0) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        RWLock::Lock lock(mutex_, WRITE_LOCKER);
        ret = index_handle_->update_block_version(step);
      }
      return ret;
    }

    int BaseLogicBlock::check_block_version(common::BlockInfoV2& info, const int32_t remote_version, const uint64_t logic_block_id) const
    {
      RWLock::Lock lock(mutex_, READ_LOCKER);
      // we don't check the version in erasure code group
      int64_t family_id = 0;
      int ret = index_handle_->get_family_id(family_id);
      if ((TFS_SUCCESS == ret) && (INVALID_FAMILY_ID == family_id))
      {
        ret = index_handle_->check_block_version(info, remote_version, logic_block_id);
      }
      return ret;
    }

    int BaseLogicBlock::get_block_info(BlockInfoV2& info) const
    {
      RWLock::Lock lock(mutex_, READ_LOCKER);
      return index_handle_->get_block_info(info);
    }

    int BaseLogicBlock::get_index_header(IndexHeaderV2& header) const
    {
      RWLock::Lock lock(mutex_, READ_LOCKER);
      return index_handle_->get_index_header(header);
    }

    int BaseLogicBlock::set_index_header(const common::IndexHeaderV2& header)
    {
      RWLock::Lock lock(mutex_, WRITE_LOCKER);
      return index_handle_->set_index_header(header);
    }

    int BaseLogicBlock::flush()
    {
      RWLock::Lock lock(mutex_, WRITE_LOCKER);
      return index_handle_->flush();
    }

    int BaseLogicBlock::load_index(const common::MMapOption mmap_option)
    {
      int32_t ret = (mmap_option.check()) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        if (TFS_SUCCESS != index_handle_->check_load())
          ret = index_handle_->mmap(id(), mmap_option);
      }
      return ret;
    }

    int BaseLogicBlock::traverse(common::IndexHeaderV2& header, std::vector<FileInfoV2>& finfos, const uint64_t logic_block_id) const
    {
      int32_t ret = (INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        RWLock::Lock lock(mutex_, READ_LOCKER);
        ret = index_handle_->traverse(header, finfos, logic_block_id);
      }
      return ret;
    }

    int BaseLogicBlock::get_attach_blocks(common::ArrayHelper<uint64_t>& blocks) const
    {
      RWLock::Lock lock(mutex_, READ_LOCKER);
      return index_handle_->get_attach_blocks(blocks);
    }

    int BaseLogicBlock::get_index_num(int32_t& index_num) const
    {
      RWLock::Lock lock(mutex_, READ_LOCKER);
      return index_handle_->get_index_num(index_num);
    }

    int BaseLogicBlock::get_family_id(int64_t& family_id) const
    {
      family_id = INVALID_FAMILY_ID;
      RWLock::Lock lock(mutex_, READ_LOCKER);
      return index_handle_->get_family_id(family_id);
    }

    int BaseLogicBlock::set_family_id(const int64_t family_id)
    {
      RWLock::Lock lock(mutex_, WRITE_LOCKER);
      return index_handle_->set_family_id(family_id);
    }

    int BaseLogicBlock::get_used_offset(int32_t& size) const
    {
      RWLock::Lock lock(mutex_, READ_LOCKER);
      return index_handle_->get_used_offset(size);
    }

    int BaseLogicBlock::set_used_offset(const int32_t size)
    {
      RWLock::Lock lock(mutex_, WRITE_LOCKER);
      return index_handle_->set_used_offset(size);
    }

    int BaseLogicBlock::get_avail_offset(int32_t& size) const
    {
      RWLock::Lock lock(mutex_, READ_LOCKER);
      return index_handle_->get_avail_offset(size);
    }

    int BaseLogicBlock::get_marshalling_offset(int32_t& offset) const
    {
      RWLock::Lock lock(mutex_, READ_LOCKER);
      return index_handle_->get_marshalling_offset(offset);
    }

    int BaseLogicBlock::set_marshalling_offset(const int32_t size)
    {
      RWLock::Lock lock(mutex_, WRITE_LOCKER);
      return index_handle_->set_marshalling_offset(size);
    }

    int BaseLogicBlock::write_file_infos(common::IndexHeaderV2& header, std::vector<FileInfoV2>& infos, const uint64_t logic_block_id, const bool partial)
    {
      RWLock::Lock lock(mutex_, WRITE_LOCKER);
      SuperBlockInfo* sbinfo = NULL;
      SuperBlockManager& supber_block_manager = get_block_manager_().get_super_block_manager();
      int ret = supber_block_manager.get_super_block_info(sbinfo);
      if (TFS_SUCCESS == ret)
      {
        ret = index_handle_->write_file_infos(header, infos,
            sbinfo->max_use_hash_bucket_ratio_, sbinfo->max_hash_bucket_count_, logic_block_id, partial);
      }
      return ret;
    }

    int BaseLogicBlock::write(uint64_t& fileid, DataFile& datafile, const uint64_t logic_block_id, const bool tmp)
    {
      UNUSED(tmp);
      UNUSED(fileid);
      UNUSED(datafile);
      UNUSED(logic_block_id);
      return TFS_SUCCESS;
    }

    int BaseLogicBlock::read(char* buf, int32_t& nbytes, const int32_t offset,
        const uint64_t fileid, const int8_t flag, const uint64_t logic_block_id)
    {
      int32_t ret = (NULL != buf && nbytes > 0 && offset >= 0 && INVALID_FILE_ID != fileid && INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        RWLock::Lock lock(mutex_, READ_LOCKER);
        SuperBlockInfo* sbinfo = NULL;
        SuperBlockManager& supber_block_manager = get_block_manager_().get_super_block_manager();
        ret = supber_block_manager.get_super_block_info(sbinfo);
        if (TFS_SUCCESS == ret)
        {
          FileInfoV2 finfo;
          finfo.id_ = fileid;
          ret = index_handle_->read_file_info(finfo, sbinfo->max_use_hash_bucket_ratio_,sbinfo->max_hash_bucket_count_, logic_block_id);
          if (TFS_SUCCESS == ret)
          {
            // truncate to right read length
            if (offset + nbytes > finfo.size_)
              nbytes = finfo.size_ - offset;
            ret = (nbytes > 0) ? TFS_SUCCESS : EXIT_READ_OFFSET_ERROR;
          }

          if (TFS_SUCCESS == ret)
          {
            if (READ_DATA_OPTION_FLAG_FORCE & flag)
              ret = (finfo.id_ != fileid || 0 != finfo.status_ & FILE_STATUS_INVALID) ? EXIT_FILE_INFO_ERROR : TFS_SUCCESS;
            else
              ret = (finfo.id_ != fileid || 0 != finfo.status_ & (FILE_STATUS_DELETE | FILE_STATUS_INVALID | FILE_STATUS_CONCEAL)) ? EXIT_FILE_INFO_ERROR : TFS_SUCCESS;
          }

          if (TFS_SUCCESS == ret)
          {
            ret = data_handle_.pread(buf, nbytes, finfo.offset_ + offset);
          }
          if (TFS_SUCCESS == ret)
          {
            index_handle_->inc_read_visit_count(1, nbytes);
          }
        }
      }
      return ret;
    }

    int BaseLogicBlock::pwrite(const char* buf, const int32_t nbytes, const int32_t offset, const bool tmp)
    {
      int32_t ret = (NULL != buf && nbytes > 0) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = extend_block_(nbytes, offset, tmp);
        common::RWLock::Lock(mutex_, WRITE_LOCKER);
        if (TFS_SUCCESS == ret)//write data
        {
          int32_t mem_offset = 0;
          while (TFS_SUCCESS == ret && mem_offset < nbytes)
          {
            int32_t length = nbytes - mem_offset;
            ret = data_handle_.pwrite((buf+ mem_offset), length, (offset + mem_offset));
            ret = ret >= 0 ? TFS_SUCCESS : ret;
            if (TFS_SUCCESS == ret)
            {
              mem_offset += length;
            }
          }
        }
        if (TFS_SUCCESS == ret)
        {
          ret = index_handle_->set_used_offset(nbytes + offset);
        }
      }
      return TFS_SUCCESS == ret ? nbytes : ret;
    }

    int BaseLogicBlock::pread(char* buf, int32_t& nbytes, const int32_t offset)
    {
      int32_t ret = (NULL != buf && nbytes > 0 && offset >= 0) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        RWLock::Lock lock(mutex_, READ_LOCKER);
        int32_t inner_offset = 0;
        ret = index_handle_->get_used_offset(inner_offset);
        if (TFS_SUCCESS == ret)
        {
          if (offset + nbytes > inner_offset)
            nbytes = inner_offset - offset;
          ret = (nbytes >= 0) ? TFS_SUCCESS : EXIT_READ_OFFSET_ERROR;
          if (TFS_SUCCESS == ret && nbytes > 0)
          {
            ret = data_handle_.pread(buf, nbytes, offset);
          }
        }
      }
      return ret;
    }

    int BaseLogicBlock::stat(FileInfoV2& info, const int8_t flag, const uint64_t logic_block_id) const
    {
      int32_t ret = (INVALID_FILE_ID != info.id_ && INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        RWLock::Lock lock(mutex_, READ_LOCKER);
        SuperBlockInfo* sbinfo = NULL;
        SuperBlockManager& supber_block_manager = manager_->get_super_block_manager();
        ret = supber_block_manager.get_super_block_info(sbinfo);
        if (TFS_SUCCESS == ret)
        {
          ret = index_handle_->read_file_info(info, sbinfo->max_use_hash_bucket_ratio_,sbinfo->max_hash_bucket_count_, logic_block_id);
          if (TFS_SUCCESS == ret)
          {
            if (FORCE_STAT & flag)
            {
              ret = (0 != info.status_ & FILE_STATUS_INVALID) ? EXIT_FILE_INFO_ERROR : TFS_SUCCESS;
            }
            else
            {
              ret = (0 != (info.status_ & (FILE_STATUS_DELETE | FILE_STATUS_INVALID | FILE_STATUS_CONCEAL))) ?
                EXIT_FILE_INFO_ERROR : TFS_SUCCESS;
            }
          }
          if (TFS_SUCCESS == ret)
          {
            index_handle_->inc_read_visit_count(1, 0);
          }
        }
      }
      return ret;
    }

    int BaseLogicBlock::unlink(int64_t& size, const uint64_t fileid, const int32_t action, const uint64_t logic_block_id)
    {
      UNUSED(size);
      UNUSED(fileid);
      UNUSED(action);
      UNUSED(logic_block_id);
      return TFS_SUCCESS;
    }

    void BaseLogicBlock::callback()
    {
      remove_self_file();
    }

    int BaseLogicBlock::statistic_visit(common::ThroughputV2& throughput, const bool reset)
    {
      return index_handle_->statistic_visit(throughput, reset);
    }

    int BaseLogicBlock::transfer_file_status_(int32_t& oper_type, FileInfoV2& finfo, const int32_t action,
        const uint64_t logic_block_id, const uint64_t fileid) const
    {
      oper_type = OPER_NONE;
      int8_t status = FILE_STATUS_NOMARL;
      int32_t ret = TFS_SUCCESS, tmp_action = action;

      if (TEST_OVERRIDE_FLAG(action))
      {
        status = GET_OVERRIDE_FLAG(action);
        tmp_action = OVERRIDE;
      }

      switch(tmp_action)
      {
        case OVERRIDE:
          if ((finfo.status_ & FILE_STATUS_DELETE) != (status & FILE_STATUS_DELETE))
          {
            oper_type = (status & FILE_STATUS_DELETE) ? OPER_DELETE : OPER_UNDELETE;
          }
          finfo.status_ = status;
          break;
        case DELETE:
          if (!(finfo.status_ & FILE_STATUS_DELETE))
          {
            oper_type = OPER_DELETE;
            finfo.status_ |= FILE_STATUS_DELETE;
          }
          break;
        case UNDELETE:
          if (finfo.status_ & FILE_STATUS_DELETE)
          {
            oper_type = OPER_UNDELETE;
            finfo.status_ &= (~FILE_STATUS_DELETE);
          }
          break;
        case CONCEAL:
          ret = (0 != (finfo.status_ & FILE_STATUS_DELETE)) ? EXIT_FILE_STATUS_ERROR : TFS_SUCCESS;
          if (TFS_SUCCESS == ret)
            finfo.status_ |= FILE_STATUS_CONCEAL;
          break;
        case REVEAL:
          ret = (0 != (finfo.status_ & FILE_STATUS_DELETE)) ? EXIT_FILE_STATUS_ERROR : TFS_SUCCESS;
          if (TFS_SUCCESS == ret)
            finfo.status_ &= (~FILE_STATUS_CONCEAL);
          break;
        default:
          ret = EXIT_FILE_ACTION_ERROR;
          TBSYS_LOG(INFO, "action is illegal. action: %d, blockid: %"PRI64_PREFIX"u, fileid: %" PRI64_PREFIX "u, ret: %d",
              tmp_action, logic_block_id, fileid, ret);
          break;
      }
      return ret;
    }

    int BaseLogicBlock::extend_block_(const int32_t size, const int32_t offset, const bool tmp)
    {
      int32_t ret = (size > 0 && (offset >= 0 || -1 == offset)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        common::RWLock::Lock lock(get_block_manager_().mutex_, WRITE_LOCKER);
        common::RWLock::Lock this_lock(mutex_, WRITE_LOCKER);
        int32_t avail_size = 0, real_offset = offset;
        SuperBlockInfo* super_info = NULL;
        SuperBlockManager& supber_block_manager = get_block_manager_().get_super_block_manager();
        PhysicalBlockManager&  physical_block_manager = get_block_manager_().get_physical_block_manager();
        ret = supber_block_manager.get_super_block_info(super_info);
        if (TFS_SUCCESS == ret)
        {
          ret = index_handle_->get_avail_offset(avail_size);
        }
        if (TFS_SUCCESS == ret)
        {
          if (real_offset < 0)
          {
            ret = index_handle_->get_used_offset(real_offset);
          }
        }
        if (TFS_SUCCESS == ret)
        {
          const int32_t total_offset      = real_offset + size;
          const int32_t total_need_length = (real_offset + size) - avail_size;
          int32_t retry_times = (total_need_length / super_info->max_extend_block_size_) + 1;
          while (TFS_SUCCESS == ret && avail_size < total_offset && retry_times-- > 0)
          {
            TBSYS_LOG(INFO, "extend logic block: %"PRI64_PREFIX"u,avail_size: %d, total_offset: %d, max_single_block_size: %d",
                id(), avail_size, total_offset, MAX_MAIN_AND_EXT_BLOCK_SIZE);
            BlockIndex index, ext_index;
            BasePhysicalBlock* new_physical_block = NULL;
            ret = (avail_size  + super_info->max_extend_block_size_) < MAX_MAIN_AND_EXT_BLOCK_SIZE ? TFS_SUCCESS : EXIT_BLOCK_SIZE_OUT_OF_RANGE;
            if (TFS_SUCCESS == ret)
            {
              ret = (!physical_block_list_.empty()) ? TFS_SUCCESS : EXIT_PHYSICALBLOCK_NUM_ERROR;
            }
            if (TFS_SUCCESS == ret)
            {
              PhysicalBlock* last_physical_block = physical_block_list_.back();
              index.physical_block_id_ = last_physical_block->id();
              assert(INVALID_PHYSICAL_BLOCK_ID != index.physical_block_id_);
              ret = supber_block_manager.get_block_index(index, index.physical_block_id_);
            }
            if (TFS_SUCCESS == ret)
            {
              ret = physical_block_manager.alloc_ext_block(index, ext_index, tmp);
              new_physical_block = physical_block_manager.get(ext_index.physical_block_id_);
              ret = (NULL == new_physical_block) ? EXIT_PHYSICAL_BLOCK_NOT_FOUND : TFS_SUCCESS;
            }
            if (TFS_SUCCESS == ret)
            {
              PhysicalBlock* physical_block = dynamic_cast<PhysicalBlock*>(new_physical_block);
              ret = add_physical_block(physical_block);
              if (TFS_SUCCESS == ret)
              {
                avail_size += new_physical_block->length();
              }
            }
          }
        }
      }
      return ret;
    }

    int BaseLogicBlock::write_(FileInfoV2& new_finfo, DataFile& datafile, const FileInfoV2& old_finfo, const bool update)
    {
      time_t now = time(NULL);
      int32_t file_size = datafile.length();
      new_finfo.create_time_ = update ? old_finfo.create_time_ : now;
      new_finfo.modify_time_ = now;
      new_finfo.crc_ = datafile.crc();
      new_finfo.status_ = FILE_STATUS_NOMARL;
      if (update)
      {
        new_finfo.next_ = old_finfo.next_;
      }
      else
      {
        new_finfo.next_   = 0;
      }
      new_finfo.size_ = file_size;
      if (datafile.get_status() >= 0)
      {
        new_finfo.status_ = datafile.get_status();
      }
      int32_t ret = index_handle_->get_used_offset(new_finfo.offset_);
      if (TFS_SUCCESS == ret)
      {
        char* data = NULL;
        int32_t read_offset = 0, length = 0, write_offset = 0;
        int32_t write_length = 0, mem_offset = 0;
        while (read_offset < new_finfo.size_  && TFS_SUCCESS == ret)
        {
          length = new_finfo.size_ - read_offset;
          ret = datafile.pread(data, length, read_offset);
          ret = (length == ret) ? TFS_SUCCESS : ret;
          if (TFS_SUCCESS == ret)
          {
            if (0 == read_offset)
            {
              TBSYS_LOG(DEBUG, "write file : blockid: %lu, fileid: %lu, size: %d, crc: %u, offset: %d", id(), new_finfo.id_, file_size, new_finfo.crc_, new_finfo.offset_);
              Func::hex_dump(data, 10, true, TBSYS_LOG_LEVEL_DEBUG);
            }
            assert(NULL != data);
            read_offset += length;
            ret = read_offset > new_finfo.size_  ? EXIT_WRITE_OFFSET_ERROR : TFS_SUCCESS;
            if (TFS_SUCCESS == ret && length > 0)
            {
              mem_offset = 0;
              while (mem_offset < length && TFS_SUCCESS == ret)
              {
                write_length = length - mem_offset;
                ret = data_handle_.pwrite((data + mem_offset), write_length, (new_finfo.offset_ + write_offset));
                ret = (ret == write_length) ? TFS_SUCCESS : ret;
                if (TFS_SUCCESS == ret)
                {
                  mem_offset   += write_length;
                  write_offset += write_length;
                }
              }//end while (write_offset < length && TFS_SUCCESS == ret)
            }
          }
        }//end while (offset < new_finfo.size_  && TFS_SUCCESS == ret)
        //这里并没有直接将数据刷到磁盘，如果掉电的话，有可能丢数据, 我们需要多个副本来保证数据的正确性
      }
      TBSYS_LOG(DEBUG, "write file : blockid: %lu, fileid: %lu, size: %d, crc: %u, offset: %d", id(), new_finfo.id_, file_size, new_finfo.crc_, new_finfo.offset_);
      return ret;
    }

    LogicBlock::LogicBlock(BlockManager* manager, const uint64_t logic_block_id, const std::string& index_path):
      BaseLogicBlock(manager, logic_block_id, index_path)
    {

    }

    LogicBlock::~LogicBlock()
    {

    }

    int LogicBlock::create_index(const int32_t bucket_size, const common::MMapOption mmap_option)
    {
      int32_t ret = (bucket_size > 0 && mmap_option.check()) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = get_index_handle_()->create(id(), bucket_size, mmap_option);
      }
      return ret;
    }

    int LogicBlock::generation_file_id(uint64_t& fileid)
    {
      RWLock::Lock lock(mutex_, WRITE_LOCKER);
      SuperBlockInfo* sbinfo = NULL;
      SuperBlockManager& supber_block_manager = get_block_manager_().get_super_block_manager();
      int ret = supber_block_manager.get_super_block_info(sbinfo);
      if (TFS_SUCCESS == ret)
      {
        ret = get_index_handle_()->generation_file_id(fileid, sbinfo->max_use_hash_bucket_ratio_, sbinfo->max_hash_bucket_count_);
      }
      return ret;
    }

    int LogicBlock::check_block_intact()
    {
      int32_t ret = TFS_SUCCESS;
      return ret;
    }

    int LogicBlock::write(uint64_t& fileid, DataFile& datafile, const uint64_t logic_block_id, const bool tmp)
    {
      int32_t ret = (INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = extend_block_(datafile.length(), -1, tmp);
        if (TFS_SUCCESS == ret)
        {
          RWLock::Lock lock(mutex_, WRITE_LOCKER);
          SuperBlockInfo* sbinfo = NULL;
          SuperBlockManager& supber_block_manager = get_block_manager_().get_super_block_manager();
          ret = supber_block_manager.get_super_block_info(sbinfo);
          if (TFS_SUCCESS == ret)
          {
            FileInfoV2 old_finfo, new_finfo;
            new_finfo.id_ = old_finfo.id_ = fileid;
            ret = get_index_handle_()->read_file_info(old_finfo, sbinfo->max_use_hash_bucket_ratio_,
                    sbinfo->max_hash_bucket_count_, logic_block_id);
            bool update = (TFS_SUCCESS == ret);
            if (update)
              TBSYS_LOG(INFO, "file exist, update! block id: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u", logic_block_id, fileid);

            ret = write_(new_finfo, datafile, old_finfo, update);
            if (TFS_SUCCESS == ret)
            {
              ret = get_index_handle_()->update_block_statistic_info(update ? OPER_UPDATE : OPER_INSERT, new_finfo.size_, old_finfo.size_, false);
              if (TFS_SUCCESS == ret)
              {
                ret = get_index_handle_()->write_file_info(new_finfo, sbinfo->max_use_hash_bucket_ratio_
                      ,sbinfo->max_hash_bucket_count_, logic_block_id, update);
                if (TFS_SUCCESS != ret)
                {
                  TBSYS_LOG(INFO, "write file info failed, we'll rollback, ret: %d, block id: %"PRI64_PREFIX"u, fileid:%"PRI64_PREFIX"u",
                      ret, id(), fileid);
                  get_index_handle_()->update_block_statistic_info(update ? OPER_UPDATE : OPER_INSERT, new_finfo.size_, old_finfo.size_, true);
                }
                else
                {
                  fileid = new_finfo.id_;
                  ret = index_handle_->flush();
                  if (TFS_SUCCESS != ret)
                  {
                    TBSYS_LOG(INFO, "write file, flush index to disk failed, ret: %d, block id: %"PRI64_PREFIX"u, fileid:%"PRI64_PREFIX"u",
                        ret, id(), fileid);
                  }
                }
              }
            }
          }
        }
      }
      return TFS_SUCCESS == ret ? datafile.length() : ret;
    }

    int LogicBlock::read(char* buf, int32_t& nbytes, const int32_t offset, const uint64_t fileid,
          const int8_t flag, const uint64_t logic_block_id)
    {
      int32_t ret = (NULL != buf && nbytes > 0 && offset >= 0 && INVALID_FILE_ID != fileid && INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = BaseLogicBlock::read(buf, nbytes, offset, fileid, flag, logic_block_id);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = get_index_handle_()->update_block_statistic_info(OPER_READ, nbytes, nbytes, false);
      }
      return ret;
    }

    int LogicBlock::unlink(int64_t& size, const uint64_t fileid, const int32_t action, const uint64_t logic_block_id)
    {
      size = 0;
      UNUSED(logic_block_id);
      int32_t ret = (INVALID_FILE_ID != fileid) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        RWLock::Lock lock(mutex_, WRITE_LOCKER);
        SuperBlockInfo* sbinfo = NULL;
        SuperBlockManager& supber_block_manager = get_block_manager_().get_super_block_manager();
        ret = supber_block_manager.get_super_block_info(sbinfo);
        if (TFS_SUCCESS == ret)
        {
          FileInfoV2 finfo;
          finfo.id_ = fileid;
          ret = index_handle_->read_file_info(finfo, sbinfo->max_use_hash_bucket_ratio_,sbinfo->max_hash_bucket_count_, id());
          if (TFS_SUCCESS == ret)
          {
            int32_t oper_type  = OPER_NONE;
            ret = transfer_file_status_(oper_type, finfo, action, id(), fileid);
            if (TFS_SUCCESS == ret)
            {
              ret = get_index_handle_()->update_block_statistic_info(oper_type, 0, finfo.size_);
              if (TFS_SUCCESS == ret)
              {
                finfo.modify_time_ = time(NULL);
                ret = get_index_handle_()->write_file_info(finfo, sbinfo->max_use_hash_bucket_ratio_,
                      sbinfo->max_hash_bucket_count_, id(), true);
                if (TFS_SUCCESS != ret)
                  get_index_handle_()->update_block_statistic_info(oper_type, 0, finfo.size_, true);
                else
                  ret = index_handle_->flush();
              }
            }
            if (TFS_SUCCESS == ret)
            {
              size = finfo.size_ - sizeof(FileInfoInDiskExt);
            }
          }
        }
      }
      return ret;
    }

    int LogicBlock::traverse(std::vector<FileInfo>& finfos, const uint64_t logic_block_id) const
    {
      int32_t ret = (INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        RWLock::Lock lock(mutex_, READ_LOCKER);
        ret = get_index_handle_()->traverse(finfos, logic_block_id);
      }
      return ret;
    }

    int LogicBlock::inc_write_visit_count(const int32_t step, const int32_t nbytes)
    {
      return get_index_handle_()->inc_write_visit_count(step, nbytes);
    }

    int LogicBlock::inc_read_visit_count(const int32_t step,  const int32_t nbytes)
    {
      return get_index_handle_()->inc_read_visit_count(step, nbytes);
    }

    int LogicBlock::inc_update_visit_count(const int32_t step,const int32_t nbytes)
    {
      return get_index_handle_()->inc_update_visit_count(step, nbytes);
    }

    int LogicBlock::inc_unlink_visit_count(const int32_t step,const int32_t nbytes)
    {
      return get_index_handle_()->inc_unlink_visit_count(step, nbytes);
    }

    bool SortFileInfoByOffset(const common::FileInfoV2& left, const common::FileInfoV2& right)
    {
      return left.offset_ < right.offset_;
    }

    LogicBlock::Iterator::Iterator(LogicBlock* logic_block):
      logic_block_(logic_block),
      used_offset_(0),
      mem_valid_size_(-1),
      last_read_disk_offset_(-1)
    {
      common::IndexHeaderV2 header;
      BaseLogicBlock* plogic_block = dynamic_cast<BaseLogicBlock*>(logic_block);
      assert(NULL != plogic_block);
      int32_t ret = plogic_block->traverse(header, finfos_, plogic_block->id());
      assert(common::TFS_SUCCESS == ret);
      std::sort(finfos_.begin(), finfos_.end(), SortFileInfoByOffset);
      iter_ = finfos_.begin();
    }

    bool LogicBlock::Iterator::empty() const
    {
      return (iter_ == finfos_.end());
    }

    bool LogicBlock::Iterator::check_offset_range(const int32_t offset, const int32_t size) const
    {
      return ( offset >= 0
            && size > 0
            && mem_valid_size_ > 0
            && last_read_disk_offset_ >= 0
            && (offset >= last_read_disk_offset_)
            && ((offset + size) <= last_read_disk_offset_ + mem_valid_size_));
    }

    int LogicBlock::Iterator::next(FileInfoV2*& info)
    {
      info = NULL;
      int32_t ret = EXIT_BLOCK_NO_DATA;
      int32_t result = TFS_SUCCESS;
      if (0 == used_offset_)
      {
        result = empty() ? EXIT_BLOCK_NO_DATA : TFS_SUCCESS;
        if (TFS_SUCCESS == result)
        {
          result = logic_block_->get_index_handle_()->get_used_offset(used_offset_);
        }
        if (TFS_SUCCESS == result)
        {
          result = used_offset_ >= 0 ? TFS_SUCCESS : EXIT_BLOCK_NO_DATA;
        }
      }
      ret = ((TFS_SUCCESS == result) && !empty()) ?  TFS_SUCCESS : EXIT_BLOCK_NO_DATA;
      if (TFS_SUCCESS == ret)
      {
        ret = EXIT_BLOCK_NO_DATA;
        for (; iter_ != finfos_.end() && TFS_SUCCESS != ret; iter_++)
        {
          info = &(*iter_);
          ret = (info->id_ != INVALID_FILE_ID && info->offset_ >= 0 && info->offset_ < used_offset_)? TFS_SUCCESS : EXIT_FILE_EMPTY;
        }
      }

      //double check
      if (TFS_SUCCESS == ret)
      {
        ret = (info->id_ != INVALID_FILE_ID && info->offset_ >= 0 && info->offset_ < used_offset_)? TFS_SUCCESS : EXIT_FILE_EMPTY;
      }

      if (TFS_SUCCESS == ret)
      {
        bool offset_in_range = check_offset_range(info->offset_, info->size_);
        if (!offset_in_range)
        {
          assert(info->offset_ <= used_offset_);
          last_read_disk_offset_ = info->offset_;
          int32_t disk_data_length = used_offset_ - info->offset_;
          const int32_t max_data_size = MAX_SINGLE_FILE_SIZE;
          const int32_t MAX_READ_SIZE = std::min(max_data_size, disk_data_length);
          mem_valid_size_ = MAX_READ_SIZE;
          ret = logic_block_->pread(data_, mem_valid_size_, info->offset_);
          ret = ret >= 0 ? TFS_SUCCESS : ret;
          if (TFS_SUCCESS == ret)
          {
            ret = (mem_valid_size_ == MAX_READ_SIZE) ? TFS_SUCCESS : EXIT_READ_FILE_ERROR;
          }
        }
      }
      return ret;
    }

    const common::FileInfoV2& LogicBlock::Iterator::get_file_info() const
    {
      return (*iter_);
    }

    const char* LogicBlock::Iterator::get_data(const int32_t offset, const int32_t size) const
    {
      return check_offset_range(offset, size) ? (data_ + (offset - last_read_disk_offset_)) : NULL;
    }

    VerifyLogicBlock::VerifyLogicBlock(BlockManager* manager, const uint64_t logic_block_id, const std::string& index_path):
      BaseLogicBlock(manager, logic_block_id, index_path)
    {

    }

    VerifyLogicBlock::~VerifyLogicBlock()
    {

    }

    int VerifyLogicBlock::create_index(const uint64_t logic_block_id, const int64_t family_id, const int16_t index_num)
    {
      int32_t ret = (INVALID_BLOCK_ID != logic_block_id && INVALID_FAMILY_ID != family_id && index_num > 0) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = get_index_handle_()->create(logic_block_id, family_id, index_num);
      }
      return ret;
    }

    int VerifyLogicBlock::write(uint64_t& fileid, DataFile& datafile, const uint64_t logic_block_id, const bool tmp)
    {
      int32_t ret = (INVALID_FILE_ID != fileid && INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = extend_block_(datafile.length(), -1, tmp);
        if (TFS_SUCCESS == ret)
        {
          common::RWLock::Lock lock(mutex_, WRITE_LOCKER);
          char* data = NULL;
          FileInfoV2* pold_finfo = NULL;
          FileInfoV2  new_finfo, old_finfo;
          VerifyIndexHandle::InnerIndex index;
          index.logic_block_id_ = logic_block_id;
          ret = get_index_handle_()->malloc_index_mem_(data, index);
          if (TFS_SUCCESS == ret)
          {
            new_finfo.id_ = old_finfo.id_ = fileid;
            ret = get_index_handle_()->read_file_info_(pold_finfo, fileid, data, index.size_, GET_SLOT_TYPE_QUERY);
            bool update = TFS_SUCCESS == ret;
            if (update)
            {
              old_finfo = *pold_finfo;
              TBSYS_LOG(INFO, "file exist, update! block id: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"d", logic_block_id, fileid );
            }
            ret = write_(new_finfo, datafile, old_finfo, update);
            if (TFS_SUCCESS == ret)
            {
              ret = get_index_handle_()->update_block_statistic_info_(data, index.size_, update ? OPER_UPDATE : OPER_INSERT, new_finfo.size_, old_finfo.size_, false);
              if (TFS_SUCCESS == ret)
              {
                ret = get_index_handle_()->insert_file_info_(new_finfo, data, index.size_, update);
                if (TFS_SUCCESS != ret)
                {
                  TBSYS_LOG(INFO, "write file info failed, we'll rollback, ret: %d, block id: %"PRI64_PREFIX"u, fileid:%"PRI64_PREFIX"u",
                      ret, logic_block_id, fileid);
                  get_index_handle_()->update_block_statistic_info_(data, index.size_, update ? OPER_UPDATE : OPER_INSERT, new_finfo.size_, old_finfo.size_, true);
                }
              }
            }
          }

          if (NULL != data)
            get_index_handle_()->free_index_mem_(data, index, TFS_SUCCESS == ret);

          if (TFS_SUCCESS == ret)
          {
            ret = get_index_handle_()->flush();
            if (TFS_SUCCESS != ret)
            {
              TBSYS_LOG(INFO, "write file, flush index to disk failed, ret: %d, block id: %"PRI64_PREFIX"u, fileid:%"PRI64_PREFIX"u",
                  ret, logic_block_id, fileid);
            }
          }
        }
      }
      return TFS_SUCCESS == ret ? datafile.length() : ret;
    }

    int VerifyLogicBlock::unlink(int64_t& size, const uint64_t fileid, const int32_t action, const uint64_t logic_block_id)
    {
      size = 0;
      int32_t ret = (INVALID_FILE_ID != fileid && INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        char* data = NULL;
        FileInfoV2* finfo = NULL;
        VerifyIndexHandle::InnerIndex index;
        index.logic_block_id_ = logic_block_id;
        RWLock::Lock lock(mutex_, WRITE_LOCKER);
        ret = get_index_handle_()->malloc_index_mem_(data, index);
        if (TFS_SUCCESS == ret)
        {
          ret = get_index_handle_()->read_file_info_(finfo, fileid, data, index.size_, GET_SLOT_TYPE_QUERY);
        }

        if (TFS_SUCCESS == ret)
        {
          int32_t oper_type  = OPER_NONE;
          ret = transfer_file_status_(oper_type, *finfo, action, logic_block_id, fileid);
          if (TFS_SUCCESS == ret)
          {
            ret = get_index_handle_()->update_block_statistic_info_(data, index.size_, oper_type, 0, finfo->size_);
            if (TFS_SUCCESS == ret)
            {
              finfo->modify_time_ = time(NULL);
            }
          }
        }
        if (TFS_SUCCESS == ret)
        {
          size = finfo->size_ - sizeof(FileInfoInDiskExt);
        }
        if (NULL != data)
          ret = get_index_handle_()->free_index_mem_(data, index, TFS_SUCCESS == ret);
      }
      return ret;
    }
  }/** end namespace dataserver **/
}/** end namespace tfs **/
