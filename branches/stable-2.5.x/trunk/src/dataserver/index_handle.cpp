/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: index_handle.cpp 409 2011-06-02 08:32:14Z nayan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   zhuhui <zhuhui_a.pt@taobao.com>
 *      - modify 2010-04-23
 *
 */
#include "index_handle.h"
#include "dataserver_define.h"
#include "common/error_msg.h"
#include <tbsys.h>

namespace tfs
{
  namespace dataserver
  {

    using namespace common;

    IndexHandle::IndexHandle(const std::string& base_path, const uint32_t main_block_id)
    {
      //create file_op handle
      std::stringstream tmp_stream;
      tmp_stream << base_path << INDEX_DIR_PREFIX << main_block_id;
      std::string index_path;
      tmp_stream >> index_path;
      file_op_ = new MMapFileOperation(index_path, O_RDWR | O_LARGEFILE | O_CREAT);
      is_load_ = false;
    }

    IndexHandle::~IndexHandle()
    {
      if (file_op_)
      {
        delete file_op_;
        file_op_ = NULL;
      }
    }

    int IndexHandle::pcreate(const uint32_t logic_block_id, const DirtyFlag dirty_flag)
    {
      int ret = TFS_SUCCESS;
      int file_size = file_op_->get_file_size();

      if (is_load_)
      {
        ret = EXIT_INDEX_ALREADY_LOADED_ERROR;
      }
      else if(file_size < 0)
      {
        ret = TFS_ERROR;
      }
      else if (file_size > 0)
      {
        ret = EXIT_INDEX_UNEXPECT_EXIST_ERROR;
      }
      else
      {
        char init_data[PARITY_INDEX_MMAP_SIZE];
        memset(init_data, 0, sizeof(init_data));

        ParityIndexHeader i_header;
        memset(&i_header, 0, sizeof(ParityIndexHeader));
        i_header.block_info_.block_id_ = logic_block_id;
        i_header.flag_ = dirty_flag;
        i_header.index_file_size_ = PARITY_INDEX_MMAP_SIZE;

        memcpy(init_data, &i_header, sizeof(ParityIndexHeader));

        // store index header
        ret = file_op_->pwrite_file((char*)&i_header, sizeof(ParityIndexHeader), 0);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "write index header fail, ret: %d\n", ret);
        }
        else
        {
          ret = file_op_->flush_file();
        }
      }

      // mmap header, for compatibility
      if (TFS_SUCCESS == ret)
      {
        MMapOption opt;
        opt.first_mmap_size_ = PARITY_INDEX_MMAP_SIZE;
        opt.max_mmap_size_ = PARITY_INDEX_MMAP_SIZE;
        opt.per_mmap_size_ = PARITY_INDEX_MMAP_SIZE;
        ret = file_op_->mmap_file(opt);
      }

      if (TFS_SUCCESS == ret)
      {
        is_load_ = true;
        TBSYS_LOG(INFO, "create parity index succeed. block id: %u, dirty flag: %d",
            logic_block_id, dirty_flag);
      }
      else
      {
        TBSYS_LOG(ERROR, "create parity index fail. blockid: %u, ret: %d", logic_block_id, ret);
      }

      return ret;
    }

    int IndexHandle::pload(const uint32_t logic_block_id)
    {
      int ret = TFS_SUCCESS;
      int file_size = file_op_->get_file_size();

      if (is_load_)
      {
        ret = EXIT_INDEX_ALREADY_LOADED_ERROR;
      }
      else if (file_size < 0)
      {
        ret = TFS_ERROR;
      }
      else if (file_size < (int)sizeof(ParityIndexHeader))
      {
        ret = EXIT_INDEX_CORRUPT_ERROR;
      }
      else
      {
        MMapOption opt;
        opt.first_mmap_size_ = PARITY_INDEX_MMAP_SIZE;
        opt.max_mmap_size_ = PARITY_INDEX_MMAP_SIZE;
        opt.per_mmap_size_ = PARITY_INDEX_MMAP_SIZE;
        ret = file_op_->mmap_file(opt);
      }

      if (TFS_SUCCESS == ret)
      {
        if (0 == block_info()->block_id_)
        {
          ret = EXIT_INDEX_CORRUPT_ERROR;
          TBSYS_LOG(ERROR, "Index corrupt error. blockid: %u", block_info()->block_id_);
        }
        else if (logic_block_id != block_info()->block_id_)
        {
          ret = EXIT_BLOCKID_CONFLICT_ERROR;
          TBSYS_LOG(ERROR, "block id conflict. blockid: %u, index blockid: %u",
              logic_block_id, block_info()->block_id_);
        }
      }

      if (TFS_SUCCESS == ret)
      {
        if (C_DATA_COMPACT == pindex_header()->flag_)
        {
          ret = EXIT_COMPACT_BLOCK_ERROR;
          TBSYS_LOG(ERROR, "It is a unfinish compact block. blockid: %u", logic_block_id);
        }
        else if (C_DATA_HALF == pindex_header()->flag_)
        {
          ret = EXIT_HALF_BLOCK_ERROR;
          TBSYS_LOG(ERROR, "It is a half state block. blockid: %u", logic_block_id);
        }
        else if (C_DATA_CLEAN == pindex_header()->flag_)
        {
          ret = TFS_ERROR;
          TBSYS_LOG(ERROR, "It is a data block. should use load() instead");
        }
      }

      if (TFS_SUCCESS == ret)
      {
        is_load_ = true;
        TBSYS_LOG(INFO, "load parity blockid: %u index succeed. dirty flag: %d, index nums: %d",
            logic_block_id, pindex_header()->flag_, pindex_header()->index_num_);
      }
      else
      {
        TBSYS_LOG(ERROR, "load parity blockid: %u index fail, ret: %d", logic_block_id, ret);
      }

      return ret;
    }

    int IndexHandle::pappend_index(const uint32_t block_id, const char* data, const int32_t size)
    {
      int ret = TFS_SUCCESS;

      if (NULL == data)
      {
        ret = EXIT_POINTER_NULL;
      }
      else if (0 == block_id)
      {
        ret = EXIT_INDEX_CORRUPT_ERROR;
      }
      else
      {
        Position* cur_pos = pindex_position(pindex_header()->index_num_);
        if (0 == pindex_header()->index_num_)
        {
          cur_pos->block_id_ = block_id;
          cur_pos->offset_ = PARITY_INDEX_START;
          cur_pos->size_ = size;
        }
        else
        {
          Position* last_pos = cur_pos - 1;
          cur_pos->block_id_ = block_id;
          cur_pos->offset_ = last_pos->offset_ + last_pos->size_;
          cur_pos->size_ = size;
        }

        ret = file_op_->pwrite_file(data, cur_pos->size_, cur_pos->offset_);
      }

      if (TFS_SUCCESS == ret)
      {
        pindex_header()->index_num_++;
        TBSYS_LOG(INFO, "append block %u index succeed, size: %d, index_num: %d",
            block_id, size, pindex_header()->index_num_);
      }
      else
      {
        TBSYS_LOG(ERROR, "append block %u index failed, size: %d, ret: %d", block_id, size, ret);
      }

      return ret;

    }

    int IndexHandle::write_data_index(const RawIndexVec& index_vec)
    {
      int ret = TFS_SUCCESS;
      if (index_vec.size() > 0)
      {
        if (NULL == index_vec[0].data_)
        {
          ret = EXIT_POINTER_NULL;
        }
        else
        {
          ret = file_op_->pwrite_file(index_vec[0].data_, index_vec[0].size_, 0);
        }

        if (TFS_SUCCESS == ret)
        {
          index_header()->flag_ = C_DATA_CLEAN;
          ret = flush();
        }
      }

      return ret;
    }

    int IndexHandle::write_parity_index(const RawIndexVec& index_vec)
    {
      int ret = TFS_SUCCESS;
      if (index_vec.size() > 0)
      {
        int32_t old_num = pindex_header()->index_num_;
        for (uint32_t i = 0; i < index_vec.size() && TFS_SUCCESS == ret; i++)
        {
          ret = pappend_index(index_vec[i].block_id_, index_vec[i].data_, index_vec[i].size_);
        }

        // rollback
        if (TFS_SUCCESS != ret)
        {
          pindex_header()->index_num_ = old_num;
        }
        else
        {
          // we can see parity block size through nameserver
          pindex_header()->block_info_.size_ = pindex_header()->data_file_offset_;
          pindex_header()->flag_ = C_DATA_PARITY;
          ret = flush();
        }
      }

      return ret;
    }

    int IndexHandle::read_data_index(char* & buf, int32_t& size)
    {
      int ret = TFS_SUCCESS;
      size = file_op_->get_file_size();

      if (size <= 0)
      {
        ret = EXIT_INDEX_CORRUPT_ERROR;
      }
      else if (C_DATA_CLEAN != index_header()->flag_)
      {
        ret = EXIT_NOT_DATA_INDEX;
      }
      else
      {
        buf = new (std::nothrow) char[size];
        if (NULL == buf)
        {
          ret = EXIT_NO_MEMORY;
        }
        else
        {
          ret = file_op_->pread_file(buf, size, 0);
        }
      }

      return ret;
    }

    int IndexHandle::read_parity_index(const uint32_t index_id, char* & buf, int32_t& size)
    {
      int ret = TFS_SUCCESS;
      int32_t file_size = file_op_->get_file_size();

      if (file_size <= 0)
      {
        ret = EXIT_INDEX_CORRUPT_ERROR;
      }
      else if (C_DATA_PARITY != pindex_header()->flag_)
      {
        ret = EXIT_NOT_PARITY_INDEX;
      }
      else
      {
        Position* pos = pindex_position_byid(index_id);
        if (NULL == pos)
        {
          ret = EXIT_INDEX_CORRUPT_ERROR;
        }
        else
        {
          size = pos->size_;
          uint64_t offset = pos->offset_;
          buf = new (std::nothrow) char[size];
          if (NULL == buf)
          {
            ret = EXIT_NO_MEMORY;
          }
          else
          {
            ret = file_op_->pread_file(buf, size, offset);
          }
        }
      }

      return ret;
    }

    int IndexHandle::update_parity_index(const uint32_t index_id, const char* buf, const int32_t size)
    {
      int ret = TFS_SUCCESS;
      int32_t file_size = file_op_->get_file_size();

      if (file_size <= 0)
      {
        ret = EXIT_INDEX_CORRUPT_ERROR;
      }
      else if (C_DATA_PARITY != pindex_header()->flag_)
      {
        ret = EXIT_NOT_PARITY_INDEX;
      }
      else
      {
        Position* pos = pindex_position_byid(index_id);
        if (NULL == pos)
        {
          ret = EXIT_INDEX_CORRUPT_ERROR;
        }
        else
        {
          uint64_t offset = pos->offset_;
          ret = file_op_->pwrite_file(buf, size, offset);
          if (TFS_SUCCESS == ret)
          {
            ret = flush();
          }
        }
      }

      return ret;
    }

    // create index file. inner format:
    // ------------------------------------------------------------------------------------------
    // | index header|   hash bucket: each slot hold     |           file meta info             |
    // |             |   offset of file's MetaInfo       |                                      |
    // ------------------------------------------------------------------------------------------
    // | IndexHeader | int32_t | int32_t | ... | int32_t | MetaInfo | MetaInfo | ... | MetaInfo |
    // ------------------------------------------------------------------------------------------
    int IndexHandle::create(const uint32_t logic_block_id, const int32_t cfg_bucket_size, const MMapOption map_option,
        const DirtyFlag dirty_flag)
    {
      TBSYS_LOG(
          INFO,
          "index create block: %u index. bucket size: %d, max mmap size: %d, first mmap size: %d, per mmap size: %d, dirty flag: %d",
          logic_block_id, cfg_bucket_size, map_option.max_mmap_size_, map_option.first_mmap_size_,
          map_option.per_mmap_size_, dirty_flag);
      if (is_load_)
      {
        return EXIT_INDEX_ALREADY_LOADED_ERROR;
      }
      int ret = TFS_SUCCESS;
      int64_t file_size = file_op_->get_file_size();
      // file size corrupt
      if (file_size < 0)
      {
        return TFS_ERROR;
      }
      else if (file_size == 0) // empty file
      {
        IndexHeader i_header;
        i_header.block_info_.block_id_ = logic_block_id;
        i_header.block_info_.seq_no_ = 1;
        i_header.index_file_size_ = sizeof(IndexHeader) + cfg_bucket_size * sizeof(int32_t);
        i_header.bucket_size_ = cfg_bucket_size;
        i_header.flag_ = dirty_flag;

        // index header + total buckets
        char* init_data = new char[i_header.index_file_size_];
        memcpy(init_data, &i_header, sizeof(IndexHeader));
        memset(init_data + sizeof(IndexHeader), 0, i_header.index_file_size_ - sizeof(IndexHeader));

        // write index header and buckets into to blockfile
        ret = file_op_->pwrite_file(init_data, i_header.index_file_size_, 0);
        delete[] init_data;
        init_data = NULL;
        if (TFS_SUCCESS != ret)
          return ret;

        // write to disk as immediately as possible
        ret = file_op_->flush_file();
        if (TFS_SUCCESS != ret)
          return ret;
      }
      else //file size > 0, index already exist
      {
        return EXIT_INDEX_UNEXPECT_EXIST_ERROR;
      }

      ret = file_op_->mmap_file(map_option);
      if (TFS_SUCCESS != ret) //mmap fail
        return ret;

      is_load_ = true;
      TBSYS_LOG(
          INFO,
          "init blockid: %u index successful. data file size: %d, index file size: %d, bucket size: %d, free head offset: %d, seqno: %d, size: %d, filecount: %d, del_size: %d, del_file_count: %d version: %d",
          logic_block_id, index_header()->data_file_offset_, index_header()->index_file_size_,
          index_header()->bucket_size_, index_header()->free_head_offset_, block_info()->seq_no_, block_info()->size_,
          block_info()->file_count_, block_info()->del_size_, block_info()->del_file_count_, block_info()->version_);
      return TFS_SUCCESS;
    }

    int IndexHandle::load(const uint32_t logic_block_id, const int32_t cfg_bucket_size, const MMapOption map_option)
    {
      if (is_load_)
      {
        return EXIT_INDEX_ALREADY_LOADED_ERROR;
      }

      int ret = TFS_SUCCESS;
      int file_size = file_op_->get_file_size();
      if (file_size < 0)
      {
        return file_size;
      }
      else if (file_size == 0) // empty file
      {
        return EXIT_INDEX_CORRUPT_ERROR;
      }

      //resize mmap size
      MMapOption tmp_map_option = map_option;
      if (file_size > tmp_map_option.first_mmap_size_ && file_size <= tmp_map_option.max_mmap_size_)
      {
        tmp_map_option.first_mmap_size_ = file_size;
      }

      // map file into memory
      ret = file_op_->mmap_file(tmp_map_option);
      if (TFS_SUCCESS != ret)
        return ret;

      // check stored logic block id and bucket size
      // meta info corrupt, may be destroyed when created by unexpect interrupt
      if (0 == block_info()->block_id_)
      {
        TBSYS_LOG(ERROR, "Index corrupt error. blockid: %u, bucket size: %d", block_info()->block_id_,
            bucket_size());
        return EXIT_INDEX_CORRUPT_ERROR;
      }

      //check file size
      int32_t index_file_size = sizeof(IndexHeader) + bucket_size() * sizeof(int32_t);
      // uncomplete index file
      if (file_size < index_file_size)
      {
        TBSYS_LOG(ERROR, "Index corrupt error. blockid: %u, bucket size: %d, file size: %d, index file size: %d",
            block_info()->block_id_, bucket_size(), file_size, index_file_size);
        return EXIT_INDEX_CORRUPT_ERROR;
      }

      // check block_id
      if (logic_block_id != block_info()->block_id_)
      {
        TBSYS_LOG(ERROR, "block id conflict. blockid: %u, index blockid: %u", logic_block_id, block_info()->block_id_);
        return EXIT_BLOCKID_CONFLICT_ERROR;
      }

      // check flag, if normal state(C_DATA_CLEAN), do nothing
      if (C_DATA_COMPACT == index_header()->flag_)
      {
        //unfinish compact block
        TBSYS_LOG(ERROR, "It is a unfinish compact block. blockid: %u", logic_block_id);
        return EXIT_COMPACT_BLOCK_ERROR;
      }
      else if (C_DATA_HALF == index_header()->flag_)
      {
        // unfinish repl block, coding block
        TBSYS_LOG(ERROR, "It is a half state block. blockid: %u", logic_block_id);
        return EXIT_HALF_BLOCK_ERROR;
      }

      // check bucket_size
      if (0 == bucket_size() || cfg_bucket_size != bucket_size())
      {
        TBSYS_LOG(ERROR, "Index configure error. old bucket size: %d, new bucket size: %d", bucket_size(),
            cfg_bucket_size);
        return EXIT_BUCKET_CONFIGURE_ERROR;
      }

      is_load_ = true;
      TBSYS_LOG(
          INFO,
          "load blockid: %u index successful. data file offset: %d, index file size: %d, bucket size: %d, free head offset: %d, seqno: %d, size: %d, filecount: %d, del size: %d, del file count: %d version: %d",
          logic_block_id, index_header()->data_file_offset_, index_header()->index_file_size_, bucket_size(),
          index_header()->free_head_offset_, block_info()->seq_no_, block_info()->size_, block_info()->file_count_,
          block_info()->del_size_, block_info()->del_file_count_, block_info()->version_);
      return TFS_SUCCESS;
    }

    // remove index: unmmap and unlink file
    int IndexHandle::remove(const uint32_t logic_block_id)
    {
      if (is_load_)
      {
        if (logic_block_id != block_info()->block_id_)
        {
          TBSYS_LOG(ERROR, "block id conflict. blockid: %d, index blockid: %d", logic_block_id, block_info()->block_id_);
          return EXIT_BLOCKID_CONFLICT_ERROR;
        }
      }

      int ret = file_op_->munmap_file();
      if (TFS_SUCCESS != ret)
        return ret;

      ret = file_op_->unlink_file();
      return ret;
    }

    // rename index file
    int IndexHandle::rename(const uint32_t logic_block_id)
    {
      if (is_load_)
      {
        if (logic_block_id != block_info()->block_id_)
        {
          TBSYS_LOG(ERROR, "block id conflict. blockid: %d, index blockid: %d", logic_block_id, block_info()->block_id_);
          return EXIT_BLOCKID_CONFLICT_ERROR;
        }
      }

      int ret = file_op_->rename_file();
      return ret;
    }

    int IndexHandle::flush()
    {
      int ret = file_op_->flush_file();
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "index flush fail. ret: %d, error desc: %s", ret, strerror(errno));
      }
      return ret;
    }

    int IndexHandle::set_block_dirty_type(const DirtyFlag dirty_flag)
    {
      index_header()->flag_ = dirty_flag;
      return flush();
    }

    int IndexHandle::find_avail_key(uint64_t& key)
    {
      // for write, get next sequence number
      if (0 == key)
      {
        key = block_info()->seq_no_;
      } // continue test

      int32_t offset = 0, slot = 0;
      int ret = TFS_SUCCESS;
      MetaInfo meta_info;
      int retry_times = MAX_RETRY_TIMES;
      bool found = false;
      do
      {
        // use low 32bit
        slot = static_cast<uint32_t> (key) % bucket_size();
        // the first metainfo node
        offset = bucket_slot()[slot];
        // if this position is empty, use this key
        if (0 == offset)
        {
          found = true;
          break;
        }

        // hash corrupt, find in the list
        for (; offset != 0;)
        {
          ret = file_op_->pread_file(reinterpret_cast<char*> (&meta_info), META_INFO_SIZE, offset);
          if (TFS_SUCCESS != ret)
            return ret;
          // compare the low 32bit. if conflict
          if (static_cast<uint32_t> (key) == static_cast<uint32_t> (meta_info.get_key()))
          {
            // if exists, test key + 1
            ++key;
            break;
          }
          offset = meta_info.get_next_meta_offset();
        }

        //this key is not exist in the list
        if (0 == offset)
        {
          found = true;
          break;
        }
      }
      while (retry_times--);

      // assign low 32bit to 64bit
      block_info()->seq_no_ = key + 1;

      if (!found)
      {
        TBSYS_LOG(ERROR, "blockid: %u, find avail key fail. new key: %" PRI64_PREFIX "u", block_info()->block_id_, key);
        return EXIT_CREATE_FILEID_ERROR;
      }

      TBSYS_LOG(DEBUG, "blockid: %u, get key: %" PRI64_PREFIX "u, seqno: %u", block_info()->block_id_, key,
          block_info()->seq_no_);
      return TFS_SUCCESS;
    }

    void IndexHandle::reset_avail_key(uint64_t key)
    {
      if (block_info()->seq_no_ <= key)
      {
        block_info()->seq_no_ = key + 1;
        // overlap ...
      }
    }

    int IndexHandle::check_block_version(common::BlockInfo& info, const int32_t remote_version)
    {
      assert(NULL != block_info());
      info = *block_info();
      return remote_version == info.version_ ? TFS_SUCCESS : EXIT_VERSION_CONFLICT_ERROR;
    }

    int IndexHandle::reset_block_version()
    {
      block_info()->version_ = 1;
      return TFS_SUCCESS;
    }

    //write at the end of the list
    int IndexHandle::write_segment_meta(const uint64_t key, const RawMeta& meta)
    {
      int32_t current_offset = 0, previous_offset = 0;
      int ret = hash_find(key, current_offset, previous_offset);
      if (TFS_SUCCESS == ret) // check not exists
      {
        return EXIT_META_UNEXPECT_FOUND_ERROR;
      }
      else if (EXIT_META_NOT_FOUND_ERROR != ret)
      {
        return ret;
      }

      int32_t slot = static_cast<uint32_t> (key) % bucket_size();
      return hash_insert(slot, previous_offset, meta);
    }

    int IndexHandle::read_segment_meta(const uint64_t key, RawMeta& meta)
    {
      int32_t current_offset = 0, previous_offset = 0;
      // find
      int ret = hash_find(key, current_offset, previous_offset);
      if (TFS_SUCCESS == ret) //exist
      {
        ret = file_op_->pread_file(reinterpret_cast<char*> (&meta), RAW_META_SIZE, current_offset);
        if (TFS_SUCCESS != ret)
          return ret;
      }
      else
      {
        return ret;
      }

      return TFS_SUCCESS;
    }

    int IndexHandle::override_segment_meta(const uint64_t key, const RawMeta& meta)
    {
      // find
      int32_t current_offset = 0, previous_offset = 0;
      int ret = hash_find(key, current_offset, previous_offset);
      //exist, update
      if (TFS_SUCCESS == ret)
      {
        //get next meta offset
        MetaInfo tmp_meta;
        ret = file_op_->pread_file(reinterpret_cast<char*> (&tmp_meta), META_INFO_SIZE, current_offset);
        if (TFS_SUCCESS != ret)
          return ret;

        tmp_meta.set_raw_meta(meta);
        ret = file_op_->pwrite_file(reinterpret_cast<const char*> (&tmp_meta), META_INFO_SIZE, current_offset);
        if (TFS_SUCCESS != ret)
          return ret;
      }
      else if (EXIT_META_NOT_FOUND_ERROR != ret)
      {
        return ret;
      }
      else if (EXIT_META_NOT_FOUND_ERROR == ret) // nonexists, insert
      {
        // insert
        int32_t slot = static_cast<uint32_t> (key) % bucket_size();
        ret = hash_insert(slot, previous_offset, meta);
        if (TFS_SUCCESS != ret)
          return ret;
      }
      return ret;
    }

    int IndexHandle::batch_override_segment_meta(const RawMetaVec& meta_list)
    {
      int ret = TFS_SUCCESS;
      for (RawMetaVecConstIter mit = meta_list.begin(); mit != meta_list.end(); ++mit)
      {
        ret = override_segment_meta(mit->get_key(), *mit);
        if (TFS_SUCCESS != ret)
        {
          return ret;
        }
      }
      return TFS_SUCCESS;
    }

    int IndexHandle::update_segment_meta(const uint64_t key, const RawMeta& meta)
    {
      // find
      int32_t current_offset = 0, previous_offset = 0;
      int ret = hash_find(key, current_offset, previous_offset);
      if (TFS_SUCCESS == ret) // exist
      {
        MetaInfo tmp_meta;
        ret = file_op_->pread_file(reinterpret_cast<char*> (&tmp_meta), META_INFO_SIZE, current_offset);
        if (TFS_SUCCESS != ret)
          return ret;

        tmp_meta.set_raw_meta(meta);
        ret = file_op_->pwrite_file(reinterpret_cast<const char*> (&tmp_meta), META_INFO_SIZE, current_offset);
        if (TFS_SUCCESS != ret)
          return ret;
      }
      else
      {
        return ret;
      }

      return TFS_SUCCESS;
    }

    int IndexHandle::delete_segment_meta(const uint64_t key)
    {
      // find
      int32_t current_offset = 0, previous_offset = 0;
      int ret = hash_find(key, current_offset, previous_offset);
      if (TFS_SUCCESS != ret)
        return ret;

      MetaInfo meta_info;
      ret = file_op_->pread_file(reinterpret_cast<char*> (&meta_info), META_INFO_SIZE, current_offset);
      if (TFS_SUCCESS != ret)
        return ret;

      int32_t tmp_pos = meta_info.get_next_meta_offset();

      int32_t slot = static_cast<uint32_t> (key) % bucket_size();
      // the header of the list
      if (0 == previous_offset)
      {
        bucket_slot()[slot] = tmp_pos;
      }
      else // delete from list, modify previous
      {
        MetaInfo pre_meta_info;
        ret = file_op_->pread_file(reinterpret_cast<char*> (&pre_meta_info), META_INFO_SIZE, previous_offset);
        if (TFS_SUCCESS != ret)
          return ret;

        pre_meta_info.set_next_meta_offset(tmp_pos);
        ret = file_op_->pwrite_file(reinterpret_cast<const char*> (&pre_meta_info), META_INFO_SIZE, previous_offset);
        if (TFS_SUCCESS != ret)
          return ret;
      }

      // get free head list, be head.
      meta_info.set_next_meta_offset(index_header()->free_head_offset_);
      ret = file_op_->pwrite_file(reinterpret_cast<const char*> (&meta_info), META_INFO_SIZE, current_offset);
      if (TFS_SUCCESS != ret)
        return ret;
      // add to free head list, if bread down at this time, current offset will not be used for ever
      index_header()->free_head_offset_ = current_offset;

      return TFS_SUCCESS;
    }

    int IndexHandle::traverse_segment_meta(RawMetaVec& raw_metas)
    {
      int ret = TFS_SUCCESS;
      // traverse hash bucket
      for (int32_t slot = 0; slot < bucket_size(); ++slot)
      {
        // to each hash bucket slot, traverse list
        for (int32_t current_offset = bucket_slot()[slot]; current_offset != 0;)
        {
          if (current_offset >= index_header()->index_file_size_)
          {
            return EXIT_META_OFFSET_ERROR;
          }

          MetaInfo tmp_meta;
          ret = file_op_->pread_file(reinterpret_cast<char*> (&tmp_meta), META_INFO_SIZE, current_offset);
          if (TFS_SUCCESS != ret)
            return ret;
          raw_metas.push_back(tmp_meta.get_raw_meta());

          current_offset = tmp_meta.get_next_meta_offset();
        }
      }
      return TFS_SUCCESS;
    }

    int IndexHandle::traverse_sorted_segment_meta(RawMetaVec& meta)
    {
      int ret = traverse_segment_meta(meta);
      if (TFS_SUCCESS != ret)
        return ret;

      std::sort(meta.begin(), meta.end(), RawMetaSort());
      return TFS_SUCCESS;
    }

    int IndexHandle::update_block_meta(const OperType oper_type, const uint32_t modify_size)
    {
      BlockInfo* info = block_info();
      int32_t ret = (NULL != info && info->block_id_ != INVALID_BLOCK_ID) ? TFS_SUCCESS: EXIT_BLOCKID_ZERO_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = update_block_version();
        if (TFS_SUCCESS == ret)
        {
          // to each operate type, update statistics eg, version count size stuff etc
          if (C_OPER_INSERT == oper_type)
          {
            ++info->file_count_;
            info->size_ += modify_size;
          }
          else if (C_OPER_DELETE == oper_type)
          {
            ++info->del_file_count_;
            info->del_size_ += modify_size;
          }
          else if (C_OPER_UNDELETE == oper_type)
          {
            --info->del_file_count_;
            info->del_size_ -= modify_size;
          }
          else if (C_OPER_UPDATE == oper_type)
          {
            info->size_ += modify_size;
          }
        }
      }
      return ret;
    }

    int IndexHandle::update_block_version(const int8_t step)
    {
      BlockInfo* info = block_info();
      int32_t ret = (NULL != info && info->block_id_ != INVALID_BLOCK_ID) ? TFS_SUCCESS: EXIT_BLOCK_NOT_FOUND;
      if (TFS_SUCCESS == ret)
      {
        info->version_ += step;
      }
      return ret;
    }

    int IndexHandle::copy_block_info(const BlockInfo* blk_info)
    {
      if (NULL == blk_info)
      {
        return EXIT_POINTER_NULL;
      }

      memcpy(block_info(), blk_info, sizeof(BlockInfo));
      return TFS_SUCCESS;
    }

    // find key in the block
    int IndexHandle::hash_find(const uint64_t key, int32_t& current_offset, int32_t& previous_offset)
    {
      // find bucket slot
      int32_t slot = static_cast<uint32_t> (key) % bucket_size();
      previous_offset = 0;
      MetaInfo meta_info;
      int ret = TFS_SUCCESS;
      // find in the list
      for (int32_t pos = bucket_slot()[slot]; pos != 0;)
      {
        ret = file_op_->pread_file(reinterpret_cast<char*> (&meta_info), META_INFO_SIZE, pos);
        if (TFS_SUCCESS != ret)
          return ret;

        if (hash_compare(key, meta_info.get_key()))
        {
          current_offset = pos;
          return TFS_SUCCESS;
        }

        previous_offset = pos;
        pos = meta_info.get_next_meta_offset();
      }
      return EXIT_META_NOT_FOUND_ERROR;
    }

    // find key in the block by index data
    int IndexHandle::hash_find(char* data, const int data_size, const uint64_t key, int32_t& file_pos)
    {
      int ret = TFS_SUCCESS;
      file_pos = -1;
      if (NULL == data)
      {
        ret = EXIT_POINTER_NULL;
      }
      else
      {
        IndexHeader* index_header = reinterpret_cast<IndexHeader*> (data);
        int32_t bucket_size = index_header->bucket_size_;
        int32_t* bucket_slot = reinterpret_cast<int32_t*> (data + sizeof(IndexHeader));
        int32_t slot = static_cast<uint32_t> (key) % bucket_size;

        // find in the hash slot
        MetaInfo* meta_info = NULL;
        for (int32_t pos = bucket_slot[slot]; pos != 0;)
        {
          if (pos < data_size)
          {
            meta_info = reinterpret_cast<MetaInfo*> (data + pos);
            if (hash_compare(key, meta_info->get_key()))
            {
              file_pos = pos;
              break;
            }
            else
            {
              pos = meta_info->get_next_meta_offset();
            }
          }
          else
          {
            break;
          }
        }

        if (file_pos < 0)
        {
          ret = EXIT_META_NOT_FOUND_ERROR;
        }

        TBSYS_LOG(DEBUG, "degrade find file id: %"PRI64_PREFIX"u, offset: %"PRI64_PREFIX"d, size: %d, ret: %d",
            key, meta_info->get_offset(), meta_info->get_size(), ret);
      }

      return ret;
    }

    // insert meta into the tail(the current tail is previous_offset) of bucket(slot)
    int IndexHandle::hash_insert(const int32_t slot, const int32_t previous_offset, const RawMeta& meta)
    {
      int ret = TFS_SUCCESS;
      MetaInfo tmp_meta_info;
      int32_t current_offset = 0;
      // get insert offset
      // reuse the node in the free list
      if (0 != index_header()->free_head_offset_)
      {
        ret = file_op_->pread_file(reinterpret_cast<char*> (&tmp_meta_info), META_INFO_SIZE,
            index_header()->free_head_offset_);
        if (TFS_SUCCESS != ret)
          return ret;

        current_offset = index_header()->free_head_offset_;
        index_header()->free_head_offset_ = tmp_meta_info.get_next_meta_offset();
      }
      else // expand index file
      {
        current_offset = index_header()->index_file_size_;
        index_header()->index_file_size_ += META_INFO_SIZE;
      }

      MetaInfo meta_info(meta);
      ret = file_op_->pwrite_file(reinterpret_cast<const char*> (&meta_info), META_INFO_SIZE, current_offset);
      if (TFS_SUCCESS != ret)
        return ret;

      // previous_offset the last elem in the list, modify node
      if (0 != previous_offset)
      {
        ret = file_op_->pread_file(reinterpret_cast<char*> (&tmp_meta_info), META_INFO_SIZE, previous_offset);
        if (TFS_SUCCESS != ret)
          return ret;

        tmp_meta_info.set_next_meta_offset(current_offset);
        ret = file_op_->pwrite_file(reinterpret_cast<const char*> (&tmp_meta_info), META_INFO_SIZE, previous_offset);
        if (TFS_SUCCESS != ret)
          return ret;
      }
      else //the first elem in bucket slot, set slot
      {
        bucket_slot()[slot] = current_offset;
      }
      return TFS_SUCCESS;
    }

  }
}
