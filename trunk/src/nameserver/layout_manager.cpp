/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   qushan<qushan@taobao.com> 
 *      - modify 2009-03-27
 *   duanfei <duanfei@taobao.com> 
 *      - modify 2010-04-23
 *
 */
#include <Memory.hpp>
#include "layout_manager.h"

using namespace tfs::common;
using namespace tbsys;

namespace tfs
{
  namespace nameserver
  {

    LayoutManager::LayoutManager() :
      block_map_array_(NULL), max_block_id_(0), currnet_elect_seq_(0x01), alive_ds_size_(0), block_chunk_num_(13)
    {
      init(13);
    }

    LayoutManager::LayoutManager(int32_t chunk_num) :
      block_map_array_(NULL), max_block_id_(0), currnet_elect_seq_(0x01), alive_ds_size_(0),
          block_chunk_num_(chunk_num)
    {
      init(chunk_num);
    }

    int LayoutManager::init(int32_t chunk_num)
    {
      block_chunk_num_ = chunk_num;
      memset(&global_info_, 0, sizeof(NsGlobalInfo));
      tbsys::gDeleteA( block_map_array_);
      block_map_array_ = new BlockChunkPtr[chunk_num];
      for (int32_t i = 0; i < chunk_num; ++i)
      {
        block_map_array_[i] = new BlockChunk();
      }
      return TFS_SUCCESS;
    }

    LayoutManager::~LayoutManager()
    {
      tbsys::gDeleteA( block_map_array_);
      clear_ds();
    }

    /**
     * locate specific block map
     */
    BlockChunkPtr LayoutManager::get_block_chunk(const uint32_t block_id) const
    {
      return block_map_array_[block_id % block_chunk_num_];
    }

    /**
     * clear all DataServerStatInfo objects.
     * no need for lock ? cause only called by ~LayoutManager()
     */
    void LayoutManager::clear_ds()
    {
      SERVER_MAP_ITER it = ds_map_.begin();
      for (; it != ds_map_.end(); ++it)
      {
        tbsys::gDelete(it->second);
      }
      ds_map_.clear();
    }

    /**
     * calculate all alive DataServerStatInfos count.
     */
    int32_t LayoutManager::get_alive_ds_size()
    {
      ScopedRWLock scoped_lock(server_mutex_, READ_LOCKER);
      return std::count_if(ds_map_.begin(), ds_map_.end(), AliveDataServer());
    }

    /**
     * found specific BlockCollect object by block_id
     * ** read lock **
     */
    BlockCollect *LayoutManager::get_block_collect(const uint32_t block_id) const
    {
      BlockChunkPtr ptr = get_block_chunk(block_id);
      return ptr->find(block_id);
    }

    /**
     * create new BlockCollect object base on block_id
     * @return if not found, return NULL
     * ** write lock **
     */
    BlockCollect *LayoutManager::create_block_collect(uint32_t block_id)
    {
      if (0 == block_id)
      {
        block_id = get_avail_block_id();
      }
      BlockChunkPtr ptr = get_block_chunk(block_id);
      return ptr->create(block_id);
    }

    /**
     * remove a BlockCollect object by block_id
     * ** write lock **
     */
    bool LayoutManager::remove_block_collect(const uint32_t block_id)
    {
      BlockChunkPtr ptr = get_block_chunk(block_id);
      return ptr->remove(block_id);
    }

    /**
     * find DataServerStatInfo ServerCollect object by server_id
     * @return if not found or DataServerStatInfo is dead, return NULL
     */
    ServerCollect*LayoutManager::get_ds_collect(const uint64_t server_id) const
    {
      SERVER_MAP::const_iterator it = ds_map_.find(server_id);
      if ((it == ds_map_.end()) || (!it->second->is_alive()))
      {
        return NULL;
      }
      return it->second;
    }

    /**
     * get DataServerStatInfo ServerCollect object by server_id
     * if DataServerStatInfo exist and alive, return it; otherwise create one then set renew [true]
     * @return ServerCollect object, always not NULL, except bad::alloc
     */
    ServerCollect *LayoutManager::get_ds_collect(const uint64_t server_id, bool& renew)
    {
      renew = false;
      SERVER_MAP_ITER it = ds_map_.find(server_id);
      ServerCollect* server_collect = NULL;
      DataServerStatInfo *ds_stat_info = NULL;
      if (it == ds_map_.end())
      {
        ARG_NEW(server_collect, ServerCollect);
        ds_stat_info = server_collect->get_ds();
        ds_stat_info->id_ = server_id;
        ds_map_.insert(SERVER_MAP::value_type(server_id, server_collect));
        renew = true;
        return server_collect;
      }

      server_collect = it->second;
      if (server_collect->is_alive())
      {
        return server_collect;
      }

      renew = true;
      server_collect->reset();
      ds_stat_info = server_collect->get_ds();
      ds_stat_info->id_ = server_id;
      return server_collect;
    }

    /**
     * remove DataServerStatInfo ServerCollect object base on server_id
     * this op don't erase object actually, only set it's dead state.
     * @return true if exist, otherwise false.
     */
    bool LayoutManager::remove_ds_collect(const uint64_t server_id)
    {
      SERVER_MAP_ITER it = ds_map_.find(server_id);
      if (it != ds_map_.end() && it->second->is_alive())
      {
        it->second->dead();
        --alive_ds_size_;
        return true;
      }
      return false;
    }

    /**
     * remove block_id from writable_block_list_, make it unwritable
     * @return true if exist in writable_block_list_.
     */
    bool LayoutManager::remove_writable_block(const uint32_t block_id)
    {
      VUINT32::iterator where = find(writable_block_list_.begin(), writable_block_list_.end(), block_id);
      if (where != writable_block_list_.end())
      {
        writable_block_list_.erase(where);
        return true;
      }
      return false;
    }

    /**
     * check block_id if exist writable_block_list_ ?
     * @return true if exist writable_block_list_
     */
    bool LayoutManager::is_writable(const uint32_t block_id) const
    {
      VUINT32::const_iterator where = find(writable_block_list_.begin(), writable_block_list_.end(), block_id);
      if (where != writable_block_list_.end())
      {
        return true;
      }
      return false;
    }

    /**
     * add block_id into writable_block_list_, make it writable.
     */
    bool LayoutManager::add_writable_block(const uint32_t block_id)
    {
      VUINT32::const_iterator where = find(writable_block_list_.begin(), writable_block_list_.end(), block_id);
      if (where == writable_block_list_.end())
      {
        writable_block_list_.push_back(block_id);
        return true;
      }
      return false;
    }

    /**
     * build relation between DataServerStatInfo and block
     * @param master, if true force server_id be master even if block full or server full
     */
    bool LayoutManager::build_ds_block_relation(const uint32_t block_id, const uint64_t server_id, bool force)
    {
      bool ds_writable = false;
      bool block_writable = false;
      bool can_be_master = false;
      ServerCollect* server_collect = NULL;
      BlockCollect* block_collect = NULL;

      //check dataserver
      {
        ScopedRWLock scoped_lock(server_mutex_, READ_LOCKER);
        SERVER_MAP_ITER it = ds_map_.find(server_id);
        if ((it == ds_map_.end()) || (it->second == NULL) || (!it->second->is_alive()))
        {
          return false;
        }

        server_collect = it->second;
        ds_writable = server_collect->get_primary_writable_block_list()->size()
            < static_cast<uint32_t> (SYSPARAM_NAMESERVER.max_write_file_count_);
      }

      {
        BlockChunkPtr ptr = get_block_chunk(block_id);
        ScopedRWLock scoped_lock(ptr->mutex_, WRITE_LOCKER);
        block_collect = ptr->find(block_id);
        block_writable = (!block_collect->is_full());
        if ((block_writable) && (ds_writable) && (block_collect->get_master_ds() == 0))
        {
          can_be_master = true;
        }

        ptr->connect(block_id, server_id, can_be_master || force);

        TBSYS_LOG(DEBUG, "block_writable(%s), ds_writable(%s), master_ds(%" PRI64_PREFIX "u), is_writable(%s), can_be_master(%s)",
            block_writable ? "true" : "false", ds_writable ? "true" : "falase", block_collect->get_master_ds(),
            is_writable(block_collect) ? "true" : "false", can_be_master || force ? "true" : "false");
        // can add to WriatbleList
        if (block_writable && is_writable(block_collect))
        {
          ScopedRWLock scoped_lock(writable_mutex_, WRITE_LOCKER);
          add_writable_block(block_id);
        }
      }

      // add to server's all kind of list
      {
        ScopedRWLock scoped_lock(server_mutex_, WRITE_LOCKER);
        server_collect->join_block(block_id);
        if (block_writable)
          server_collect->join_writable_block(block_id);
        if (can_be_master)
          server_collect->join_primary_writable_block(block_id);
      }
      return true;
    }

    /**
     * release relation between DataServerStatInfo and block
     * oppsite function to build_ds_block_relation
     */
    bool LayoutManager::release_ds_relation(uint32_t block_id, uint64_t server_id)
    {
      ServerCollect* server_collect = NULL;

      {
        ScopedRWLock scoped_lock(server_mutex_, READ_LOCKER);
        SERVER_MAP_ITER it = ds_map_.find(server_id);
        if (it == ds_map_.end() /*|| !it->second->is_alive()*/)
        {
          return false;
        }
        server_collect = it->second;
      }

      BlockChunkPtr ptr = get_block_chunk(block_id);
      {
        ScopedRWLock scoped_lock(ptr->mutex_, WRITE_LOCKER);
        BlockCollect* block_collect = ptr->find(block_id);
        if (block_collect == NULL)
          return false;
        ptr->release(block_id, server_id);
        if (!is_writable(block_collect))
        {
          ScopedRWLock scoped_lock(writable_mutex_, WRITE_LOCKER);
          remove_writable_block(block_id);
        }
      }

      // TODO  clean from dataserver's block list;
      {
        ScopedRWLock scoped_lock(server_mutex_, WRITE_LOCKER);
        server_collect->release(block_id);
      }
      return true;
    }

    /*
     * block is full, need to be release write relation,
     * and remove from wriatble list.
     */
    bool LayoutManager::release_block_write_relation(const uint32_t block_id)
    {
      VUINT64 ds_list;
      uint64_t master = 0;

      BlockChunkPtr ptr = get_block_chunk(block_id);
      {
        ScopedRWLock scoped_lock(ptr->mutex_, WRITE_LOCKER);
        BlockCollect* block_collect = ptr->find(block_id);
        if (block_collect == NULL)
          return false;

        if (!is_writable(block_collect))
        {
          ScopedRWLock scoped_lock(writable_mutex_, WRITE_LOCKER);
          remove_writable_block(block_id);
        }

        master = block_collect->get_master_ds();
        block_collect->set_master_ds(0);
        ds_list = *block_collect->get_ds();
      }

      ServerCollect* server_collect = NULL;
      ScopedRWLock scoped_lock(server_mutex_, WRITE_LOCKER);
      const uint32_t ds_list_length = ds_list.size();
      for (uint32_t i = 0; i < ds_list_length; ++i)
      {
        server_collect = get_ds_collect(ds_list[i]);
        if (server_collect != NULL)
        {
          server_collect->leave_writable_block(block_id);
          if (server_collect->get_ds()->id_ == master)
          {
            server_collect->leave_primary_writable_block(block_id);
          }
        }
      }
      return true;
    }

    bool LayoutManager::release_ds_relation(const uint64_t server_id)
    {
      ServerCollect * server_collect = NULL;
      std::set < uint32_t > blocks;
      {
        ScopedRWLock scoped_lock(server_mutex_, READ_LOCKER);
        SERVER_MAP_ITER it = ds_map_.find(server_id);
        if (it == ds_map_.end())
          return false;
        server_collect = it->second;
        if (server_collect == NULL)
          return false;
        // copy all blocks belongs to this DataServer
        blocks = server_collect->get_block_list();
      }

      // release any of blocks relation with this DataServer
      std::set<uint32_t>::const_iterator iter = blocks.begin();
      for (; iter != blocks.end(); ++iter)
      {
        BlockChunkPtr ptr = get_block_chunk(*iter);
        ScopedRWLock scoped_lock(ptr->mutex_, WRITE_LOCKER);
        ptr->release(*iter, server_id);
        if (ptr->find(*iter)->get_ds()->size() < static_cast<uint32_t> (common::SYSPARAM_NAMESERVER.min_replication_))
        {
          // unwrite block if not enough DataServer relate it
          ScopedRWLock scoped_lock(writable_mutex_, WRITE_LOCKER);
          remove_writable_block(*iter);
        }
      }

      // clean all blocks list & writable block list & primary writable block list
      {
        ScopedRWLock scoped_lock(server_mutex_, WRITE_LOCKER);
        // server_collect object nerver deleted, so no need to refind
        server_collect->clear_all();
      }
      return true;
    }

    /**
     * to check a block if writable satisfied two conditions.
     * 1: related to common::SYSPARAM_NAMESERVER.min_replication_ DataServerStatInfos.
     * 2: all related DataServerStatInfos has empty space
     */
    bool LayoutManager::is_writable(const BlockCollect* block_collect) const
    {
      TBSYS_LOG(DEBUG, "block(%u) is writabe, is full(%s) , master_ds(%" PRI64_PREFIX "u)",
          block_collect->get_block_info()->block_id_, block_collect->is_full() ? "true" : "false",
          block_collect->get_master_ds());
      if ((block_collect != NULL) && (!block_collect->is_full()) && (block_collect->get_master_ds() != 0))
      {
        const VUINT64* ds_list = block_collect->get_ds();
        if (ds_list->size() < static_cast<uint32_t> (common::SYSPARAM_NAMESERVER.min_replication_))
        {
          TBSYS_LOG(DEBUG, "block(%u) copy count(%u) < min_replication_(%d), can't join writable block list",
              block_collect->get_block_info()->block_id_, ds_list->size(), SYSPARAM_NAMESERVER.min_replication_);
          return false;
        }
        uint32_t ds_list_length = ds_list->size();
        ServerCollect* server_collect = NULL;
        for (uint32_t i = 0; i < ds_list_length; ++i)
        {
          server_collect = get_ds_collect(ds_list->at(i));
          if ((server_collect == NULL) || (server_collect->is_disk_full()))
          {
            TBSYS_LOG(DEBUG, "ds full or server_collect == NULLL , can't join writable block list");
            return false;
          }
        }
        return true;
      }
      return false;
    }

    bool LayoutManager::block_writable(const BlockCollect* block_collect)
    {
      if (block_collect == NULL)
        return false;

      if (is_writable(block_collect))
      {
        return add_writable_block(block_collect->get_block_info()->block_id_);
      }
      return false;
    }

    bool LayoutManager::server_writable(const ServerCollect* server_collect) const
    {
      if ((server_collect == NULL) || (server_collect->is_disk_full()))
      {
        return false;
      }

      if ((ds_map_.size() <= 0) && (global_info_.use_capacity_ > 0) && (server_collect->get_ds()->use_capacity_
          >= (global_info_.use_capacity_ / alive_ds_size_) * 2))
      {
        return false;
      }
      return true;
    }

    int32_t LayoutManager::join_ds(const DataServerStatInfo & ds_info, bool & isnew)
    {
      ScopedRWLock scoped_lock(server_mutex_, WRITE_LOCKER);
      uint64_t serverId = ds_info.id_;
      ServerCollect* server_collect = get_ds_collect(serverId, isnew);
      if (server_collect == NULL)
        return TFS_ERROR;
      DataServerStatInfo *ds_stat_info = server_collect->get_ds();
      if (ds_stat_info == NULL)
        return TFS_ERROR;
      memcpy(ds_stat_info, &ds_info, sizeof(DataServerStatInfo));

      ds_stat_info->last_update_time_ = time(NULL);

      if (isnew)
        ++alive_ds_size_;

      return TFS_SUCCESS;
    }

    void LayoutManager::update_global_info(const DataServerStatInfo & ds_info, bool isnew)
    {
      ScopedRWLock scoped_lock(global_mutex_, WRITE_LOCKER);
      if (isnew)
      {
        global_info_.use_capacity_ += ds_info.use_capacity_;
        global_info_.total_capacity_ += ds_info.total_capacity_;
        global_info_.total_load_ += ds_info.current_load_;
        global_info_.total_block_count_ += ds_info.block_count_;
        global_info_.alive_server_count_ += 1;
      }
      if (ds_info.current_load_ > global_info_.max_load_)
        global_info_.max_load_ = ds_info.current_load_;

      if (ds_info.block_count_ > global_info_.max_block_count_)
        global_info_.max_block_count_ = ds_info.block_count_;
    }

    int32_t LayoutManager::check_ds(const time_t ds_dead_time, VUINT64 &dead_ds_list, VUINT64 &writable_ds_list)
    {
      dead_ds_list.clear();
      writable_ds_list.clear();

      NsGlobalInfo stat_info;
      memset(&stat_info, 0, sizeof(NsGlobalInfo));

      int32_t now = time(NULL) - ds_dead_time;
      int32_t check_write_count = std::max(1, SYSPARAM_NAMESERVER.max_write_file_count_);

      {
        ScopedRWLock scoped_lock(server_mutex_, READ_LOCKER);
        SERVER_MAP_ITER iter = ds_map_.begin();
        ServerCollect *server_collect = NULL;
        DataServerStatInfo* ds_stat_info = NULL;
        for (; iter != ds_map_.end(); ++iter)
        {
          server_collect = iter->second;
          if (server_collect == NULL || !server_collect->is_alive())
            continue;

          ds_stat_info = server_collect->get_ds();
          if (ds_stat_info->last_update_time_ < now)
          {
            dead_ds_list.push_back(ds_stat_info->id_);
          }
          else
          {
            stat_info.use_capacity_ += ds_stat_info->use_capacity_;
            stat_info.total_capacity_ += ds_stat_info->total_capacity_;
            stat_info.total_load_ += ds_stat_info->current_load_;
            stat_info.total_block_count_ += ds_stat_info->block_count_;
            stat_info.alive_server_count_ += 1;

            if (ds_stat_info->current_load_ > stat_info.max_load_)
              stat_info.max_load_ = ds_stat_info->current_load_;
            if (ds_stat_info->block_count_ > stat_info.max_block_count_)
              stat_info.max_block_count_ = ds_stat_info->block_count_;

            if (static_cast<int32_t> (server_collect->get_primary_writable_block_list()->size()) < check_write_count)
              writable_ds_list.push_back(ds_stat_info->id_);
          }
        }
      }

      // write to NsGlobalInfo
      {
        ScopedRWLock scoped_lock(global_mutex_, WRITE_LOCKER);
        memcpy(&global_info_, &stat_info, sizeof(NsGlobalInfo));
      }

      return dead_ds_list.size();
    }

    uint32_t LayoutManager::calc_max_block_id()
    {
      max_block_id_ = 0;
      uint32_t current = 0;
      for (int32_t i = 0; i < block_chunk_num_; ++i)
      {
        current = block_map_array_[i]->calc_max_block_id();
        if (max_block_id_ < current)
          max_block_id_ = current;
      }
      max_block_id_ += 100;
      return max_block_id_;
    }

    uint32_t LayoutManager::get_avail_block_id()
    {
      ++max_block_id_;
      while (true)
      {
        BlockChunkPtr ptr = get_block_chunk(max_block_id_);
        ScopedRWLock scoped_lock(ptr->mutex_, READ_LOCKER);
        if (!ptr->exist(max_block_id_))
          break;
        ++max_block_id_;
      }

      return max_block_id_;
    }

    /**
     * foreach every BlockCollect in block_map_array_
     */
    int LayoutManager::foreach(const MetaScanner & scanner) const
    {
      int32_t ret = 0;
      for (int32_t i = 0; i < block_chunk_num_; ++i)
      {
        const BLOCK_MAP& current = block_map_array_[i]->get_block_map();
        BLOCK_MAP::const_iterator it = current.begin();
        for (; it != current.end(); ++it)
        {
          ret = scanner.process(it->second);
          if (ret != MetaScanner::CONTINUE)
            return ret;
        }
      }
      return ret;
    }

    int64_t LayoutManager::cacl_all_block_bytes() const
    {
      int64_t ret = 0;
      for (int32_t i = 0; i < block_chunk_num_; ++i)
      {
        ret += block_map_array_[i]->calc_all_block_bytes();
      }
      return ret;
    }

    bool LayoutManager::insert(const BlockCollect* block_collect, bool overwrite)
    {
      if (block_collect == NULL)
        return TFS_ERROR;
      uint32_t block_id = block_collect->get_block_info()->block_id_;
      if (block_id <= 0)
        return TFS_ERROR;
      BlockChunkPtr ptr = get_block_chunk(block_id);
      return ptr->insert(block_collect, overwrite);
    }

    void LayoutManager::sort()
    {
      ScopedRWLock scoped_lock(writable_mutex_, WRITE_LOCKER);
      std::sort(writable_block_list_.begin(), writable_block_list_.end());
    }
  }
}

