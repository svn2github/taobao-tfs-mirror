/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
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
#include "server_collect.h"
#include "block_collect.h"
#include "block_chunk.h"
#include "layout_manager.h"
#include "global_factory.h"
#include <tbsys.h>

using namespace tfs::common;
using namespace tbsys;

namespace tfs
{
  namespace nameserver
  {
    const int8_t ServerCollect::DUMP_FLAG_INFO = 0x1;
    const int8_t ServerCollect::DUMP_FLAG_HOLD = 0x2;
    const int8_t ServerCollect::DUMP_FLAG_WRITABLE = 0x4;
    const int8_t ServerCollect::DUMP_FLAG_MASTER = 0x8;
    const uint16_t ServerCollect:: DUMP_SLOTS_MAX = 128;
    const int8_t ServerCollect::MULTIPLE = 4;
    const int8_t ServerCollect::MAX_LOAD_DOUBLE = 2;
    const int8_t ServerCollect::AVERAGE_USED_CAPACITY_MULTIPLE = 2;

    bool ServerCollect::BlockIdComp::operator() (const BlockCollect* lhs, const BlockCollect* rhs) const
    {
      return lhs->id() < rhs->id();
    }

    ServerCollect::ServerCollect(const DataServerStatInfo& info, const time_t now):
      GCObject(now),
      DEAD_TIME(SYSPARAM_NAMESERVER.heart_interval_ * ServerCollect::MULTIPLE),
#ifdef TFS_NS_DEBUG
      total_elect_num_(0),
#endif
      id_(info.id_),
      write_byte_(0),
      read_byte_(0),
      write_count_(0),
      read_count_(0),
      unlink_count_(0),
      use_capacity_(info.use_capacity_),
      total_capacity_(info.total_capacity_),
      elect_num_(NsGlobalStatisticsInfo::ELECT_SEQ_NO_INITIALIZE),
      elect_seq_(NsGlobalStatisticsInfo::ELECT_SEQ_NO_INITIALIZE),
      startup_time_(now),
      last_update_time_(now),
      current_load_(info.current_load_ <= 0 ? 1 : info.current_load_),
      block_count_(info.block_count_),
      write_index_(0),
      status_(info.status_),
      elect_flag_(1)
      {
      }

    bool ServerCollect::add(BlockCollect* block, const bool writable)
    {
      bool bret = block != NULL;
      if (bret)
      {
        RWLock::Lock lock(*this, WRITE_LOCKER);
        std::pair<std::set<BlockCollect*>::iterator, bool> res = hold_.insert(block);
        if (!res.second)
        {
          TBSYS_LOG(WARN, "block: %u is exist", block->id());
        }

        if (writable)
        {
          std::pair<std::set<BlockCollect*>::iterator, bool> res = writable_.insert(block);
          if (!res.second)
          {
            TBSYS_LOG(WARN, "block: %u is exist, which in writable list", block->id());
          }
        }
      }
      return bret;
    }

    bool ServerCollect::remove(BlockCollect* block)
    {
      bool bret = block != NULL;
      if (bret)
      {
        TBSYS_LOG(DEBUG, "server: %s remove block: %u", CNetUtil::addrToString(id_).c_str(), block->id());
        RWLock::Lock lock(*this, WRITE_LOCKER);
        hold_.erase(block);
        writable_.erase(block);
      }
      return bret;
    }

    bool ServerCollect::add_master(BlockCollect* block)
    {
      bool bret = block != NULL;
      if (bret)
      {
        bret = !block->is_full();
        if (bret)
        {
          RWLock::Lock lock(*this, WRITE_LOCKER);
          int32_t current = static_cast<int32_t>(hold_master_.size());
          if (current < SYSPARAM_NAMESERVER.max_write_file_count_)
          {
            TBSYS_LOG(DEBUG, "server: %s add master block: %u", tbsys::CNetUtil::addrToString(id()).c_str(), block->id());
            std::vector<BlockCollect*>::iterator iter = find(hold_master_.begin(), hold_master_.end(), block);
            if (iter == hold_master_.end())
            {
              hold_master_.push_back(block);
            }
            else
            {
              TBSYS_LOG(WARN, "block: %u is exist, which in hold_master_ list", block->id());
            }
          }
        }
      }
      return bret;
    }

    bool ServerCollect::add_writable(BlockCollect* block)
    {
      bool bret = block != NULL;
      if (bret)
      {
        bret = !block->is_full();
        if (bret)
        {
          RWLock::Lock lock(*this, WRITE_LOCKER);
          std::pair<std::set<BlockCollect*>::iterator, bool > res = writable_.insert(block);
          if (!res.second)
          {
            TBSYS_LOG(INFO, "block: %u object is exist in writable list", block->id());
          }
        }
      }
      return bret;
    }

    bool ServerCollect::remove_master(BlockCollect* block)
    {
      bool bret = block != NULL;
      if (bret)
      {
        RWLock::Lock lock(*this, WRITE_LOCKER);
        std::vector<BlockCollect*>::iterator iter = find(hold_master_.begin(), hold_master_.end(), block);
        if (iter != hold_master_.end())
        {
          hold_master_.erase(iter);
        }
        TBSYS_LOG(DEBUG, "server: %s remove master block: %u", CNetUtil::addrToString(id_).c_str(), block->id());
      }
      return bret;
    }

    bool ServerCollect::remove_writable(BlockCollect* block)
    {
      bool bret = block != NULL;
      if (bret)
      {
        RWLock::Lock lock(*this, WRITE_LOCKER);
        writable_.erase(block);
        TBSYS_LOG(DEBUG, "server: %s remove writable block: %u", CNetUtil::addrToString(id_).c_str(), block->id());
      }
      return bret;
    }

    bool ServerCollect::clear(LayoutManager& manager, const time_t now)
    {
      bool remove = false;
      //release any of blocks relation with this dataserver
      std::set<BlockCollect*, BlockIdComp> tmp;
      {
        RWLock::Lock lock(*this, WRITE_LOCKER);
        tmp = hold_;
        hold_.clear();
        hold_master_.clear();
        writable_.clear();
      }

      std::set<BlockCollect*>::const_iterator iter = tmp.begin();
      for (; iter != tmp.end(); ++iter)
      {
        BlockChunkPtr ptr = manager.get_chunk((*iter)->id());
        RWLock::Lock lock(*ptr, WRITE_LOCKER);
        BlockCollect* block= (*iter);
        //unwrite block if not enough dataserver relate it
        bool bret = block->remove(this, now, remove);
        if (!bret)
        {
          TBSYS_LOG(ERROR, "failed when relieve between block: %u and dataserver: %s",
              block->id(), CNetUtil::addrToString(id()).c_str());
        }
      }
      return true;
    }

    BlockCollect* ServerCollect::elect_write_block()
    {
      RWLock::Lock lock(*this, READ_LOCKER);
      BlockCollect* result = NULL;
      int32_t count = static_cast<int32_t>(hold_master_.size());
      if (count > 0
          && atomic_dec(&elect_flag_) == 0)
      {
        int64_t loop = 0;
        do
        {
          ++loop;
          if (write_index_ >= count)
            write_index_ = 0;
          BlockCollect* block = hold_master_[write_index_];
          ++write_index_;
          if ((block == NULL)
              || (block->is_full())
              || (GFactory::get_lease_factory().has_valid_lease(block->id())))
          {
            continue;
          }
          result = block;
          break;
        }
        while (loop < count);

        TBSYS_LOG(DEBUG, "server: %s hold master size: %d, write index: %d",
            CNetUtil::addrToString(id()).c_str(), count, write_index_);
        atomic_exchange(&elect_flag_, 0x01);
      }
      return result;
    }

    BlockCollect* ServerCollect::force_elect_write_block(void)
    {
      RWLock::Lock lock(*this, READ_LOCKER);
      BlockCollect* result = NULL;
      TBSYS_LOG(DEBUG, "write_index: %d, hold_master size: %u", write_index_, hold_master_.size());
      uint32_t size = hold_master_.size();
      if (size > 0U)
      {
        TBSYS_LOG(DEBUG, "write_index: %d: %u", write_index_, size );
        write_index_ = rand() % size;
        result = hold_master_[write_index_];
      }
      return result;
    }

    int ServerCollect::scan(SSMScanParameter& param, const int8_t scan_flag)
    {
      RWLock::Lock lock(*this, READ_LOCKER);
      if (scan_flag & SSM_CHILD_SERVER_TYPE_INFO)
      {
#ifdef TFS_NS_DEBUG
        param.data_.writeInt64(total_elect_num_);
#endif
        param.data_.writeInt64(id_);
        param.data_.writeInt64(use_capacity_);
        param.data_.writeInt64(total_capacity_);
        param.data_.writeInt32(current_load_);
        param.data_.writeInt32(block_count_);
        param.data_.writeInt64(last_update_time_);
        param.data_.writeInt64(startup_time_);
        param.data_.writeInt64(write_byte_);
        param.data_.writeInt64(write_count_);
        param.data_.writeInt64(read_byte_);
        param.data_.writeInt64(read_count_);
        param.data_.writeInt64(time(NULL));
        param.data_.writeInt32(status_);
      }

      if (scan_flag & SSM_CHILD_SERVER_TYPE_HOLD)
      {
        param.data_.writeInt32(hold_.size());
        std::set<BlockCollect*>::const_iterator iter = hold_.begin();
        for (; iter != hold_.end(); ++iter)
        {
          param.data_.writeInt32((*iter)->id());
        }
      }

      if (scan_flag & SSM_CHILD_SERVER_TYPE_WRITABLE)
      {
        param.data_.writeInt32(writable_.size());
        std::set<BlockCollect*>::const_iterator iter = writable_.begin();
        for (; iter != writable_.end(); ++iter)
        {
          param.data_.writeInt32((*iter)->id());
        }
      }

      if (scan_flag & SSM_CHILD_SERVER_TYPE_MASTER)
      {
        TBSYS_LOG(DEBUG,"server : %s, hold_master_: %u", CNetUtil::addrToString(id_).c_str(),hold_master_.size());
        param.data_.writeInt32(hold_master_.size());
        std::vector<BlockCollect*>::const_iterator iter = hold_master_.begin();
        for (; iter != hold_master_.end(); ++iter)
        {
          param.data_.writeInt32((*iter)->id());
        }
      }
      return TFS_SUCCESS;
    }

    void ServerCollect::dump() const
    {

    }

    /**
     * to check a dataserver if writable
     */
    bool  ServerCollect::is_writable(const int64_t average_used_capacity) const
    {
      bool bret = true;
      if (is_full())
      {
        TBSYS_LOG(DEBUG, "dataserver: %s disk is full", CNetUtil::addrToString(id_).c_str());
        bret = false;
      }
      else if (use_capacity_ > (average_used_capacity * AVERAGE_USED_CAPACITY_MULTIPLE))
      {
        TBSYS_LOG(DEBUG, "dataserver: %s can't write that capactiy > average capactiy",
            CNetUtil::addrToString(id_).c_str());
        bret = false;
      }
      return bret;
    }

    bool ServerCollect::touch(LayoutManager& manager, const time_t now, bool& promote, int32_t& count)
    {
      bool bret = true;
      if (in_safe_mode_time(now))
      {
        count = 0;
      }
      else
      {
        if (!promote)
        {
          count = 0;
        }
        else
        {
          if (!is_full())
          {
            int32_t current = 0;
            {
              RWLock::Lock lock(*this, READ_LOCKER);
              current = static_cast<int32_t>(hold_master_.size());
            }
            if (current < SYSPARAM_NAMESERVER.max_write_file_count_)
            {
              bool add_new_block = true;
              count = std::min(count, (SYSPARAM_NAMESERVER.max_write_file_count_ - current));
              if (count <= 0)
              {
                TBSYS_LOG(INFO, "%s", "there is any block need add into hold_master_");
              }
              else
              {
                int32_t should = count * 16;
                BlockCollect* block = NULL;
                std::vector<BlockCollect*> writable;
                {
                  RWLock::Lock lock(*this, READ_LOCKER);
                  std::set<BlockCollect*>::iterator iter = writable_.begin();
                  while ( iter != writable_.end() && should > 0)
                  {
                    block = *iter++;
                    std::vector<BlockCollect*>::iterator where 
                      = std::find(hold_master_.begin(), hold_master_.end(), block);
                    if (where == hold_master_.end()
                        && (block->is_writable()// block is writable
                          && (!block->in_master_set())
                          || block->is_need_master())) 
                    {
                      --should;
                      writable.push_back(block);
                    }
                  }
                }

                should = count;

                ServerCollect* server = NULL;
                std::vector<BlockCollect*> master;
                BlockChunkPtr ptr = 0;
                std::vector<BlockCollect*>::const_iterator iter = writable.begin();
                while (iter != writable.end() && should > 0)
                {
                  block = *iter++;
                  ptr = manager.get_chunk(block->id());
                  RWLock::Lock lock(*ptr, WRITE_LOCKER);
                  if (block->is_need_master())
                  {
                    bool writable = false;
                    block->add(this, now, true, writable);
                  }
                  server = block->find_master();
                  if ((block->is_writable())
                      && (!block->in_master_set())
                      && (server != NULL)
                      && (server == this))
                  {
                    --should;
                    master.push_back(block);
                  }
                }

                int32_t acutal = 0;
                RWLock::Lock lock(*this, WRITE_LOCKER);
                iter = master.begin();
                while(iter != master.end())
                {
                  block = *iter++;
                  std::vector<BlockCollect*>::iterator where 
                    = find(hold_master_.begin(), hold_master_.end(), block);
                  if (where == hold_master_.end())
                  {
                    ++acutal;
                    hold_master_.push_back(block);
                  }
                }
                count = !add_new_block ? 0 : count - acutal;
              }
            }
            else
            {
              count = 0;
              //TBSYS_LOG(INFO, "dataserer: %s hold_master: %d >= max_write_file_count: %u, can't add new block",
              //    tbsys::CNetUtil::addrToString(id()).c_str(), current, SYSPARAM_NAMESERVER.max_write_file_count_);
            }
          }
          else
          {
            count = 0;
            TBSYS_LOG(WARN, " dataserver: %s dsik is full, can't add new block", CNetUtil::addrToString(id()).c_str());
          }
        }
      }
      return bret;
    }

    bool ServerCollect::is_readable(const int32_t average_load) const
    {
      return (is_alive() && current_load_ < average_load * MAX_LOAD_DOUBLE);
    }

    void ServerCollect::statistics(NsGlobalStatisticsInfo& stat, const bool is_new)
    {
      if (is_new)
      {
        stat.use_capacity_ += use_capacity_;
        stat.total_capacity_ += total_capacity_;
        stat.total_load_ += current_load_;
        stat.total_block_count_ += block_count_;
        stat.alive_server_count_ += 1;
      }
      stat.max_load_ = std::max(current_load_, stat.max_load_);
      stat.max_block_count_ = std::max(block_count_, stat.max_block_count_);
    }

    void ServerCollect::update(const common::DataServerStatInfo& info, const time_t now, const bool is_new)
    {
      RWLock::Lock lock(*this, WRITE_LOCKER);
      id_ = info.id_;
      use_capacity_ = info.use_capacity_;
      total_capacity_ = info.total_capacity_;
      current_load_ = info.current_load_;
      block_count_ = info.block_count_;
      last_update_time_ = now;
      startup_time_ = is_new ? now : info.startup_time_;
      status_ = info.status_;
      read_count_ = info.total_tp_.read_file_count_;
      read_byte_ = info.total_tp_.read_byte_;
      write_count_ = info.total_tp_.write_file_count_;
      write_byte_ = info.total_tp_.write_byte_;
#ifdef TFS_NS_DEBUG
      char buf[128] ={'\0'};
      tbsys::CTimeUtil::timeToStr(startup_time_, buf);
      TBSYS_LOG(DEBUG, "server: %s use_capacity: %"PRI64_PREFIX"d, total_capacity: %"PRI64_PREFIX"d, current_load: %d, block_count: %d,startup_time: %s, read_count: %"PRI64_PREFIX"d, read_byte: %"PRI64_PREFIX"d, write_count: %"PRI64_PREFIX"d, write_byte: %"PRI64_PREFIX"d",
          tbsys::CNetUtil::addrToString(id()).c_str(), use_capacity_, total_capacity_, current_load_, block_count_, buf, read_count_, read_byte_, write_count_, write_byte_); 
#endif
    }

    bool ServerCollect::can_be_master(const int32_t max_write_block_count)
    {
      RWLock::Lock lock(*this, READ_LOCKER);
      return static_cast<int32_t>(hold_master_.size()) < max_write_block_count;
    }

    void ServerCollect::callback(LayoutManager* manager)
    {
      if (manager != NULL)
      {
        clear(*manager, time(NULL));
      }
    }
  } /** nameserver **/
}/** tfs **/
