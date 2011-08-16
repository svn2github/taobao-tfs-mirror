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
#include <tbsys.h>
#include "common/parameter.h"
#include "block_collect.h"
#include "server_collect.h"

using namespace tfs::common;
using namespace tbsys;

namespace tfs
{
namespace nameserver
{
  const int8_t BlockCollect::HOLD_MASTER_FLAG_NO = 0x00;
  const int8_t BlockCollect::HOLD_MASTER_FLAG_YES = 0x01;
  const int8_t BlockCollect::BLOCK_CREATE_FLAG_NO = 0x00;
  const int8_t BlockCollect::BLOCK_CREATE_FLAG_YES = 0x01;
  const int8_t BlockCollect::BLOCK_IN_MASTER_SET_NO = 0x00;
  const int8_t BlockCollect::BLOCK_IN_MASTER_SET_YES = 0x01;
  const int8_t BlockCollect::VERSION_AGREED_MASK = 2;
 
  BlockCollect::BlockCollect(const uint32_t block_id, const time_t now):
    GCObject(now),
    last_update_time_(now),
    hold_master_(HOLD_MASTER_FLAG_NO),
    create_flag_(BLOCK_CREATE_FLAG_NO),
    in_master_set_(BLOCK_IN_MASTER_SET_NO)
  {
    memset(reserve, 0, sizeof(reserve));
    memset(&info_, 0, sizeof(info_));
    info_.block_id_ = block_id;
    info_.seq_no_ = 1;
  }

  bool BlockCollect::add(ServerCollect* server, const time_t now, const bool force, bool& writable)
  {
    bool bret = server != NULL;
    if (bret)
    {
      dump();
      last_update_time_ = now;
      writable = !is_full();
      bool can_be_master = ((writable && hold_master_ == HOLD_MASTER_FLAG_NO 
                && server->can_be_master(SYSPARAM_NAMESERVER.max_write_file_count_)) );
      TBSYS_LOG(DEBUG, "server: %s can_be_master: %d, block: %u writable: %d", tbsys::CNetUtil::addrToString(server->id()).c_str(), can_be_master, id(), writable);
      std::vector<ServerCollect*>::iterator where = 
          std::find(hold_.begin(), hold_.end(), server);
      if (force 
          && (!hold_.empty())
          && (hold_[0] != server))
      {
        assert(hold_[0] != NULL);
        hold_[0]->remove_master(this);
        in_master_set_ = BLOCK_IN_MASTER_SET_YES;
      }
      if (can_be_master || force)
      {
        if (where != hold_.end())
        {
          TBSYS_LOG(DEBUG,"server: %p: %s object is exist", server, CNetUtil::addrToString(server->id()).c_str());
          hold_.erase(where);
        }
        hold_.insert(hold_.begin(), server);
        hold_master_ = HOLD_MASTER_FLAG_YES;
      }
      else 
      {
        if (where == hold_.end())
        {
          hold_.push_back(server);
        }
        else
        {
          TBSYS_LOG(DEBUG,"server: %p: %s object is exist", server, CNetUtil::addrToString(server->id()).c_str());
        }

      }
      if (is_writable())
      {
        assert(hold_[0] != NULL);
        TBSYS_LOG(DEBUG,"server: %s insert master block: %u", 
            CNetUtil::addrToString(server->id()).c_str(), id());
        hold_[0]->add_master(this);
        in_master_set_ = BLOCK_IN_MASTER_SET_YES;
      }
    }
    return bret;
  }

  bool BlockCollect::remove(ServerCollect* server, const time_t now, const bool remove)
  {
    TBSYS_LOG(DEBUG, "remove block: %u" , info_.block_id_);
    if (server != NULL && !hold_.empty())
    {
      std::vector<ServerCollect*>::iterator where = find(hold_.begin(), hold_.end(), server);
      if (where == hold_.end())
      {
        TBSYS_LOG(WARN, "dataserver: %s not found in hold_", CNetUtil::addrToString(server->id()).c_str());
      }
      else
      {
        if (where == hold_.begin())//master
        {
          hold_master_ = HOLD_MASTER_FLAG_NO;
          if (remove)
          {
            in_master_set_ = BLOCK_IN_MASTER_SET_YES;
            server->remove_master(this);
          }
        }
        if (remove)
        {
          if (is_full())
          {
            server->remove_writable(this);
          }
        }

        TBSYS_LOG(DEBUG, "block: %u remove server: %s, hold_master: %d", 
            id(), tbsys::CNetUtil::addrToString(server->id()).c_str(), hold_master_);

        hold_.erase(where);
        last_update_time_ = now;

        if (is_relieve_writable_relation())
        {
          relieve_relation();
        }
      }
    }
    return true;
  }

  bool BlockCollect::exist(const ServerCollect* const server) const
  {
    if (server == NULL) return false;
    std::vector<ServerCollect*>::const_iterator iter = 
      std::find(hold_.begin(), hold_.end(), server);
    return iter != hold_.end();
  }

  ServerCollect* BlockCollect::find_master()
  {
    return (hold_.empty() || hold_master_ == HOLD_MASTER_FLAG_NO) ?  NULL : (*hold_.begin());
  }

  bool BlockCollect::is_master(const ServerCollect* const server) const
  {
    bool bret = (!hold_.empty() && hold_master_ == HOLD_MASTER_FLAG_YES);
    if (bret)
    {
      bret = (hold_[0] == server);
    }
    return bret;
  }

  bool BlockCollect::is_need_master() const
  {
    return ((!is_full())
        && (hold_master_ == HOLD_MASTER_FLAG_NO)
        && (static_cast<int32_t>(hold_.size()) >= common::SYSPARAM_NAMESERVER.min_replication_));
  }

  bool BlockCollect::is_writable() const
  {
    return ((!is_full())
        && (hold_master_ == HOLD_MASTER_FLAG_YES)
        && (static_cast<int32_t>(hold_.size()) >= common::SYSPARAM_NAMESERVER.min_replication_));
    /*if (bret)
    {
      std::vector<ServerCollect*>::const_iterator iter = hold_.begin();
      for (; iter != hold_.end(); ++iter)
      {
        assert ((*iter) != NULL);
        if ((*iter)->is_full())
        {
          bret = false;
          break;
        }
      }
    }
    return bret;*/
  }

  bool BlockCollect::is_relieve_writable_relation() const
  {
    bool bret = hold_.empty() 
      || is_full() 
      || (static_cast<int32_t>(hold_.size()) < SYSPARAM_NAMESERVER.min_replication_);
    /*if (!bret)
    {
      bool all_server_writable = true;
      std::vector<ServerCollect*>::const_iterator iter = hold_.begin();
      for (; iter != hold_.end(); ++iter)
      {
        assert(*iter != NULL);
        if ((*iter)->is_full())
        {
          all_server_writable = false;
          break;
        }
      }
      bret = !all_server_writable;
      TBSYS_LOG(DEBUG,"we will check whether the relationship between lift write, is_full: %d, all_writable: %d, hold size: %u", is_full(), all_server_writable, hold_.size());
    }*/
    return bret;
  }

  bool BlockCollect::relieve_relation(const bool remove)
  {
    std::vector<ServerCollect*>::iterator iter = hold_.begin();
    for (; iter != hold_.end(); ++iter)
    {
      if (remove)
      {
        assert(*iter != NULL);
        if (is_full())
        {
          (*iter)->remove_writable(this);//remove block form server's writable_
        }
      }
    }

    if (hold_master_ != HOLD_MASTER_FLAG_NO)//
    {
      hold_master_ = HOLD_MASTER_FLAG_NO;
      if (remove)
      {
        assert (!hold_.empty());
        assert (hold_[0] != NULL);
        in_master_set_ = BLOCK_IN_MASTER_SET_YES;
        hold_[0]->remove_master(this);
      }
    }
    return true;
  }

  bool BlockCollect::check_version(ServerCollect* server,
      const NsRole role, const bool is_new, const common::BlockInfo& new_block_info, 
      EXPIRE_BLOCK_LIST& expires, bool& force_be_master, const time_t now)
  {
    bool bret = server != NULL;
    if (bret)
    {
      const int32_t ds_size = static_cast<int32_t>(hold_.size()); 
      if ((ds_size > SYSPARAM_NAMESERVER.min_replication_)
          && (find(hold_.begin(), hold_.end(), server) == hold_.end()))
      {
        if ((info_.file_count_ > new_block_info.file_count_)
            || (info_.size_ != new_block_info.size_)) 
        {
          TBSYS_LOG(WARN, "block: %u info not match");
          if (role == NS_ROLE_MASTER)
          {
            register_expire_block(expires, server, this);
          }
          return false;
        }
      }

      //check block version
      if (__gnu_cxx::abs(info_.version_ - new_block_info.version_) <= VERSION_AGREED_MASK)//version agreed
      {
        if (((info_.version_ > new_block_info.version_)
              && (ds_size <= 0))
            || (info_.version_ <= new_block_info.version_))
        {
          memcpy(&info_, &new_block_info, sizeof(info_));
        }
      }
      else
      {
        if (info_.version_ > new_block_info.version_)// nameserver version > dataserver version
        {
          if (ds_size > 0)//has dataserver hold block, release
          {
            TBSYS_LOG(WARN, "block: %u in dataserver: %s version error %d:%d",
                new_block_info.block_id_, tbsys::CNetUtil::addrToString(server->id()).c_str(),
                info_.version_, new_block_info.block_id_);
            if (role == NS_ROLE_MASTER)
            {
              register_expire_block(expires, server, this);
            }
            return false;
          }
          else //we'll accept current version
          {
            TBSYS_LOG(WARN, "block: %u in dataserver: %s version error %d:%d, but not found dataserver",
                new_block_info.block_id_, tbsys::CNetUtil::addrToString(server->id()).c_str(),
                info_.version_, new_block_info.block_id_);
            memcpy(&info_,&new_block_info, sizeof(info_));
          }
        }
        else if ( info_.version_ < new_block_info.version_) // nameserver version < dataserver version , we'll accept new version and release all dataserver
        {
          int32_t old_version = info_.version_;
          memcpy(&info_, &new_block_info, sizeof(info_));
          if (!is_new)//release dataserver 
          {
            TBSYS_LOG(WARN, "block: %u in dataserver: %s version error %d:%d,replace ns version, current dataserver size: %u",
                new_block_info.block_id_, tbsys::CNetUtil::addrToString(server->id()).c_str(),
                old_version, new_block_info.version_, ds_size);
            if (role == NS_ROLE_MASTER)
            {
              std::vector<ServerCollect*> hold(hold_.begin(), hold_.end());
              std::vector<ServerCollect*>::iterator iter = hold.begin();
              for (; iter != hold.end(); ++iter)
              {
                ServerCollect* server = (*iter);
                remove(server, now);
                assert (server != NULL);
                server->remove(this);
                register_expire_block(expires, server, this);
                TBSYS_LOG(WARN, "release relation dataserver: %s, block: %u",
                    tbsys::CNetUtil::addrToString((*iter)->id()).c_str(), info_.block_id_);
              }
              hold_master_ = HOLD_MASTER_FLAG_NO;
              hold_.clear();
              last_update_time_ = now;
            }
          }
        }
      }

      if ((is_full())
          && (ds_size <= 0)
          && (hold_master_ == HOLD_MASTER_FLAG_NO))
      {
        TBSYS_LOG(DEBUG, "force_be_master: %s", force_be_master ? "true" : "false");
        force_be_master = true;
      }
    }
    return bret;
  }

  uint32_t BlockCollect::register_expire_block(EXPIRE_BLOCK_LIST& result, ServerCollect* server, BlockCollect* block)
  {
    bool bret = (server != NULL && block != NULL);
    if (bret)
    {
      EXPIRE_BLOCK_LIST::iterator iter = result.find(server);
      if (iter != result.end())
      {
        iter->second.push_back(block);
      }
      else
      {
        result.insert(EXPIRE_BLOCK_LIST::value_type(server, std::vector<BlockCollect*>(1, block)));
      }
    }
    return result.size();
  }

  /**
   * to check a block if replicate
   * @return: -1: none, 0: normal, 1: emergency
   */
  PlanPriority BlockCollect::check_replicate(const time_t now) const
  {
    PlanPriority priority = PLAN_PRIORITY_NONE;
    if (BLOCK_CREATE_FLAG_YES == create_flag_)
    {
      TBSYS_LOG(DEBUG, "block: %u creating, do not replicate", info_.block_id_);
    }
    else
    {
      int32_t size = static_cast<int32_t>(hold_.size());
      TBSYS_LOG(DEBUG, "size: %d, block: %u", size, this->info_.block_id_);
      if (size <= 0)
      {
        TBSYS_LOG(ERROR, "block: %u has been lost, do not replicate", info_.block_id_);
      }
      else
      {
        if (size < SYSPARAM_NAMESERVER.min_replication_)// 1 ~ min_replication_
        {
          TBSYS_LOG(DEBUG, "last update time: %"PRI64_PREFIX"d, now: %"PRI64_PREFIX"d", last_update_time_, now);
          if ((last_update_time_ + SYSPARAM_NAMESERVER.replicate_wait_time_) <= now)
          {
            TBSYS_LOG(DEBUG, "emergency replicate block: %u", info_.block_id_);
            priority = PLAN_PRIORITY_EMERGENCY;
          }
        } 
        else if ((size >= SYSPARAM_NAMESERVER.min_replication_) &&
            (size < SYSPARAM_NAMESERVER.max_replication_))
        {
          float ratio = 1.0f - static_cast<float>(size) / static_cast<float>(SYSPARAM_NAMESERVER.max_replication_);
          int32_t current = static_cast<int32_t>(ratio * 100);
          bool replicate = current >= SYSPARAM_NAMESERVER.replicate_ratio_;
          if ((last_update_time_ + SYSPARAM_NAMESERVER.replicate_wait_time_) <= now
              && replicate)
          {
            TBSYS_LOG(DEBUG, "replicate block: %u", info_.block_id_);
            priority = PLAN_PRIORITY_NORMAL;
          }
        }
      }
    }
    return priority;
  }

  /**
   * to check a block if compact
   * @return: return true if need compact
   */
  bool BlockCollect::check_compact() const
  {
    bool bret = false;
    if (BLOCK_CREATE_FLAG_YES == create_flag_)
    {
      TBSYS_LOG(DEBUG, "block: %u creating, do not compact", info_.block_id_);
    } 
    else
    {
      int32_t size = static_cast<int32_t>(hold_.size());
      //TBSYS_LOG(DEBUG, "the block: %u hold : %u dataserver < min_replication: %d, or not full: %s",
      //  info_.block_id_, hold_.size(), SYSPARAM_NAMESERVER.min_replication_, is_full()? "true":"false");
      if ((size <= 0)
          || (size < SYSPARAM_NAMESERVER.min_replication_)
          || (size > SYSPARAM_NAMESERVER.max_replication_)
          || (!is_full()))
      {
        //TBSYS_LOG(DEBUG, "the block: %u hold : %u dataserver < min_replication: %d, or not full: %s",
        //    info_.block_id_, hold_.size(), SYSPARAM_NAMESERVER.min_replication_, is_full()? "true":"false");
      }
      else
      {
        if ((info_.file_count_ <= 0)
            || (info_.size_ <= 0)
            || (info_.del_file_count_ <= 0)
            || (info_.del_size_ <= 0))
        {
          TBSYS_LOG(DEBUG, "the block: %u hold file_count: %d, size: %d, delete_file_count: %d, delete_size: %d",
              info_.block_id_, info_.file_count_, info_.size_, info_.del_file_count_, info_.del_size_);
        }
        else
        {
          int32_t delete_file_num_ratio = 
            static_cast<int32_t>(100 * static_cast<float>(info_.del_file_count_) / static_cast<float>(info_.file_count_));
          int32_t delete_size_ratio = 
            static_cast<int32_t>(100 * static_cast<float>(info_.del_size_) / static_cast<float>(info_.size_));
          if ((delete_file_num_ratio >  SYSPARAM_NAMESERVER.compact_delete_ratio_)
              || (delete_size_ratio > SYSPARAM_NAMESERVER.compact_delete_ratio_))
          {
            TBSYS_LOG(DEBUG, "block: %u need compact", info_.block_id_);
            bret = true;
          }
        }
      }
    }
    return bret;
  }

  int BlockCollect::check_redundant() const
  {
    return hold_.size() - SYSPARAM_NAMESERVER.max_replication_;
  }

  bool BlockCollect::check_balance() const
  {
    TBSYS_LOG(DEBUG, "check balance block: %u, hold: %u, is_full: %s", info_.block_id_, hold_.size(), is_full() ? "true" : "false");
    return ((static_cast<int32_t>(hold_.size()) >= SYSPARAM_NAMESERVER.min_replication_) 
        && (is_full()));
  }

  int BlockCollect::scan(SSMScanParameter& param) const
  {
    int16_t child_type = param.child_type_;
    bool has_dump = (child_type & SSM_CHILD_BLOCK_TYPE_FULL) ? is_full() : true;
    if (has_dump)
    {
      if (child_type & SSM_CHILD_BLOCK_TYPE_INFO)
      {
        param.data_.writeBytes(&info_, sizeof(info_));
      }
      if (child_type & SSM_CHILD_BLOCK_TYPE_SERVER)
      {
        param.data_.writeInt8(hold_.size());
        std::vector<ServerCollect*>::const_iterator iter = hold_.begin();
        for (; iter != hold_.end(); ++iter)
        {
          param.data_.writeInt64((*iter)->id());
        }
      }
    }
    return has_dump ? TFS_SUCCESS : TFS_ERROR;
  }

  void BlockCollect::dump() const
  {
#ifndef TFS_NS_DEBUG
    std::string str;
    std::vector<ServerCollect*>::const_iterator iter = hold_.begin();
    for (; iter != hold_.end(); ++iter)
    {
      str += CNetUtil::addrToString((*iter)->id());
      str += "/";
    }
    TBSYS_LOG(INFO, "block_id: %u, version: %d, file_count: %d, size: %d, del_file_count: %d, del_size: %d, seq_no: %d, servers: %s, hold_master: %d",
        info_.block_id_, info_.version_, info_.file_count_, info_.size_, info_.del_file_count_, info_.del_size_, info_.seq_no_, str.c_str(), hold_master_);
#endif
  }
}
}
