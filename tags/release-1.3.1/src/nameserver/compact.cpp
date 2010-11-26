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
#include <bitset>
#include <Memory.hpp>
#include "compact.h"
#include "common/error_msg.h"
#include "message/client.h"
#include "nameserver.h"

using namespace tfs::common;
using namespace tfs::message;
using namespace tbsys;

namespace tfs
{
  namespace nameserver
  {
    CompactLauncher::CompactLauncher(MetaManager & m) :
      meta_mgr_(m)
    {
      base_type::set_thread_parameter(this, 1, NULL);
    }

    CompactLauncher::~CompactLauncher()
    {
      clean();
    }

    void CompactLauncher::clean()
    {
      mutex_.wrlock();
      COMPACTING_SERVER_MAP::iterator it = compacting_ds_map_.begin();
      while (it != compacting_ds_map_.end())
      {
        delete it->second;
        ++it;
      }
      compacting_ds_map_.clear();
      compacting_block_map_.clear();
      mutex_.unlock();
    }

    bool CompactLauncher::is_compacting_time()
    {
      return Func::hour_range(SYSPARAM_NAMESERVER.compact_time_lower_, SYSPARAM_NAMESERVER.compact_time_upper_);
    }

    bool CompactLauncher::is_compacting_block(const uint32_t block_id)
    {
      ScopedRWLock lock(mutex_, READ_LOCKER);
      COMPACTING_BLOCK_MAP::iterator iter = compacting_block_map_.find(block_id);
      if (iter == compacting_block_map_.end())
      {
        TBSYS_LOG(DEBUG, "block not found in compact block map by block id(%u)", block_id);
        return false;
      }

      bool is_complete = true;
      vector < pair<uint64_t, int32_t> > &ds_status_list = iter->second;
      uint32_t ds_status_list_size = ds_status_list.size();
      for (uint32_t i = 0; i < ds_status_list_size; ++i)
      {
        pair < uint64_t, int32_t > &status = ds_status_list[i];
        if ((status.second == COMPACT_COMPLETE_STATUS_START) && check_compact_status(status.first, block_id, false,
            false) == COMPACT_STATUS_OK)
        {
          is_complete = false;
        }
      }
      return (!is_complete);
    }

    /**
     * check dataserver has running a compacting job.
     * nonlock, lock by caller
     * @param server_id : check dataserver id
     * @param block_id : check compacting block id (ignore when set 0)
     * @param complete : if handle complete mesage
     * @param timeout: erase this compacting job when found expired.
     * @return 1: time out 0: not time out -1: record is exist 
     */
    int CompactLauncher::check_compact_status(const uint64_t server_id, const uint32_t block_id, const bool complete,
        const bool timeout)
    {
      COMPACTING_SERVER_MAP::iterator iter = compacting_ds_map_.find(server_id);
      if (iter != compacting_ds_map_.end())
      {
        if ((block_id == 0) || (block_id == iter->second->block_id_))
        {
          bool is_timeout = (iter->second->start_time_ < time(NULL) - SYSPARAM_NAMESERVER.compact_preserve_time_);
          if (complete || (timeout && is_timeout))
          {
            gDelete(iter->second);
            compacting_ds_map_.erase(iter);
          }
          return is_timeout ? COMPACT_STATUS_EXPIRED : COMPACT_STATUS_OK;
        }
      }
      return COMPACT_STATUS_NOT_EXIST;
    }

    int CompactLauncher::check_compact_ds(const vector<uint64_t>& ds_list)
    {
      LayoutManager & block_ds_map = meta_mgr_.get_block_ds_mgr();
      uint32_t ds_size = ds_list.size();
      int32_t time_out_flag = 0;
      ServerCollect* server_collect = NULL;
      for (uint32_t i = 0; i < ds_size; ++i)
      {
        server_collect = block_ds_map.get_ds_collect(ds_list.at(i));
        if (server_collect == NULL)
        {
          TBSYS_LOG(WARN, "server collect not found in dataserver map by (%s), not compact", CNetUtil::addrToString(
              ds_list.at(i)).c_str());
          return EXIT_DATASERVER_NOT_FOUND;
        }
        if (server_collect->get_ds()->current_load_ > SYSPARAM_NAMESERVER.compact_max_load_)
        {
          TBSYS_LOG(WARN, "current load > compact max load, not compact", server_collect->get_ds()->current_load_,
              SYSPARAM_NAMESERVER.compact_max_load_);
          return EXIT_GENERAL_ERROR;
        }
        mutex_.wrlock();
        time_out_flag = check_compact_status(ds_list.at(i), 0, false, true);
        mutex_.unlock();
        if (time_out_flag == COMPACT_STATUS_OK)
        {
          TBSYS_LOG(WARN, "dataserver(%s) compact timeout, not compact", CNetUtil::addrToString(ds_list.at(i)).c_str());
          return EXIT_GENERAL_ERROR;
        }
      }
      return TFS_SUCCESS;
    }

    bool CompactLauncher::check(const BlockCollect* block_collect)
    {
      uint32_t count = SYSPARAM_NAMESERVER.min_replication_;

      if (!block_collect->is_full())
        return false;

      if (block_collect->get_ds()->size() != count)
        return false;

      const BlockInfo* blkinfo = block_collect->get_block_info();
      if (blkinfo->file_count_ == 0 || blkinfo->size_ == 0)
        return false;

      int32_t fr = (int32_t)(100.0 * blkinfo->del_file_count_ / blkinfo->file_count_);
      int32_t sr = (int32_t)(100.0 * blkinfo->del_size_ / blkinfo->size_);
      if (fr > SYSPARAM_NAMESERVER.compact_delete_ratio_ || sr > SYSPARAM_NAMESERVER.compact_delete_ratio_)
      {
        TBSYS_LOG(INFO, "block(%u) need compact", blkinfo->block_id_);
        return true;
      }
      return false;
    }

    int CompactLauncher::build_plan(const VUINT32& compact_block_list)
    {
      if (base_type::executing_)
      {
        TBSYS_LOG(INFO, "there's a compacting task already running!");
        return TFS_SUCCESS;
      }
      if (!is_compacting_time())
        return TFS_SUCCESS;
      base_type::push(compact_block_list);
      return TFS_SUCCESS;
    }

    int CompactLauncher::execute(vector<uint32_t>& compact_block_list, void*)
    {
      LayoutManager & block_ds_map = meta_mgr_.get_block_ds_mgr();
      vector < uint64_t > ds_list;
      ds_list.reserve(SYSPARAM_NAMESERVER.min_replication_);

      uint32_t block_size = compact_block_list.size();
      BlockCollect* block_collect = NULL;
      for (uint32_t i = 0; i < block_size; ++i)
      {
        block_collect = block_ds_map.get_block_collect(compact_block_list[i]);
        if (block_collect == NULL)
          continue;
        ds_list.assign(block_collect->get_ds()->begin(), block_collect->get_ds()->end());
        if (static_cast<int32_t> (ds_list.size()) != SYSPARAM_NAMESERVER.min_replication_)
          continue;
        if (check_compact_ds(ds_list) != TFS_SUCCESS)
          continue;
        send_compact_cmd(ds_list, block_collect->get_block_info()->block_id_);
        if (!is_compacting_time())
          break;
      }
      return TFS_SUCCESS;
    }

    int CompactLauncher::register_compact_block(const uint64_t server_id, const uint32_t block_id)
    {
      DsCompacting* c = new DsCompacting(block_id, time(NULL));
      compacting_ds_map_.insert(COMPACTING_SERVER_MAP::value_type(server_id, c));

      COMPACTING_BLOCK_MAP::iterator iter = compacting_block_map_.find(block_id);
      if (iter != compacting_block_map_.end())
      {
        iter->second.push_back(make_pair(server_id, COMPACT_COMPLETE_STATUS_START));
      }
      else
      {
        pair < uint64_t, int32_t > v(server_id, COMPACT_COMPLETE_STATUS_START);
        compacting_block_map_.insert(
            COMPACTING_BLOCK_MAP::value_type(block_id, vector<pair<uint64_t, int32_t> > (1, v)));
      }

      return TFS_SUCCESS;
    }

    int CompactLauncher::send_compact_cmd(const vector<uint64_t>& servers, uint32_t block_id)
    {
      uint32_t ds_size = servers.size();
      for (uint32_t i = 0; i < ds_size; ++i)
      {
        if (send_compact_cmd(servers.at(i), block_id, i == 0 ? 1 : 0) == TFS_SUCCESS)
        {
          mutex_.wrlock();
          register_compact_block(servers.at(i), block_id);
          mutex_.unlock();
        }
      }
      return TFS_SUCCESS;
    }

    int CompactLauncher::send_compact_cmd(uint64_t server_id, uint32_t block_id, int owner)
    {
      CompactBlockMessage cbmsg;
      cbmsg.set_block_id(block_id);
      cbmsg.set_owner(owner);
      cbmsg.set_preserve_time(SYSPARAM_NAMESERVER.compact_preserve_time_);
      int32_t ret = send_message_to_server(server_id, &cbmsg, NULL);
      TBSYS_LOG(INFO, "send compact message block (%u) owner(%d) to server(%s), ret(%d)", block_id, owner,
          CNetUtil::addrToString(server_id).c_str(), ret);
      return ret;
    }

    int CompactLauncher::handle_complete_msg(CompactBlockCompleteMessage* message)
    {
      NsRuntimeGlobalInformation* ngi = meta_mgr_.get_fs_name_system()->get_ns_global_info();
      const BlockInfo* block_info = message->get_block_info();
      CheckBlockCompactParam cbcp(message->get_server_id(), message->get_block_id(), message->get_success());
      VUINT64 ds_list;
      int iret = TFS_SUCCESS;
      //transaction message of master
      if (ngi->owner_role_ == NS_ROLE_MASTER)
      {
        iret = check_compact_complete(cbcp, ds_list, block_info);
        if (iret != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "check compact complete failed(%d)", iret);
          message->reply_message(new StatusMessage(iret));
          return iret;
        }
        iret = do_compact_complete(cbcp, ds_list, block_info);
        if (iret != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "do compact complete failed(%d)", iret);
        }
        if (iret == TFS_SUCCESS)
        {
          //transmit message
          std::bitset < 4 > bset;
          bset[0] = cbcp.all_success_;
          bset[1] = cbcp.has_success_;
          bset[2] = cbcp.is_complete_;
          bset[3] = cbcp.is_first_success_;
          message->set_flag(bset.to_ulong());
          message->set_ds_list(ds_list);
          TBSYS_LOG(DEBUG, "check compact complete flag(%d)", message->get_flag());
          meta_mgr_.get_oplog_sync_mgr()->register_msg(message);
        }
        message->reply_message(new StatusMessage(iret));
        return iret;
      }

      //Transaction Message of Slave
      std::bitset < 4 > bset(message->get_flag());
      cbcp.all_success_ = bset[0];
      cbcp.has_success_ = bset[1];
      cbcp.is_complete_ = bset[2];
      cbcp.is_first_success_ = bset[3];
      TBSYS_LOG(DEBUG, "Check Compact Complete Flag(%u)", message->get_flag());
      VUINT64& fail_ds_list = const_cast<VUINT64&> (message->get_ds_list());
      iret = do_compact_complete(cbcp, fail_ds_list, block_info);
      if (iret != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "do_compact_complete failed(%d)...", iret);
      }
      return iret;
    }

    int CompactLauncher::check_compact_complete(CheckBlockCompactParam& cbcp, VUINT64& ds_list, const BlockInfo*)
    {
      TBSYS_LOG(DEBUG, "check compacting complete in dataserver(%s) block(%u),status(%s)", CNetUtil::addrToString(
          cbcp.id_).c_str(), cbcp.block_id_, cbcp.status_ == COMPACT_COMPLETE_STATUS_SUCCESS ? "success" : "failed");

      //check block compact status
      ScopedRWLock lock(mutex_, WRITE_LOCKER);
      if (check_compact_status(cbcp.id_, cbcp.block_id_, true, false) < COMPACT_STATUS_OK)
      {
        TBSYS_LOG(ERROR, " dataserver(%s) block(%u) not found in compact map, maybe expired", CNetUtil::addrToString(
            cbcp.id_).c_str(), cbcp.block_id_);
      }

      //find information of block form compacting_block_map_
      int32_t time_out_flag = 0;
      COMPACTING_BLOCK_MAP::iterator iter = compacting_block_map_.find(cbcp.block_id_);
      if (iter == compacting_block_map_.end())
      {
        TBSYS_LOG(ERROR, "dataserver(%s) block(%u) not found in compactiong block map, maybe timeout complete message",
            CNetUtil::addrToString(cbcp.id_).c_str(), cbcp.block_id_);
        return EXIT_GENERAL_ERROR;
      }

      vector < pair<uint64_t, int32_t> > &ds_status_list = iter->second;
      for (uint32_t i = 0; i < ds_status_list.size(); ++i)
      {
        pair < uint64_t, int32_t > &status = ds_status_list[i];
        if (status.first == cbcp.id_)
        {
          if (cbcp.status_ == COMPACT_COMPLETE_STATUS_SUCCESS)
            status.second = cbcp.status_;
          else
            status.second = COMPACT_COMPLETE_STATUS_FAILED;

          if (cbcp.status_ != COMPACT_COMPLETE_STATUS_SUCCESS)
          {
            cbcp.all_success_ = false;
            ds_list.push_back(status.first);
          }
          else
          {
            cbcp.has_success_ = true;
          }
        }
        else
        {
          if (status.second == COMPACT_COMPLETE_STATUS_START)
          {
            // check that server is timeout
            time_out_flag = check_compact_status(status.first, cbcp.block_id_, false, true);
            if (time_out_flag == COMPACT_STATUS_OK)
            { // not timeout yet, wait
              cbcp.is_complete_ = false;
              cbcp.all_success_ = false;
            }
            else
            { // already timeout,
              cbcp.all_success_ = false;
              status.second = COMPACT_COMPLETE_STATUS_FAILED;
              ds_list.push_back(status.first);
            }
          }
          else if (status.second == COMPACT_COMPLETE_STATUS_FAILED)
          {
            cbcp.all_success_ = false;
            ds_list.push_back(status.first);
          }
          else if (status.second == COMPACT_COMPLETE_STATUS_SUCCESS)
          {
            cbcp.is_first_success_ = false;
          }
        } // end if (status->first..)
      } // end for

      if (cbcp.is_complete_)
      {
        TBSYS_LOG(INFO, "block(%u) all compact complete.", cbcp.block_id_);
        compacting_block_map_.erase(iter);
      }
      return TFS_SUCCESS;
    }

    int CompactLauncher::do_compact_complete(CheckBlockCompactParam& cbcp, VUINT64& ds_list,
        const BlockInfo* new_block_info)
    {
      LayoutManager& block_ds_map = meta_mgr_.get_block_ds_mgr();
      BlockChunkPtr ptr = block_ds_map.get_block_chunk(cbcp.block_id_);

      if ((cbcp.status_ == COMPACT_COMPLETE_STATUS_SUCCESS) && (cbcp.is_first_success_))
      {
        // copy new block info to meta
        ScopedRWLock scoped_lock(ptr->mutex_, WRITE_LOCKER);
        BlockCollect* block_collect = ptr->find(cbcp.block_id_);
        if ((block_collect != NULL) && (new_block_info != NULL))
        {
          memcpy(const_cast<BlockInfo*> (block_collect->get_block_info()), const_cast<BlockInfo*> (new_block_info),
              BLOCKINFO_SIZE);
          TBSYS_LOG(DEBUG,
              "check compacting complete server(%s), block(%u),copy blockinfo into metadata, block size(%d)",
              CNetUtil::addrToString(cbcp.id_).c_str(), cbcp.block_id_, new_block_info->size_);
        }
      }

      NsRuntimeGlobalInformation* ngi = meta_mgr_.get_fs_name_system()->get_ns_global_info();

      if ((cbcp.is_complete_) && (cbcp.has_success_) && (ds_list.size() > 0))
      {
        // expire block on this failed servers.
        for (uint32_t i = 0; i < ds_list.size(); ++i)
        {
          block_ds_map.release_ds_relation(cbcp.block_id_, ds_list[i]);
          if (ngi->owner_role_ == NS_ROLE_MASTER)
          {
            NameServer::rm_block_from_ds(ds_list[i], cbcp.block_id_);
          }
        }
      }

      if ((cbcp.is_complete_) && (cbcp.all_success_))
      {
        // rewritable block
        ScopedRWLock scoped_lock(block_ds_map.get_writable_mutex(), WRITE_LOCKER);
        block_ds_map.block_writable(ptr->find(cbcp.block_id_));
      }
      return TFS_SUCCESS;
    }

    int CompactLauncher::check_time_out()
    {
      if (compacting_block_map_.size() == 0)
      {
        TBSYS_LOG(INFO, "compacting map size(%u) <= 0, not check time out", compacting_block_map_.size());
        return TFS_SUCCESS;
      }

      mutex_.wrlock();
      COMPACTING_BLOCK_MAP current_compact_block_map = compacting_block_map_;
      mutex_.unlock();

      COMPACTING_BLOCK_MAP::iterator it = current_compact_block_map.begin();
      uint32_t block_id = 0;
      uint64_t server_id = 0;
      uint32_t ds_list_size = 0;
      uint32_t i = 0;
      int32_t time_out_flag = 0;
      int32_t iret = TFS_SUCCESS;
      while (it != current_compact_block_map.end())
      {
        block_id = it->first;

        vector < pair<uint64_t, int32_t> > &ds_status_list = it->second;
        ds_list_size = ds_status_list.size();
        for (i = 0; i < ds_list_size; ++i)
        {
          pair < uint64_t, int32_t > &status = ds_status_list[i];
          server_id = status.first;
          if (status.second == COMPACT_COMPLETE_STATUS_START)
          {
            mutex_.rdlock();
            time_out_flag = check_compact_status(server_id, block_id, false, false);
            mutex_.unlock();
            if (time_out_flag != COMPACT_STATUS_OK)
            {
              TBSYS_LOG(INFO, "check compact status in dataserver(%s) , block(%u), time out flag(%d)",
                  CNetUtil::addrToString(server_id).c_str(), block_id, time_out_flag);
              VUINT64 ds_list;
              CheckBlockCompactParam cbcp(block_id, server_id, COMPACT_COMPLETE_STATUS_FAILED);
              iret = check_compact_complete(cbcp, ds_list, NULL);
              if (iret != TFS_SUCCESS)
              {
                TBSYS_LOG(ERROR, "check compact complete failed, iret(%d)", iret);
              }
              iret = do_compact_complete(cbcp, ds_list, NULL);
              if (iret != TFS_SUCCESS)
              {
                TBSYS_LOG(ERROR, "do compact complete failed, iret(%d)", iret);
              }
            }
          }
        }
        ++it;
      }
      return TFS_SUCCESS;
    }
  }
}
