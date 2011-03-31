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
#include "common/error_msg.h"
#include "message/client.h"
#include "meta_manager.h"
#include "nameserver.h"
#include "strategy.h"
#include <numeric>

using namespace tfs::common;
using namespace tfs::message;
using namespace std;

namespace tfs
{
  namespace nameserver
  {

    MetaManager::MetaManager(NameServer* fsns) :
      fs_name_system_(fsns), oplog_sync_mgr_(this), last_rotate_log_time_(0), current_writing_index_(0)
    {
      tzset();
      zonesec_ = 86400 + timezone;
    }

    MetaManager::~MetaManager()
    {

    }

    // init block chunk and log sync manager
    int MetaManager::initialize(const int32_t chunk_num)
    {
      meta_mgr_.init(chunk_num);
      const int iret = oplog_sync_mgr_.initialize(meta_mgr_);
      if (iret != TFS_SUCCESS)
      {
        return iret;
      }
      meta_mgr_.calc_max_block_id();
      return TFS_SUCCESS;
    }

    // save the snapshot of nameserver info
    int MetaManager::save()
    {
      return oplog_sync_mgr_.get_ns_fs_image()->save(meta_mgr_);
    }

    // backup restore point
    int MetaManager::checkpoint()
    {
      time_t now = time(NULL);
      if ((now % 86400 >= zonesec_) && (now % 86400 < zonesec_ + 300) && (last_rotate_log_time_ < now - 600))
      {
        last_rotate_log_time_ = now;
        if (oplog_sync_mgr_.rotate() != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "slave fsimage failed");
        }
        else
        {
          TBSYS_LOG(ERROR, "slave fsimage success");
        }
        TBSYS_LOGGER.rotateLog(SYSPARAM_NAMESERVER.log_file_.c_str());
      }
      return TFS_SUCCESS;
    }

    // get the latest dataserver stat, check whether the ds is new
    // Parameters:
    // @param [out] ds_info: DataServerStatInfo system info, like capacity, load, etc..
    // @param [out] is_new: mark whether the ds is new
    // @return sucess or failure
    int MetaManager::join_ds(const DataServerStatInfo& ds_info, bool& is_new)
    {
      int iret = meta_mgr_.join_ds(ds_info, is_new);
      if (iret != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "ds(%s) join faild", tbsys::CNetUtil::addrToString(ds_info.id_).c_str());
        return iret;
      }
      meta_mgr_.update_global_info(ds_info, is_new);
      return TFS_SUCCESS;
    }

    // register block to the expire list, each ds keep one expire block list
    // parameters:
    // @param [out] expire_list: expire block list
    // @param [in] ds_id: ds id
    // @param [in] block_id: block id
    // return the size of expire block list
    uint32_t MetaManager::register_expire_block(EXPIRE_BLOCK_LIST & expire_list, const uint64_t ds_id,
        const uint32_t block_id)
    {
      EXPIRE_BLOCK_LIST::iterator it = expire_list.find(ds_id);
      if (it != expire_list.end())
        it->second.push_back(block_id);
      else
        expire_list.insert(EXPIRE_BLOCK_LIST::value_type(ds_id, vector<uint32_t> (1, block_id)));
      return expire_list.size();
    }

    /**
     * MetaManager::report_blocks
     *
     * DataServerStatInfo start, send heartbeat message to nameserver.
     *
     * Parameters:
     * @param [in] dsInfo: DataServerStatInfo system info , like capacity, load, etc..
     * @param [in] blocks: data blocks' info which belongs to DataServerStatInfo.
     * @param [out] expires: need expire blocks
     *
     * @return  success or failure
     */
    int MetaManager::report_blocks(const uint64_t ds_id, const vector<BlockInfo>& blocks, EXPIRE_BLOCK_LIST& expires)
    {
      NsRuntimeGlobalInformation * ngi = fs_name_system_->get_ns_global_info();

      // check whether ds info has been reported
      meta_mgr_.get_server_mutex().rdlock();
      ServerCollect* server_collect = meta_mgr_.get_ds_collect(ds_id);
      if ((server_collect == NULL) || (!server_collect->is_alive()))
      {
        meta_mgr_.get_server_mutex().unlock();
        TBSYS_LOG(ERROR, "dataserver(%s)report,can't find server collect", tbsys::CNetUtil::addrToString(ds_id).c_str());
        return EXIT_DATASERVER_NOT_FOUND;
      }
      meta_mgr_.get_server_mutex().unlock();

      uint32_t ds_list_size = meta_mgr_.get_ds_size();
      uint32_t blocks_size = blocks.size();
      bool force_be_master = false;
      bool first = false;
      VUINT64 expire_ds_list;

      meta_mgr_.release_ds_relation(ds_id);

      for (uint32_t i = 0; i < blocks_size; ++i)
      {
        const BlockInfo & new_block_info = blocks.at(i);
        if (new_block_info.block_id_ == 0)
        {
          TBSYS_LOG(WARN, "dataserver(%s) report, block = 0", tbsys::CNetUtil::addrToString(ds_id).c_str());
          continue;
        }
        expire_ds_list.clear();

        first = false;

        force_be_master = false;

        // check block version, rebuilding metadata.
        BlockChunkPtr ptr = meta_mgr_.get_block_chunk(new_block_info.block_id_);
        {
          ScopedRWLock scoped_lock(ptr->mutex_, WRITE_LOCKER);
          BlockCollect *block_collect = ptr->find(new_block_info.block_id_);
          if (block_collect == NULL)
          {
            TBSYS_LOG(INFO, "block(%u) not found in dataserver(%s), must be create",
                new_block_info.block_id_, tbsys::CNetUtil::addrToString(ds_id).c_str());
            block_collect = ptr->create(new_block_info.block_id_);
            first = true;
          }

          const VUINT64* ds_list = block_collect->get_ds();

          const int32_t current_block_ds_size = static_cast<int32_t> (ds_list->size());

          // check block info
          const BlockInfo *block_info = block_collect->get_block_info();
          if ((current_block_ds_size > SYSPARAM_NAMESERVER.min_replication_)
              && (find(ds_list->begin(), ds_list->end(), ds_id) == ds_list->end()))
          {
            if ((block_info->file_count_ > new_block_info.file_count_)
                || (block_info->file_count_ <= new_block_info.file_count_
                  && block_info->size_ != new_block_info.size_))
            {
              TBSYS_LOG(WARN, "block info not match");
              if (ngi->owner_role_ == NS_ROLE_MASTER)
                register_expire_block(expires, ds_id, block_info->block_id_);
              continue;
            }
          }
          /*if (((lease_mgr_.has_valid_lease(new_block_info.block_id_))
            && (find(ds_list->begin(), ds_list->end(), ds_id) == ds_list->end()))
            || ((current_block_ds_size > SYSPARAM_NAMESERVER.min_replication_)
            && ((block_info->file_count_ > new_block_info.file_count_)
            ||(block_info->file_count_ <= new_block_info.file_count_
            && block_info->size_ != new_block_info.size_))))
            {
            TBSYS_LOG(WARN, "block info not match");
            if (ngi->owner_role_ == NS_ROLE_MASTER)
            register_expire_block(expires, ds_id, block_info->block_id_);
            continue;
            }*/

          // check version
          if (__gnu_cxx::abs(block_info->version_ - new_block_info.version_) > 2)
          {
            if (block_info->version_ > new_block_info.version_)
            {
              if (current_block_ds_size > 0)
              {
                TBSYS_LOG(WARN, "block(%u) in dataserver(%s) version error(%d:%d)",
                    new_block_info.block_id_, tbsys::CNetUtil::addrToString(ds_id).c_str(),
                    block_info->version_, new_block_info.block_id_);
                if (ngi->owner_role_ == NS_ROLE_MASTER)
                  register_expire_block(expires, ds_id, block_info->block_id_);
                continue;
              }
              else
              {
                TBSYS_LOG(WARN, "block(%u) in dataserver(%s) version error(%d:%d), but not found dataserver",
                    new_block_info.block_id_, tbsys::CNetUtil::addrToString(ds_id).c_str(),
                    block_info->version_, new_block_info.block_id_);
                memcpy(const_cast<BlockInfo*> (block_info), const_cast<BlockInfo*> (&new_block_info), BLOCKINFO_SIZE);
              }
            }
            else if (block_info->version_ < new_block_info.version_)
            {
              int32_t old_version = block_info->version_;
              memcpy(const_cast<BlockInfo*>(block_info), const_cast<BlockInfo*> (&new_block_info), BLOCKINFO_SIZE);
              if (!first)
              {
                TBSYS_LOG(WARN, "block(%u) in dataserver(%s) version error(%d:%d),replace ns version, current dataserver size(%u)",
                    new_block_info.block_id_, tbsys::CNetUtil::addrToString(ds_id).c_str(),
                    old_version, new_block_info.version_, ds_list->size());
                if (ngi->owner_role_ == NS_ROLE_MASTER)
                {
                  expire_ds_list = *ds_list;
                  for (uint32_t k = 0; k < expire_ds_list.size(); ++k)
                  {
                    uint64_t other_server_id = expire_ds_list.at(k);
                    register_expire_block(expires, other_server_id, block_info->block_id_);
                    int32_t ret = ptr->release(new_block_info.block_id_, other_server_id);
                    TBSYS_LOG(WARN, "release relation dataserver(%s), block(%u), result(%d)",
                        tbsys::CNetUtil::addrToString(other_server_id).c_str(), block_info->block_id_, ret);
                  }
                }
              }
            }
          }
          else if (block_info->version_ < new_block_info.version_)
          {
            memcpy(const_cast<BlockInfo*> (block_info), const_cast<BlockInfo*> (&new_block_info), BLOCKINFO_SIZE);
          }

          if ((block_collect != NULL)
              && (block_collect->is_full())
              && (ds_list_size > 0)
              && (new_block_info.block_id_ % ds_list_size == ds_list_size - 1))
          {
            force_be_master = true;
          }
          TBSYS_LOG(DEBUG, "force_be_master(%s), is_full(%s), ds_list_size(%u), mode=%d", force_be_master ? "true": "false",
              block_collect->is_full() ? "true" : "fase", ds_list_size, new_block_info.block_id_ % ds_list_size);
        }

        // release the block in expire ds
        for (uint32_t k = 0; k < expire_ds_list.size(); ++k)
        {
          if (ds_id == expire_ds_list.at(k))
            continue;
          ScopedRWLock scoped_lock(meta_mgr_.get_server_mutex(), WRITE_LOCKER);
          server_collect= meta_mgr_.get_ds_collect(expire_ds_list.at(k));
          if (server_collect != NULL)
            server_collect->release(new_block_info.block_id_);
          TBSYS_LOG(WARN, "release relation dataserver(%s), block(%u)",
              tbsys::CNetUtil::addrToString(ds_id).c_str(), new_block_info.block_id_);
        }

        meta_mgr_.build_ds_block_relation(new_block_info.block_id_, ds_id, force_be_master);

        TBSYS_LOG(DEBUG, "dataserver(%s) report: block(%u),version(%d),filecount(%d),"
            "size(%d), delete_file_count(%d), delete_size(%d), seqno(%u), dataserver count(%u), "
            "block count(%u), writable blocks(%u), primary write block count(%u), total writable block count(%u)",
            tbsys::CNetUtil::addrToString(ds_id).c_str(),
            new_block_info.block_id_, new_block_info.version_, new_block_info.file_count_,
            new_block_info.size_, new_block_info.del_file_count_, new_block_info.del_size_,
            new_block_info.seq_no_, ptr->find(new_block_info.block_id_)->get_ds()->size(),
            server_collect->get_block_list().size(), server_collect->get_writable_block_list()->size(),
            server_collect->get_primary_writable_block_list()->size(), meta_mgr_.get_writable_block_list().size());
      }
      meta_mgr_.sort();
      return TFS_SUCCESS;
    }

    /**
     * MetaManager::downDataServer
     *
     * DataServerStatInfo exit, when nameserver cannot keep touch with DataServerStatInfo.
     *
     * Parameters:
     * @param [in] serverid: DataServerStatInfo's id (ip&port)
     *
     * @return: success or failure
     */
    int MetaManager::leave_ds(const uint64_t ds_id)
    {
      // release all relations of blocks belongs to it
      meta_mgr_.release_ds_relation(ds_id);
      // then remove ServerCollect object, actually set it to dead state.
      ScopedRWLock scoped_lock(meta_mgr_.get_server_mutex(), WRITE_LOCKER);
      meta_mgr_.remove_ds_collect(ds_id);

      return TFS_SUCCESS;
    }

    /**
     * MetaManager::read_block_info
     *
     * client read: get block 's location of DataServerStatInfo
     * only choose DataServerStatInfo with normal load
     *
     * Parameters:
     * @param [in] block_id: query block id, must be a valid blockid
     * @param [out] ds_list: block location of DataServerStatInfos.
     *
     * @return: success or failure
     */
    int MetaManager::read_block_info(const uint32_t block_id, VUINT64& ds_list)
    {
      if (meta_mgr_.get_ds_size() == 0)
      {
        TBSYS_LOG(ERROR, "dataserver(%u) not found", meta_mgr_.get_ds_size());
        return EXIT_NO_DATASERVER;
      }

      std::vector<ServerCollect*> ds_collect_list;
      BlockChunkPtr ptr = meta_mgr_.get_block_chunk(block_id);
      {
        ScopedRWLock scoped_lock(ptr->mutex_, READ_LOCKER);

        BlockCollect* block_collect = ptr->find(block_id);

        if (block_collect == NULL)
        {
          TBSYS_LOG(ERROR, "block(%u) not exist", block_id);
          return EXIT_BLOCK_NOT_FOUND;
        }

        const VUINT64* ds_id_list = block_collect->get_ds();
        const uint32_t ds_id_list_size = ds_id_list->size();
        ServerCollect* server_collect = NULL;
        for (uint32_t i = 0; i < ds_id_list_size; ++i)
        {
          server_collect = meta_mgr_.get_ds_collect(ds_id_list->at(i));
          if (server_collect != NULL)
            ds_collect_list.push_back(server_collect);
        }
      }

      if (ds_collect_list.size() == 0)
      {
        TBSYS_LOG(ERROR, "dataserver not found by block(%u)", block_id);
        return EXIT_NO_DATASERVER;
      }

      // choose ds
      int32_t average_load = accumulate(ds_collect_list.begin(), ds_collect_list.end(), 0, AddLoad())
        / ds_collect_list.size();

      const uint32_t ds_collect_list_size = ds_collect_list.size();
      for (uint32_t i = 0; i < ds_collect_list_size; ++i)
      {
        if (ds_collect_list[i]->get_ds()->current_load_ < average_load * 2)
          ds_list.push_back(ds_collect_list[i]->get_ds()->id_);
      }
      return TFS_SUCCESS;
    }

    // return an available block id
    uint32_t MetaManager::elect_write_block(const VINT64& fail_ds)
    {
      uint32_t block_id = 0;
      uint32_t second_block_id = 0;
      int32_t second_index = 0;

      // get all writable blocks
      ScopedRWLock scoped_lock(meta_mgr_.get_writable_mutex(), WRITE_LOCKER);
      const VUINT32& writable_block_list = meta_mgr_.get_writable_block_list();
      int32_t writable_block_list_size = static_cast<int32_t>(writable_block_list.size());
      uint32_t fail_ds_size = fail_ds.size();

      TBSYS_LOG(DEBUG, "current writable block list size(%u), failed dataserver size(%u)",
          writable_block_list_size, fail_ds_size);

      ServerCollect* ds_collect = NULL;
      if (writable_block_list_size > 0)
      {
        if (current_writing_index_ >= writable_block_list_size)
          current_writing_index_ = 0;

        for (int32_t i = 0; (block_id == 0) && (i < writable_block_list_size); ++i)
        {
          block_id = writable_block_list.at(current_writing_index_);

          //block is in use, turn to next
          if (lease_mgr_.has_valid_lease(block_id))
          {
            ++current_writing_index_;
            if (current_writing_index_ >= writable_block_list_size)
              current_writing_index_ = 0;
            second_block_id = block_id;
            second_index = current_writing_index_;
            block_id = 0;
            continue;
          }

          for (uint32_t j = 0; j < fail_ds_size; ++j)
          {
            ds_collect = meta_mgr_.get_ds_collect(fail_ds.at(j));
            if (ds_collect == NULL)
              continue;

            const std::set<uint32_t>& blocks = ds_collect->get_block_list();

            if (blocks.find(block_id) != blocks.end())
            {
              block_id = 0;
              ++current_writing_index_;
              if (current_writing_index_ >= writable_block_list_size)
                current_writing_index_ = 0;
              TBSYS_LOG(INFO, "block(%u) in failed dataserer(%s)",
                  block_id, tbsys::CNetUtil::addrToString(fail_ds.at(j)).c_str());
              break;
            }
          }
          ++current_writing_index_;
          if (current_writing_index_ >= writable_block_list_size)
            current_writing_index_ = 0;
        }
      }

      if (block_id == 0)
      {
        if (second_block_id == 0)
        {
          TBSYS_LOG(ERROR, "there's no any writable block id(%u), index(%d)",
              writable_block_list_size, current_writing_index_);
        }
        else
        {
          block_id = second_block_id;
          current_writing_index_ = (second_index + rand()) % writable_block_list_size;
          TBSYS_LOG(WARN, "block(%u) thers's no free writable block, return a busy one, index(%d)",
              block_id, current_writing_index_);
        }
      }
      return block_id;
    }

    /**
     * MetaManager::write_block_info
     *
     * client read: get block 's location of DataServerStatInfo
     * client write: get new block and location of DataServerStatInfo
     *
     * Parameters:
     * @param [in] block_id: query block id, in write mode
     * can be set to zero for assign a new write block id.
     * @param [in] mode: read | write
     * @param [out] lease_id: write transaction id only for write mode.
     * @param [out] ds_list: block location of DataServerStatInfos.
     *
     * @return: success or failure
     */
    int MetaManager::write_block_info(uint32_t& block_id, int32_t mode, uint32_t& lease_id, int32_t& version,
        VUINT64& ds_list)
    {
      if (meta_mgr_.get_ds_size() == 0)
      {
        TBSYS_LOG(ERROR, "dataserver(%u) not found", meta_mgr_.get_ds_size());
        return EXIT_NO_DATASERVER;
      }

      //nameserver assign a new write block
      if ((block_id == 0)
          && (mode & BLOCK_CREATE))
      {
        VINT64 fail_ds;
        block_id = elect_write_block(fail_ds);
        if (block_id == 0)
        {
          TBSYS_LOG(ERROR, "elect write block faild...");
          return EXIT_NO_BLOCK;
        }
      }

      BlockChunkPtr ptr = meta_mgr_.get_block_chunk(block_id);

      ptr->mutex_.rdlock();

      BlockCollect* block_collect = ptr->find(block_id);

      // create block in slave nameserver.
      if ((mode & BLOCK_NEWBLK))
      {
        if (block_collect == NULL)
        {
          ptr->mutex_.unlock();
          block_collect = add_new_block(block_id);
          if (block_collect == NULL)
          {
            TBSYS_LOG(ERROR, "add new block(%u) failed, dataserver not found", block_id);
            return EXIT_NO_DATASERVER;
          }
          ptr->mutex_.rdlock();
          TBSYS_LOG(DEBUG, "block(%u), not found meta data, add new(%p)", block_id, block_collect);
        }
        else if (block_collect->get_ds()->size() == 0)
        {
          if (block_collect->get_creating_flag())
          {
            ptr->mutex_.unlock();
            TBSYS_LOG(ERROR, "block(%u) found meta data, but creating by another thread, must be return", block_id);
            return EXIT_NO_BLOCK;
          }
          else
          {
            ptr->mutex_.unlock();
            TBSYS_LOG(ERROR, "block(%u) found meta data, but no dataserver hold it.", block_id);
            return EXIT_NO_DATASERVER;
          }
        }
      }

      // now check blockcollect object if has any dataserver.
      if (block_collect == NULL)
      {
        ptr->mutex_.unlock();
        TBSYS_LOG(ERROR, "add new block(%u) failed, dataserver not found", block_id);
        return EXIT_NO_BLOCK;
      }

      if (block_collect->get_ds()->size() == 0)
      {
        ptr->mutex_.unlock();
        TBSYS_LOG(ERROR, "add new block(%u) failed, dataserver not found", block_id);
        return EXIT_NO_DATASERVER;
      }

      version = block_collect->get_block_info()->version_;

      ds_list.assign(block_collect->get_ds()->begin(), block_collect->get_ds()->end());

      ptr->mutex_.unlock();

      // register a lease for write..
      if (!(mode & BLOCK_NOLEASE))
      {
        lease_id = lease_mgr_.register_lease(block_id);
        if (lease_id == WriteLease::INVALID_LEASE)
        {
          TBSYS_LOG(ERROR, "lease not found by block id(%u)", block_id);
          return EXIT_CANNOT_GET_LEASE;
        }
      }
      //std::string dsList = OpLogSyncManager::printDsList(ds_list);
      //TBSYS_LOG(DEBUG, "elect current write block(%d), dsList(%s)", block_id, dsList.c_str());
      return TFS_SUCCESS;
    }

    /**
     * MetaManager::write_commit
     *
     * Write commit operation, nameserver confirm this write op and update meta info.
     *
     * @param [in] new_block_info: new block info when write finished.
     * @param [in] lease_id: write transaction id
     * @param [in] success: write op status.
     * @param [out] neednew: commit block full, or error, need a new block for write.
     * @param [out] errmsg: error message when commit failed.
     *
     * @return: success or failure
     */
    int MetaManager::write_commit(const BlockInfo& new_block_info, const uint64_t ds_id,
        const uint32_t lease_id, const UnlinkFlag unlink_flag, const WriteCompleteStatus status,
        bool & neednew, string & errmsg)
    {
      // get block collect
      BlockChunkPtr ptr = meta_mgr_.get_block_chunk(new_block_info.block_id_);
      ptr->mutex_.rdlock();
      BlockCollect* block_collect = ptr->find(new_block_info.block_id_);
      if (block_collect == NULL)
      {
        ptr->mutex_.unlock();
        TBSYS_LOG(ERROR, "not found block collect by block id(%u) when write commit", new_block_info.block_id_);
        errmsg.assign("cannot found commit block");
        return EXIT_BLOCK_NOT_FOUND;
      }

      int32_t last_version = block_collect->get_block_info()->version_;
      ptr->mutex_.unlock();

      int32_t ret = TFS_SUCCESS;
      if (status != WRITE_COMPLETE_STATUS_YES)
      {
        TBSYS_LOG(WARN, "write operation error, block(%u), lease(%u)", new_block_info.block_id_, lease_id);
        if (!lease_mgr_.write_commit(new_block_info.block_id_, lease_id, WriteLease::FAILED))
        {
          TBSYS_LOG(ERROR, "block(%u) lease(%u) write commit failed", new_block_info.block_id_, lease_id);
          ret = EXIT_COMMIT_ERROR;
        }
        return ret;
      }

      // wrong version
      if ((last_version >= new_block_info.version_ && unlink_flag == UNLINK_FLAG_NO)
          || (last_version > new_block_info.version_ && unlink_flag == UNLINK_FLAG_YES))
      {
        TBSYS_LOG(ERROR, "write operation error, block(%u), lease(%u), version(%d >= %d) , unlink falg(%d)",
            new_block_info.block_id_, lease_id, last_version, new_block_info.version_, unlink);
        return EXIT_COMMIT_ERROR;
      }

      WriteLease::LeaseStatus commit_status = WriteLease::DONE;
      if ((BlockCollect::is_full(new_block_info.size_))
          && (unlink_flag == UNLINK_FLAG_NO))
      {
        commit_status = WriteLease::OBSOLETE;
        neednew = true;
      }

      if (!lease_mgr_.write_commit(new_block_info.block_id_, lease_id, commit_status))
      {
        TBSYS_LOG(ERROR, "block(%u) lease(%u) write commit failed", new_block_info.block_id_, lease_id);
        return EXIT_COMMIT_ERROR;
      }

      if (unlink_flag == UNLINK_FLAG_YES)
        return ret;

      // update block info
      ret = update_block_info(new_block_info, ds_id, false);
      if (ret != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "update meta failed, block(%u), lease(%u)", new_block_info.block_id_, lease_id);
      }
      return ret;
    }

    int MetaManager::update_block_info(const BlockInfo& new_block_info, const uint64_t ds_id, bool addnew)
    {
      int32_t ret = EXIT_GENERAL_ERROR;
      // update master DataServerStatInfo
      meta_mgr_.get_server_mutex().wrlock();

      ServerCollect* server_collect = meta_mgr_.get_ds_collect(ds_id);

      if (server_collect != NULL)
        server_collect->get_ds()->last_update_time_ = time(NULL);

      meta_mgr_.get_server_mutex().unlock();

      BlockChunkPtr ptr = meta_mgr_.get_block_chunk(new_block_info.block_id_);
      ptr->mutex_.wrlock();

      BlockCollect* block_collect = ptr->find(new_block_info.block_id_);

      bool isnew = false;
      if (block_collect == NULL && addnew)
      {
        block_collect = ptr->create(new_block_info.block_id_);
        isnew = true;
      }

      if (block_collect == NULL)
      {
        ptr->mutex_.unlock();
        TBSYS_LOG(ERROR, "update_block_info failed, block:%u not found", new_block_info.block_id_);
        return EXIT_BLOCK_NOT_FOUND;
      }

      if (block_collect->get_block_info()->version_ < new_block_info.version_)
      {
        memcpy(const_cast<BlockInfo*> (block_collect->get_block_info()), const_cast<BlockInfo*> (&new_block_info),
            BLOCKINFO_SIZE);
        ret = TFS_SUCCESS;
      }
      else
      {
        TBSYS_LOG(ERROR, "update_block_info error, block:%u, old version(%d)>=new version(%d)",
            new_block_info.block_id_, block_collect->get_block_info()->version_, new_block_info.version_);
      }
      ptr->mutex_.unlock();

      if (TFS_SUCCESS == ret)
      {
        VUINT64 dsList;
        dsList.push_back(ds_id);
        if (oplog_sync_mgr_.log(&new_block_info, isnew ? OPLOG_INSERT : OPLOG_UPDATE, dsList) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "log failed, block(%u), isnew(%s)", new_block_info.block_id_, isnew == OPLOG_INSERT ? "insert" : isnew == OPLOG_UPDATE ? "update" : "unknow");
        }
      }

      if (TFS_SUCCESS == ret && isnew && server_collect)
      {
        meta_mgr_.build_ds_block_relation(new_block_info.block_id_, ds_id, false);
      }
      if (TFS_SUCCESS == ret && BlockCollect::is_full(new_block_info.size_))
      {
        meta_mgr_.release_block_write_relation(new_block_info.block_id_);
      }
      return ret;
    }

    /**
     * MetaManager::add_new_block
     *
     * add_new_block for write
     *
     * Parameters:
     * @param [in] block_id: add new block id. if 0, find available block id
     * @param [in] ds_id: if not 0, treat it as master write server
     *
     * @return: blockcollect obj pointer of this block
     */
    BlockCollect* MetaManager::add_new_block(uint32_t& block_id, const uint64_t ds_id)
    {
      vector < uint64_t > elect_ds_list;
      if (ds_id != 0)
      {
        ServerCollect * server_collect = meta_mgr_.get_ds_collect(ds_id);
        if ((server_collect != NULL)
            && (meta_mgr_.server_writable(server_collect)))
        {
          elect_ds_list.push_back(ds_id);
        }
        else
        {
          TBSYS_LOG(DEBUG, "add_new_block : server %s not writable", tbsys::CNetUtil::addrToString(ds_id).c_str());
          return NULL;
        }
      }

      uint32_t need_ds_size = SYSPARAM_NAMESERVER.min_replication_ - elect_ds_list.size();
      TBSYS_LOG(DEBUG, "add new block , need_ds_size(%u)", elect_ds_list.size());

      if (need_ds_size > 0)
      {
        ScopedRWLock scoped_lock(meta_mgr_.get_server_mutex(), WRITE_LOCKER);

        elect_write_ds(meta_mgr_, need_ds_size, elect_ds_list);

        //if (elect_ds_list.size() == 0x00)
        if (static_cast<int32_t>(elect_ds_list.size()) < SYSPARAM_NAMESERVER.min_replication_)
        {
          TBSYS_LOG(ERROR, "there's no any dataserver can be writable.");
          return NULL;
        }
      }

      // find a usable block id, TODO lock
      if (0 == block_id)
        block_id = meta_mgr_.get_avail_block_id();

      BlockChunkPtr ptr = meta_mgr_.get_block_chunk(block_id);

      ptr->mutex_.wrlock();

      BlockCollect* block_collect = ptr->create(block_id);

      const BlockInfo *block_info = block_collect->get_block_info();

      block_collect->set_creating_flag(BlockCollect::BLOCK_CREATE_FLAG_YES);

      uint32_t new_block_id = block_info->block_id_;

      ptr->mutex_.unlock();

      // insert a new block, write op log.
      if (oplog_sync_mgr_.log(block_info, OPLOG_INSERT, elect_ds_list) != TFS_SUCCESS)
      {
        // treat log operation as a trivial thing..
        // don't rollback the insert operation, cause block meta info
        // build from all DataServerStatInfos, log info not been very serious.
        TBSYS_LOG(WARN, "LogBlockInfo Fail, block_id: %u", block_id);
      }

      VUINT64 add_success_ds_list;
      // send add new block message to DataServerStatInfo
      for (uint32_t i = 0; i < elect_ds_list.size(); ++i)
      {
        TBSYS_LOG(DEBUG, "dataserver(%s)", tbsys::CNetUtil::addrToString(elect_ds_list[i]).c_str());
        NewBlockMessage nbmsg;
        nbmsg.add_new_id(new_block_id);
        if (send_message_to_server(elect_ds_list[i], &nbmsg, NULL) == TFS_SUCCESS)
        {
          add_success_ds_list.push_back(elect_ds_list[i]);
          TBSYS_LOG(INFO, "add block:%u on server:%s succeed", new_block_id, tbsys::CNetUtil::addrToString(
                elect_ds_list[i]).c_str());
        }
        else
        {
          TBSYS_LOG(INFO, "add block:%u on server:%s failed", new_block_id, tbsys::CNetUtil::addrToString(
                elect_ds_list[i]).c_str());
        }
      }

      // rollback;
      if (add_success_ds_list.size() == 0)
      {
        oplog_sync_mgr_.log(block_info, OPLOG_REMOVE, add_success_ds_list);
        ptr->mutex_.wrlock();
        ptr->remove(new_block_id);
        ptr->mutex_.unlock();
        TBSYS_LOG(ERROR, "add block(%u) failed, rollback", new_block_id);
        return NULL;
      }

      for (uint32_t i = 0; i < add_success_ds_list.size(); ++i)
      {
        meta_mgr_.build_ds_block_relation(new_block_id, add_success_ds_list[i], false);
      }

      ptr->mutex_.wrlock();
      block_collect->set_creating_flag();
      ptr->mutex_.unlock();

      return block_collect;
    }

    int MetaManager::check_primary_writable_block(const uint64_t ds_id, const int32_t add_block_count, bool promote)
    {
      ServerCollect* server_collect = meta_mgr_.get_ds_collect(ds_id);
      int32_t need_add_block_count = 0;
      if (server_collect != NULL)
      {
        if (server_collect->is_disk_full())
          return 0;

        // check whether need to add block
        int32_t current = static_cast<int32_t> (server_collect->get_primary_writable_block_list()->size());
        if (current >= SYSPARAM_NAMESERVER.max_write_file_count_)
        {
          TBSYS_LOG(INFO, "check primary writableblock in dataserver(%s), current_primary_block_count(%u) >= max_write_file_count(%d), no need to add new block",
              tbsys::CNetUtil::addrToString(ds_id).c_str(), current, SYSPARAM_NAMESERVER.max_write_file_count_);
          return 0;
        }
        need_add_block_count = std::min(add_block_count, (SYSPARAM_NAMESERVER.max_write_file_count_ - current));

        TBSYS_LOG(INFO, "check primary writableblock in dataserver(%s), current primary block count(%u), need add block count(%d)",
            tbsys::CNetUtil::addrToString(ds_id).c_str(), current, need_add_block_count);
      }

      if (need_add_block_count > 0)
      {
        // first, get the block from writable block
        if (promote)
          promote_primary_write_block(server_collect, need_add_block_count);

        if (fs_name_system_->get_ns_global_info()->owner_role_ != NS_ROLE_MASTER)
          return 0;

        uint32_t next_block_id = meta_mgr_.get_max_block_id() + 0x01;
        int32_t alive_ds_size = meta_mgr_.get_alive_ds_size();
        uint32_t max_block_id = server_collect->get_max_block_id();
        uint32_t diff = __gnu_cxx::abs(next_block_id - max_block_id);
        if ((diff < static_cast<uint32_t> (add_block_count)) && (alive_ds_size > 0x01))
        {
          TBSYS_LOG(INFO, "next_block_id(%u) - max_block_id(%d) <= add_block_count(%d),"
              "can't add new block in this dataserver",
              next_block_id, max_block_id, add_block_count);
          return 0;
        }

        // if block is still not enough, then add new block
        uint32_t new_block_id = 0;
        for (int32_t i = 0; i < need_add_block_count; ++i, new_block_id = 0)
        {
          add_new_block(new_block_id, ds_id);
        }
      }
      return need_add_block_count;
    }

    // add some writable blocks to primary writable block
    // Parameter:
    // @param [in] server_collect: ds block info collect
    // @param [out] need_count: block number need to add
    // return success or failure.
    int MetaManager::promote_primary_write_block(const ServerCollect* server_collect, int32_t& need_count)
    {
      if (server_collect == NULL)
      {
        TBSYS_LOG(ERROR, "dataserver not exist, server_collect is null");
        return EXIT_NO_DATASERVER;
      }

      std::set < uint32_t > writable_blocks = *server_collect->get_writable_block_list();
      std::set < uint32_t > primary_writable_blocks = *server_collect->get_primary_writable_block_list();

      if (writable_blocks.size() <= primary_writable_blocks.size())
      {
        TBSYS_LOG(WARN, "not found primary writable block, writable_block_size(%u) <= primary_writable_block_size(%u)",
            writable_blocks.size(), primary_writable_blocks.size());
        return EXIT_NO_BLOCK;
      }

      BlockCollect* block_collect = NULL;
      bool promoted = false;
      std::set<uint32_t>::iterator iter = writable_blocks.begin();
      while (iter != writable_blocks.end() && need_count > 0)
      {
        // if writable block is stll available
        if (primary_writable_blocks.find(*iter) == primary_writable_blocks.end())
        {
          BlockChunkPtr ptr = meta_mgr_.get_block_chunk(*iter);

          ptr->mutex_.wrlock();

          block_collect = ptr->find(*iter);

          if ((block_collect != NULL) && (!block_collect->is_full()) && (block_collect->get_master_ds() == 0)
              && (static_cast<int32_t> (block_collect->get_ds()->size()) >= SYSPARAM_NAMESERVER.min_replication_))
          {
            block_collect->set_master_ds(server_collect->get_ds()->id_);
            ScopedRWLock scoped_lock(meta_mgr_.get_writable_mutex(), WRITE_LOCKER);
            if (meta_mgr_.block_writable(block_collect))
            {
              promoted = true;
              need_count--;
            }
          }
          ptr->mutex_.unlock();

          // join to primary writable block
          if (promoted)
          {
            ScopedRWLock scoped_lock(meta_mgr_.get_server_mutex(), WRITE_LOCKER);
            const_cast<ServerCollect*> (server_collect)->join_primary_writable_block(*iter);
          }
        }
        ++iter;
      }
      return TFS_SUCCESS;
    }

    int MetaManager::wait_for_shut_down()
    {
      return oplog_sync_mgr_.wait_for_shut_down();
    }
    int MetaManager::destroy()
    {
      return oplog_sync_mgr_.destroy();
    }
  }
}

