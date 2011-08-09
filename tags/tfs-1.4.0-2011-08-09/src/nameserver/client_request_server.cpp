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
 *   daoan <daoan@taobao.com>
 *      - initial release
 *
 */
#include <tbsys.h>
#include <tbnetutil.h>
#include "common/status_message.h"
#include "common/base_service.h"
#include "message/client_cmd_message.h"
#include "client_request_server.h"
#include "layout_manager.h"
#include "strategy.h"

namespace tfs
{
  namespace nameserver
  {
    using namespace common;
    using namespace tbsys;
    using namespace message;
    ClientRequestServer::ClientRequestServer(LayoutManager& lay_out_manager)
      :lay_out_manager_(lay_out_manager)
    {

    }

    int ClientRequestServer::keepalive(const common::DataServerStatInfo& ds_info,
        const common::HasBlockFlag flag,
        common::BLOCK_INFO_LIST& blocks, common::VUINT32& expires,
        bool& need_sent_block)
    {
      int32_t iret = TFS_ERROR;
      time_t now = time(NULL);
      //check dataserver status
      if (ds_info.status_ == DATASERVER_STATUS_DEAD)//dataserver dead
      {
        iret = lay_out_manager_.remove_server(ds_info.id_, now);
        lay_out_manager_.interrupt(INTERRUPT_ALL, now);//interrupt
      }
      else
      {
        bool isnew = false;
        iret = lay_out_manager_.add_server(ds_info, now, isnew);
        if (iret == TFS_SUCCESS)
        {
          if (isnew) //new dataserver
          {
            TBSYS_LOG(INFO, "dataserver: %s join: use capacity: %" PRI64_PREFIX "u, total capacity: %" PRI64_PREFIX "u, has_block: %s",
                tbsys::CNetUtil::addrToString(ds_info.id_).c_str(), ds_info.use_capacity_,
                ds_info.total_capacity_,flag == HAS_BLOCK_FLAG_YES ? "Yes" : "No");
            lay_out_manager_.interrupt(INTERRUPT_ALL, now);//interrupt
          }
          ServerCollect* server = lay_out_manager_.get_server(ds_info.id_);
          iret = NULL == server ? TFS_ERROR : TFS_SUCCESS;
          if (TFS_SUCCESS == iret)
          {
            #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
            server->dump();
            #endif
            NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
            if (flag == HAS_BLOCK_FLAG_NO)
            {
              int32_t block_count = server->block_count();
              need_sent_block = (isnew || block_count <= 0);

              //switching occurred between master and slave
              if ((!need_sent_block)
                  && (block_count != ds_info.block_count_))
              {
                //dataserver need to re-report if complete switch
                if (now < ngi.switch_time_)
                {
                  need_sent_block = true;
                }
              }
            }
            else//have blocks list
            {
              //update all relations of blocks belongs to it
              EXPIRE_BLOCK_LIST current_expires;
              #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
              TBSYS_LOG(DEBUG, "server: %s update_relation, flag: %s", tbsys::CNetUtil::addrToString(ds_info.id_).c_str(), flag == HAS_BLOCK_FLAG_YES ? "Yes" : "No");
              #endif
              iret = lay_out_manager_.update_relation(server, blocks, current_expires, now);
              if (TFS_SUCCESS == iret)
              {
                if (ngi.owner_role_ == NS_ROLE_MASTER)//i'm master, we're going to expire blocks
                {
                  std::vector<uint32_t> rm_list;
                  EXPIRE_BLOCK_LIST::iterator iter = current_expires.begin();
                  for (; iter != current_expires.end(); ++iter)
                  {
                    std::vector<BlockCollect*>& expires_blocks = iter->second;
                    std::vector<BlockCollect*>::iterator r_iter = expires_blocks.begin();
                    rm_list.clear();
                    if (iter->first->id() == ds_info.id_)
                    {
                      for (; r_iter != expires_blocks.end(); ++r_iter)
                      {
                        if (!lay_out_manager_.find_block_in_plan((*r_iter)->id()))
                        {
                          expires.push_back((*r_iter)->id());
                        }
                      }
                    }
                    else
                    {
                      for (; r_iter != expires_blocks.end(); ++r_iter)
                      {
                        //TODO rm_list will cause ds core for now
                        rm_list.push_back((*r_iter)->id());
                      }
                    }
                    if (!rm_list.empty())
                    {
                      std::vector<stat_int_t> stat(1, rm_list.size());
                      GFactory::get_stat_mgr().update_entry(GFactory::tfs_ns_stat_block_count_, stat, false);
                      lay_out_manager_.rm_block_from_ds(iter->first->id(), rm_list);
                    }
                  }//end for
                  #if !defined(TFS_NS_GTEST) && !defined(TFS_NS_INTEGRATION)
                  lay_out_manager_.touch(server,now);
                  #endif
                }
              }
              else
              {
                TBSYS_LOG(ERROR, "%s", "update relationship failed between block and dataserver");
              }
            }
          }
          else
          {
            TBSYS_LOG(ERROR, "ServerCollect object not found by : %s", CNetUtil::addrToString(ds_info.id_).c_str());
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "%s", "update information failed in keepalive fuction");
        }
      }
      return iret;
    }

    int ClientRequestServer::open(uint32_t& block_id, const int32_t mode, uint32_t& lease_id, int32_t& version, common::VUINT64& ds_list)
    {
      int32_t iret = lay_out_manager_.get_alive_server_size() <= 0 ? EXIT_NO_DATASERVER : TFS_SUCCESS;
      if (iret == TFS_SUCCESS)
      {
        if (mode & T_READ)//read mode
        {
          iret = open_read_mode(block_id, ds_list);
        }
        else//write mode
        {
          NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
          iret = ngi.owner_role_ == NS_ROLE_SLAVE ? EXIT_ACCESS_PERMISSION_ERROR: TFS_SUCCESS;
          if (iret == TFS_SUCCESS)//master
          {
            //check this block if doing any operations like replicating, moving, compacting...
            if ((block_id > 0)
                && (!(mode & T_NOLEASE)))
            {
              if (lay_out_manager_.find_block_in_plan(block_id))
              {
                TBSYS_LOG(ERROR, "it's error when we'll get block information in open this block: %u with write mode because block: %u is busy.",
                    block_id,  mode);
                iret = EXIT_BLOCK_BUSY;
              }
            }

            if (iret == TFS_SUCCESS)
            {
              iret = open_write_mode(mode, block_id, lease_id, version, ds_list);
            }
          }
        }
      }

      std::vector<stat_int_t> stat(4,0);

      mode & T_READ ? iret == TFS_SUCCESS ? stat[0] = 1 : stat[1] = 1
        : iret == TFS_SUCCESS ? stat[2] = 1 : stat[3] = 1;
      GFactory::get_stat_mgr().update_entry(GFactory::tfs_ns_stat_, stat);
      return iret;
    }

    int ClientRequestServer::open_read_mode(const uint32_t block_id, VUINT64& readable_list)
    {
      int32_t iret = 0 == block_id  ? EXIT_BLOCK_NOT_FOUND : TFS_SUCCESS;
      if (TFS_SUCCESS == iret)
      {
        BlockChunkPtr ptr = lay_out_manager_.get_chunk(block_id);
        RWLock::Lock lock(*ptr, READ_LOCKER);
        BlockCollect* block = ptr->find(block_id);
        iret = NULL == block ? EXIT_BLOCK_NOT_FOUND : TFS_SUCCESS;
        if (TFS_SUCCESS == iret)
        {
          std::vector<ServerCollect*>& readable = block->get_hold();
          iret = readable.empty() ? EXIT_NO_DATASERVER : TFS_SUCCESS;
          if (TFS_SUCCESS == iret)
          {
            std::vector<ServerCollect*>::iterator iter = readable.begin();
            for (; iter != readable.end(); ++iter)
            {
              readable_list.push_back((*iter)->id());
            }
            if (readable_list.empty())
              iret = EXIT_NO_DATASERVER;
          }
          if (TFS_SUCCESS != iret)
          {
            TBSYS_LOG(ERROR, "block: %u hold not any dataserver when open this block with read mode", block_id);
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "block: %u not exist when open this block with read mode", block_id);
        }
      }
      return iret;
    }

    int ClientRequestServer::batch_open(const common::VUINT32& blocks, const int32_t mode, const int32_t block_count, std::map<uint32_t, common::BlockInfoSeg>& out)
    {
      int32_t iret = lay_out_manager_.get_alive_server_size() <= 0 ? EXIT_NO_DATASERVER : TFS_SUCCESS;
      if (iret == TFS_SUCCESS)
      {
        if (mode & T_READ)
        {
          iret = batch_open_read_mode(blocks, out);
        }
        else
        {
          iret = batch_open_write_mode(mode, block_count, out);
        }
      }
      std::vector<stat_int_t> stat(4, 0);
      if (mode & T_READ)
      {
        if (iret == TFS_SUCCESS)
        {
          stat[0] = out.size();
        }
        else
        {
          stat[1] = __gnu_cxx::abs(out.size() - blocks.size());
        }
      }
      else
      {
        if (iret == TFS_SUCCESS)
        {
          stat[2] = out.size();
        }
        else
        {
          stat[3] = __gnu_cxx::abs(out.size() - block_count);
        }
      }
      GFactory::get_stat_mgr().update_entry(GFactory::tfs_ns_stat_, stat);
      return iret;
    }

    /**
     * Write commit operation, nameserver confirm this write op and update meta info.
     * @param [inout] parameter: information.
     * @return: success or failure
     */
    int ClientRequestServer::close(CloseParameter& parameter)
    {
      int32_t iret = TFS_SUCCESS;
      uint32_t block_id = parameter.block_info_.block_id_;
      LeaseStatus commit_status = parameter.status_ != WRITE_COMPLETE_STATUS_YES ?
        LEASE_STATUS_FAILED : LEASE_STATUS_FINISH;

      if (parameter.unlink_flag_ == UNLINK_FLAG_YES)//unlink file
      {
        if (!GFactory::get_lease_factory().commit(block_id, parameter.lease_id_, commit_status))
        {
          iret = EXIT_COMMIT_ERROR;
          snprintf(parameter.error_msg_, 256, "close block: %u successful,but lease: %u commit fail", block_id, parameter.lease_id_);
        }

        std::vector<stat_int_t> stat(6,0);
        iret == TFS_SUCCESS ? stat[4] = 0x01 : stat[5] = 0x01;
        GFactory::get_stat_mgr().update_entry(GFactory::tfs_ns_stat_, stat);
      }
      else //write file
      {
        if (commit_status != LEASE_STATUS_FINISH)
        {
          TBSYS_LOG(WARN, "close block: %u successful, but cleint write operation error,lease: %u commit begin", block_id, parameter.lease_id_);
          if (!GFactory::get_lease_factory().commit(block_id, parameter.lease_id_, commit_status))
          {
            iret = EXIT_COMMIT_ERROR;
            snprintf(parameter.error_msg_, 256, "close block: %u successful,but lease: %u commit fail", block_id, parameter.lease_id_);
          }
        }
        else
        {
          time_t now = time(NULL);
          int32_t last_version = 0;
          BlockCollect* block = NULL;
          BlockChunkPtr ptr = lay_out_manager_.get_chunk(block_id);
          {
            RWLock::Lock lock(*ptr, READ_LOCKER);
            block = ptr->find(block_id);
            if (block == NULL)
            {
              iret = EXIT_BLOCK_NOT_FOUND;
              snprintf(parameter.error_msg_, 256, "close block: %u fail, block not exist", block_id);
            }
            else
            {
              last_version = block->version();
            }
          }

          if (TFS_SUCCESS == iret)
          {
            //check version
            if (last_version >= parameter.block_info_.version_)
            {//version errro
              if (!GFactory::get_lease_factory().commit(block_id, parameter.lease_id_, LEASE_STATUS_FAILED))
              {
                snprintf(parameter.error_msg_, 256, "close block: %u successful, but lease: %u commit fail", block_id, parameter.lease_id_);
              }
              iret = EXIT_COMMIT_ERROR;
            }
            if (TFS_SUCCESS == iret)
            {
              //check block is full
              if (BlockCollect::is_full(parameter.block_info_.size_))
              {
                commit_status = LEASE_STATUS_OBSOLETE;
                parameter.need_new_ = true;
              }
            }

            if (TFS_SUCCESS == iret)
            {
              //commit lease
              if (!GFactory::get_lease_factory().commit(block_id, parameter.lease_id_, commit_status))
              {
                iret = EXIT_COMMIT_ERROR;
                snprintf(parameter.error_msg_, 256, "close block: %u successful,but lease: %u commit fail", block_id, parameter.lease_id_);
              }
            }

            if (TFS_SUCCESS == iret)
            {
              //update block information
              iret = lay_out_manager_.update_block_info(parameter.block_info_, parameter.id_, now, false);
              if (iret != TFS_SUCCESS)
              {
                snprintf(parameter.error_msg_,256,"close block: %u successful, but update block information fail", block_id);
              }
            }
          }
        }
      }
      return iret;
    }

    /**
     * client read: get block 's location of DataServerStatInfo
     * client write: get new block and location of DataServerStatInfo
     * @param [in] block_id: query block id, in write mode
     * can be set to zero for assign a new write block id.
     * @param [in] mode: read | write
     * @param [out] lease_id: write transaction id only for write mode.
     * @param [out] ds_list: block location of DataServerStatInfos.
     * @return: success or failure
     */
    int ClientRequestServer::open_write_mode(const int32_t mode,
        uint32_t& block_id,
        uint32_t& lease_id,
        int32_t& version,
        VUINT64& servers)
    {
      int32_t iret = mode & T_WRITE ? TFS_SUCCESS : EXIT_ACCESS_MODE_ERROR;
      if (TFS_SUCCESS != iret)
      {
        TBSYS_LOG(WARN, "access mode: %d error", mode);
      }
      else
      {
        //nameserver assign a new write block
        BlockCollect* block = NULL;
        if (mode & T_CREATE)
        {
          iret = 0 != block_id ? TFS_ERROR : TFS_SUCCESS;
          if (TFS_SUCCESS == iret)
          {
            //elect a writable block
            block = lay_out_manager_.elect_write_block();
            iret = NULL == block ? EXIT_NO_BLOCK : TFS_SUCCESS;
            if (TFS_SUCCESS == iret)
            {
              block_id = block->id();
            }
            else
            {
              TBSYS_LOG(ERROR, "%s", "it's failed when elect write block");
            }
          }
        }
        else if (mode & T_NEWBLK)
        {
          iret = 0 == block_id ? TFS_ERROR : TFS_SUCCESS;
          if (TFS_SUCCESS == iret)
          {
            //create new block by block_id
            iret = lay_out_manager_.open_helper_create_new_block_by_id(block_id);
            if (iret != TFS_SUCCESS)
            {
              TBSYS_LOG(ERROR, "create new block by block id: %u failed", block_id);
            }
          }
        }
        if (TFS_SUCCESS == iret)
        {
          iret = block_id == 0 ? EXIT_NO_BLOCK : TFS_SUCCESS;
          if (TFS_SUCCESS == iret)
          {
            BlockChunkPtr ptr = lay_out_manager_.get_chunk(block_id);
            RWLock::Lock lock(*ptr, READ_LOCKER);
            if (block == NULL)
            {
              block = ptr->find(block_id);
            }
            // now check blockcollect object if has any dataserver.
            iret = NULL == block ? EXIT_NO_BLOCK : TFS_SUCCESS;
            if (TFS_SUCCESS == iret)
            {
              iret = 0 == block->get_hold_size() ? EXIT_NO_DATASERVER : TFS_SUCCESS;
              if (TFS_SUCCESS == iret)
              {
                version = block->version();
                std::vector<ServerCollect*>& hold = block->get_hold();
                std::vector<ServerCollect*>::iterator iter = hold.begin();
                for (; iter != hold.end(); ++iter)
                {
                  servers.push_back((*iter)->id());
#if defined(TFS_NS_DEBUG)
                  if ((*iter)->is_alive())
                  {
                    (*iter)->total_elect_num_inc();
                  }
#endif
                }
                iret = servers.empty() ? EXIT_NO_DATASERVER : TFS_SUCCESS;
                if (TFS_SUCCESS != iret)
                {
                  TBSYS_LOG(ERROR, "any dataserver was't found in this block: %u object", block_id);
                }
              }
              else
              {
                TBSYS_LOG(ERROR, "block is invalid because there's any dataserver was found", block_id);
              }
            }
            else
            {
              TBSYS_LOG(WARN, "block: %u not found in open with write mode", block_id);
            }
          }
          else
          {
            TBSYS_LOG(WARN, "block: %u not found in open with write mode", block_id);
          }
        }

        if (TFS_SUCCESS == iret)
        {
          // register a lease for write..
          if (!(mode & T_NOLEASE))
          {
            lease_id = GFactory::get_lease_factory().add(block_id);
            iret = lease_id == INVALID_LEASE_ID ? EXIT_CANNOT_GET_LEASE : TFS_SUCCESS;
            if (TFS_SUCCESS != iret)
            {
              TBSYS_LOG(ERROR, "block: %u register lease failed", block_id);
            }
          }
        }
      }
      return iret;
    }
    /**
     * client read: get block 's location of dataserver
     * only choose dataserver with normal load
     * @param [in] block_id: query block id, must be a valid blockid
     * @param [out] ds_list: block location of dataserver.
     * @return: success or failure
     */

    int ClientRequestServer::batch_open_read_mode(const common::VUINT32& blocks, std::map<uint32_t, common::BlockInfoSeg>& out)
    {
      std::map<uint32_t, common::BlockInfoSeg>::iterator it;
      VUINT32::const_iterator iter = blocks.begin();
      for (; iter != blocks.end(); ++iter)
      {
        it = out.find((*iter));
        if (it == out.end())
        {
          std::pair<std::map<uint32_t, common::BlockInfoSeg>::iterator, bool> res =
            out.insert(std::pair<uint32_t, common::BlockInfoSeg>((*iter), common::BlockInfoSeg()));
          it = res.first;
        }
        open_read_mode((*iter), it->second.ds_);
      }
      return TFS_SUCCESS;
    }

    int ClientRequestServer::batch_open_write_mode(const int32_t mode, const int32_t block_count, std::map<uint32_t, common::BlockInfoSeg>& out)
    {
      int32_t iret =  (mode & T_WRITE) ? TFS_SUCCESS : EXIT_ACCESS_MODE_ERROR;
      if (TFS_SUCCESS == iret)
      {
        uint32_t block_id = 0;
        BlockInfoSeg seg;
        int32_t count = 0;
        do
        {
          ++count;
          block_id = 0;
          seg.ds_.clear();
          iret = open_write_mode(mode, block_id, seg.lease_id_, seg.version_, seg.ds_);
          if (iret == TFS_SUCCESS)
          {
            out.insert(std::pair<uint32_t, common::BlockInfoSeg>(block_id, seg));
          }
        }
        while (count < block_count);
      }
      else
      {
        TBSYS_LOG(WARN, "access mode: %d error", mode);
      }
      return iret;
    }

    int ClientRequestServer::handle_control_load_block(const common::ClientCmdInformation& info, common::BasePacket* message, const int64_t buf_length, char* buf)
    {
      int32_t iret = NULL != buf && buf_length > 0 ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        uint32_t block_id = info.value3_;
        BlockChunkPtr ptr = lay_out_manager_.get_chunk(block_id);
        RWLock::Lock lock(*ptr, WRITE_LOCKER);
        BlockCollect* block = ptr->find(block_id);
        iret = NULL == block ? TFS_ERROR: TFS_SUCCESS;
        if (TFS_SUCCESS == iret)
        {
          iret = block->get_hold_size() <= 0 ? TFS_SUCCESS : TFS_ERROR;
          if (TFS_SUCCESS == iret)
          {
            uint64_t id = info.value1_;
            ServerCollect* server = NULL;
            {
              server = lay_out_manager_.get_server(id);
            }
            iret = NULL == server ? TFS_ERROR : TFS_SUCCESS;
            if (TFS_SUCCESS == iret)
            {
#if !defined(TFS_NS_GTEST) && !defined(TFS_NS_INTEGRATION)
              int32_t status = STATUS_MESSAGE_ERROR;
              iret = send_msg_to_server(id, message, status);
              if (STATUS_MESSAGE_OK != status
                || TFS_SUCCESS != iret)
              {
                snprintf(buf, buf_length, "send load block: %u  message to server: %s failed",
                    block_id, CNetUtil::addrToString(id).c_str());
                TBSYS_LOG(ERROR, "%s", buf);
              }
#else
              UNUSED(message);
#endif

              if (TFS_SUCCESS == iret)
              {
                iret = lay_out_manager_.build_relation(block, server, time(NULL));
                if (TFS_SUCCESS != iret)
                {
                  snprintf(buf, buf_length, " build relation fail, block: %u dataserver: %s",
                      block_id, CNetUtil::addrToString(id).c_str());
                  TBSYS_LOG(ERROR, "%s", buf);
                }
              }
            }
            else
            {
              snprintf(buf, buf_length, " server: %s no exist",
                  CNetUtil::addrToString(id).c_str());
              TBSYS_LOG(ERROR, "%s", buf);
            }
          }
          else
          {
            snprintf(buf, buf_length, " block : %u not lose, hold dataserver: %d",
                block_id, block->get_hold_size());
            TBSYS_LOG(ERROR, "%s", buf);
          }
        }
        else
        {
          snprintf(buf, buf_length, " block: %u no exist", block_id);
          TBSYS_LOG(ERROR, "%s", buf);
        }
      }
      return iret;
    }

    int ClientRequestServer::handle_control_delete_block(const time_t now, const common::ClientCmdInformation& info,const int64_t buf_length, char* buf)
    {
      UNUSED(buf);
      UNUSED(buf_length);
      uint64_t id = info.value1_;
      uint32_t block_id = info.value3_;
      ServerCollect* server = lay_out_manager_.get_server(id);
      BlockChunkPtr ptr = lay_out_manager_.get_chunk(block_id);
      RWLock::Lock lock(*ptr, WRITE_LOCKER);
      BlockCollect* block = ptr->find(block_id);
      int32_t iret = TFS_SUCCESS;
      if (NULL != block)
      {
        if (block->get_hold_size() <= 0)
        {
          ptr->remove(block_id);
        }
        else
        {
          if (NULL != server)
          {
            iret = lay_out_manager_.relieve_relation(block, server, now) ? TFS_SUCCESS : TFS_ERROR;
          }
        }
      }
      return iret;
    }

    int ClientRequestServer::handle_control_compact_block(const time_t now, const common::ClientCmdInformation& info, const int64_t buf_length, char* buf)
    {
      uint32_t block_id = info.value3_;
      BlockChunkPtr ptr = lay_out_manager_.get_chunk(block_id);
      RWLock::Lock lock(*ptr, READ_LOCKER);
      BlockCollect* block = ptr->find(block_id);
      int32_t iret = NULL == block ? TFS_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == iret)
      {
        LayoutManager::CompactTaskPtr task = new LayoutManager::CompactTask(&lay_out_manager_, PLAN_PRIORITY_NORMAL, block_id, now, now, block->get_hold(), 0);
        if (!lay_out_manager_.add_task(task))
        {
          task = 0;
          iret = TFS_ERROR;
          snprintf(buf, buf_length, " add task(compact) failed, block: %u", block_id);
          TBSYS_LOG(ERROR, "%s", buf);
        }
      }
      else
      {
        snprintf(buf, buf_length, " block: %u no exist", block_id);
        TBSYS_LOG(ERROR, "%s", buf);
      }
      return iret;
    }

    int ClientRequestServer::handle_control_immediately_replicate_block(const time_t now, const common::ClientCmdInformation& info, const int64_t buf_length, char* buf)
    {
      uint64_t source = info.value1_;
      uint64_t target = info.value2_;
      uint32_t block_id = info.value3_;
      uint32_t flag = info.value4_;
      std::vector<ServerCollect*> runer;
      BlockChunkPtr ptr = lay_out_manager_.get_chunk(block_id);
      RWLock::Lock lock(*ptr, READ_LOCKER);
      BlockCollect* block = ptr->find(block_id);
      int32_t iret = NULL == block ? TFS_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == iret)
      {
        bool bret = false;
        if (REPLICATE_BLOCK_MOVE_FLAG_YES == flag)
        {
          if ( 0 != source
              && 0 != target
              && 0 != block_id
              && target != source)
          {
            bret = true;
          }
        }
        else
        {
          if ((0 != block_id)
              && ((source != target)
                || ((source == target)
                  && (0 == target))))
          {
            bret = true;
          }
        }
        iret = bret ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          ServerCollect* source_collect = NULL;
          ServerCollect* target_collect = NULL;
          if (source != 0)
          {
            source_collect = lay_out_manager_.get_server(source);
          }
          else
          {
            std::vector<ServerCollect*> source = block->get_hold();
            std::vector<ServerCollect*> except;
            lay_out_manager_.find_server_in_plan_helper(source, except);
            std::vector<ServerCollect*> result;
            int32_t count = elect_replicate_source_ds(lay_out_manager_, source, except,1, result);
            if (count != 1)
            {
              snprintf(buf, buf_length, "immediately %s block: %u fail, cannot found source dataserver",
                  flag == REPLICATE_BLOCK_MOVE_FLAG_NO ? "replicate" : "move", block_id);
              TBSYS_LOG(ERROR, "%s", buf);
              iret = TFS_ERROR;
            }
            else
            {
              source_collect = result.back();
            }
          }
          if (TFS_SUCCESS == iret)
          {
            if (target != 0)
            {
              target_collect = lay_out_manager_.get_server(target);
            }
            else
            {
              //elect target server
              std::vector<ServerCollect*> except = block->get_hold();
              std::vector<ServerCollect*> target_servers(except);
              int32_t count = elect_replicate_dest_ds(lay_out_manager_, except, 1, target_servers);
              if (count != 1)
              {
                snprintf(buf, buf_length, "immediately %s block: %u fail, cannot found target dataserver",
                    flag == REPLICATE_BLOCK_MOVE_FLAG_NO ? "replicate" : "move", block_id);
                TBSYS_LOG(ERROR, "%s", buf);
                iret = TFS_ERROR;
              }
              else
              {
                target_collect = target_servers.back();
              }
            }
          }

          if (TFS_SUCCESS == iret)
          {
            iret = NULL != source_collect  && NULL != target_collect ? TFS_SUCCESS : TFS_ERROR;
            if (TFS_SUCCESS == iret)
            {
              runer.push_back(source_collect);
              runer.push_back(target_collect);
              LayoutManager::TaskPtr task = flag == REPLICATE_BLOCK_MOVE_FLAG_NO ?
                new LayoutManager::ReplicateTask(&lay_out_manager_, PLAN_PRIORITY_EMERGENCY, block_id, now, now, runer, 0):
                new LayoutManager::MoveTask(&lay_out_manager_, PLAN_PRIORITY_EMERGENCY, block_id, now, now, runer, 0);
              if (!lay_out_manager_.add_task(task))
              {
                task = 0;
                iret = TFS_ERROR;
                snprintf(buf, buf_length, "add task %s fail block: %u",
                    flag == REPLICATE_BLOCK_MOVE_FLAG_NO ? "replicate" : "move", block_id);
                TBSYS_LOG(ERROR, "%s", buf);
              }
            }
            else
            {
              snprintf(buf, buf_length, "immediately %s block: %u fail, parameter is illegal, flag: %s, source: %s, target: %s",
                  flag == REPLICATE_BLOCK_MOVE_FLAG_NO ? "replicate" : "move",
                  block_id, flag == REPLICATE_BLOCK_MOVE_FLAG_NO ? "no" : "yes",
                  CNetUtil::addrToString(source).c_str(), CNetUtil::addrToString(target).c_str());
              TBSYS_LOG(ERROR, "%s", buf);
            }
          }
        }
        else
        {
          snprintf(buf, buf_length, "immediately %s block: %u fail, parameter is illegal, flag: %s, source: %s, target: %s",
              flag == REPLICATE_BLOCK_MOVE_FLAG_NO ? "replicate" : "move",
              block_id, flag == REPLICATE_BLOCK_MOVE_FLAG_NO ? "no" : "yes",
              CNetUtil::addrToString(source).c_str(), CNetUtil::addrToString(target).c_str());
          TBSYS_LOG(ERROR, "%s", buf);
        }
      }
      else
      {
        snprintf(buf, buf_length, "immediately %s block: %u fail, block: %u not exist",
            flag == REPLICATE_BLOCK_MOVE_FLAG_NO ? "replicate" : "move",
            block_id, block_id);
        TBSYS_LOG(ERROR, "%s", buf);
      }
      return iret;
    }

    int ClientRequestServer::handle_control_rotate_log(void)
    {
      BaseService* service = dynamic_cast<BaseService*>(BaseService::instance());
      TBSYS_LOGGER.rotateLog(service->get_log_path());
      return TFS_SUCCESS;
    }


    int ClientRequestServer::handle_control_set_runtime_param(const common::ClientCmdInformation& info, const int64_t buf_length, char* buf)
    {
      UNUSED(buf_length);
      return lay_out_manager_.set_runtime_param(info.value3_, info.value4_, buf);
    }

    int ClientRequestServer::handle_control_cmd(const ClientCmdInformation& info, common::BasePacket* msg, const int64_t buf_length, char* buf)
    {
      time_t now = time(NULL);
      int32_t iret = TFS_ERROR;
      switch (info.cmd_)
      {
        case CLIENT_CMD_LOADBLK:
          iret = handle_control_load_block(info, msg, buf_length, buf);
          break;
        case CLIENT_CMD_EXPBLK:
          iret = handle_control_delete_block(now, info, buf_length, buf);
          break;
        case CLIENT_CMD_COMPACT:
          iret = handle_control_compact_block(now, info, buf_length, buf);
          break;
        case CLIENT_CMD_IMMEDIATELY_REPL:  //immediately replicate
          iret = handle_control_immediately_replicate_block(now, info, buf_length, buf);
          break;
        case CLIENT_CMD_REPAIR_GROUP:
          //TODO
          break;
        case CLIENT_CMD_SET_PARAM:
          iret = handle_control_set_runtime_param(info, buf_length, buf);
          break;
        case CLIENT_CMD_ROTATE_LOG:
          iret = handle_control_rotate_log();
          break;
        default:
          snprintf(buf, buf_length, "unknow client cmd: %d", info.cmd_);
          iret = TFS_ERROR;
          break;
      }
      return iret;
    }

    int ClientRequestServer::handle(common::BasePacket* msg)
    {
      int32_t iret = TFS_SUCCESS;
      bool bret = msg != NULL;
      if (bret)
      {
        int32_t pcode = msg->getPCode();
        switch (pcode)
        {
          case REPLICATE_BLOCK_MESSAGE:
          case BLOCK_COMPACT_COMPLETE_MESSAGE:
          case REMOVE_BLOCK_RESPONSE_MESSAGE:
            iret = lay_out_manager_.handle_task_complete(msg);
            break;
          default:
            TBSYS_LOG(WARN, "unkonw message PCode = %d", pcode);
            break;
        }
      }
      return bret ? iret : TFS_ERROR;
    }

    int ClientRequestServer::dump_plan(tbnet::DataBuffer& output)
    {
      return lay_out_manager_.dump_plan(output);
    }
  }
}
