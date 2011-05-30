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
#include "client_request_server.h"
#include "layout_manager.h"

namespace tfs
{
namespace nameserver
{
  using namespace common;
  using namespace tbsys;
  ClientRequestServer::ClientRequestServer(LayoutManager& lay_out_manager)
    :lay_out_manager_(lay_out_manager)
  {
  }

  int ClientRequestServer::keepalive(const common::DataServerStatInfo& ds_info,
      const common::HasBlockFlag flag,
      common::BLOCK_INFO_LIST& blocks,
      common::VUINT32& expires,
      bool& need_sent_block)
  {
    time_t now = time(NULL);
    //check dataserver status
    if (ds_info.status_ == DATASERVER_STATUS_DEAD)//dataserver dead
    {
      lay_out_manager_.remove_server(ds_info.id_, now);
      lay_out_manager_.interrupt(INTERRUPT_ALL, now);//interrupt
      return TFS_SUCCESS;
    }

    bool isnew = false;
    int iret = lay_out_manager_.add_server(ds_info, now, isnew);
    if (iret != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "%s", "update information failed in keepalive fuction");
      return iret;
    }

    if (isnew) //new dataserver
    {
      TBSYS_LOG(INFO, "dataserver(%s) join: use capacity(%" PRI64_PREFIX "u),total capacity(%" PRI64_PREFIX "u), has_block(%s)",
          tbsys::CNetUtil::addrToString(ds_info.id_).c_str(), ds_info.use_capacity_,
          ds_info.total_capacity_,flag == HAS_BLOCK_FLAG_YES ? "Yes" : "No");
      lay_out_manager_.interrupt(INTERRUPT_ALL, now);//interrupt
    }

    ServerCollect* server = NULL;
    {
      server = lay_out_manager_.get_server(ds_info.id_);
    }

    if (server == NULL)
    {
      TBSYS_LOG(ERROR, "ServerCollect object not found by (%s)", CNetUtil::addrToString(ds_info.id_).c_str());
      return TFS_ERROR;
    }

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
      return TFS_SUCCESS;
    }

    //update all relations of blocks belongs to it
    EXPIRE_BLOCK_LIST current_expires;
#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
    TBSYS_LOG(DEBUG, "server(%s) update_relation, flag(%s)", tbsys::CNetUtil::addrToString(ds_info.id_).c_str(), flag == HAS_BLOCK_FLAG_YES ? "Yes" : "No");
#endif
    iret = lay_out_manager_.update_relation(server, blocks, current_expires, now);
    if (iret == TFS_ERROR)
    {
      TBSYS_LOG(ERROR, "%s", "update relationship failed between block and dataserver");
      return iret;
    }

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
              rm_list.push_back((*r_iter)->id());
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
          std::vector<int64_t> stat(1, rm_list.size());
          GFactory::get_stat_mgr().update_entry(GFactory::tfs_ns_stat_block_count_, stat, false);
          lay_out_manager_.rm_block_from_ds(iter->first->id(), rm_list);
        }
      }
    }

#if !defined(TFS_NS_GTEST) && !defined(TFS_NS_INTEGRATION)
    lay_out_manager_.touch(server,now);
#endif
    return TFS_SUCCESS;
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
              TBSYS_LOG(ERROR, "it's error when we'll get block information in open this block(%u) with write mode because block(%u) is busy.",
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
    std::vector<int64_t> stat(4,0);
    mode & T_READ ? iret == TFS_SUCCESS ? stat[0] = 1 : stat[1] = 1
      : iret == TFS_SUCCESS ? stat[2] = 1 : stat[3] = 1;
    GFactory::get_stat_mgr().update_entry(GFactory::tfs_ns_stat_, stat);
    return iret;
  }

  int ClientRequestServer::open_read_mode(const uint32_t block_id, VUINT64& readable_list)
    {
      if (block_id == 0)
      {
        TBSYS_LOG(ERROR, "block(%u) is invalid when open this block with read mode", block_id);
        return EXIT_BLOCK_NOT_FOUND;
      }
      BlockChunkPtr ptr = lay_out_manager_.get_chunk(block_id);
      RWLock::Lock lock(*ptr, READ_LOCKER);
      BlockCollect* block = ptr->find(block_id);

      if (block == NULL)
      {
        TBSYS_LOG(ERROR, "block(%u) not exist when open this block with read mode", block_id);
        return EXIT_BLOCK_NOT_FOUND;
      }

      std::vector<ServerCollect*>& readable = block->get_hold();
      if (readable.empty())
      {
        TBSYS_LOG(ERROR, "block(%u) hold not any dataserver when open this block with read mode", block_id);
        return EXIT_NO_DATASERVER;
      }
      std::vector<ServerCollect*>::iterator iter = readable.begin();
      for (; iter != readable.end(); ++iter)
      {
        readable_list.push_back((*iter)->id());
      }
      return TFS_SUCCESS;
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
      std::vector<int64_t> stat(4, 0);
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
          snprintf(parameter.error_msg_, 256, "close block(%u) successful,but lease(%u) commit fail", block_id, parameter.lease_id_);
          TBSYS_LOG(ERROR, "%s", parameter.error_msg_);
          iret = EXIT_COMMIT_ERROR;
        }
        std::vector<int64_t> stat(6,0);
        iret == TFS_SUCCESS ? stat[4] = 0x01 : stat[5] = 0x01;
        GFactory::get_stat_mgr().update_entry(GFactory::tfs_ns_stat_, stat);
        return iret;
      }
      else //write file
      {
        if (commit_status != LEASE_STATUS_FINISH)
        {
          TBSYS_LOG(WARN, "close block(%u) successful, but cleint write operation error,lease(%u) commit begin", block_id, parameter.lease_id_);
          if (!GFactory::get_lease_factory().commit(block_id, parameter.lease_id_, commit_status))
          {
            snprintf(parameter.error_msg_, 256, "close block(%u) successful,but lease(%u) commit fail", block_id, parameter.lease_id_);
            TBSYS_LOG(ERROR, "%s", parameter.error_msg_);
            return EXIT_COMMIT_ERROR;
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
              snprintf(parameter.error_msg_, 256, "close block(%u) fail, block not exist", block_id);
              TBSYS_LOG(ERROR, "%s", parameter.error_msg_);
              return EXIT_BLOCK_NOT_FOUND;
            }
            last_version = block->version();
          }

          //check version
          if (last_version >= parameter.block_info_.version_)
          {//version errro
            if (!GFactory::get_lease_factory().commit(block_id, parameter.lease_id_, LEASE_STATUS_FAILED))
            {
              snprintf(parameter.error_msg_, 256, "close block(%u) successful,but lease(%u) commit fail", block_id, parameter.lease_id_);
              TBSYS_LOG(ERROR, "%s", parameter.error_msg_);
            }
            return EXIT_COMMIT_ERROR;
          }

          //check block is full
          if (BlockCollect::is_full(parameter.block_info_.size_))
          {
            commit_status = LEASE_STATUS_OBSOLETE;
            parameter.need_new_ = true;
          }

          //commit lease
          if (!GFactory::get_lease_factory().commit(block_id, parameter.lease_id_, commit_status))
          {
            snprintf(parameter.error_msg_, 256, "close block(%u) successful,but lease(%u) commit fail", block_id, parameter.lease_id_);
            TBSYS_LOG(ERROR, "%s", parameter.error_msg_);
            return EXIT_COMMIT_ERROR;
          }

          //update block information
          iret = lay_out_manager_.update_block_info(parameter.block_info_, parameter.id_, now, false);
          if (iret != TFS_SUCCESS)
          {
            TBSYS_LOG(ERROR, "close block(%u) successful, but update block information fail", block_id);
            TBSYS_LOG(ERROR, "%s", parameter.error_msg_);
          }
          return iret;
        }
      }
      return TFS_SUCCESS;
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
      //check mode
      if (!(mode & T_WRITE))//mode error
      {
        TBSYS_LOG(WARN, "access mode(%d) error", mode);
        return EXIT_ACCESS_MODE_ERROR;
      }

      int32_t iret = TFS_SUCCESS;
      //nameserver assign a new write block
      BlockCollect* block = NULL;
      if ((block_id == 0) && (mode & T_CREATE))//elect a writable block
      {
        block = lay_out_manager_.elect_write_block();
        if (block == NULL)
        {
          TBSYS_LOG(ERROR, "%s", "it's failed when elect write block");
          return EXIT_NO_BLOCK;
        }
        block_id = block->id();
      }
      else if ((block_id != 0) && (mode & T_NEWBLK)) //create new block by block_id
      {
        iret = lay_out_manager_.open_helper_create_new_block_by_id(block_id);
        if (iret != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "create new block(%u) by block id fail", block_id);
          return iret;
        }
      }

      if (block_id == 0)
      {
        TBSYS_LOG(WARN, "block(%u) not found in open with write mode", block_id);
        return EXIT_NO_BLOCK;
      }

      {
        BlockChunkPtr ptr = lay_out_manager_.get_chunk(block_id);
        RWLock::Lock lock(*ptr, READ_LOCKER);
        if (block == NULL)
        {
          block = ptr->find(block_id);
        }
        // now check blockcollect object if has any dataserver.
        if (block == NULL)
        {
          TBSYS_LOG(WARN, "block(%u) not found in open with write mode", block_id);
          return EXIT_NO_BLOCK;
        }

        if (block->get_hold_size() == 0)
        {
          TBSYS_LOG(ERROR, "block is invalid because there's any dataserver was found", block_id);
          return EXIT_NO_DATASERVER;
        }

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

        if (servers.empty())
        {
          TBSYS_LOG(ERROR, "any dataserver was't found in this block(%u) object", block_id);
          return EXIT_NO_DATASERVER;
        }
      }

      // register a lease for write..
      if (!(mode & T_NOLEASE))
      {
        lease_id = GFactory::get_lease_factory().add(block_id);
        if (lease_id == INVALID_LEASE_ID)
        {
          TBSYS_LOG(ERROR, "block(%u) register lease fail", block_id);
          return EXIT_CANNOT_GET_LEASE;
        }
      }
      return TFS_SUCCESS;
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
      //check mode
      if (!(mode & T_WRITE))//mode error
      {
        TBSYS_LOG(WARN, "access mode(%d) error", mode);
        return EXIT_ACCESS_MODE_ERROR;
      }
      uint32_t block_id = 0;
      BlockInfoSeg seg;
      int32_t count = 0;
      int32_t iret = TFS_ERROR;
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
      return TFS_SUCCESS;
    }

}
}
