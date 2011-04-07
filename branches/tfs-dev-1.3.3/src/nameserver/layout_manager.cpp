/*
 * LayoutManager.cpp
 *
 *  Created on: 2010-11-5
 *      Author: duanfei
 */

#include <time.h>
#include <iostream>
#include <functional>
#include <numeric>
#include <tbsys.h>
#include <Memory.hpp>
#include "strategy.h"
#include "layout_manager.h"
#include "global_factory.h"
#include "common/error_msg.h"
#include "message/message.h"
#include "message/client_manager.h"
#include "message/block_info_message.h"
#include "message/replicate_block_message.h"
#include "message/compact_block_message.h"

using namespace tfs::common;
using namespace tfs::message;
using namespace tbsys;

namespace tfs
{
  namespace nameserver
  {
    const int8_t LayoutManager::INTERVAL = 10;
    const int16_t LayoutManager::SKIP_BLOCK = 128;
    const int8_t LayoutManager::LOAD_BASE_MULTIPLE = 2;
    const int8_t LayoutManager::ELECT_SEQ_INITIALIE_NUM = 1;
 
    const std::string LayoutManager::dynamic_parameter_str[] = {
      "min_replication",
      "max_replication",
      "max_write_file_count",
      "max_use_capacity_ratio",
      "heart_interval",
      "replicate_wait_time",
      "compact_delete_ratio",
      "compact_max_load",
      "plan_run_flag",
      "run_plan_expire_interval"
      "run_plan_ratio",
      "object_dead_max_time",
      "balance_max_diff_block_num",
      "log_level",
      "add_primary_block_count",
      "build_plan_interval"
      "replicate_ratio",
      "max_wait_write_lease",
      "cluster_index",
      "build_plan_default_wait_time",
  };

  static void print_servers(std::vector<ServerCollect*>& servers, std::string& result)
  {
    std::vector<ServerCollect*>::iterator iter = servers.begin();
    for (; iter != servers.end(); ++iter)
    {
      result += "/";
      result += tbsys::CNetUtil::addrToString((*iter)->id());
    }
  }

  LayoutManager::LayoutManager():
      build_plan_thread_(0),
      run_plan_thread_(0),
      check_dataserver_thread_(0),
      oplog_sync_mgr_(*this),
      block_chunk_(NULL),
      block_chunk_num_(32),
      write_index_(-1),
      write_second_index_(-1),
      last_rotate_log_time_(0),
      max_block_id_(0),
      alive_server_size_(0),
      interrupt_(INTERRUPT_NONE),
      plan_run_flag_(PLAN_RUN_FLAG_REPLICATE)
    {
      srand(time(NULL));
      tzset();
      zonesec_ = 86400 + timezone;
      plan_run_flag_ |= PLAN_RUN_FLAG_MOVE;
      plan_run_flag_ |= PLAN_RUN_FLAG_COMPACT;
      plan_run_flag_ |= PLAN_RUN_FLAG_DELETE;
    }

    LayoutManager::~LayoutManager()
    {
      build_plan_thread_ = 0;
      run_plan_thread_ = 0;
      check_dataserver_thread_ = 0;
      for (int32_t i = 0; i < block_chunk_num_; ++i)
      {
        if (block_chunk_ != NULL)
          block_chunk_[i] = 0;
      } 
      tbsys::gDeleteA(block_chunk_);

      {
        RWLock::Lock lock(server_mutex_, WRITE_LOCKER);
        SERVER_MAP::iterator iter = servers_.begin();
        for (; iter != servers_.end(); ++iter)
        {
          tbsys::gDelete(iter->second);
        }
        servers_.clear();
        servers_index_.clear();
      }

      {
        tbutil::Monitor<tbutil::Mutex>::Lock lock(run_plan_monitor_);
        pending_plan_list_.clear();
        running_plan_list_.clear();
        finish_plan_list_.clear();
      }

      {
        RWLock::Lock tlock(maping_mutex_, WRITE_LOCKER);
        block_to_task_.clear();
        server_to_task_.clear();
      }
    }

    BlockChunkPtr LayoutManager::get_chunk(const uint32_t block_id) const
    {
      return block_chunk_[block_id % block_chunk_num_];
    }

    BlockCollect* LayoutManager::get_block(const uint32_t block_id)
    {
      BlockChunkPtr ptr = get_chunk(block_id);
      return ptr->find(block_id);
    }

    ServerCollect* LayoutManager::get_server(const uint64_t server)
    {
      SERVER_MAP::const_iterator iter = servers_.find(server);
      return ((iter == servers_.end()) 
          || (iter->second != NULL && !iter->second->is_alive())) ? NULL : iter->second;
    }

    int LayoutManager::initialize(int32_t chunk_num)
    {
      block_chunk_num_ = chunk_num == 0 ? 32 : chunk_num > 1024 ? 1024 : chunk_num;
      tbsys::gDeleteA(block_chunk_);
      block_chunk_ = new BlockChunkPtr[block_chunk_num_];
      for (int32_t i = 0; i < block_chunk_num_; i++)
      {
        block_chunk_[i] = new BlockChunk();
      }

    #if !defined(TFS_NS_GTEST) && !defined(TFS_NS_INTEGRATION)
      int32_t iret = oplog_sync_mgr_.initialize();
      if (iret != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "initialize oplog sync manager fail, must be exit, iret(%d)", iret);
        return iret;
      }

      //initialize thread
      build_plan_thread_ = new BuildPlanThreadHelper(*this);
      check_dataserver_thread_ = new CheckDataServerThreadHelper(*this);
      run_plan_thread_ = new RunPlanThreadHelper(*this);
    #elif defined(TFS_NS_INTEGRATION)
      run_plan_thread_ = new RunPlanThreadHelper(*this);
    #endif

      //find max block id
      calc_max_block_id();
      return TFS_SUCCESS;
    }

    void LayoutManager::wait_for_shut_down()
    {
      if (build_plan_thread_ != 0)
      {
        build_plan_thread_->join();
      }
      if (run_plan_thread_ != 0)
      {
        run_plan_thread_->join();
      }
      if (check_dataserver_thread_ != 0)
      {
        check_dataserver_thread_->join();
      }

      oplog_sync_mgr_.wait_for_shut_down();
    }

    void LayoutManager::destroy()
    {
      {
        tbutil::Monitor<tbutil::Mutex>::Lock lock(build_plan_monitor_);
        build_plan_monitor_.notifyAll(); 
      }

      {
        tbutil::Monitor<tbutil::Mutex>::Lock lock(run_plan_monitor_);
        run_plan_monitor_.notifyAll();
      }

      {
        tbutil::Monitor<tbutil::Mutex>::Lock lock(check_server_monitor_);
        check_server_monitor_.notifyAll();
      }
      oplog_sync_mgr_.destroy();
    }

    int LayoutManager::keepalive(const common::DataServerStatInfo& ds_info, common::HasBlockFlag flag,
        common::BLOCK_INFO_LIST& blocks, common::VUINT32& expires, bool& need_sent_block)
    {
      time_t now = time(NULL);
      //check dataserver status
      if (ds_info.status_ == DATASERVER_STATUS_DEAD)//dataserver dead
      {
        remove_server(ds_info.id_, now);
        interrupt(INTERRUPT_ALL, now);//interrupt
        return TFS_SUCCESS;
      }

      bool isnew = false;
      int iret = add_server(ds_info, isnew, now);
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
        interrupt(INTERRUPT_ALL, now);//interrupt
      }

      ServerCollect* server = NULL;
      {
        RWLock::Lock lock(server_mutex_, READ_LOCKER);
        server = get_server(ds_info.id_);
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
      iret = update_relation(server, blocks, current_expires, now);
      if (iret == TFS_ERROR)
      {
        TBSYS_LOG(ERROR, "%s", "update relationship failed between block and dataserver");
        return iret;
      }

      if (ngi.owner_role_ == NS_ROLE_MASTER)//i'm master, we're going expire blocks
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
              if (!find_block_in_plan((*r_iter)->id()))
              {
                rm_list.push_back((*r_iter)->id());     
              }
            }
          }
          else
          {
            for (; r_iter != expires_blocks.end(); ++r_iter)
            {
            //TODO rm_list will case ds core for now
              rm_list.push_back((*r_iter)->id());     
            }
          }
          if (!rm_list.empty())
          {
            std::vector<int64_t> stat(1, rm_list.size());
            GFactory::get_stat_mgr().update_entry(GFactory::tfs_ns_stat_block_count_, stat, false);
            rm_block_from_ds(iter->first->id(), rm_list);
          }
        }
      }

      #if !defined(TFS_NS_GTEST) && !defined(TFS_NS_INTEGRATION)
      touch(server,now); 
      #endif
      return TFS_SUCCESS;
    }

    int LayoutManager::open(uint32_t& block_id, const int32_t mode, uint32_t& lease_id, int32_t& version, common::VUINT64& ds_list)
    {
      int32_t iret = alive_server_size_<= 0 ? EXIT_NO_DATASERVER : TFS_SUCCESS;
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
              RWLock::Lock tlock(maping_mutex_, READ_LOCKER);
              if (find_block_in_plan(block_id))
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

    int LayoutManager::batch_open(const common::VUINT32& blocks, const int32_t mode, const int32_t block_count, std::map<uint32_t, common::BlockInfoSeg>& out)
    {
      int32_t iret = alive_server_size_<= 0 ? EXIT_NO_DATASERVER : TFS_SUCCESS;
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
      std::vector<int64_t> stat(4,0);
      mode & T_READ ? iret == TFS_SUCCESS ? stat[0] = out.size() : stat[1] = __gnu_cxx::abs(out.size() - blocks.size()) 
                    : iret == TFS_SUCCESS ? stat[2] = out.size() : stat[3] = __gnu_cxx::abs(out.size() - block_count);
      GFactory::get_stat_mgr().update_entry(GFactory::tfs_ns_stat_, stat);
      return iret;
    }

    /**
     * Write commit operation, nameserver confirm this write op and update meta info.
     * @param [inout] parameter: information.
     * @return: success or failure
     */
    int LayoutManager::close(CloseParameter& parameter)
    {
      int32_t iret = TFS_SUCCESS;
      uint32_t block_id = parameter.block_info_.block_id_;
      LeaseStatus commit_status = parameter.status_ != WRITE_COMPLETE_STATUS_YES ?
                                   LEASE_STATUS_FAILED : LEASE_STATUS_FINISH;

      if (parameter.unlink_flag_ == UNLINK_FLAG_YES)//unlink file 
      {
        if (!GFactory::get_lease_factory().commit(block_id, parameter.lease_id_, commit_status))
        {
          snprintf(parameter.error_msg_, 0xff, "close block(%u) successful,but lease(%u) commit fail", block_id, parameter.lease_id_); 
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
            snprintf(parameter.error_msg_, 0xff, "close block(%u) successful,but lease(%u) commit fail", block_id, parameter.lease_id_); 
            TBSYS_LOG(ERROR, "%s", parameter.error_msg_);
            return EXIT_COMMIT_ERROR;
          }
        }
        else
        {
          time_t now = time(NULL);
          int32_t last_version = 0;
          BlockCollect* block = NULL;
          BlockChunkPtr ptr = get_chunk(block_id);
          {
            RWLock::Lock lock(*ptr, READ_LOCKER);
            block = ptr->find(block_id);
            if (block == NULL)
            {
              snprintf(parameter.error_msg_, 0xff, "close block(%u) fail, block not exist", block_id);
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
              snprintf(parameter.error_msg_, 0xff, "close block(%u) successful,but lease(%u) commit fail", block_id, parameter.lease_id_); 
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
            snprintf(parameter.error_msg_, 0xff, "close block(%u) successful,but lease(%u) commit fail", block_id, parameter.lease_id_); 
            TBSYS_LOG(ERROR, "%s", parameter.error_msg_);
            return EXIT_COMMIT_ERROR;
          }

          //update block information
          iret = update_block_info(parameter.block_info_, parameter.id_,now, false);
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
     * client read: get block 's location of dataserver 
     * only choose dataserver with normal load
     * @param [in] block_id: query block id, must be a valid blockid
     * @param [out] ds_list: block location of dataserver.
     * @return: success or failure
     */
    int LayoutManager::open_read_mode(const uint32_t block_id, VUINT64& readable_list)
    {
      if (block_id == 0)
      {
        TBSYS_LOG(ERROR, "block(%u) is invalid when open this block with read mode", block_id);
        return EXIT_BLOCK_NOT_FOUND;
      }
      BlockChunkPtr ptr = get_chunk(block_id);
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

    int LayoutManager::batch_open_read_mode(const common::VUINT32& blocks, std::map<uint32_t, common::BlockInfoSeg>& out)
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

    int LayoutManager::batch_open_write_mode(const int32_t mode, const int32_t block_count, std::map<uint32_t, common::BlockInfoSeg>& out)
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
        iret = open_write_mode(mode, block_id, seg.lease_, seg.version_, seg.ds_);
        if (iret == TFS_SUCCESS)
        {
          out.insert(std::pair<uint32_t, common::BlockInfoSeg>(block_id, seg));
        }
      }
      while (count < block_count);
      return TFS_SUCCESS;
    }

    int LayoutManager::open_helper_create_new_block_by_id(uint32_t block_id)
    {
      BlockChunkPtr ptr = get_chunk(block_id);
      ptr->rdlock();//lock
      BlockCollect* block = ptr->find(block_id);
      if (block == NULL)//block not found by block_id
      {
        ptr->unlock();//unlock
        block = add_new_block(block_id);
        if (block == NULL)
        {
          TBSYS_LOG(ERROR, "add new block(%u) failed because there's any dataserver was found", block_id);
          return EXIT_NO_DATASERVER;
        }
        ptr->rdlock();
        block = ptr->find(block_id);
        if (block == NULL)
        {
          ptr->unlock();
          TBSYS_LOG(ERROR, "add new block(%u) failed because there's any dataserver was found", block_id);
          return EXIT_NO_DATASERVER;
        }
      }

      if (block->get_hold_size() == 0)
      {
        if (block->get_creating_flag() == BlockCollect::BLOCK_CREATE_FLAG_YES)
        {
          ptr->unlock();
          TBSYS_LOG(ERROR, "block(%u) found meta data, but creating by another thread, must be return", block_id);
          return EXIT_NO_BLOCK;
        }
        else
        {
          ptr->unlock();
          TBSYS_LOG(ERROR, "block(%u) found meta data, but no dataserver hold it.", block_id);
          return EXIT_NO_DATASERVER;
        }
      }
      ptr->unlock();
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
    int LayoutManager::open_write_mode(const int32_t mode,
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
        block = elect_write_block();
        if (block == NULL)
        {
          TBSYS_LOG(ERROR, "%s", "it's failed when elect write block");
          return EXIT_NO_BLOCK;
        }
        block_id = block->id();
      }
      else if ((block_id != 0) && (mode & T_NEWBLK)) //create new block by block_id
      {
        iret = open_helper_create_new_block_by_id(block_id);
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
        BlockChunkPtr ptr = get_chunk(block_id);
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
        if (lease_id == LeaseClerk::INVALID_LEASE)
        {
          TBSYS_LOG(ERROR, "block(%u) register lease fail", block_id);
          return EXIT_CANNOT_GET_LEASE;
        }
      }
      return TFS_SUCCESS;
    }

    int LayoutManager::update_block_info(
        const BlockInfo& new_block_info,
        const uint64_t id,
        time_t now,
        bool addnew)
    {
      ServerCollect* server = NULL;
      {
        RWLock::Lock lock(server_mutex_, READ_LOCKER);
        server = get_server(id);
        if (server != NULL)
        {
          server->touch(now);
        }
      }

      bool isnew = false;
      {
        BlockChunkPtr ptr = get_chunk(new_block_info.block_id_);
        RWLock::Lock lock(*ptr, WRITE_LOCKER);
        BlockCollect* block = ptr->find(new_block_info.block_id_);
        if ((block == NULL) && addnew)
        {
          block = ptr->add(new_block_info.block_id_, now);
          isnew = block != NULL;
          if (block != NULL && server != NULL)
          {
            int iret = build_relation(block, server, now);
            if (iret != TFS_SUCCESS)
            {
              TBSYS_LOG(ERROR, "it's error that build relation betweed block(%u) and server(%s)",
                  new_block_info.block_id_, CNetUtil::addrToString(server->id()).c_str());
              return iret;
            }
          }
        }

        if (block == NULL)
        {
          TBSYS_LOG(ERROR, "it's error that update block(%u) information because block(%u) not found",
              new_block_info.block_id_, new_block_info.block_id_);
          return EXIT_BLOCK_NOT_FOUND;
        }

        if (block->version() > new_block_info.version_)//check version
        {
          //version error
          TBSYS_LOG(ERROR, "it's error that update block(%u) information because old version(%d) >= new version(%d)",
              new_block_info.block_id_, block->version(), new_block_info.version_);
          return EXIT_UPDATE_BLOCK_INFO_VERSION_ERROR;
        }

        block->update(new_block_info);//update block information

        if (block->is_relieve_writable_relation())
        {
          block->relieve_relation();
        }
      }

      BlockOpLog oplog;
      memset(&oplog, 0, sizeof(oplog));
      oplog.info_ = new_block_info;
      oplog.cmd_ = isnew ? OPLOG_INSERT : OPLOG_UPDATE;
      oplog.servers_.push_back(id);
      int64_t size = oplog.get_serialize_size();
      int64_t pos = 0;
      char buf[size];
      if (oplog.serialize(buf, size, pos) < 0)
      {
        TBSYS_LOG(ERROR, "%s", "oplog serialize error");
      }
      else
      {
        if (oplog_sync_mgr_.log(OPLOG_TYPE_BLOCK_OP, buf, size) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "write oplog failed, block(%u), isnew(%s)", new_block_info.block_id_, isnew == OPLOG_INSERT ? "insert" : isnew == OPLOG_UPDATE ? "update" : "unknow");
        }
      }
      return TFS_SUCCESS;
    }

    int LayoutManager::repair(uint32_t block_id, uint64_t server, int32_t flag, time_t now, std::string& error_msg)
    {
      char msg[512];
      memset(msg, 0, sizeof(msg));
      std::vector<ServerCollect*> hold;
      {
        BlockChunkPtr ptr = get_chunk(block_id);
        RWLock::Lock lock(*ptr, READ_LOCKER);
        BlockCollect* block = ptr->find(block_id);
        if (block == NULL)
        {
          snprintf(msg, 512, "repair block, block collect not found by block(%u)", block_id);
          error_msg.append(msg);
          TBSYS_LOG(ERROR, "%s", msg);
          return EXIT_BLOCK_NOT_FOUND;
        }
        hold = block->get_hold();
      }

      //need repair this block.
      int32_t hold_size = static_cast<int32_t>(hold.size());
      if ((flag == UPDATE_BLOCK_MISSING)
          && (hold_size >= SYSPARAM_NAMESERVER.min_replication_))
      {
        snprintf(msg, 512, "already got block(%u) replica(%d)", block_id, hold_size);
        TBSYS_LOG(ERROR, "%s", msg);
        error_msg.append(msg);
        return EXIT_BLOCK_NOT_FOUND;
      }
      if (hold_size <= 0)
      {
        snprintf(msg, 512, "repair block(%u) no any dataserver hold it", block_id);
        TBSYS_LOG(ERROR, "%s", msg);
        error_msg.append(msg);
        return EXIT_BLOCK_NOT_FOUND;
      }

      std::vector<ServerCollect*> runer;
      std::vector<ServerCollect*>::iterator iter = hold.begin();
      for (; iter != hold.end(); ++iter)
      {
        if ((*iter)->id() != server)
        {
          runer.push_back((*iter));
        }
      }
      if (runer.size() ==0 || runer.empty())
      {
        snprintf(msg, 512, "repair block(%u) no any other dataserver(%d) hold a correct replica", block_id, hold_size);
        TBSYS_LOG(ERROR, "%s", msg);
        error_msg.append(msg);
        return EXIT_NO_DATASERVER;
      }
      GFactory::get_lease_factory().cancel(block_id);
      ServerCollect* dest_server = NULL;
      {
        RWLock::Lock slock(server_mutex_, READ_LOCKER);
        dest_server = get_server(server);
        runer.push_back(dest_server);
      }
      BlockCollect* block = NULL;
      BlockChunkPtr ptr = get_chunk(block_id);
      RWLock::Lock lock(*ptr, WRITE_LOCKER);
      block = ptr->find(block_id);
      bool bret = relieve_relation(block, dest_server, now);
      if (!bret)
      {
        TBSYS_LOG(WARN, "relieve relation failed between block(%u) and server(%s)", block_id, CNetUtil::addrToString(dest_server->id()).c_str());
      }

      ReplicateTaskPtr task = new ReplicateTask(this, PLAN_PRIORITY_EMERGENCY, block_id, now, now, runer);
      bret = add_task(task);
      if (!bret)
      {
        TBSYS_LOG(WARN, "add task(ReplicateTask) failed, block(%u)", block_id);
      }

      TBSYS_LOG(DEBUG, "add task, type(%d)", task->type_);
      return STATUS_MESSAGE_REMOVE;
    }

    int LayoutManager::scan(SSMScanParameter& param)
    {
      int32_t start = (param.start_next_position_ & 0xFFFF0000) >> 16;
      int32_t next  = 0;
      int32_t should= (param.should_actual_count_ & 0xFFFF0000) >> 16;
      int32_t actual= 0;
      int32_t jump_count = 0;
      bool    end = true;
      bool    all_over = true;
      bool    cutover_chunk = ((param.end_flag_) & SSM_SCAN_CUTOVER_FLAG_YES);

      if (param.type_ & SSM_TYPE_SERVER)
      {
        int16_t child_type = param.child_type_;
        if (child_type == SSM_CHILD_SERVER_TYPE_ALL)
        {
          child_type |= SSM_CHILD_SERVER_TYPE_HOLD;
          child_type |= SSM_CHILD_SERVER_TYPE_WRITABLE;
          child_type |= SSM_CHILD_SERVER_TYPE_MASTER;
          child_type |= SSM_CHILD_SERVER_TYPE_INFO;
        }
        bool has_server = start <= alive_server_size_ ; 
        if (has_server)
        {
          int32_t jump_count = 0;
          uint64_t start_server = CNetUtil::ipToAddr(param.addition_param1_, param.addition_param2_);
          RWLock::Lock lock(server_mutex_, READ_LOCKER);
          SERVER_MAP::const_iterator iter = start_server <= 0 ? servers_.begin() : servers_.find(start_server); 
          for (; iter != servers_.end(); ++iter, ++jump_count)
          {
            if (iter->second->is_alive())
            {
              ++actual;
              iter->second->scan(param, child_type);
              if (actual == should)
              {
                ++iter;
                ++jump_count;
                break;
              }
            }
          }
          all_over = iter == servers_.end();
          next = (actual == should && start + jump_count < alive_server_size_) ? start + jump_count : start;
          if (!all_over)
          {
            uint64_t host = iter->second->id();
            IpAddr* addr = reinterpret_cast<IpAddr*>(&host);
            param.addition_param1_ = addr->ip_;
            param.addition_param2_ = addr->port_;
          }
          else
          {
            param.addition_param1_ = 0;
            param.addition_param2_ = 0;
          }
        }
      }
      else if (param.type_ & SSM_TYPE_BLOCK)
      {
        bool has_block = (start <= block_chunk_num_); 
        if (has_block)
        {
          next = start;
          for (; next < block_chunk_num_; ++next, ++start, cutover_chunk = true)
          {
            jump_count += block_chunk_[next]->scan(param, actual, end, should, cutover_chunk);
            if (actual == should)
            {
              next = end ? start + 1 : start;
              break;
            }
          }
          all_over = start < block_chunk_num_? start == block_chunk_num_ - 1? end ? true : false : false : true; 
          cutover_chunk = end;
        }
      }
      next &= 0x0000FFFF;
      param.start_next_position_ &= 0xFFFF0000;
      param.start_next_position_ |= next;
      actual &= 0x0000FFFF;
      param.should_actual_count_ &= 0xFFFF0000;
      param.should_actual_count_ |= actual;
      param.end_flag_ = all_over ? SSM_SCAN_END_FLAG_YES : SSM_SCAN_END_FLAG_NO;
      param.end_flag_ <<= 4;
      param.end_flag_ &= 0xF0;
      param.end_flag_ |= cutover_chunk ? SSM_SCAN_CUTOVER_FLAG_YES : SSM_SCAN_CUTOVER_FLAG_NO;
      return TFS_SUCCESS;
    }

    int LayoutManager::dump_plan(tbnet::DataBuffer& stream)
    {
      tbutil::Monitor<tbutil::Mutex>::Lock lock(run_plan_monitor_);
      stream.writeInt32(running_plan_list_.size() + pending_plan_list_.size());
      std::set<TaskPtr, TaskCompare>::iterator iter = running_plan_list_.begin();
      for (; iter != running_plan_list_.end(); ++iter)
      {
        (*iter)->dump(stream);
      }
      iter = pending_plan_list_.begin();
      for (; iter != pending_plan_list_.end(); ++iter)
      {
        (*iter)->dump(stream);
      }
      return TFS_SUCCESS;
    }

    int LayoutManager::dump_plan(void)
    {
      tbutil::Monitor<tbutil::Mutex>::Lock lock(run_plan_monitor_);
      std::set<TaskPtr, TaskCompare>::iterator iter = running_plan_list_.begin();
      for (; iter != running_plan_list_.end(); ++iter)
      {
        (*iter)->dump(TBSYS_LOG_LEVEL_DEBUG);
      }
      iter = pending_plan_list_.begin();
      for (; iter != pending_plan_list_.end(); ++iter)
      {
        (*iter)->dump(TBSYS_LOG_LEVEL_DEBUG);
      }
      return TFS_SUCCESS;
    }

    int LayoutManager::handle_task_complete(message::Message* msg)
    {
      //handle complete message
      bool bret = msg != NULL;
      if (bret)
      {
        NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
        if (ngi.owner_role_ == NS_ROLE_MASTER)//master
        {
          std::vector<uint32_t> works;//find block
          switch (msg->getPCode())
          {
            case BLOCK_COMPACT_COMPLETE_MESSAGE:
              {
                CompactBlockCompleteMessage* compact_msg = dynamic_cast<CompactBlockCompleteMessage*>(msg);
                works.push_back(compact_msg->get_block_id());
              }
              break;
            case REPLICATE_BLOCK_MESSAGE:
              {
                ReplicateBlockMessage* replicate_msg = dynamic_cast<ReplicateBlockMessage*>(msg);
                works.push_back(replicate_msg->get_repl_block()->block_id_);
              }
              break;
            case REMOVE_BLOCK_RESPONSE_MESSAGE:
              {
                RemoveBlockResponseMessage* rm_msg = dynamic_cast<RemoveBlockResponseMessage*>(msg);
                works.push_back(rm_msg->get_block_id());
              }
              break;
            default:

              break;
          }

          std::vector<TaskPtr> complete;
          bool all_complete_flag = true;
          bool bfind = false;
          TaskPtr task = 0;
          std::vector<uint32_t>::iterator v_iter= works.begin();
          for (; v_iter != works.end(); ++v_iter, all_complete_flag = true)
          {
            {
              RWLock::Lock tlock(maping_mutex_, READ_LOCKER);
              std::map<uint32_t, TaskPtr>::iterator iter = block_to_task_.find((*v_iter));
              bfind = iter != block_to_task_.end();
              if (bfind)
              {
                task = iter->second;
              }
            }
            if (bfind)
            {
              if (task->handle_complete(msg, all_complete_flag) != TFS_SUCCESS)
              {
                task->dump(TBSYS_LOG_LEVEL_ERROR, "handle complete message fail");
              }

              if (all_complete_flag)
              {
                complete.push_back(task);
              }

              task->dump(TBSYS_LOG_LEVEL_INFO, "handle message complete, show result");
            }
          }

          std::vector<TaskPtr>::iterator iter = complete.begin();
          {
            RWLock::Lock tlock(maping_mutex_, WRITE_LOCKER);
            for (; iter != complete.end(); ++iter)//relieve reliation
            {
              block_to_task_.erase(task->block_id_);
              std::vector<ServerCollect*>::iterator r_iter = task->runer_.begin();
              for (; r_iter != task->runer_.end(); ++r_iter)
              {
                server_to_task_.erase((*r_iter));
              }
            }
          }

          tbutil::Monitor<tbutil::Mutex>::Lock lock(run_plan_monitor_);
          iter = complete.begin();
          for (; iter != complete.end(); ++iter)
          {
            finish_plan_list_.push_back((*iter));

            GFactory::get_timer()->cancel((*iter));

            std::set<TaskPtr, TaskCompare>::iterator r_iter = running_plan_list_.find((*iter));
            if (r_iter != running_plan_list_.end())
              running_plan_list_.erase(r_iter);//remove task from running_plan_list
          }
        }
        else if (ngi.owner_role_ == NS_ROLE_SLAVE)//slave
        {
          bool all_complete_flag = true;
          std::vector<ServerCollect*> runer;
          TaskPtr task  = 0;
          switch (msg->getPCode())
          {
            case BLOCK_COMPACT_COMPLETE_MESSAGE:
              task = new CompactTask(this, PLAN_PRIORITY_NONE, 0, 0, 0, runer);
              break;
            case REPLICATE_BLOCK_MESSAGE: 
            {
              ReplicateBlockMessage* replicate_msg = dynamic_cast<ReplicateBlockMessage*>(msg);
              task = replicate_msg->get_move_flag() == REPLICATE_BLOCK_MOVE_FLAG_NO
                ? new ReplicateTask(this, PLAN_PRIORITY_NONE, 0, 0, 0, runer)
                : new MoveTask(this, PLAN_PRIORITY_NONE, 0, 0, 0, runer);
              break;
            }
            default:
              break;
          }
          if (task != 0)
          {
            if (task->handle_complete(msg, all_complete_flag) != TFS_SUCCESS)
            {
              TBSYS_LOG(ERROR, "%s", "slave handle message failed");
            }
            task = 0;
          }
        }
      }
      return bret ? TFS_SUCCESS : TFS_ERROR;
    }

    #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION)
    StatusMessage* LayoutManager::handle(message::Message* msg)
    #else
    int LayoutManager::handle(message::Message* msg)
    #endif
    {
      int32_t iret = TFS_SUCCESS;
      bool bret = msg != NULL;
      if (bret)
      {
        int32_t pcode = msg->getPCode();
        switch (pcode)
        {
        case CLIENT_CMD_MESSAGE:
        {
          ClientCmdMessage* message = dynamic_cast<ClientCmdMessage*>(msg);
          StatusMessage* rmsg = new StatusMessage(STATUS_MESSAGE_ERROR, "unknown client cmd.");
          time_t now = time(NULL);
          int32_t cmd = message->get_cmd();
          switch (cmd)
          {
            case CLIENT_CMD_LOADBLK:
              {
                uint32_t block_id = message->get_value3();
                BlockChunkPtr ptr = get_chunk(block_id);
                RWLock::Lock lock(*ptr, WRITE_LOCKER);
                BlockCollect* block = ptr->find(block_id);
                if (block == NULL)
                {
                  rmsg->set_message(STATUS_MESSAGE_ERROR, "block no exist");
                  TBSYS_LOG(ERROR, "block(%u) no exist", block_id);
                  break;
                }
                if (block->get_hold_size() > 0)
                {
                  rmsg->set_message(STATUS_MESSAGE_ERROR, "block no lose");
                  TBSYS_LOG(ERROR, "block(%u) no lose, hold dataserver(%d)", block_id, block->get_hold_size());
                  break;
                }
                uint64_t id = message->get_value1();
                ServerCollect* server = NULL;
                {
                  RWLock::Lock lock(server_mutex_, READ_LOCKER);
                  server = get_server(id);
                }
                if (server == NULL)
                {
                  rmsg->set_message(STATUS_MESSAGE_ERROR, "server no exist");
                  TBSYS_LOG(ERROR, "server(%s) no exist", CNetUtil::addrToString(id).c_str());
                  break;
                }
                #if !defined(TFS_NS_GTEST) && !defined(TFS_NS_INTEGRATION)
                Message* ret_msg = NULL;
                if (message::NewClientManager::get_instance().call(id, message, DEFAULT_NETWORK_CALL_TIMEOUT, ret_msg) != TFS_SUCCESS)
                {
                  rmsg->set_message(STATUS_MESSAGE_ERROR, "send messge to dataserver fail");
                  break;
                }
                #endif
                if (build_relation(block, server, time(NULL)))
                {
                  TBSYS_LOG(WARN, "build relation fail, block(%u) dataserver(%s)", block_id,
                      CNetUtil::addrToString(id).c_str());
                  rmsg->set_message(STATUS_MESSAGE_ERROR, "build relation fail");
                  break;
                }
                rmsg->set_message(STATUS_MESSAGE_OK);
                break;
              }
            case CLIENT_CMD_EXPBLK:
              {
                uint64_t id = message->get_value1();
                uint32_t block_id = message->get_value3();
                BlockCollect* block = NULL;
                {
                  BlockChunkPtr ptr = get_chunk(block_id);
                  RWLock::Lock lock(*ptr, WRITE_LOCKER);
                  block = ptr->find(block_id);
                  if (block == NULL)
                  {
                    rmsg->set_message(STATUS_MESSAGE_ERROR, "block no exist");
                    TBSYS_LOG(ERROR, "block(%u) no exist", block_id);
                    break;
                  }
                  if (block->get_hold_size() <= 0 && id == 0)
                  {
                    ptr->remove(block_id);
                    rmsg->set_message(STATUS_MESSAGE_OK);
                    break;
                  }
                }
                ServerCollect* server = NULL;
                {
                  RWLock::Lock lock(server_mutex_, READ_LOCKER);
                  server = get_server(id);
                }
                if (server == NULL)
                {
                  rmsg->set_message(STATUS_MESSAGE_ERROR, "server not exist");
                  TBSYS_LOG(ERROR, "server(%s) not exist", CNetUtil::addrToString(id).c_str());
                  break;
                }
                
                std::vector<ServerCollect*> runer;
                runer.push_back(server);
                RedundantTaskPtr task = new RedundantTask(this, PLAN_PRIORITY_NORMAL, block_id, now, now, runer);
                if (!add_task(task))
                {
                  task = 0;
                  rmsg->set_message(STATUS_MESSAGE_ERROR, "add task error");
                  TBSYS_LOG(ERROR, "add task(delete) failed, server: %s, block: %u", CNetUtil::addrToString(id).c_str(), block_id);
                  break;
                }
                else
                {
                  relieve_relation(block, server, now);
                }
                rmsg->set_message(STATUS_MESSAGE_OK);
                break;
              }
            case CLIENT_CMD_COMPACT:
              {
                uint32_t block_id = message->get_value3();
                BlockChunkPtr ptr = get_chunk(block_id);
                RWLock::Lock lock(*ptr, READ_LOCKER);
                BlockCollect* block = ptr->find(block_id); 
                if (block == NULL)
                {
                  rmsg->set_message(STATUS_MESSAGE_ERROR, "block not exist");
                  TBSYS_LOG(ERROR, "block(%u) not exist", block_id);
                  break;
                }
                CompactTaskPtr task = new CompactTask(this, PLAN_PRIORITY_NORMAL, block_id, now, now, block->get_hold());
                if (!add_task(task))
                {
                  task = 0;
                  rmsg->set_message(STATUS_MESSAGE_ERROR, "add task(compact) fail");
                  TBSYS_LOG(ERROR, "add task(compact) failed, block(%u)", block_id);
                  break;
                }
                rmsg->set_message(STATUS_MESSAGE_OK);
                break;
              }
            case CLIENT_CMD_IMMEDIATELY_REPL:
              {
                char buf[0xff];
                uint64_t source = message->get_value1();
                uint64_t target = message->get_value2();
                uint32_t block_id = message->get_value3();
                uint32_t flag = message->get_value4();
                std::vector<ServerCollect*> runer;
                BlockChunkPtr ptr = get_chunk(block_id);
                RWLock::Lock lock(*ptr, READ_LOCKER);
                BlockCollect* block = ptr->find(block_id); 
                if (block == NULL)
                {
                  snprintf(buf, 0xff, "immediately %s block(%u) fail, block(%u) not exist",
                      flag == REPLICATE_BLOCK_MOVE_FLAG_NO ? "replicate" : "move",
                      block_id, block_id);
                  rmsg->set_message(STATUS_MESSAGE_ERROR, buf);
                  TBSYS_LOG(ERROR, "%s", buf);
                  break; 
                }

                bool bret = flag == REPLICATE_BLOCK_MOVE_FLAG_YES ? (source != 0 && target != 0 && block_id != 0 && target != source) ? true : false :
                      block_id != 0  && ((source != target) || (source == target && source == 0 && target == 0)) ? true : false;

                if (!bret)
                {
                  snprintf(buf, 0xff, "immediately %s block(%u) fail, parameter is illegal, flag(%s), source(%s), target(%s)",
                      flag == REPLICATE_BLOCK_MOVE_FLAG_NO ? "replicate" : "move",
                      block_id, flag == REPLICATE_BLOCK_MOVE_FLAG_NO ? "no" : "yes",
                      CNetUtil::addrToString(source).c_str(), CNetUtil::addrToString(target).c_str());
                  TBSYS_LOG(ERROR, "%s", buf);
                  rmsg->set_message(STATUS_MESSAGE_ERROR, buf);
                  break;
                }

                ServerCollect* source_collect = NULL;
                ServerCollect* target_collect = NULL;
                if (source != 0)
                {
                  RWLock::Lock lock(server_mutex_, READ_LOCKER);
                  source_collect = get_server(source);
                }
                else
                {
                  std::vector<ServerCollect*> source = block->get_hold();
                  std::vector<ServerCollect*> except;
                  find_server_in_plan_helper(source, except);
                  std::vector<ServerCollect*> result;
                  int32_t iret = elect_replicate_source_ds(*this, source, except,1, result);
                  if (iret != 1)
                  {
                    snprintf(buf, 0xff, "immediately %s block(%u) fail, cannot found source dataserver",
                        flag == REPLICATE_BLOCK_MOVE_FLAG_NO ? "replicate" : "move", block_id);
                    TBSYS_LOG(ERROR, "%s", buf);
                    rmsg->set_message(STATUS_MESSAGE_ERROR, buf);
                    break;
                  }
                  source_collect = result.back();
                }

                if (target != 0)
                {
                  RWLock::Lock lock(server_mutex_, READ_LOCKER);
                  target_collect = get_server(target);
                }
                else
                {
                  //elect target server
                  std::vector<ServerCollect*> except = block->get_hold();
                  std::vector<ServerCollect*> target_servers(except);
                  int32_t iret = elect_replicate_dest_ds(*this, except, 1, target_servers);
                  if (iret != 1)
                  {
                    snprintf(buf, 0xff, "immediately %s block(%u) fail, cannot found target dataserver",
                        flag == REPLICATE_BLOCK_MOVE_FLAG_NO ? "replicate" : "move", block_id);
                    TBSYS_LOG(ERROR, "%s", buf);
                    rmsg->set_message(STATUS_MESSAGE_ERROR, "immediately replicate or move block fail");
                    break;
                  }
                  target_collect = target_servers.back();
                }

                if ((source_collect == NULL)
                    || (target_collect == NULL))
                {
                  snprintf(buf, 0xff, "immediately %s block(%u) fail, parameter is illegal, flag(%s), source(%s), target(%s)",
                      flag == REPLICATE_BLOCK_MOVE_FLAG_NO ? "replicate" : "move",
                      block_id, flag == REPLICATE_BLOCK_MOVE_FLAG_NO ? "no" : "yes",
                      CNetUtil::addrToString(source).c_str(), CNetUtil::addrToString(target).c_str());
                  TBSYS_LOG(ERROR, "%s", buf);
                  rmsg->set_message(STATUS_MESSAGE_ERROR, buf);
                  break;
                }

                runer.push_back(source_collect);
                runer.push_back(target_collect);
                TaskPtr task = flag == REPLICATE_BLOCK_MOVE_FLAG_NO ? 
                  new ReplicateTask(this, PLAN_PRIORITY_EMERGENCY, block_id, now, now, runer):
                  new MoveTask(this, PLAN_PRIORITY_EMERGENCY, block_id, now, now, runer);

                if (!add_task(task))
                {
                  task = 0;
                  snprintf(buf, 0xff, "add task %s fail block(%u)",
                      flag == REPLICATE_BLOCK_MOVE_FLAG_NO ? "replicate" : "move", block_id);
                  TBSYS_LOG(ERROR, "%s", buf);
                  rmsg->set_message(STATUS_MESSAGE_ERROR, buf);
                  break;
                }
                rmsg->set_message(STATUS_MESSAGE_OK);
                break;
              }
              break;
            case CLIENT_CMD_REPAIR_GROUP:
              break;
            case CLIENT_CMD_SET_PARAM:
              {
                uint32_t index = message->get_value3();
                uint32_t value = message->get_value4();
                char error_msg[0xff];
                set_runtime_param(index, value, error_msg);
                rmsg->set_message(STATUS_MESSAGE_OK, error_msg);
                break;
              }
            default:
              break;
          }
          #if !defined(TFS_NS_GTEST) && !defined(TFS_NS_INTEGRATION)
          msg->reply_message(rmsg);
          return TFS_SUCCESS;
          #else
          return rmsg;
          #endif
        }
        break;
        case DUMP_PLAN_MESSAGE:
        {
          DumpPlanResponseMessage* rmsg = new DumpPlanResponseMessage();
          dump_plan(rmsg->get_data());
          msg->reply_message(msg);
          iret = TFS_SUCCESS;
        }
        break;
        case REPLICATE_BLOCK_MESSAGE:
        case BLOCK_COMPACT_COMPLETE_MESSAGE:
        case REMOVE_BLOCK_RESPONSE_MESSAGE:
        iret = handle_task_complete(msg);
        break;
        default:
        break;
        }
      }
      #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION)
      return NULL;
      #else
      return bret ? iret : TFS_ERROR;
      #endif
    }

    void LayoutManager::interrupt(uint8_t interrupt, time_t now)
    {
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info(); 
      TBSYS_LOG(INFO, "owner role(%d), status(%d), receive interrupt(%d), now(%"PRI64_PREFIX"d), switch time(%"PRI64_PREFIX"d)", ngi.owner_role_, ngi.owner_status_, interrupt, now, ngi.switch_time_);
      if ((ngi.owner_role_ == NS_ROLE_MASTER)
          && (ngi.owner_status_ == NS_STATUS_INITIALIZED)
          && (ngi.switch_time_ < now))
      {
        TBSYS_LOG(DEBUG, "%s", "notify build plan thread");
        tbutil::Monitor<tbutil::Mutex>::Lock lock(build_plan_monitor_);
        interrupt_ |= interrupt;
        build_plan_monitor_.notify();
      }
    }

    /*
     * expire blocks on DataServerStatInfo only post expire message to ds, dont care result.
     * @param [in] ds_id  DataServerStatInfo id the one who post to.
     * @param [in] block_id block id, the one need expired.
     * @return TFS_SUCCESS success.
     */
    int LayoutManager::rm_block_from_ds(const uint64_t ds_id, const uint32_t block_id)
    {
      TBSYS_LOG(INFO, "remove  block (%u) on server (%s)", block_id, tbsys::CNetUtil::addrToString(ds_id).c_str());
      #if !defined(TFS_NS_GTEST) && !defined(TFS_NS_INTEGRATION)
      RemoveBlockMessage rbmsg;
      rbmsg.add_remove_id(block_id);
      BlockOpLog log;
      memset(&log, 0, sizeof(log));
      log.info_.block_id_ = block_id;
      log.blocks_.push_back(block_id);
      log.cmd_ = OPLOG_RELIEVE_RELATION;
      log.servers_.push_back(ds_id);
      int64_t size = log.get_serialize_size();
      int64_t pos  = 0;
      char buf[size];
      if (log.serialize(buf, size, pos) < 0)
      {
        TBSYS_LOG(ERROR, "%s", "serialize error");
      }
      else
      {
        oplog_sync_mgr_.log(OPLOG_TYPE_BLOCK_OP, buf, size);
      }
      uint16_t wait_id = 0;
      NewClientManager::get_instance().get_wait_id(wait_id);
      return NewClientManager::get_instance().post_request(ds_id, &rbmsg, wait_id);
      #else
      return TFS_SUCCESS;
      #endif
    }

    int LayoutManager::rm_block_from_ds(const uint64_t ds_id, const std::vector<uint32_t>& block_ids)
    {
      TBSYS_LOG(INFO, "remove  block count (%u) on server (%s)", block_ids.size(),
          tbsys::CNetUtil::addrToString(ds_id).c_str());
      #if !defined(TFS_NS_GTEST) && !defined(TFS_NS_INTEGRATION)
      RemoveBlockMessage rbmsg;
      rbmsg.set_remove_list(block_ids);
      BlockOpLog log;
      memset(&log, 0, sizeof(log));
      log.blocks_ = block_ids;
      log.cmd_ = OPLOG_RELIEVE_RELATION;
      log.servers_.push_back(ds_id);
      int64_t size = log.get_serialize_size();
      int64_t pos  = 0;
      char buf[size];
      if (log.serialize(buf, size, pos) < 0)
      {
        TBSYS_LOG(ERROR, "%s", "serialize error");
      }
      else
      {
        oplog_sync_mgr_.log(OPLOG_TYPE_BLOCK_OP, buf, size);
      }
      uint16_t wait_id = 0;
      NewClientManager::get_instance().get_wait_id(wait_id);
      return NewClientManager::get_instance().post_request(ds_id, &rbmsg, wait_id);
      #else
      return TFS_SUCCESS;
      #endif
    }

    /**
     * dataserver join this nameserver, update dataserver of information 
     * and update global statistic information
     * @param[in] info: dataserver of information
     * @param[in] isnew: true if new dataserver
     * @param[in] now: current time
     * @return  return TFS_SUCCESS if success, otherwise failed
     */
    int LayoutManager::add_server(const DataServerStatInfo& info, bool& isnew, time_t now)
    {
      //update server information
      {
        isnew = false;
        ServerCollect* server_collect = NULL;
        RWLock::Lock lock(server_mutex_, WRITE_LOCKER);
        SERVER_MAP::iterator iter = servers_.find(info.id_);
        if ((iter == servers_.end())
            || (iter->second == NULL)
            || (!iter->second->is_alive()))
        {
          if (iter != servers_.end())
          {
            --alive_server_size_;
            std::vector<ServerCollect*>::iterator where = 
            std::find(servers_index_.begin(), servers_index_.end(), iter->second); 
            if (where != servers_index_.end())
            {
              servers_index_.erase(where);
            }
            iter->second->set_dead_time(now);
            GFactory::get_gc_manager().add(iter->second);
            servers_.erase(iter);
          }
          ARG_NEW(server_collect, ServerCollect, info, now);
          std::pair<SERVER_MAP::iterator, bool> res =
            servers_.insert(SERVER_MAP::value_type(info.id_, server_collect));
          if (!res.second)
          {
            tbsys::gDelete(server_collect);
            return TFS_ERROR;
          }
          isnew = true; 
          ++alive_server_size_;
          servers_index_.push_back(server_collect);
          std::vector<int64_t> stat(1, info.block_count_);
          GFactory::get_stat_mgr().update_entry(GFactory::tfs_ns_stat_block_count_, stat);
        }
        else
        {
          server_collect = iter->second;
        }
        server_collect->update(info, now);
      }

      //update global statistic information
      GFactory::get_global_info().update(info, isnew);
      GFactory::get_global_info().dump();
      return TFS_SUCCESS;
    }

    /**
     * dataserver exit, when nameserver cannot keep touch with dataserver.
     * release all relation of blocks belongs to it
     * @param[in] server: dataserver's id (ip&port)
     * @param[in] now : current time
     * @return 
     */
    int LayoutManager::remove_server(uint64_t server, time_t now)
    {
      TBSYS_LOG(WARN, "server(%s) exit", CNetUtil::addrToString(server).c_str());
      //remove ServerCollect
      ScopedRWLock scoped_lock(server_mutex_, WRITE_LOCKER);
      SERVER_MAP::iterator iter = servers_.find(server);
      if (iter != servers_.end())
      {
        std::vector<int64_t> stat(1,iter->second->block_count());
        GFactory::get_stat_mgr().update_entry(GFactory::tfs_ns_stat_block_count_, stat, false);

        //release all relations of blocks belongs to it
        relieve_relation(iter->second, now);
        iter->second->dead();
        std::vector<ServerCollect*>::iterator where = 
        std::find(servers_index_.begin(), servers_index_.end(), iter->second); 
        if (where != servers_index_.end())
        {
          servers_index_.erase(where);
        }
        iter->second->set_dead_time(now);
        GFactory::get_gc_manager().add(iter->second);
        servers_.erase(iter);
        --alive_server_size_;
      }
      return TFS_SUCCESS;
    }

    /**
     * add new BlockCollect object base on block_id
     * @param[in] block_id : block id
     * @return  NULL if not found or add failed
     */
    BlockCollect* LayoutManager::add_block(uint32_t& block_id)
    {
      if (block_id == 0)
        return NULL;
      BlockChunkPtr ptr = get_chunk(block_id);
      return ptr->add(block_id, time(NULL));
    }

    BlockCollect* LayoutManager::add_new_block(uint32_t& block_id, ServerCollect* server, time_t now)
    {
      std::vector<ServerCollect*> need;
      BlockChunkPtr ptr = 0;
      BlockCollect* block = NULL;
      std::vector<ServerCollect*> success;
      std::vector<ServerCollect*> failed;
      std::vector<ServerCollect*>::iterator iter;
      int64_t size = 0;
      int64_t pos = 0;
      BlockOpLog oplog;
      memset(&oplog, 0, sizeof(oplog));

      #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
      std::string result;
      #endif

      if (alive_server_size_ <= 0)
        goto rollback;

      //find a usable block id, TODO lock
      if (block_id == 0)
      {
        block_id = get_alive_block_id();
        ptr = get_chunk(block_id);
        RWLock::Lock lock(*ptr, WRITE_LOCKER);
        block = ptr->add(block_id, now);
        if (block == NULL)
        {
          TBSYS_LOG(ERROR, "failed when add new block(%u)", block_id);
          goto rollback;
        }
        block->set_create_flag(BlockCollect::BLOCK_CREATE_FLAG_YES);
      }
      else
      {
        ptr = get_chunk(block_id);
        RWLock::Lock lock(*ptr, WRITE_LOCKER);
        block = ptr->find(block_id);
        if (block == NULL)
        {
          block = ptr->add(block_id, now);
          if (block != NULL)
          {
            block->set_create_flag(BlockCollect::BLOCK_CREATE_FLAG_YES);
          }
        }
        if (block == NULL)
        {
          TBSYS_LOG(ERROR, "failed when add new block(%u)", block_id);
          goto rollback;
        }
        if (block->get_hold_size() > 0)
        {
          need.assign(block->get_hold().begin(), block->get_hold().end());
        }
      }

      {
        RWLock::Lock lock(server_mutex_, READ_LOCKER);
        int32_t count = SYSPARAM_NAMESERVER.max_replication_ - need.size();
        TBSYS_LOG(DEBUG, "begin count: %d, need size: %u", count, need.size());
        if (server != NULL)
        {
          int64_t use_capacity = GFactory::get_global_info().use_capacity_ <= 0 ? alive_server_size_ : GFactory::get_global_info().use_capacity_;
          if (server->is_writable(use_capacity/alive_server_size_))
          {
            need.push_back(server);
            --count;
          }
        }
        if (count > 0)
        {
          elect_write_server(*this, count, need);
          TBSYS_LOG(DEBUG, "end count: %d, need size: %u", count, need.size());
        } 
    
        if (need.empty())
        {
          TBSYS_LOG(ERROR, "add block(%u) failed,rollback", block_id);
          goto rollback;
        }
      }

      oplog.blocks_.push_back(block_id);
      oplog.info_.block_id_ = block_id;
      oplog.cmd_ = OPLOG_INSERT;
      iter = need.begin();
      for (; iter != need.end(); ++iter)
      {
        oplog.servers_.push_back((*iter)->id());
      }

      size = oplog.get_serialize_size();
      pos = 0;

      {
        char buf[size];
        if (oplog.serialize(buf, size, pos) < 0)
        {
          TBSYS_LOG(ERROR, "%s", "oplog serialize error");
        }
        else
        {
          //insert a new block, write op log.
          if (oplog_sync_mgr_.log(OPLOG_TYPE_BLOCK_OP, buf, size) != TFS_SUCCESS)
          {
            // treat log operation as a trivial thing..
            // don't rollback the insert operation, cause block meta info
            // build from all dataserver, log info not been very serious.
            TBSYS_LOG(ERROR, "write oplog failed, block(%u)", block_id);
          }
        }
      }

      #if defined(TFS_NS_DEBUG)
      print_servers(need, result);
      TBSYS_LOG(DEBUG, "need dataserver size: %u, %s", need.size(), result.c_str());
      #endif

      //send add new block message to dataserver
      iter = need.begin();
      for (; iter != need.end(); ++iter)
      {
      #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION)
        success.push_back((*iter));
      #else
        Message* ret_msg = NULL;
        NewBlockMessage msg;
        msg.add_new_id(block_id);
        if (message::NewClientManager::get_instance().call((*iter)->id(), &msg, DEFAULT_NETWORK_CALL_TIMEOUT, ret_msg) != TFS_SUCCESS)
        {
          success.push_back((*iter));
          TBSYS_LOG(INFO, "add block(%u) on server(%s) successful",
              block_id, CNetUtil::addrToString((*iter)->id()).c_str());
        }
        else
        {
          failed.push_back((*iter));
          TBSYS_LOG(INFO, "add block(%u) on server(%s) failed",
              block_id, CNetUtil::addrToString((*iter)->id()).c_str());
        }
      #endif
      }

      if (success.empty() || !failed.empty())
      {
        TBSYS_LOG(ERROR, "add block(%u) failed, successful size(%u), failed size(%u), rollback",
          block_id, success.size(), failed.size()); 
        goto rollback;
      }

      {
        RWLock::Lock lock(*ptr, WRITE_LOCKER);
        iter = success.begin();
        for (; iter != success.end(); ++iter)
        {
          if (build_relation(block, (*iter), now) != TFS_SUCCESS)
          {
            TBSYS_LOG(WARN, "it's failed that build relation between block(%u) and server(%s)",
                block_id, CNetUtil::addrToString((*iter)->id()).c_str());
          }
        }
        block->set_create_flag();
        std::vector<int64_t> stat(1,success.size());
        GFactory::get_stat_mgr().update_entry(GFactory::tfs_ns_stat_block_count_, stat);
        TBSYS_LOG(INFO, "add new block(%u) successful", block->id());
      }
      return block;
rollback:
      if (block != NULL)
      {
        {
          RWLock::Lock lock(*ptr, WRITE_LOCKER);
          ptr->remove(block_id);
        }

        if (!need.empty())
        {
          oplog.cmd_ = OPLOG_REMOVE;
          size = oplog.get_serialize_size();
          if (!failed.empty())
          {
            oplog.servers_.clear();
            iter = failed.begin();
            for (; iter != failed.end(); ++iter)
            {
              oplog.servers_.push_back((*iter)->id());
            }
          }
          pos = 0;
          char buf[size];
          if (oplog.serialize(buf, size, pos) < 0)
          {
            TBSYS_LOG(ERROR, "%s", "oplog serialize error");
          }
          else
          {
            //insert a new block, write op log.
            if (oplog_sync_mgr_.log(OPLOG_TYPE_BLOCK_OP, buf, size) != TFS_SUCCESS)
            {
              // treat log operation as a trivial thing..
              // don't rollback the insert operation, cause block meta info
              // build from all dataserver, log info not been very serious.
              TBSYS_LOG(ERROR, "write oplog failed, block(%u)", block_id);
            }
          }
        }
      }
      return NULL;
    }
    /**
     * dsataserver start, send heartbeat message to nameserver.
     * update all relations of blocks belongs to it
     * @param [in] dsInfo: dataserver system info , like capacity, load, etc..
     * @param [in] blocks: data blocks' info which belongs to dataserver.
     * @param [out] expires: need expire blocks
     * @return success or failure
     */
    int LayoutManager::update_relation(ServerCollect* server, const std::vector<BlockInfo>& blocks, EXPIRE_BLOCK_LIST& expires, time_t now)
    {
      bool all_success = true;
      bool bret = ((server != NULL && server->is_alive()));
      if (bret)
      {
        //release relation
        bool ret = relieve_relation(server, now);
        if (!ret)
        {
          all_success = false;
          TBSYS_LOG(WARN, "relieve relation failed in dataserver(%s)", CNetUtil::addrToString(server->id()).c_str());
        }

        uint32_t blocks_size = blocks.size();
        NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
        for (uint32_t i = 0; i < blocks_size; ++i)
        {
          if (blocks[i].block_id_ == 0)
          {
            TBSYS_LOG(WARN, "dataserver(%s) report, block == 0", tbsys::CNetUtil::addrToString(server->id()).c_str()); 
            continue;
          }

          bool first = false;
          bool force_be_master = false;

          // check block version, rebuilding relation.
          {
            BlockChunkPtr ptr = get_chunk(blocks[i].block_id_);
            RWLock::Lock lock(*ptr, WRITE_LOCKER);
            BlockCollect* block = ptr->find(blocks[i].block_id_);
            if (block == NULL)
            {
              TBSYS_LOG(INFO, "block(%u) not found in dataserver(%s), must be create", 
                  blocks[i].block_id_, tbsys::CNetUtil::addrToString(server->id()).c_str());
              block = ptr->add(blocks[i].block_id_, now);
              first = true;
            }
            if (!block->check_version(server, alive_server_size_, ngi.owner_role_, first,
                  blocks[i], expires, force_be_master, now)) 
            {
              continue;//version error, not argeed
            }

            //build relation
            int iret = build_relation(block, server, force_be_master);
            if (iret != TFS_SUCCESS)
            {
              all_success = false;
              TBSYS_LOG(WARN, "build relation fail between dataserver(%s) and block(%u)", CNetUtil::addrToString(server->id()).c_str(), block->id());
            }   
          }
        }
      }
      return bret ? all_success ? TFS_SUCCESS : TFS_ERROR : TFS_ERROR;
    }

    int LayoutManager::build_relation(BlockCollect* block, ServerCollect* server, time_t now, bool force)
    {
      bool bret = (block != NULL && server != NULL && server->is_alive());
      if (bret)
      {
        bool writable = false;
        BlockChunkPtr ptr = get_chunk(block->id());
        bret = ptr->connect(block, server, now, force, writable);
        if (!bret)
        {
          ptr->remove(block->id());
          TBSYS_LOG(WARN, "build relation fail between dataserver(%s) and block(%u)", CNetUtil::addrToString(server->id()).c_str(), block->id());
          return TFS_ERROR;
        }
        //build relation between dataserver and block
        //add to dataserver's all kind of list
        bret = server->add(block, writable);
      }
      return bret ? TFS_SUCCESS : TFS_ERROR;
    }

    bool LayoutManager::relieve_relation(BlockCollect* block, ServerCollect* server, time_t now)
    {
      bool bret= (block != NULL &&  server != NULL);
      if (bret)
      {
        //release relation between block and dataserver
        bool bremove = block->remove(server, now);
        if (!bremove)
        {
          TBSYS_LOG(ERROR, "failed when relieve between block(%u) and dataserver(%s)",
              block->id(), CNetUtil::addrToString(server->id()).c_str()); 
        }
        bool sremove = server->remove(block);
        if (!sremove)
        {
          TBSYS_LOG(ERROR, "failed when relieve between block(%u) and dataserver(%s)",
              block->id(), CNetUtil::addrToString(server->id()).c_str()); 
        }
        bret = bremove && sremove;
      }
      return bret;
    }

    bool LayoutManager::relieve_relation(ServerCollect* server, time_t now)
    {
      bool bret = server != NULL;
      return bret ? server->clear(*this, now) : bret;
    }

    void LayoutManager::rotate(time_t now)
    {
      if ((now % 86400 >= zonesec_)
          && (now % 86400 < zonesec_ + 300)
          && (last_rotate_log_time_ < now - 600)) 
      {
        last_rotate_log_time_ = now;
        oplog_sync_mgr_.rotate();
        TBSYS_LOGGER.rotateLog(SYSPARAM_NAMESERVER.config_log_file_);
      }
    }

    uint32_t LayoutManager::get_alive_block_id() const
    {
      uint32_t max_block_id = atomic_inc(&max_block_id_);
      while (true)
      {
        BlockChunkPtr ptr = get_chunk(max_block_id);
        RWLock::Lock lock(*ptr, READ_LOCKER);
        if (!ptr->exist(max_block_id))
          break;
        max_block_id = atomic_inc(&max_block_id_);
      }
      return max_block_id;
    }

    uint32_t LayoutManager::calc_max_block_id()
    {
      uint32_t max_block_id = 0;
      uint32_t current = 0;
      for (int32_t i = 0; i < block_chunk_num_; ++i)
      {
        common::RWLock::Lock lock(*block_chunk_[i], common::READ_LOCKER);
        current = block_chunk_[i]->calc_max_block_id();
        max_block_id = std::max(max_block_id, current);
      }
      max_block_id += SKIP_BLOCK;
      atomic_add(&max_block_id_, max_block_id);
      return max_block_id;
    }

    int LayoutManager::touch(uint64_t server, time_t now, bool promote)
    {
      RWLock::Lock lock(server_mutex_, READ_LOCKER);
      ServerCollect* object = get_server(server);
      return touch(object, now, promote);
    }

    int64_t LayoutManager::calc_all_block_bytes() const
    {
      int64_t ret = 0;
      for (int32_t i = 0; i < block_chunk_num_; ++i)
      {
        RWLock::Lock lock(*block_chunk_[i], READ_LOCKER);
        ret += block_chunk_[i]->calc_all_block_bytes();
      }
      return ret;
    }

    int64_t LayoutManager::calc_all_block_count() const
    {
      int64_t ret = 0;
      for (int32_t i = 0; i < block_chunk_num_; ++i)
      {
        RWLock::Lock lock(*block_chunk_[i], READ_LOCKER);
        ret += block_chunk_[i]->calc_size();
      }
      return ret;
    }

    BlockCollect* LayoutManager::elect_write_block()
    {
      BlockCollect* block = NULL;
      ServerCollect* server = NULL;
      RWLock::Lock lock(server_mutex_, READ_LOCKER);
      const int32_t count = static_cast<int32_t>(servers_index_.size());
      int64_t loop = 0;
      int64_t index = 0;

      while(count > 0 && block == NULL && loop < count)
      {
        ++loop;
        {
          tbutil::Mutex::Lock r_lock(elect_index_mutex_);
          ++write_index_;
          if (write_index_ >= count)
            write_index_ = 0;
          index = write_index_;
        }
        server = servers_index_[index];
        block = server->elect_write_block();
        #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
        TBSYS_LOG(DEBUG, "block(%u) server: %s, write_index: %"PRI64_PREFIX"d, count: %u", block != NULL ? block->id() : 1, tbsys::CNetUtil::addrToString(server->id()).c_str(), index, count);
        #endif
      }

      loop = 0;
      while(count > 0 && block == NULL && loop < count)
      {
        ++loop;
        {
          tbutil::Mutex::Lock r_lock(elect_index_mutex_);
          ++write_second_index_;
          if (write_second_index_ >= count)
            write_second_index_ = 0;
          index = write_second_index_;
        }
        server = servers_index_[index];
        block = server->force_elect_write_block();
        #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
        TBSYS_LOG(DEBUG, "block(%u) server: %s, second_write_index: %"PRI64_PREFIX"d, count: %u", block != NULL ? block->id() : 0, tbsys::CNetUtil::addrToString(server->id()).c_str(), index, count);
        #endif
      }
      return block;
    }

    /**
     * dataserver is the need to add testing of new block
     * @param[in] server: dataserver object
     * @param[in] promote:  true: check writable block, false: no check
     * @return: return 0 if no need add, otherwise need block count
     */
    int LayoutManager::touch(ServerCollect* server, time_t now, bool promote)
    {
      bool bret = server != NULL;
      if (bret)
      {
        int32_t count = SYSPARAM_NAMESERVER.add_primary_block_count_;
        server->touch(*this, now, max_block_id_, alive_server_size_, promote, count);

        if (GFactory::get_runtime_info().owner_role_ != NS_ROLE_MASTER)
          return TFS_SUCCESS;

        uint32_t new_block_id = 0;
        for (int32_t i = 0; i < count; i++, new_block_id = 0)
        {
          add_new_block(new_block_id, server, now);
        }
      }
      return TFS_SUCCESS;
    }

    void LayoutManager::check_server()
    {
      {
      #if !defined(TFS_NS_GTEST) && !defined(TFS_NS_INTEGRATION)
        tbutil::Monitor<tbutil::Mutex>::Lock lock(check_server_monitor_);
        check_server_monitor_.timedWait(tbutil::Time::seconds(SYSPARAM_NAMESERVER.safe_mode_time_));
      #endif
      }
      bool isnew = true;
      VUINT64 dead_servers;
      NsGlobalInfo stat_info;
      ServerCollect *server = NULL;
      std::list<ServerCollect*> alive_servers;
      #if !defined(TFS_NS_GTEST) && !defined(TFS_NS_INTEGRATION)
      const NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      while (ngi.destroy_flag_ != NS_DESTROY_FLAGS_YES)
      #endif
      {
        server = NULL;
        dead_servers.clear();
        alive_servers.clear();
        memset(&stat_info, 0, sizeof(NsGlobalInfo));
        time_t now = time(NULL);
        {
          //check dataserver is alive
          RWLock::Lock lock(server_mutex_, WRITE_LOCKER);
          SERVER_MAP_ITER iter = servers_.begin();
          for (; iter != servers_.end(); ++iter)
          {
            server = iter->second;
            if (!server->is_alive(now))
            {
              if (test_server_alive(server->id()) == TFS_SUCCESS)
              {
                server->touch(now);
              }
              else
              {
                server->dead();
                dead_servers.push_back(server->id());
              }
            }
            else
            {
              server->statistics(stat_info, isnew);
              alive_servers.push_back(server);
            }
          }
        }

        // write global information
        #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
        TBSYS_LOG(INFO, "dead dataserver size: %u, alive dataserver size: %u", dead_servers.size(), alive_servers.size());
        GFactory::get_global_info().dump();
        #endif
        GFactory::get_global_info().update(stat_info);
        #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
        GFactory::get_global_info().dump();
        #endif

        std::list<ServerCollect*>::iterator it = alive_servers.begin();
        for (; it != alive_servers.end(); ++it)
        {
          TBSYS_LOG(DEBUG, "server touch, block count(%u), master block count(%u)", (*it)->block_count(), (*it)->get_hold_master_size());
          touch((*it), now, true);
        }

        VUINT64::iterator iter = dead_servers.begin();
        for (; iter != dead_servers.end(); ++iter)
        {
          remove_server((*iter), now);
        }

         if (!dead_servers.empty())
        {
          interrupt(INTERRUPT_ALL, now);
        }
        tbutil::Monitor<tbutil::Mutex>::Lock lock(check_server_monitor_);
        check_server_monitor_.timedWait(tbutil::Time::seconds(SYSPARAM_NAMESERVER.heart_interval_));
      }
    }
    int LayoutManager::set_runtime_param(uint32_t value1, uint32_t value2, char *retstr)
    {
      bool bret = retstr != NULL;
      if (bret)
      {
        retstr[0] = '\0';
        int32_t index = (value1 & 0x0FFFFFFF);
        int32_t set = (value1& 0xF0000000);
        int32_t* param[] =
        {
          &SYSPARAM_NAMESERVER.min_replication_,
          &SYSPARAM_NAMESERVER.max_replication_,
          &SYSPARAM_NAMESERVER.max_write_file_count_,
          &SYSPARAM_NAMESERVER.max_use_capacity_ratio_,
          &SYSPARAM_NAMESERVER.heart_interval_,
          &SYSPARAM_NAMESERVER.replicate_wait_time_,
          &SYSPARAM_NAMESERVER.compact_delete_ratio_,
          &SYSPARAM_NAMESERVER.compact_max_load_,
          &plan_run_flag_,
          &SYSPARAM_NAMESERVER.run_plan_expire_interval_,
          &SYSPARAM_NAMESERVER.run_plan_ratio_,
          &SYSPARAM_NAMESERVER.object_dead_max_time_,
          &SYSPARAM_NAMESERVER.balance_max_diff_block_num_,
          &TBSYS_LOGGER._level,
          &SYSPARAM_NAMESERVER.add_primary_block_count_,
          &SYSPARAM_NAMESERVER.build_plan_interval_,
          &SYSPARAM_NAMESERVER.replicate_ratio_,
          &SYSPARAM_NAMESERVER.max_wait_write_lease_,
          &SYSPARAM_NAMESERVER.cluster_index_,
          &SYSPARAM_NAMESERVER.build_plan_default_wait_time_,
        };
        int32_t size = sizeof(param) / sizeof(int32_t*);
        if (index < 0x01 || index > size)
        {
          snprintf(retstr, 0xff, "index (%d) invalid.", index);
          TBSYS_LOG(ERROR, "index(%d) invalid.", index);
          return TFS_SUCCESS;
         }
        int32_t* current_value = param[index - 1];
        if (set)
        {
          *current_value = (int32_t)(value2 & 0xFFFFFFFF);
        }
        else
        {
          snprintf(retstr, 0xff, "%d", *current_value);
        }
        TBSYS_LOG(INFO, "index(%d) set(%d) name(%s) value(%d)", index, set, dynamic_parameter_str[index - 1].c_str(), *current_value);
      }
      return bret ? TFS_SUCCESS : TFS_ERROR;
    }

    int LayoutManager::build_plan()
    {
      bool bwait = true;
      bool interrupt = true;
      int64_t emergency_replicate_count = 0;
      const NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();

      #if (!defined(TFS_NS_GTEST))
      while (ngi.destroy_flag_ != NS_DESTROY_FLAGS_YES)
      #endif
      {
        time_t now = time(NULL);
        int64_t should  = static_cast<int64_t>((alive_server_size_ * SYSPARAM_NAMESERVER.run_plan_ratio_)/ 100);
        int64_t current = 0;
        int64_t need =  0;
        int64_t adjust = 0;
        int64_t total = 0;
        {
          tbutil::Monitor<tbutil::Mutex>::Lock lock(build_plan_monitor_);
          if (ngi.owner_role_ == NS_ROLE_SLAVE)
          {
            build_plan_monitor_.wait();
          }

          time_t wait_time = interrupt ? 0 : !bwait ? SYSPARAM_NAMESERVER.build_plan_default_wait_time_ : ngi.switch_time_ > now ? ngi.switch_time_ - now : SYSPARAM_NAMESERVER.build_plan_interval_;
          bwait = true;
          interrupt = false;
          build_plan_monitor_.timedWait(tbutil::Time::seconds(wait_time));
  
          current = get_pending_plan_size() + get_running_plan_size();
          need    = should - current;
          total   = need;

          #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
          TBSYS_LOG(DEBUG, "wait_time: %"PRI64_PREFIX"d, switch_time: %"PRI64_PREFIX"d, now: %"PRI64_PREFIX"d", wait_time, ngi.switch_time_, now);
          #endif
        }

        //checkpoint
        rotate(now);

        #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
        TBSYS_LOG(DEBUG, "SYSPARAM_NAMESERVER.run_plan_ratio_(%d), alive_server_size_: %d", SYSPARAM_NAMESERVER.run_plan_ratio_, alive_server_size_);
        #endif

        if (need <= 0)
        {
          TBSYS_LOG(WARN, "plan size(%"PRI64_PREFIX"d) > should(%"PRI64_PREFIX"d), nothing to do", current, should);
        }

        TBSYS_LOG(INFO, "current plan size: %"PRI64_PREFIX"d, should: %"PRI64_PREFIX"d, need: %"PRI64_PREFIX"d",
          current, should, need);

      #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION)
        std::vector<uint32_t> blocks;
        bool bret = false;
        if ((plan_run_flag_ & PLAN_RUN_FLAG_REPLICATE)
            && (!(interrupt_ & INTERRUPT_ALL))
            && (need > 0))
        {
          bret = build_replicate_plan(now, need, adjust, emergency_replicate_count, blocks);
          if (!bret)
          {
            TBSYS_LOG(ERROR, "%s", "build replicate plan failed");
          } 
          TBSYS_LOG(INFO, "adjust: %"PRI64_PREFIX"d, remainder emergency replicate count: %"PRI64_PREFIX"d",
            adjust, emergency_replicate_count);
          bwait = (emergency_replicate_count <= 0);
          emergency_replicate_count = 0;
        }

        if ((plan_run_flag_ & PLAN_RUN_FLAG_MOVE)
            && (!(interrupt_ & INTERRUPT_ALL))
            && (need > 0))
        {
          bret = build_balance_plan(now, need, blocks);
          if (!bret)
          {
            TBSYS_LOG(ERROR, "%s", "build balance plan failed");
          }
        }

        if ((plan_run_flag_ & PLAN_RUN_FLAG_COMPACT)
            && (!(interrupt_ & INTERRUPT_ALL))
            && (need > 0))
        {
          bret = build_compact_plan(now, need, blocks);
          if (!bret)
          {
            TBSYS_LOG(ERROR, "%s", "build compact plan failed");
          }
        }

        if ((plan_run_flag_ & PLAN_RUN_FLAG_DELETE)
            && (!(interrupt_ & INTERRUPT_ALL))
            && (need > 0))
        {
          bret = build_redundant_plan(now, need, blocks);
          if (!bret)
          {
            TBSYS_LOG(ERROR, "%s", "build redundant plan failed");
          }
        }

      #else
        bool bret = false;
        if ((plan_run_flag_ & PLAN_RUN_FLAG_REPLICATE)
            && (!(interrupt_ & INTERRUPT_ALL))
            && (need > 0))
        {
          bret = build_replicate_plan(now, need, adjust, emergency_replicate_count);
          if (!bret)
          {
            TBSYS_LOG(ERROR, "%s", "build replicate plan failed");
          } 
          TBSYS_LOG(INFO, "adjust: %"PRI64_PREFIX"d, remainder emergency replicate count: %"PRI64_PREFIX"d",
            adjust, emergency_replicate_count);
          bwait = (emergency_replicate_count <= 0);
          emergency_replicate_count = 0;
        }

        if ((plan_run_flag_ & PLAN_RUN_FLAG_MOVE)
            && (!(interrupt_ & INTERRUPT_ALL))
            && (need > 0))
        {
          bret = build_balance_plan(now, need);
          if (!bret)
          {
            TBSYS_LOG(ERROR, "%s", "build balance plan failed");
          }
        }

        if ((plan_run_flag_ & PLAN_RUN_FLAG_COMPACT)
            && (!(interrupt_ & INTERRUPT_ALL))
            && (need > 0))
        {
          bret = build_compact_plan(now, need);
          if (!bret)
          {
            TBSYS_LOG(ERROR, "%s", "build compact plan failed");
          }
        }

        if ((plan_run_flag_ & PLAN_RUN_FLAG_DELETE)
            && (!(interrupt_ & INTERRUPT_ALL))
            && (need > 0))
        {
          bret = build_redundant_plan(now, need);
          if (!bret)
          {
            TBSYS_LOG(ERROR, "%s", "build redundant plan failed");
          }
        }
      #endif

        if((interrupt_ & INTERRUPT_ALL))
        {
          interrupt = true;
          TBSYS_LOG(INFO, "receive interrupt(%d)", interrupt_);
          tbutil::Monitor<tbutil::Mutex>::Lock lock(run_plan_monitor_);
          std::set<TaskPtr, TaskCompare>::iterator iter = pending_plan_list_.begin();
          for (; iter != pending_plan_list_.end(); ++iter)
            finish_plan_list_.push_back((*iter));
        }
        interrupt_ = INTERRUPT_NONE;
        TBSYS_LOG(INFO, "build plan complete, complete: %"PRI64_PREFIX"d", ((total  + adjust)- need));
      }
      return TFS_SUCCESS;
    }

    void LayoutManager::run_plan()
    {
      const NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
    #if !defined(TFS_NS_GTEST)
      while (ngi.destroy_flag_ != NS_DESTROY_FLAGS_YES)
    #endif
      {
        tbutil::Monitor<tbutil::Mutex>::Lock lock(run_plan_monitor_);
        while ((pending_plan_list_.empty())
            && (ngi.destroy_flag_ != NS_DESTROY_FLAGS_YES))
        {
          run_plan_monitor_.wait();
        }

        //handle task
        if (!pending_plan_list_.empty())//find task
        {
          std::set<TaskPtr, TaskCompare>::const_iterator iter = pending_plan_list_.begin();
          if (iter != pending_plan_list_.end())
          {
            TaskPtr task = (*iter);
            if (task->handle() != TFS_SUCCESS)
            {
              task->dump(TBSYS_LOG_LEVEL_ERROR, "task handle fail");
              RWLock::Lock tlock(maping_mutex_, WRITE_LOCKER);
              block_to_task_.erase(task->block_id_);
              std::vector<ServerCollect*>::iterator i_iter = task->runer_.begin();
              for (; i_iter != task->runer_.end(); ++i_iter)
              {
                server_to_task_.erase((*i_iter));
              }
            }
            else
            {
              std::pair<std::set<TaskPtr, TaskCompare>::iterator, bool> res = running_plan_list_.insert((*iter));
              if (!res.second)
              {
                TBSYS_LOG(WARN, "%s", "task exist");
              }
              else
              {
                GFactory::get_timer()->schedule((*iter), tbutil::Time::seconds((*iter)->end_time_ - (*iter)->begin_time_));
              }
            }
            pending_plan_list_.erase(iter);
          }
        }
       }
    }

    void LayoutManager::destroy_plan()
    {
      {
        tbutil::Monitor<tbutil::Mutex>::Lock lock(run_plan_monitor_);
        pending_plan_list_.clear();
        running_plan_list_.clear();
        finish_plan_list_.clear();
      }

      {
        RWLock::Lock tlock(maping_mutex_, WRITE_LOCKER);
        block_to_task_.clear();
        server_to_task_.clear();
      }
    }

    void LayoutManager::find_need_replicate_blocks(const int64_t need,
                                                  const time_t now,
                                                  int64_t& emergency_replicate_count,
                                                  std::multimap<PlanPriority, BlockCollect*>& middle)
    {
      for (int32_t i = 0; i < block_chunk_num_ && !(interrupt_ & INTERRUPT_ALL) && need > 0; ++i)
      {
        RWLock::Lock lock(*block_chunk_[i], READ_LOCKER);
        const BLOCK_MAP& blocks = block_chunk_[i]->block_map_;
        BLOCK_MAP::const_iterator iter = blocks.begin();
        for (; iter != blocks.end() && !(interrupt_ & INTERRUPT_ALL) && need > 0; ++iter)
        {
          PlanPriority level = PLAN_PRIORITY_NONE;
          if ((level = iter->second->check_replicate(now)) >= PLAN_PRIORITY_NORMAL)
          {
            if (level == PLAN_PRIORITY_EMERGENCY)
            {
              ++emergency_replicate_count;
            }
            middle.insert(std::pair<PlanPriority, BlockCollect*>(level, iter->second));
          }
        }
      }
    }

#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION)
    bool LayoutManager::build_replicate_plan(time_t now,
        int64_t& need,
        int64_t& adjust,
        int64_t& emergency_replicate_count,
        std::vector<uint32_t>& blocks)
#else
    bool LayoutManager::build_replicate_plan(time_t now,
        int64_t& need,
        int64_t& adjust,
        int64_t& emergency_replicate_count)
#endif
      {
      std::multimap<PlanPriority, BlockCollect*> middle;

      find_need_replicate_blocks(need, now, emergency_replicate_count, middle);

      TBSYS_LOG(INFO, "block count: %"PRI64_PREFIX"d, emergency_replicate_count: %"PRI64_PREFIX"d, need: %"PRI64_PREFIX"d", calc_all_block_count(), emergency_replicate_count, need);

      //adjust plan size
      if (emergency_replicate_count > need)
      {
        adjust = need;
        need += need;
      }

      uint32_t block_id  = 0;
      bool has_replicate = false;
      bool all_find_flag = true;
      std::vector<ServerCollect*> except;
      std::vector<ServerCollect*> source;
      std::multimap<PlanPriority, BlockCollect*>::const_reverse_iterator iter = middle.rbegin();
      for (; iter != middle.rend() && !(interrupt_ & INTERRUPT_ALL) && need > 0; ++iter)
      {
        except.clear();
        {
          BlockChunkPtr ptr = get_chunk(iter->second->id());
          RWLock::Lock lock(*ptr, READ_LOCKER);
          RWLock::Lock tlock(maping_mutex_, READ_LOCKER);
          has_replicate = ((!GFactory::get_lease_factory().has_valid_lease(iter->second->id()))
              && (!find_block_in_plan(iter->second->id()))
              && (!find_server_in_plan(iter->second->get_hold(), all_find_flag, except)));
          if (has_replicate)
          {
            source = iter->second->get_hold();
            block_id = iter->second->id();
          }
        }
        if (has_replicate)//need replicate
        {
          //elect source server
          find_server_in_plan_helper(source, except);
          std::vector<ServerCollect*> runer;
          std::vector<ServerCollect*> result;
          std::vector<ServerCollect*> except;
          int32_t iret = 0;
          {
            RWLock::Lock tlock(maping_mutex_, READ_LOCKER);
            iret = elect_replicate_source_ds(*this, source, except,1, result);
          }
          if (iret != 1)
          {
            TBSYS_LOG(WARN, "replicate block(%u) cannot found source dataserver", block_id);
            continue;
          }
          runer.push_back(result.back());

          //elect target server
          std::vector<ServerCollect*> target;
          except = source;
          {
            RWLock::Lock rlock(server_mutex_, READ_LOCKER);
            RWLock::Lock tlock(maping_mutex_, READ_LOCKER);
            iret = elect_replicate_dest_ds(*this, except, 1, target);
          }
          if (iret != 1)
          {
            TBSYS_LOG(WARN, "replicate block(%u) cannot found target dataserver", block_id);
            continue;
          }
          runer.push_back(target.back());
          ReplicateTaskPtr task = new ReplicateTask(this, iter->first, block_id,now, now, runer);
          #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
          task->dump(TBSYS_LOG_LEVEL_DEBUG);
          #endif
          if (!add_task(task))
          {
            task = 0;
            TBSYS_LOG(ERROR, "add task(replicate) fail, block(%u)", block_id);
            continue;
          }

          #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
          TBSYS_LOG(DEBUG, "add task, type(%d)", task->type_);
          #endif
          --need;
          --emergency_replicate_count;
          #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
          blocks.push_back(task->block_id_);
          #endif
        }
      }
      return true;
    }

  #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION)
    bool LayoutManager::build_compact_plan(time_t now, int64_t& need, std::vector<uint32_t>& plans)
  #else
    bool LayoutManager::build_compact_plan(time_t now, int64_t& need)
  #endif
    {
      bool has_compact = false;
      bool all_find_flag = false;
      std::vector<ServerCollect*> except;
      for (int32_t i = 0; i < block_chunk_num_ && !(interrupt_ & INTERRUPT_ALL) && need > 0; ++i)
      {
        RWLock::Lock lock(*block_chunk_[i], READ_LOCKER);
        const BLOCK_MAP& blocks = block_chunk_[i]->block_map_;
        BLOCK_MAP::const_iterator iter = blocks.begin();
        for (; iter != blocks.end() && !(interrupt_ & INTERRUPT_ALL) && need > 0; ++iter)
        {
          {
            RWLock::Lock tlock(maping_mutex_, READ_LOCKER);
            has_compact = ((iter->second->check_compact())
                && (!find_block_in_plan(iter->second->id()))
                && (!find_server_in_plan(iter->second->get_hold(), all_find_flag, except)));
          }

          if (has_compact)
          {
            CompactTaskPtr task = new CompactTask(this, PLAN_PRIORITY_NORMAL, iter->second->id(), now, now, iter->second->get_hold());
            if (!add_task(task))
            {
              task = 0;
              TBSYS_LOG(ERROR, "add task(compact) fail, block(%u)", iter->second->id());
              continue;
            }
            #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
            TBSYS_LOG(DEBUG, "add task, type(%d)", task->type_);
            #endif
            --need;
            #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
            plans.push_back(task->block_id_);
            #endif
          }
        }
      }
      return true;
    }

    int64_t LayoutManager::calc_average_block_size()
    {
      int64_t actual_total_block_num = calc_all_block_count();
      int64_t total_bytes = calc_all_block_bytes();
      return (actual_total_block_num > 0 && total_bytes > 0) 
        ? total_bytes/actual_total_block_num : SYSPARAM_NAMESERVER.max_block_size_;
    }
    
    /**
     * statistic all dataserver 's information(capactiy, block count, load && alive server size)
     */
    void LayoutManager::statistic_all_server_info(const int64_t need, 
        const int64_t average_block_size,
        double& total_capacity,
        int64_t& total_block_count,
        int64_t& total_load,
        int64_t& alive_server_size)
    {
      RWLock::Lock lock(server_mutex_, READ_LOCKER);
      SERVER_MAP::const_iterator iter = servers_.begin();
      for (; iter != servers_.end() && !(interrupt_ & INTERRUPT_ALL) && need > 0; ++iter)
      {
        if (iter->second->is_alive())
        {
          total_block_count += iter->second->block_count();
          total_capacity += (iter->second->total_capacity() * SYSPARAM_NAMESERVER.max_use_capacity_ratio_) / 100;
          total_capacity -= iter->second->use_capacity();
          total_load     += iter->second->load();
          ++alive_server_size;
        }
      }
      total_capacity += total_block_count * average_block_size;
    }

    void LayoutManager::split_servers(const int64_t need,
        const int64_t average_load,
        const double total_capacity,
        const int64_t total_block_count,
        const int64_t average_block_size,
        std::set<ServerCollect*>& source,
        std::set<ServerCollect*>& target)
    {
      bool has_move = false;
      int64_t current_block_count = 0;
      int64_t should_block_count  = 0;
      double current_total_capacity = 0;
      RWLock::Lock lock(server_mutex_, READ_LOCKER);
      SERVER_MAP::const_iterator iter = servers_.begin();
      for (; iter != servers_.end() && !(interrupt_ & INTERRUPT_ALL) && need > 0; ++iter)
      {
        {
          RWLock::Lock tlock(maping_mutex_, READ_LOCKER);
          has_move = ((iter->second->is_alive())
              && (!find_server_in_plan(iter->second)));

          #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
          TBSYS_LOG(DEBUG, "server(%s), alive(%d) find(%d)", CNetUtil::addrToString(iter->first).c_str(),
              iter->second->is_alive(), !find_server_in_plan(iter->second));
          #endif
        }
        if (has_move)
        {
          current_block_count = iter->second->block_count();
          current_total_capacity = current_block_count * average_block_size +
            iter->second->total_capacity() * SYSPARAM_NAMESERVER.max_use_capacity_ratio_ / 100;
          should_block_count = static_cast<int64_t>((current_total_capacity / total_capacity) * total_block_count);
          #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
          TBSYS_LOG(DEBUG, "server(%s), current_block_count(%"PRI64_PREFIX"d) should_block_count(%"PRI64_PREFIX"d)", CNetUtil::addrToString(iter->first).c_str(), current_block_count, should_block_count);
          #endif
          if (current_block_count > should_block_count  + SYSPARAM_NAMESERVER.balance_max_diff_block_num_)
          {
            source.insert(iter->second);
          }
          else
          {
            if ((average_load <= 0)
                || (iter->second->load() < average_load * LOAD_BASE_MULTIPLE))
            {
              target.insert(iter->second);
            }
          }
        }
      }
    }

  #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION)
    bool LayoutManager::build_balance_plan(time_t now, int64_t& need, std::vector<uint32_t>& plans)
  #else
    bool LayoutManager::build_balance_plan(time_t now, int64_t& need)
  #endif
    {
      double total_capacity  = 0;
      int64_t total_block_count = 0;
      int64_t total_load = 1;
      int64_t alive_server_size = 0;
      int64_t average_block_size = calc_average_block_size();

      statistic_all_server_info(need, average_block_size,total_capacity, total_block_count, total_load, alive_server_size);

      if (total_capacity <= 0)
      {
        TBSYS_LOG(INFO, "total_capacity(%"PRI64_PREFIX"d) <= 0, we'll doesn't build moveing plan", total_capacity);
        return true;
      }
      
      if (alive_server_size <= 0)
      {
        TBSYS_LOG(INFO, "alive_server_size(%"PRI64_PREFIX"d) <= 0, we'll doesn't build moveing plan", alive_server_size);
        return true;
      }

      std::set<ServerCollect*> target;
      std::set<ServerCollect*> source;
      int64_t average_load = total_load / alive_server_size;

      split_servers(need, average_load, total_capacity, total_block_count, average_block_size, source, target);

      TBSYS_LOG(INFO, "need(%"PRI64_PREFIX"d), source size(%u), target(%u)", need, source.size(), target.size());

      bool has_move = false;
      uint32_t block_id = 0;
      std::vector<ServerCollect*> except;
      std::vector<ServerCollect*> servers;
      std::set<uint32_t> need_move_block_list;
      std::set<ServerCollect*>::const_iterator it = source.begin();
      for (; it != source.end() && !(interrupt_ & INTERRUPT_ALL) && need > 0 && !target.empty(); ++it)
      {
        (*it)->rdlock();
        std::set<BlockCollect*, ServerCollect::BlockIdComp> blocks((*it)->hold_);
        (*it)->unlock();

        std::set<BlockCollect*, ServerCollect::BlockIdComp>::const_iterator cn_iter = blocks.begin();
        for (; cn_iter != blocks.end() && !(interrupt_ & INTERRUPT_ALL) && need > 0; ++cn_iter)
        {
          except.clear();
          BlockCollect* block_collect = *cn_iter;
          {
            BlockChunkPtr ptr = get_chunk(block_collect->id());
            RWLock::Lock r_lock(*ptr, READ_LOCKER);
            RWLock::Lock lock(maping_mutex_, READ_LOCKER);
            has_move = ((block_collect != NULL)
                && (block_collect->check_balance())
                && (!find_server_in_plan((*it)))
                && (need_move_block_list.find(block_collect->id()) == need_move_block_list.end())
                && (!find_block_in_plan(block_collect->id())));
            #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
            TBSYS_LOG(DEBUG, "block(%u) check balance has_move(%s)", block_collect->id(), has_move ? "true" : "false");
            #endif
            if (has_move)
            {
              servers = block_collect->get_hold();
              block_id = block_collect->id();
            }
          }
          if (has_move)
          {
            std::vector<ServerCollect*>::iterator where = std::find(servers.begin(), servers.end(), (*it)); 
            if (where == servers.end())
            {
              TBSYS_LOG(ERROR, "cannot elect move source server block(%u), source(%s)",
                  block_id, CNetUtil::addrToString((*it)->id()).c_str());
              continue;
            }
            servers.erase(where);
            
            //elect dest dataserver
            ServerCollect* target_ds = NULL;
            bool bret = elect_move_dest_ds(target, servers, (*it), &target_ds);
            if (!bret)
            {
              TBSYS_LOG(ERROR, "cannot elect move dest server block(%u), source(%s)",
                  block_collect->id(), CNetUtil::addrToString((*it)->id()).c_str());
              continue;
            }
            std::vector<ServerCollect*> runer;
            runer.push_back((*it));
            runer.push_back(target_ds);
            MoveTaskPtr task = new MoveTask(this, PLAN_PRIORITY_NORMAL,  block_id, now , now, runer);

            #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
            task->dump(TBSYS_LOG_LEVEL_DEBUG);
            #endif

            //push task to pending_plan_list_
            if (!add_task(task))
            {
              task = 0;
              TBSYS_LOG(ERROR, "add task(balance) fail, block(%u)", block_id);
              continue;
            }
            #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
            TBSYS_LOG(DEBUG, "add task, type(%d)", task->type_);
            #endif

            --need;
            need_move_block_list.insert(block_collect->id());
            std::set<ServerCollect*>::iterator tmp = target.find((*it));
            if (tmp != target.end())
            {
              target.erase(tmp);
            }
            tmp = target.find(target_ds);
            if (tmp != target.end())
            {
              target.erase(tmp);
            }
            #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
            plans.push_back(task->block_id_);
            #endif
            break;
          }
        }
      }
      return true;
    }

  #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION)
    bool LayoutManager::build_redundant_plan(time_t now, int64_t& need, std::vector<uint32_t>& plans)
  #else
    bool LayoutManager::build_redundant_plan(time_t now, int64_t& need)
  #endif
    {
      bool all_find_flag = true;
      bool has_delete = false;
      int32_t  count = 0;
      std::vector<ServerCollect*> except;
      std::vector<ServerCollect*> servers;
      for (int32_t i = 0; i < block_chunk_num_ && !(interrupt_ & INTERRUPT_ALL) && need > 0; ++i)
      {
        RWLock::Lock lock(*block_chunk_[i], READ_LOCKER);
        const BLOCK_MAP& blocks = block_chunk_[i]->block_map_;
        BLOCK_MAP::const_iterator iter = blocks.begin();
        for (; iter != blocks.end() && !(interrupt_ & INTERRUPT_ALL) && need > 0; ++iter)
        {
          except.clear();
          count = iter->second->check_redundant();
          {
            RWLock::Lock tlock(maping_mutex_, READ_LOCKER);
            has_delete = ((count > 0)
                && (!find_block_in_plan(iter->second->id()))
                && (!find_server_in_plan(iter->second->get_hold(), all_find_flag, except)));
            if (has_delete)
            {
              servers = iter->second->get_hold();
            }
          }
          if (has_delete)
          {
            std::vector<ServerCollect*> result;
            find_server_in_plan_helper(servers, except);
            if ((delete_excess_backup(servers, count, result) > 0) 
                && (!result.empty()))
            {
              TBSYS_LOG(INFO, "we will need delete less than block(%u)", iter->second->id());
              RedundantTaskPtr task = new RedundantTask(this, PLAN_PRIORITY_NORMAL, iter->second->id(), now, now, result);
              if (!add_task(task))
              {
                task = 0;
                TBSYS_LOG(ERROR, "add task(delete) fail, block(%u)", iter->second->id());
                continue;
              }
              --need;
              #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
              TBSYS_LOG(DEBUG, "add task, type(%d)", task->type_);
              #endif 
              #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
              plans.push_back(task->block_id_);
              #endif
            } 
          }
        }
      }
      return true;
    }

    bool LayoutManager::add_task(const TaskPtr& task)
    {
      tbutil::Monitor<tbutil::Mutex>::Lock lock(run_plan_monitor_);
      std::pair<std::set<TaskPtr, TaskCompare>::iterator, bool> res = pending_plan_list_.insert(task);
      if (!res.second)
      {
        TBSYS_LOG(ERROR, "object was found by task in plan, block(%u), type(%d)", task->block_id_, task->type_);
        return true;
      }

      RWLock::Lock tlock(maping_mutex_, WRITE_LOCKER);
      std::pair<std::map<uint32_t, TaskPtr>::iterator, bool> rs = 
        block_to_task_.insert(std::map<uint32_t, TaskPtr>::value_type(task->block_id_, task));
      if (!rs.second)
      {
        TBSYS_LOG(ERROR, "object was found by block(%u) in block list", task->block_id_);
        pending_plan_list_.erase(res.first);
        return false;
      }
      std::pair<std::map<ServerCollect*,TaskPtr>::iterator, bool> iter;
      std::vector<ServerCollect*>::iterator index = task->runer_.begin();
      for (; index != task->runer_.end(); ++index)
      {
        iter = server_to_task_.insert(std::map<ServerCollect*,TaskPtr>::value_type((*index), task));
        #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
        //TBSYS_LOG(ERROR, "server(%ld), server(%ld)", (*index)->id(), (iter.first->first)->id());
        #endif
      }
      run_plan_monitor_.notify();
      #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
      std::stringstream out;
      out << task->type_ << " " ;
      out << task->block_id_ << " " ;
      index = task->runer_.begin();
      {
        out << (*index)->id() << " ";
      }
      TBSYS_LOG(ERROR, " add_task (%s)", out.str().c_str());
      #endif
      return true;
    }
    bool LayoutManager::remove_task(const TaskPtr& task)
    {
      return true;
    }

    LayoutManager::TaskPtr LayoutManager::find_task(const uint32_t block_id)
    {
      std::map<uint32_t, TaskPtr>::const_iterator iter = block_to_task_.find(block_id);
      if (iter != block_to_task_.end())
      {
        return iter->second;
      }
      return 0;
    }

    bool LayoutManager::find_block_in_plan(const uint32_t block_id)
    {
      return (block_to_task_.end() != block_to_task_.find(block_id));
    }

    bool LayoutManager::find_server_in_plan(ServerCollect* server)
    {
      return (server_to_task_.end() != server_to_task_.find(server));
    }

    bool LayoutManager::find_server_in_plan(const std::vector<ServerCollect*> & servers, bool all_find, std::vector<ServerCollect*>& result)
    {
      std::vector<ServerCollect*>::const_iterator iter = servers.begin();
      for (; iter != servers.end(); ++iter)
      {
        std::map<ServerCollect*, TaskPtr>::iterator it = server_to_task_.find((*iter));
        if (it != server_to_task_.end())
        {
          if (all_find)
          {
            result.push_back((*iter));
          }
          else
          {
            return true;
          }
        }
      }
      return all_find ? result.size() == servers.size() ? true : false : false;
    }

    void LayoutManager::find_server_in_plan_helper(std::vector<ServerCollect*>& servers, std::vector<ServerCollect*>& except)
    {
      std::vector<ServerCollect*>::iterator iter = except.begin();
      for (; iter != except.end(); ++iter)
      {
        std::vector<ServerCollect*>::iterator ret = std::find(servers.begin(), servers.end(), (*iter));
        if (ret != servers.end())
        {
          servers.erase(ret);
        }
      }
    }

    bool LayoutManager::expire()
    {
      tbutil::Monitor<tbutil::Mutex>::Lock lock(run_plan_monitor_);
      finish_plan_list_.clear();
      return true;
    }
  }
}
