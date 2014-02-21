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

#include <time.h>
#include <iostream>
#include <functional>
#include <numeric>
#include <math.h>
#include <tbsys.h>
#include <Memory.hpp>
#include "ns_define.h"
#include "nameserver.h"
#include "layout_manager.h"
#include "global_factory.h"
#include "common/error_msg.h"
#include "common/base_packet.h"
#include "common/base_service.h"
#include "common/status_message.h"
#include "common/client_manager.h"
#include "common/array_helper.h"
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
    LayoutManager::LayoutManager(NameServer& manager):
      build_plan_thread_(0),
      run_plan_thread_(0),
      check_dataserver_thread_(0),
      regular_create_block_for_serversthread_(0),
      balance_thread_(0),
      timeout_thread_(0),
      redundant_thread_(0),
      load_family_info_thread_(0),
      last_rotate_log_time_(0),
      plan_run_flag_(PLAN_RUN_FLAG_REPLICATE),
      manager_(manager),
      block_manager_(*this),
      server_manager_(*this),
      task_manager_(*this),
      oplog_sync_mgr_(*this),
      client_request_server_(*this),
      gc_manager_(*this, SYSPARAM_NAMESERVER.object_wait_free_time_ms_, SYSPARAM_NAMESERVER.object_wait_clear_time_ms_),
      family_manager_(*this)
    {
      srandom(time(NULL));
      tzset();
      zonesec_ = 86400 + timezone;
      last_rotate_log_time_ = 0;
      plan_run_flag_ |= PLAN_RUN_FLAG_MOVE;
      plan_run_flag_ |= PLAN_RUN_FLAG_COMPACT;
      plan_run_flag_ |= PLAN_RUN_FLAG_ADJUST_COPIES_LOCATION;
      //plan_run_flag_ |= PLAN_RUN_FALG_MARSHALLING;//TODO
      plan_run_flag_ |= PLAN_RUN_FALG_REINSTATE;
      plan_run_flag_ |= PLAN_RUN_FALG_DISSOLVE;
    }


    LayoutManager::~LayoutManager()
    {
      build_plan_thread_ = 0;
      run_plan_thread_ = 0;
      check_dataserver_thread_ = 0;
      regular_create_block_for_serversthread_ = 0;
      balance_thread_ = 0;
      timeout_thread_ = 0;
      redundant_thread_ = 0;
      load_family_info_thread_ = 0;
    }

    int LayoutManager::initialize()
    {
      int32_t ret = oplog_sync_mgr_.initialize();
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "initialize oplog sync manager fail, must be exit, ret: %d", ret);
      }
      else
      {
        //initialize thread
        build_plan_thread_ = new BuildPlanThreadHelper(*this);
        check_dataserver_thread_ = new CheckDataServerThreadHelper(*this);
        regular_create_block_for_serversthread_ = new AddBlockInAllServerThreadHelper(*this);
        run_plan_thread_ = new RunPlanThreadHelper(*this);
        balance_thread_  = new BuildBalanceThreadHelper(*this);
        timeout_thread_  = new TimeoutThreadHelper(*this);
        redundant_thread_= new RedundantThreadHelper(*this);
        load_family_info_thread_ = new LoadFamilyInfoThreadHelper(*this);
      }
      return ret;
    }

    void LayoutManager::wait_for_shut_down()
    {
      if (build_plan_thread_ != 0)
      {
        build_plan_thread_->join();
      }
      if (check_dataserver_thread_ != 0)
      {
        check_dataserver_thread_->join();
      }
      if (run_plan_thread_ != 0)
      {
        run_plan_thread_->join();
      }
      if (regular_create_block_for_serversthread_ != 0)
      {
        regular_create_block_for_serversthread_->join();
      }
      if (balance_thread_ != 0)
      {
        balance_thread_->join();
      }
      if (timeout_thread_ != 0)
      {
        timeout_thread_->join();
      }
      if (redundant_thread_ != 0)
      {
        redundant_thread_->join();
      }
      if (load_family_info_thread_ != 0)
      {
        load_family_info_thread_->join();
      }
      oplog_sync_mgr_.wait_for_shut_down();
    }

    void LayoutManager::destroy()
    {
      oplog_sync_mgr_.destroy();
    }

    /**
     * dsataserver start, send heartbeat message to nameserver.
     * update all relations of blocks belongs to it
     * @param [in] dsInfo: dataserver system info , like capacity, load, etc..
     * @param [in] blocks: data blocks' info which belongs to dataserver.
     * @param [out] expires: need expire blocks
     * @return success or failure
     */
    int LayoutManager::update_relation(std::vector<uint64_t>& expires, ServerCollect* server,
        const common::ArrayHelper<BlockInfoV2>& blocks, const time_t now)
    {
      int32_t ret = ((NULL != server) && (server->is_alive())) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = get_block_manager().update_relation(expires, server, blocks, now);
      }
      return ret;
    }

    int LayoutManager::build_relation(BlockCollect* block, ServerCollect* server, const time_t now, const bool set)
    {
      int32_t ret = ((NULL != block) && (NULL != server)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        bool writable = false;
        bool master   = false;
        ret = get_block_manager().build_relation(block, writable, master, server->id(), now, set);
        if (TFS_SUCCESS == ret)
        {
          ret = get_server_manager().build_relation(server, block->id(), writable, master);
        }
      }
      return ret;
    }

    bool LayoutManager::relieve_relation(BlockCollect* block, ServerCollect* server, time_t now, const bool print)
    {
      int32_t result = TFS_ERROR;
      int32_t ret = ((NULL != block) && (NULL != server)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        //release relation between block and dataserver
        ret = get_block_manager().relieve_relation(block, server->id(), now);
        if (TFS_SUCCESS != ret && print)
        {
          TBSYS_LOG(INFO, "failed when relieve between block: %"PRI64_PREFIX"u and dataserver: %s, ret: %d",
              block->id(), CNetUtil::addrToString(server->id()).c_str(), ret);
        }
        result = get_server_manager().relieve_relation(server,block->id());
        if (TFS_SUCCESS != result && print)
        {
          TBSYS_LOG(INFO, "failed when relieve between block: %"PRI64_PREFIX"u and dataserver: %s, ret: %d",
              block->id(), CNetUtil::addrToString(server->id()).c_str(), result);
        }
      }
      return (TFS_SUCCESS == ret && TFS_SUCCESS == result);
    }


    bool LayoutManager::relieve_relation(BlockCollect* block, const uint64_t server, const time_t now)
    {
      int32_t ret = ((NULL != block) && (INVALID_SERVER_ID != server)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ServerCollect* pserver = get_server_manager().get(server);
        ret = (NULL != pserver) ? TFS_SUCCESS : EIXT_SERVER_OBJECT_NOT_FOUND;
        if (TFS_SUCCESS == ret)
          ret = relieve_relation(block, pserver, now) ? TFS_SUCCESS : EXIT_RELIEVE_RELATION_ERROR;
      }
      return TFS_SUCCESS == ret;
    }

    bool LayoutManager::relieve_relation(const uint64_t block, ServerCollect* server, const time_t now)
    {
      int32_t ret = ((INVALID_BLOCK_ID != block) && (NULL != server)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BlockCollect* pblock = get_block_manager().get(block);
        ret = (NULL != pblock) ? TFS_SUCCESS : EXIT_NO_BLOCK;
        if (TFS_SUCCESS == ret)
          ret = relieve_relation(pblock, server, now) ? TFS_SUCCESS : EXIT_RELIEVE_RELATION_ERROR;
      }
      return TFS_SUCCESS == ret;
    }

    bool LayoutManager::relieve_relation(const uint64_t block, const uint64_t server, const time_t now)
    {
      int32_t ret = ((INVALID_BLOCK_ID != block) && (INVALID_BLOCK_ID != server)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BlockCollect* pblock = get_block_manager().get(block);
        ret = (NULL != pblock) ? TFS_SUCCESS : EXIT_NO_BLOCK;
        if (TFS_SUCCESS == ret)
        {
          ServerCollect* pserver = get_server_manager().get(server);
          ret = (NULL != pserver) ? TFS_SUCCESS : EIXT_SERVER_OBJECT_NOT_FOUND;
          if (TFS_SUCCESS == ret)
            ret = relieve_relation(block, pserver, now) ? TFS_SUCCESS : EXIT_RELIEVE_RELATION_ERROR;
        }
      }
      return TFS_SUCCESS == ret;
    }

    int LayoutManager::update_block_info(const BlockInfoV2& info, const uint64_t server, const time_t now,const bool addnew)
    {
      bool isnew = false;
      bool master = false;
      bool writable = false;
      BlockCollect* block = NULL;
      ServerCollect* pserver = get_server_manager().get(server);
      int32_t ret = (NULL != pserver) ? TFS_SUCCESS : EXIT_DATASERVER_NOT_FOUND;
      if (TFS_SUCCESS == ret)
      {
        block = get_block_manager().get(info.block_id_);
        ret = (NULL != block) ? TFS_SUCCESS : EXIT_BLOCK_NOT_FOUND;
      }
      if (TFS_SUCCESS == ret || addnew)
      {
        ret = get_block_manager().update_block_info(block, isnew, writable, master,
            info, pserver, now, addnew);
      }
      if ((TFS_SUCCESS == ret)
          && (isnew))
      {
        get_server_manager().build_relation(pserver, block->id(), writable, master);
      }
      if (TFS_SUCCESS == ret)
      {
        //write oplog
        uint64_t arrays[1];
        common::ArrayHelper<uint64_t> servers(1, arrays);
        servers.push_back(server);
        #ifndef TFS_GTEST
        ret = block_oplog_write_helper(isnew ? OPLOG_INSERT : OPLOG_UPDATE, info, servers, now);
        #endif
      }
      return ret;
    }

    int LayoutManager::repair(char* msg, const int32_t length, const uint64_t block_id,
          const uint64_t server, const int64_t family_id, const int32_t type, const time_t now)
    {
      int32_t ret = ((NULL != msg) && (length > 0)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        uint64_t servers[MAX_REPLICATION_NUM];
        ArrayHelper<uint64_t> helper(MAX_REPLICATION_NUM, servers);
        BlockCollect* block = get_block_manager().get(block_id);
        ret = (NULL == block) ? EXIT_BLOCK_NOT_FOUND : TFS_SUCCESS;
        if (TFS_SUCCESS != ret)
        {
          snprintf(msg, length, "repair block, block object not found by block: %"PRI64_PREFIX"u, ret: %d", block_id, ret);
        }
        else
        {
          get_block_manager().get_servers(helper, block_id);
          if (REAPIR_BLOCK_NOT_EXIST == type)
          {
            relieve_relation(block, server, now);
            get_block_manager().push_to_emergency_replicate_queue(block);
          }
          if (REPAIR_FAMILY_ID_CONFLICT == type)
          {
            TBSYS_LOG(INFO, "Family id: %"PRI64_PREFIX"d: %"PRI64_PREFIX"d conflict, block: %"PRI64_PREFIX"u, server: %s",
              family_id,block->get_family_id(),block_id, tbsys::CNetUtil::addrToString(server).c_str());
            if (get_family_manager().exist(block->get_family_id(), block_id) && helper.exist(server))
            {
              relieve_relation(block, server, now);
              if (block->get_servers_size() <= 0)
              {
                FamilyCollect* family = get_family_manager().get(block->get_family_id());
                get_family_manager().push_to_reinstate_or_dissolve_queue(family, PLAN_TYPE_EC_REINSTATE);
              }
            }
          }
        }
      }
      return ret;
    }

    int LayoutManager::scan(SSMScanParameter& param)
    {
      int32_t start = (param.start_next_position_ & 0xFFFF0000) >> 16;
      int32_t next  = start;
      int32_t should= (param.should_actual_count_ & 0xFFFF0000) >> 16;
      int32_t actual= 0;
      bool    all_over = true;
      bool    cutover = ((param.end_flag_) & SSM_SCAN_CUTOVER_FLAG_YES);

      if (param.type_ & SSM_TYPE_SERVER)
      {
        actual = get_server_manager().scan(param, should, start, next, all_over);
      }
      else if (param.type_ & SSM_TYPE_BLOCK)
      {
        actual = get_block_manager().scan(param, next, all_over, cutover, should);
      }
      else if(param.type_ & SSM_TYPE_FAMILY)
      {
        actual = get_family_manager().scan(param, next, all_over, cutover, should);
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
      param.end_flag_ |= cutover ? SSM_SCAN_CUTOVER_FLAG_YES : SSM_SCAN_CUTOVER_FLAG_NO;
      return TFS_SUCCESS;
    }

    int LayoutManager::handle_task_complete(common::BasePacket* msg)
    {
      //handle complete message
      int32_t ret = (NULL != msg) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = (msg->getPCode() == BLOCK_COMPACT_COMPLETE_MESSAGE
          || msg->getPCode() == REPLICATE_BLOCK_MESSAGE
          || msg->getPCode() == REQ_EC_MARSHALLING_COMMIT_MESSAGE
          || msg->getPCode() == REQ_EC_REINSTATE_COMMIT_MESSAGE
          || msg->getPCode() == REQ_EC_DISSOLVE_COMMIT_MESSAGE) ? TFS_SUCCESS : EXIT_UNKNOWN_MSGTYPE;
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(INFO, "handle_task_complete unkonw message PCode = %d", msg->getPCode());
        }
        else
        {
          BaseTaskMessage* base = dynamic_cast<BaseTaskMessage*>(msg);
          ret = get_task_manager().handle(msg, base->get_seqno());
        }
      }
      return ret;
    }

    int LayoutManager::open_helper_create_new_block_by_id(uint64_t block_id)
    {
      BlockCollect* block = get_block_manager().get(block_id);
      int32_t ret = (NULL != block) ? TFS_SUCCESS : EXIT_BLOCK_NOT_FOUND;
      if (TFS_SUCCESS != ret)//block not found by block_id
      {
        block = add_new_block(block_id);
        ret = (NULL == block) ? EXIT_CREATE_BLOCK_BY_ID_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(INFO, "add new block: %"PRI64_PREFIX"u failed because create block by blockid error", block_id);
        }
      }

      if (TFS_SUCCESS == ret)
      {
        block = get_block_manager().get(block_id);
        ret = NULL != block ? TFS_SUCCESS : EXIT_BLOCK_NOT_FOUND;
        if (TFS_SUCCESS == ret)
        {
          if (block->get_servers_size() <= 0)
          {
            if (block->is_creating())
            {
              TBSYS_LOG(INFO, "block: %"PRI64_PREFIX"u found meta data, but creating by another thread, must be return", block_id);
              ret = EXIT_NO_BLOCK;
            }
            else
            {
              TBSYS_LOG(INFO, "block: %"PRI64_PREFIX"u found meta data, but no dataserver hold it.", block_id);
              ret = EXIT_NO_DATASERVER;
            }
          }
        }
      }
      return ret;
    }

    int LayoutManager::block_oplog_write_helper(const int32_t cmd, const common::BlockInfoV2& info,
          const common::ArrayHelper<uint64_t>& servers, const time_t now)
    {
      BlockOpLog oplog;
      oplog.info_ = info;
      oplog.cmd_ = cmd;
      oplog.server_num_ = servers.get_array_index();
      memcpy(oplog.servers_, servers.get_base_address(), oplog.server_num_ * INT64_SIZE);
      int64_t size = oplog.length();
      int64_t pos = 0;
      char buf[size];
      int32_t ret = oplog.serialize(buf, size, pos);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(INFO, "%s", "oplog serialize error");
      }
      else
      {
        // treat log operation as a trivial thing..
        // don't rollback the insert operation, cause block meta info
        // build from all dataserver, log info not been very serious.
        ret = oplog_sync_mgr_.log(OPLOG_TYPE_BLOCK_OP, buf, size, now);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(INFO, "write oplog failed, block: %"PRI64_PREFIX"u", info.block_id_);
        }
      }
      return ret;
    }

    int LayoutManager::set_runtime_param(const uint32_t value1, const uint32_t value2, const int64_t length, char *retstr)
    {
      int32_t ret = ((NULL != retstr) && (length > 0)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        retstr[0] = '\0';
        int32_t index = (value1 & 0x0FFFFFFF);
        int32_t set = (value1& 0xF0000000);
        int32_t* param[] =
        {
          &TBSYS_LOGGER._level,
          &plan_run_flag_,
          &SYSPARAM_NAMESERVER.task_expired_time_,
          &SYSPARAM_NAMESERVER.safe_mode_time_,
          &SYSPARAM_NAMESERVER.max_write_timeout_,
          &SYSPARAM_NAMESERVER.max_write_file_count_,
          &SYSPARAM_NAMESERVER.add_primary_block_count_,
          &SYSPARAM_NAMESERVER.cleanup_write_timeout_threshold_,
          &SYSPARAM_NAMESERVER.max_use_capacity_ratio_,
          &SYSPARAM_NAMESERVER.heart_interval_,
          &SYSPARAM_NAMESERVER.replicate_ratio_,
          &SYSPARAM_NAMESERVER.replicate_wait_time_,
          &SYSPARAM_NAMESERVER.compact_delete_ratio_,
          &SYSPARAM_NAMESERVER.compact_task_ratio_,
          &SYSPARAM_NAMESERVER.compact_time_lower_,
          &SYSPARAM_NAMESERVER.compact_time_upper_,
          &SYSPARAM_NAMESERVER.max_task_in_machine_nums_,
          &SYSPARAM_NAMESERVER.discard_newblk_safe_mode_time_,
          &SYSPARAM_NAMESERVER.discard_max_count_,
          &SYSPARAM_NAMESERVER.cluster_index_,
          &SYSPARAM_NAMESERVER.object_wait_free_time_ms_,
          &SYSPARAM_NAMESERVER.group_count_,
          &SYSPARAM_NAMESERVER.group_seq_,
          &SYSPARAM_NAMESERVER.object_wait_clear_time_ms_,
          &SYSPARAM_NAMESERVER.report_block_queue_size_,
          &SYSPARAM_NAMESERVER.report_block_time_lower_,
          &SYSPARAM_NAMESERVER.report_block_time_upper_,
          &SYSPARAM_NAMESERVER.report_block_time_interval_,
          &SYSPARAM_NAMESERVER.report_block_expired_time_,
          &SYSPARAM_NAMESERVER.choose_target_server_random_max_nums_,
          &SYSPARAM_NAMESERVER.keepalive_queue_size_,
          &SYSPARAM_NAMESERVER.marshalling_delete_ratio_,
          &SYSPARAM_NAMESERVER.marshalling_time_lower_,
          &SYSPARAM_NAMESERVER.marshalling_time_upper_,
          &SYSPARAM_NAMESERVER.marshalling_type_,
          &SYSPARAM_NAMESERVER.max_data_member_num_,
          &SYSPARAM_NAMESERVER.max_check_member_num_,
          &SYSPARAM_NAMESERVER.max_marshalling_queue_timeout_,
          &SYSPARAM_NAMESERVER.move_task_expired_time_,
          &SYSPARAM_NAMESERVER.compact_task_expired_time_,
          &SYSPARAM_NAMESERVER.marshalling_task_expired_time_,
          &SYSPARAM_NAMESERVER.reinstate_task_expired_time_,
          &SYSPARAM_NAMESERVER.dissolve_task_expired_time_,
          &SYSPARAM_NAMESERVER.compact_update_ratio_,
          &SYSPARAM_NAMESERVER.max_mr_network_bandwith_ratio_,
          &SYSPARAM_NAMESERVER.max_rw_network_bandwith_ratio_,
          &SYSPARAM_NAMESERVER.compact_family_member_ratio_,
          &SYSPARAM_NAMESERVER.max_single_machine_network_bandwith_,
          &SYSPARAM_NAMESERVER.write_file_check_copies_complete_,
          &SYSPARAM_NAMESERVER.choose_target_server_retry_max_nums_,
          &SYSPARAM_NAMESERVER.max_marshalling_num_,
          &SYSPARAM_NAMESERVER.enable_old_interface_,
          &SYSPARAM_NAMESERVER.enable_version_check_,
          &SYSPARAM_NAMESERVER.marshalling_visit_time_,
          &SYSPARAM_NAMESERVER.client_keepalive_interval_
          &SYSPARAM_NAMESERVER.verify_index_reserved_space_ratio_
        };
        int32_t size = sizeof(param) / sizeof(int32_t*);
        ret = (index >= 1 && index <= size) ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS != ret)
        {
          snprintf(retstr, length, "index : %d invalid.", index);
        }
        else
        {
          int32_t* current_value = param[index - 1];
          if (set)
            *current_value = (int32_t)(value2 & 0xFFFFFFFF);
          else
            snprintf(retstr, 256, "%d", *current_value);
          TBSYS_LOG(INFO, "index: %d %s name: %s value: %d", index, set ? "set" : "get", dynamic_parameter_str[index - 1], *current_value);
        }
      }
      return ret;
    }

    void LayoutManager::switch_role(time_t now)
    {
      get_task_manager().clear();
      GFactory::get_runtime_info().switch_role(false, now);
      get_server_manager().set_all_server_next_report_time(SET_SERVER_NEXT_REPORT_BLOCK_TIME_FLAG_SWITCH, now);
      oplog_sync_mgr_.switch_role();
    }

    void LayoutManager::rotate_(time_t now)
    {
      if ((now % 86400 >= zonesec_)
          && (now % 86400 < zonesec_ + 300)
          && (last_rotate_log_time_ < now - 600))
      {
        last_rotate_log_time_ = now;
        TBSYS_LOGGER.rotateLog(NULL);
      }
    }

    uint64_t LayoutManager::get_alive_block_id_( const bool verify)
    {
      uint64_t block_id = oplog_sync_mgr_.generation(verify);
      while (true)
      {
        if (!get_block_manager().exist(block_id))
          break;
        block_id = oplog_sync_mgr_.generation(verify);
      }
      return block_id;
    }

    uint64_t LayoutManager::get_alive_block_id(const bool verify)
    {
      return get_alive_block_id_(verify);
    }

    void LayoutManager::build_()
    {
      bool over = false;
      int32_t loop = 0, sleep_time = 0;
      uint64_t block_start = 0;
      time_t  now = 0, current = 0;
      int64_t need = 0, family_start = 0, max_compact_task_count = 0, max_marshalling_num = SYSPARAM_NAMESERVER.max_marshalling_num_;
      const int32_t MAX_QUERY_FAMILY_NUMS = 4096;
      const int32_t MAX_QUERY_BLOCK_NUMS = 4096 * 4;
      const int32_t MIN_SLEEP_TIME_US= 1000000;//1s
      const int32_t MAX_SLEEP_TIME_US = 1000000;//1s
      const int32_t MAX_LOOP_NUMS = 1000000 / MIN_SLEEP_TIME_US;
      BlockCollect* blocks[MAX_QUERY_BLOCK_NUMS];
      ArrayHelper<BlockCollect*> results(MAX_QUERY_BLOCK_NUMS, blocks);

      uint64_t query[MAX_QUERY_BLOCK_NUMS];
      ArrayHelper<uint64_t> query_helper(MAX_QUERY_BLOCK_NUMS, query);

      FamilyCollect* families[MAX_QUERY_FAMILY_NUMS];
      ArrayHelper<FamilyCollect*> helpers(MAX_QUERY_FAMILY_NUMS, families);
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();

      while (!ngi.is_destroyed())
      {
        current = time(NULL);
        now = Func::get_monotonic_time();
        if (ngi.in_safe_mode_time(now))
          Func::sleep(SYSPARAM_NAMESERVER.safe_mode_time_, ngi.destroy_flag_);

        //checkpoint
        rotate_(current);

        if (ngi.is_master())
        {
          while (((need = has_space_in_task_queue_()) <= 0) && (!ngi.is_destroyed()))
            usleep(1000);

          now = Func::get_monotonic_time();

          need = SYSPARAM_NAMESERVER.max_replication_ > 1 ? need / SYSPARAM_NAMESERVER.max_replication_ : need / 2;
          max_compact_task_count = need > 0 ? (need * SYSPARAM_NAMESERVER.compact_task_ratio_) / 100 : 0;
          max_marshalling_num = SYSPARAM_NAMESERVER.max_marshalling_num_;

          query_helper.clear();
          if (need > 0)
          {
            scan_replicate_queue_(need, now);
          }

          const int64_t replicate_queue_size = get_block_manager().get_emergency_replicate_queue_size();
          if (need > 0)
          {
            scan_reinstate_or_dissolve_queue_(need , now);
          }

          results.clear();
          const int64_t reinsate_or_dissolve_queue_size = get_family_manager().get_reinstate_or_dissolve_queue_size();
          const bool report_time      = ngi.in_report_block_time(current);
          const bool compact_time     = in_hour_range(current, SYSPARAM_NAMESERVER.compact_time_lower_, SYSPARAM_NAMESERVER.compact_time_upper_);
          const bool marshalling_time = in_hour_range(current, SYSPARAM_NAMESERVER.marshalling_time_lower_, SYSPARAM_NAMESERVER.marshalling_time_upper_);
          const bool adjust_copies_location_time = in_hour_range(current, SYSPARAM_NAMESERVER.adjust_copies_location_time_lower_, SYSPARAM_NAMESERVER.adjust_copies_location_time_upper_);
          if (need > 0)
          {
            now = Func::get_monotonic_time();
            over = scan_block_(results, need, block_start, max_compact_task_count, MAX_QUERY_BLOCK_NUMS, now, compact_time, marshalling_time, adjust_copies_location_time, report_time);
            if (over)
              block_start = 0;
          }
          if (need > 0)
          {
            over = scan_family_(helpers, need, family_start, MAX_QUERY_FAMILY_NUMS, now, compact_time);
            if (over)
              family_start = 0;
          }

          if (need > 0 && !report_time && reinsate_or_dissolve_queue_size <= 0 && replicate_queue_size <= 0 && max_marshalling_num > 0)
          {
            int32_t rt = build_marshalling_(need, now);
            if (TFS_SUCCESS == rt)
              --max_marshalling_num;
          }

          if (loop >= MAX_LOOP_NUMS)
          {
            loop = 0;
            const int64_t marshalling_queue_size = get_family_manager().get_marshalling_queue_size();
            TBSYS_LOG(INFO, "need: %"PRI64_PREFIX"d, emergency_replicate_queue: %"PRI64_PREFIX"d, reinsate or dissolve queue: %"PRI64_PREFIX"d, marshalling queue: %"PRI64_PREFIX"d",
              need, replicate_queue_size, reinsate_or_dissolve_queue_size, marshalling_queue_size);
            get_task_manager().dump(TBSYS_LOG_LEVEL_DEBUG, "task manager all queues information: ");
            get_family_manager().dump_marshalling_queue(TBSYS_LOG_LEVEL_DEBUG, "marshalling queue information: ");
          }
        }
        ++loop;
        sleep_time = has_space_in_task_queue_() <= 0 ? MAX_SLEEP_TIME_US : MIN_SLEEP_TIME_US;
        usleep(sleep_time);
      }
    }

    void LayoutManager::balance_()
    {
      const int64_t MAX_SLEEP_NUMS = 1000 * 10;
      int64_t total_capacity  = 0;
      int64_t total_use_capacity = 0;
      int64_t alive_server_nums = 0;
      int64_t need = 0, sleep_nums = 0, now = 0;
      int32_t ret = TFS_SUCCESS;
      uint64_t block = INVALID_BLOCK_ID;
      const int32_t MAX_RETRY_COUNT = 3;
      std::multimap<int64_t, ServerCollect*> source;
      TfsSortedVector<ServerCollect*, ServerIdCompare> targets(MAX_PROCESS_NUMS, 1024, 0.1);
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      while (!ngi.is_destroyed())
      {
        now = Func::get_monotonic_time();
        if (ngi.in_safe_mode_time(now))
          Func::sleep(SYSPARAM_NAMESERVER.safe_mode_time_ * 4, ngi.destroy_flag_);

        if (ngi.is_master())
        {
          while ((get_block_manager().has_emergency_replicate_in_queue()) && (!ngi.is_destroyed()) && sleep_nums++ <= MAX_SLEEP_NUMS)
            usleep(1000);

          while ((!get_family_manager().reinstate_or_dissolve_queue_empty()) && (!ngi.is_destroyed()) && sleep_nums++ <= MAX_SLEEP_NUMS)
            usleep(1000);

          while (((need = has_space_in_task_queue_()) <= 0) && (!ngi.is_destroyed()))
            usleep(1000);

          while ((!(plan_run_flag_ & PLAN_TYPE_MOVE)) && (!ngi.is_destroyed()))
            usleep(100000);

          need = SYSPARAM_NAMESERVER.max_replication_ > 1 ? need / SYSPARAM_NAMESERVER.max_replication_ : need / 2;

          total_capacity = 0, total_use_capacity = 0, alive_server_nums = 0, sleep_nums = 0;
          get_server_manager().move_statistic_all_server_info(total_capacity,
              total_use_capacity, alive_server_nums);
          if (total_capacity > 0 && total_use_capacity > 0 && alive_server_nums > 0
             && !get_block_manager().has_emergency_replicate_in_queue()
             && get_family_manager().reinstate_or_dissolve_queue_empty())
          {
            source.clear();
            targets.clear();
            double percent = calc_capacity_percentage(total_use_capacity, total_capacity);

            // find move src and dest ds list
            get_server_manager().move_split_servers(source, targets, percent);

            const int64_t replicate_queue_size = get_block_manager().get_emergency_replicate_queue_size();
            const int64_t reinsate_or_dissolve_queue_size = get_family_manager().get_reinstate_or_dissolve_queue_size();
            const int64_t marshalling_queue_size = get_family_manager().get_marshalling_queue_size();
            TBSYS_LOG(INFO, "need: %"PRI64_PREFIX"d, source: %zd, target: %d, percent: %e, emergency_replicate_queue: %"PRI64_PREFIX"d, reinsate or dissolve queue: %"PRI64_PREFIX"d, marshalling queue: %"PRI64_PREFIX"d",
              need, source.size(), targets.size(), percent, replicate_queue_size, reinsate_or_dissolve_queue_size, marshalling_queue_size);

            bool complete = false;
            now = Func::get_monotonic_time();

            // we'd better start from the most needed ds
            std::multimap<int64_t, ServerCollect*>::const_reverse_iterator it = source.rbegin();
            for (; it != source.rend() && need > 0 && !targets.empty(); ++it, complete = false)
            {
              if (get_task_manager().has_space_do_task_in_machine(it->second->id(), false)
                  && !get_task_manager().exist_server(it->second->id()))
              {
                for (int32_t index = 0; index < MAX_RETRY_COUNT && !complete; ++index)
                {
                  block = INVALID_BLOCK_ID;
                  ret = it->second->choose_move_block_random(block);
                  if (TFS_SUCCESS == ret)
                  {
                    BlockCollect* pblock = get_block_manager().get(block);
                    ret = (NULL != pblock) ? TFS_SUCCESS : EXIT_NO_BLOCK;
                    if (TFS_SUCCESS == ret
                        && build_balance_task_(need, targets, it->second, pblock, now))
                    {
                      --need;
                      complete = true;
                    }
                  }
                }//end for ...
              }
            }//end for ...
          }
        }
        Func::sleep(SYSPARAM_NAMESERVER.heart_interval_, ngi.destroy_flag_);
      }
    }

    void LayoutManager::timeout_()
    {
      time_t now = 0;
      int32_t block_expand_index = 0;
      int32_t server_expand_index = 0;
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      while (!ngi.is_destroyed())
      {
        now = Func::get_monotonic_time();

        get_task_manager().timeout(now);

        get_block_manager().timeout(now);

        get_family_manager().marshalling_queue_timeout(now);

        get_block_manager().expand_ratio(block_expand_index);

        get_server_manager().expand_ratio(server_expand_index);

        get_gc_manager().gc(now);

        usleep(50000);
      }
    }

    void LayoutManager::redundant_()
    {
      const int32_t MAX_REDUNDNAT_NUMS = 256;
      const int32_t MAX_SLEEP_TIME_US  = 500000;
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      while (!ngi.is_destroyed())
      {
        while ((!ngi.in_report_block_time(Func::get_monotonic_time()) && !ngi.is_destroyed()))
          Func::sleep(SYSPARAM_NAMESERVER.heart_interval_, ngi.destroy_flag_);

        while ((get_block_manager().delete_queue_empty() && !ngi.is_destroyed()))
          Func::sleep(SYSPARAM_NAMESERVER.heart_interval_, ngi.destroy_flag_);

        int64_t need = MAX_REDUNDNAT_NUMS;
        build_redundant_(need, 0);
        usleep(MAX_SLEEP_TIME_US);
      }
    }

    void LayoutManager::load_family_info_()
    {
      time_t now = 0;
      int64_t family_id = 0;
      int32_t ret = TFS_SUCCESS;
      const int32_t MAX_SLEEP_TIME = 10;//10s
      std::vector<common::FamilyInfo> infos;
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      const int32_t SAFE_MODE_TIME = SYSPARAM_NAMESERVER.safe_mode_time_ * 4;
      while (!ngi.is_destroyed())
      {
        now = Func::get_monotonic_time();
        if (ngi.in_safe_mode_time(now))
          Func::sleep(SAFE_MODE_TIME, ngi.destroy_flag_);
        if (!ngi.is_master())
        {
          do
          {
            infos.clear();
            ret = get_oplog_sync_mgr().scan_family(infos, family_id);
            if (TFS_SUCCESS == ret)
            {
              std::pair<uint64_t, int32_t> members[MAX_MARSHALLING_NUM];
              common::ArrayHelper<std::pair<uint64_t, int32_t> > helper(MAX_MARSHALLING_NUM, members);
              std::vector<common::FamilyInfo>::const_iterator iter = infos.begin();
              for (; iter != infos.end(); ++iter)
              {
                helper.clear();
                family_id = (*iter).family_id_;
                std::vector<std::pair<uint64_t, int32_t> >::const_iterator it = (*iter).family_member_.begin();
                for (; it != (*iter).family_member_.end(); ++it)
                {
                  helper.push_back(std::make_pair((*it).first, (*it).second));
                  BlockCollect* block =  get_block_manager().insert((*it).first, now);
                  assert(block);
                  block->set_family_id(family_id);
                }
                ret = get_family_manager().insert(family_id, (*iter).family_aid_info_, helper, now);
                if (EXIT_ELEMENT_EXIST == ret)
                  ret = TFS_SUCCESS;
                if (TFS_SUCCESS != ret)
                  TBSYS_LOG(WARN, "load family information error,family id: %"PRI64_PREFIX"d, ret: %d", family_id, ret);
              }
            }
          }
          while (infos.size() > 0 && TFS_SUCCESS == ret);
        }
        Func::sleep(MAX_SLEEP_TIME, ngi.destroy_flag_);
      }
    }

    void LayoutManager::check_all_server_lease_timeout_()
    {
      bool all_over = false;
      uint64_t last_traverse_server = INVALID_SERVER_ID;
      const int32_t SLEEP_TIME_US = 2000000;  // 2 seconds
      NsGlobalStatisticsInfo stat_info;
      memset(&stat_info, 0, sizeof(stat_info));
      ServerCollect* servers[MAX_PROCESS_NUMS];
      ArrayHelper<ServerCollect*> helper(MAX_PROCESS_NUMS, servers);
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      while (!ngi.is_destroyed())
      {
        int64_t now = Func::get_monotonic_time();
        /*
        if (ngi.in_safe_mode_time(now))
        {
          Func::sleep(SYSPARAM_NAMESERVER.safe_mode_time_, ngi.destroy_flag_);
        }
        now = Func::get_monotonic_time();
        */

        //check dataserver is alive
        helper.clear();  // avoid report block dead loop
        get_server_manager().timeout(now, stat_info, helper,last_traverse_server, all_over);

        get_server_manager().gc(now);

        for (int64_t index = 0; index < helper.get_array_index(); ++index)
        {
          ServerCollect* pserver = *helper.at(index);
          assert(NULL != pserver);
          now = Func::get_monotonic_time();
          CallDsReportBlockRequestMessage req;
          req.set_server(ngi.heart_ip_port_);
          post_msg_to_server(pserver->id(), &req, ns_async_callback);
          now = Func::get_monotonic_time();
          pserver->set_report_block_expire_time(now);
        }
        if (all_over)
        {
          memset(&stat_info, 0, sizeof(stat_info));
          last_traverse_server = INVALID_SERVER_ID;
          usleep(SLEEP_TIME_US);
        }
        else
        {
          usleep(500);
        }
      }
    }

    void LayoutManager::regular_create_block_for_servers()
    {
      uint64_t begin = 0;
      const int32_t SLEEP_TIME_S  = 2;
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      while (!ngi.is_destroyed())
      {
        if (ngi.in_safe_mode_time(Func::get_monotonic_time()))
          Func::sleep(SYSPARAM_NAMESERVER.safe_mode_time_, ngi.destroy_flag_);

        bool complete = get_server_manager().size() < SYSPARAM_NAMESERVER.max_replication_;
        if (!complete)
        {
          get_server_manager().regular_create_block_for_servers(begin, complete);
        }
        if (complete)
        {
          begin = 0;
          Func::sleep(SLEEP_TIME_S, ngi.destroy_flag_);
        }
      }
    }

    int LayoutManager::add_new_block_helper_write_log_(const uint64_t block_id, const ArrayHelper<uint64_t>& servers, const time_t now)
    {
      int32_t ret = ((0 != block_id) && (!servers.empty())) ?  TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BlockInfoV2 info;
        info.block_id_ = block_id;
        ret = block_oplog_write_helper(OPLOG_INSERT, info, servers, now);
      }
      return ret;
    }

    int LayoutManager::add_new_block_helper_send_msg_(const uint64_t block_id, const ArrayHelper<uint64_t>& servers)
    {
      int32_t ret = ((0 != block_id) && (!servers.empty())) ?  TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        NewClient* client = NewClientManager::get_instance().create_client();
        ret = NULL != client ? TFS_SUCCESS : EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
        if (TFS_SUCCESS == ret)
        {
          NewBlockMessageV2 msg;
          msg.set_block_id(block_id);
          uint8_t send_id = 0;
          std::string all_servers, success_servers;
          uint64_t send_msg_success[MAX_REPLICATION_NUM];
          ArrayHelper<uint64_t> send_msg_success_helper(MAX_REPLICATION_NUM, send_msg_success);
          uint64_t send_msg_fail[MAX_REPLICATION_NUM];
          ArrayHelper<uint64_t> send_msg_fail_helper(MAX_REPLICATION_NUM, send_msg_fail);

          for (int64_t index = 0; index < servers.get_array_index() && TFS_SUCCESS == ret; ++index)
          {
            uint64_t id= *servers.at(index);
            //send add new block message to dataserver
            ret = client->post_request(id, &msg, send_id);
            if ( TFS_SUCCESS != ret)
            {
              send_msg_fail_helper.push_back(id);
              TBSYS_LOG(INFO, "send 'New block: %"PRI64_PREFIX"u' msg to server : %s fail",
                  block_id, CNetUtil::addrToString(id).c_str());
            }
            else
            {
              send_msg_success_helper.push_back(id);
              TBSYS_LOG(INFO, "send 'New block: %"PRI64_PREFIX"u' msg to server : %s successful",
                  block_id, CNetUtil::addrToString(id).c_str());
            }
          }

          ret = send_msg_success_helper.empty() ? EXIT_CREATE_BLOCK_SEND_MSG_ERROR: TFS_SUCCESS;
          // post message fail, rollback
          if (TFS_SUCCESS != ret)
          {
            print_int64(send_msg_success_helper, all_servers);
            print_int64(send_msg_fail_helper, success_servers);
            TBSYS_LOG(INFO, "add block: %"PRI64_PREFIX"u failed, we'll rollback, send msg successful: %s, failed: %s, alls: %s",
                block_id, all_servers.c_str(), success_servers.c_str(), all_servers.c_str());
          }
          else //有发消息成的
          {
            uint64_t success[MAX_REPLICATION_NUM];
            ArrayHelper<uint64_t> success_helper(MAX_REPLICATION_NUM, success);
            client->wait();
            NewClient::RESPONSE_MSG_MAP* sresponse = client->get_success_response();
            NewClient::RESPONSE_MSG_MAP* fresponse = client->get_fail_response();
            ret = send_msg_success_helper.get_array_index() == servers.get_array_index() ? TFS_SUCCESS : EXIT_SEND_RECV_MSG_COUNT_ERROR;
            if (TFS_SUCCESS == ret)
            {
              assert(NULL != sresponse);
              assert(NULL != fresponse);
              tbnet::Packet* packet = NULL;
              StatusMessage* message = NULL;
              for (NewClient::RESPONSE_MSG_MAP_ITER iter = sresponse->begin(); iter != sresponse->end(); ++iter)
              {
                packet = iter->second.second;
                assert(NULL != packet);
                if (packet->getPCode() == STATUS_MESSAGE)
                {
                  message =  dynamic_cast<StatusMessage*>(packet);
                  if (STATUS_MESSAGE_OK == message->get_status())
                  {
                    if (send_msg_success_helper.exist(iter->second.first))
                      success_helper.push_back(iter->second.first);
                  }
                }
              }
              print_int64(success_helper, success_servers);
              ret = success_helper.get_array_index() != servers.get_array_index() ? EXIT_ADD_NEW_BLOCK_ERROR : TFS_SUCCESS;
              if (TFS_SUCCESS != ret)//add block failed, rollback
              {
                print_int64(send_msg_success_helper, all_servers);
                TBSYS_LOG(INFO, "add block: %"PRI64_PREFIX"u fail, we'll rollback, servers: %s, success: %s",
                    block_id, all_servers.c_str(), success_servers.c_str());
              }
              else
              {
                TBSYS_LOG(INFO, "add block: %"PRI64_PREFIX"u on servers: %s successful", block_id, success_servers.c_str());
              }
            }
          }
          NewClientManager::get_instance().destroy_client(client);
        }
      }
      return ret;
    }

    int LayoutManager::add_new_block_helper_build_relation_(BlockCollect* block, const common::ArrayHelper<uint64_t>& servers, const time_t now)
    {
      //build relation
      int32_t ret =  (NULL != block) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        for (int64_t index = 0; index < servers.get_array_index() && TFS_SUCCESS == ret; ++index)
        {
          uint64_t server = *servers.at(index);
          ServerCollect* pserver = get_server_manager().get(server);
          ret = (NULL != pserver) ? TFS_SUCCESS : EIXT_SERVER_OBJECT_NOT_FOUND;
          if (TFS_SUCCESS == ret)
            ret = build_relation(block, pserver, now, true);
        }
      }
      return ret;
    }

    BlockCollect* LayoutManager::add_new_block(uint64_t& block_id, ServerCollect* server, const time_t now)
    {
      return block_id != 0 ? add_new_block_helper_create_by_id_(block_id, now)
        : add_new_block_helper_create_by_system_(block_id, server, now);
    }

    BlockCollect* LayoutManager::add_new_block_helper_create_by_id_(const uint64_t block_id, const time_t now)
    {
      BlockCollect* block = NULL;
      int32_t ret =  (0 != block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        uint64_t exist[MAX_REPLICATION_NUM];
        ArrayHelper<uint64_t> helper(MAX_REPLICATION_NUM, exist);
        uint64_t servers[MAX_REPLICATION_NUM];
        ArrayHelper<uint64_t> news(MAX_REPLICATION_NUM, servers);
        bool new_create_block_collect = false;
        block = get_block_manager().get(block_id);
        if (NULL == block)//block not found in nameserver meta
        {
          block = get_block_manager().insert(block_id, now, true);//create block && insert block map
          new_create_block_collect = NULL != block;
        }
        ret = (NULL != block) ? TFS_SUCCESS : EXIT_NO_BLOCK;
        if (TFS_SUCCESS == ret)//find or create block successful
        {
          get_block_manager().get_servers(helper, block);
          int32_t count = SYSPARAM_NAMESERVER.max_replication_ - helper.get_array_index();
          if (count > 0)
          {
            BlockCollect* pobject = NULL;
            get_server_manager().choose_create_block_target_server(helper, news, count);
            ret = helper.empty() ? EXIT_CHOOSE_CREATE_BLOCK_TARGET_SERVER_ERROR : TFS_SUCCESS;
            if (TFS_SUCCESS != ret)
            {
              TBSYS_LOG(INFO, "create block: %"PRI64_PREFIX"u by block id fail, dataserver is not enough", block_id);
              if (new_create_block_collect)
                get_block_manager().remove(pobject,block_id);
            }
            else//elect dataserver successful
            {
              if (!news.empty())
              {
                ret = add_new_block_helper_send_msg_(block_id, news);
                if (TFS_SUCCESS == ret)
                {
                  //build relation
                  ret = add_new_block_helper_build_relation_(block, news, now);
                  if (TFS_SUCCESS == ret)
                  {
                    add_new_block_helper_write_log_(block_id, news, now);
                    oplog_sync_mgr_.generation(block_id);
                  }
                }//end send message to dataserver successful
                else
                {
                  block = get_block_manager().get(block_id);
                  if ((new_create_block_collect)
                      || ((NULL != block)
                        && (!new_create_block_collect)
                        && (!block->is_creating())
                        && (block->get_servers_size() <= 0)))
                  {
                    get_block_manager().remove(pobject,block_id);
                  }
                }
              }
              get_gc_manager().insert(pobject, now);
            }//end elect dataserver successful
          }//end if (count >0)
        }//end find or create block successful
      }//end if (bret)
      return ret == TFS_SUCCESS ? block : NULL;
    }

    BlockCollect* LayoutManager::add_new_block_helper_create_by_system_(uint64_t& block_id, ServerCollect* server, const time_t now)
    {
      BlockCollect* block = NULL;
      int32_t ret =  (0 == block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        uint64_t result[MAX_REPLICATION_NUM];
        ArrayHelper<uint64_t> helper(MAX_REPLICATION_NUM, result);
        uint64_t news[MAX_REPLICATION_NUM];
        ArrayHelper<uint64_t> news_helper(MAX_REPLICATION_NUM, news);
        if (NULL != server)
          helper.push_back(server->id());

        block_id = get_alive_block_id_(false);
        ret = (INVALID_BLOCK_ID == block_id) ? EXIT_BLOCK_ID_INVALID_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          //add block collect object
          block = get_block_manager().insert(block_id, now, true);
          ret = (NULL != block) ? TFS_SUCCESS : EXIT_NO_BLOCK;
        }

        if (TFS_SUCCESS == ret)
        {
          int32_t count = SYSPARAM_NAMESERVER.max_replication_ - helper.get_array_index();
          if (count > 0)
          {
            get_server_manager().choose_create_block_target_server(helper, news_helper, count);
          }
          BlockCollect* pobject = NULL;
          ret = !helper.empty() ? TFS_SUCCESS : EXIT_CHOOSE_CREATE_BLOCK_TARGET_SERVER_ERROR;
          if (TFS_SUCCESS == ret)//add block collect object successful
          {
            ret = add_new_block_helper_send_msg_(block_id, helper);
            if (TFS_SUCCESS == ret)
            {
              //build relation
              ret = add_new_block_helper_build_relation_(block, helper, now);
              if (TFS_SUCCESS == ret)
              {
                add_new_block_helper_write_log_(block_id, helper, now);
              }
            }//end send message to dataserver successful
            else
            {
              get_block_manager().remove(pobject, block_id);//rollback
            }
          }
          else
          {
            get_block_manager().remove(pobject, block_id);//rollback
          }
          get_gc_manager().insert(pobject, now);
        }
      }//end if (TFS_SUCCESS == ret) check parameter
      return TFS_SUCCESS == ret ? block : NULL;
    }

    bool LayoutManager::scan_replicate_queue_(int64_t& need, const time_t now)
    {
      BlockCollect* block = NULL;
      int64_t count = get_block_manager().get_emergency_replicate_queue_size();
      while (need > 0 && count > 0 && (NULL != (block = get_block_manager().pop_from_emergency_replicate_queue())))
      {
        --count;
        if (!build_replicate_task_(need, block, now))
        {
          if (get_block_manager().need_replicate(block))
            get_block_manager().push_to_emergency_replicate_queue(block);
        }
        else
        {
          --need;
        }
      }
      return true;
    }

    bool LayoutManager::scan_reinstate_or_dissolve_queue_(int64_t& need, const time_t now)
    {
      bool ret = need > 0;
      if (ret)
      {
        FamilyCollect* family = NULL;
        bool reinstate = false, dissolve = false;
        const int64_t MAX_QUERY_FAMILY_NUMS = 8;
        FamilyMemberInfo members[MAX_MARSHALLING_NUM];
        ArrayHelper<FamilyMemberInfo> helper(MAX_MARSHALLING_NUM, members);
        int64_t count = get_family_manager().get_reinstate_or_dissolve_queue_size();
        count = std::min(count, MAX_QUERY_FAMILY_NUMS);
        while (need > 0 && count > 0 && (NULL != (family = get_family_manager().pop_from_reinstate_or_dissolve_queue())))
        {
          --count;
          helper.clear();
          reinstate = get_family_manager().check_need_reinstate(helper, family, now);
          dissolve  = get_family_manager().check_need_dissolve(family, helper, now);
          if (!reinstate && !dissolve)
            dissolve = get_family_manager().check_need_compact(family, now);
          if (reinstate)
            ret = build_reinstate_task_(need, family, helper, now);
          if (dissolve)
            ret = build_dissolve_task_(need, family, helper, now);
          if (ret)
          {
            --need;
          }
          else
          {
            if (reinstate)
              get_family_manager().push_to_reinstate_or_dissolve_queue(family, PLAN_TYPE_EC_REINSTATE);
            if (dissolve)
              get_family_manager().push_to_reinstate_or_dissolve_queue(family, PLAN_TYPE_EC_DISSOLVE);
          }
        }
      }
      return ret;
    }

    bool LayoutManager::build_replicate_task_(int64_t& need, const BlockCollect* block, const time_t now)
    {
      bool ret = ((NULL != block) && (plan_run_flag_ & PLAN_RUN_FLAG_REPLICATE) && (need > 0));
      if (ret)
      {
        PlanPriority priority = PLAN_PRIORITY_NONE;
        uint64_t tmp_servers[MAX_REPLICATION_NUM];
        ArrayHelper<uint64_t> helper(MAX_REPLICATION_NUM, tmp_servers);
        uint64_t servers[MAX_REPLICATION_NUM];
        ArrayHelper<uint64_t> result_array(MAX_REPLICATION_NUM, servers);
        if ((ret = get_block_manager().need_replicate(helper, priority, block, now)))
        {
          std::string result;
          ServerCollect* target = NULL;
          ServerCollect* source = NULL;
          get_server_manager().choose_replicate_source_server(source, helper);
          if (NULL == source)
          {
            print_int64(helper, result);
            TBSYS_LOG(DEBUG, "replicate block: %"PRI64_PREFIX"u cannot found source dataserver, %s", block->id(), result.c_str());
          }
          else
          {
            get_server_manager().choose_replicate_target_server(target, helper);
            if (NULL == target)
            {
              print_int64(helper, result);
              TBSYS_LOG(DEBUG, "replicate block: %"PRI64_PREFIX"u cannot found target dataserver, %s", block->id(), result.c_str());
            }
          }

          ret = ((NULL != source) && (NULL != target));
          if (ret)
          {
            result_array.push_back(source->id());
            result_array.push_back(target->id());
            for (int64_t index = 0; index < helper.get_array_index(); ++index)
            {
              uint64_t server = *helper.at(index);
              if (server != source->id())
                result_array.push_back(server);
            }
            ret = TFS_SUCCESS == get_task_manager().add(block->id(), result_array, PLAN_TYPE_REPLICATE, now);
          }
        }
      }
      return ret;
    }

    bool LayoutManager::build_compact_task_(const BlockCollect* block, const time_t now)
    {
      int ret = ((NULL != block) && (plan_run_flag_ & PLAN_RUN_FLAG_COMPACT)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        uint64_t servers[MAX_REPLICATION_NUM];
        ArrayHelper<uint64_t> helper(MAX_REPLICATION_NUM, servers);
        if (get_block_manager().need_compact(helper, block, now))
        {
          ret = get_task_manager().add(block->id(), helper, PLAN_TYPE_COMPACT, now);
        }
      }
      return (TFS_SUCCESS == ret);
    }

    bool LayoutManager::build_resolve_block_conflict_(const BlockCollect* block, const time_t now)
    {
      int ret = ((NULL != block) && (plan_run_flag_ & PLAN_RUN_FLAG_RESLOVE_VERSION_CONFLICT)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        uint64_t servers[MAX_REPLICATION_NUM];
        ArrayHelper<uint64_t> helper(MAX_REPLICATION_NUM, servers);
        if (get_block_manager().check_version_conflict(block, now, helper))
        {
          ret = get_task_manager().add(block->id(), helper, PLAN_TYPE_RESOLVE_VERSION_CONFLICT, now);
        }
      }
      return ret;
    }

    bool LayoutManager::build_balance_task_(int64_t& need, common::TfsSortedVector<ServerCollect*,ServerIdCompare>& targets,
        const ServerCollect* source, const BlockCollect* block, const time_t now)
    {
      bool ret = ((NULL != block) && (NULL != source) && (plan_run_flag_ & PLAN_RUN_FLAG_MOVE) && (need > 0));
      if (ret)
      {
        ServerCollect* result= NULL;
        uint64_t servers[MAX_REPLICATION_NUM];
        ArrayHelper<uint64_t> helper(MAX_REPLICATION_NUM, servers);
        ret = get_block_manager().need_balance(helper, block, now);
        if (ret)
        {
          ret = helper.exist(source->id());
          if (!ret)
          {
            TBSYS_LOG(DEBUG, "cannot choose move source server, block: %"PRI64_PREFIX"u, source: %s",
                block->id(), CNetUtil::addrToString(source->id()).c_str());
          }
          else
          {
            get_server_manager().choose_move_target_server(result, targets, helper);
            ret = NULL != result;
            if (!ret)
            {
              TBSYS_LOG(DEBUG, "cannot choose move target server, block: %"PRI64_PREFIX"u, source: %s",
                  block->id(), CNetUtil::addrToString(source->id()).c_str());
            }
            else
            {
              helper.clear();
              helper.push_back(source->id());
              helper.push_back(result->id());
              ret = TFS_SUCCESS == get_task_manager().add(block->id(), helper, PLAN_TYPE_MOVE, now);
            }
          }
        }
      }
      return ret;
    }

    bool LayoutManager::build_reinstate_task_(int64_t& need, const FamilyCollect* family,
          const common::ArrayHelper<FamilyMemberInfo>& reinstate_members, const time_t now)
    {
      int32_t ret = ((NULL != family) && reinstate_members.get_array_index() > 0
              && (plan_run_flag_ & PLAN_RUN_FALG_REINSTATE) && need > 0) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        const int64_t REINSTATE_NUM = reinstate_members.get_array_index();
        uint64_t servers[REINSTATE_NUM];
        common::ArrayHelper<uint64_t> results(REINSTATE_NUM, servers);
        ret = get_family_manager().reinstate_family_choose_members(results, family->get_family_id(), family->get_family_aid_info(), REINSTATE_NUM);
        if (TFS_SUCCESS == ret)
        {
          int32_t family_aid_info_index = 0;
          ret = REINSTATE_NUM == results.get_array_index() ? TFS_SUCCESS: EXIT_CHOOSE_TARGET_SERVER_INSUFFICIENT_ERROR;
          if (TFS_SUCCESS == ret)
          {
            const int32_t MEMBER_NUM = family->get_data_member_num() + family->get_check_member_num();
            FamilyMemberInfo members[MEMBER_NUM];
            ArrayHelper<FamilyMemberInfo> helper(MEMBER_NUM, members);
            ret = get_family_manager().get_members(helper, reinstate_members, family->get_family_id());
            if (TFS_SUCCESS == ret)
            {
              for (int64_t index = 0; index < helper.get_array_index() && TFS_SUCCESS == ret; ++index)
              {
                FamilyMemberInfo* info = helper.at(index);
                assert(info);
                ret = get_task_manager().exist_block(info->block_) ? EXIT_TASK_EXIST_ERROR : TFS_SUCCESS;
                if (TFS_SUCCESS == ret)
                {
                  bool target = false;
                  if (INVALID_SERVER_ID == info->server_)
                  {
                    if (0 == family_aid_info_index)
                      family_aid_info_index = index;
                    ret = !results.empty() ? TFS_SUCCESS : EXIT_CHOOSE_TARGET_SERVER_INSUFFICIENT_ERROR;
                    if (TFS_SUCCESS == ret)
                      info->server_ = *results.pop();
                    ret = INVALID_SERVER_ID == info->server_? EXIT_CHOOSE_TARGET_SERVER_INSUFFICIENT_ERROR : TFS_SUCCESS;
                    if (TFS_SUCCESS == ret)
                    {
                      target = get_task_manager().is_target(*info, index, PLAN_TYPE_EC_REINSTATE, family->get_family_aid_info());
                      ret = ((!get_task_manager().exist_server(info->server_))
                            && (get_task_manager().has_space_do_task_in_machine(info->server_, target))) ? TFS_SUCCESS : EXIT_TASK_EXIST_ERROR;
                    }
                  }
                  else
                  {
                    target = get_task_manager().is_target(*info, index, PLAN_TYPE_EC_REINSTATE, family->get_family_aid_info());
                    ret = ((!get_task_manager().exist_server(info->server_))
                          &&(get_task_manager().has_space_do_task_in_machine(info->server_, target))) ? TFS_SUCCESS : EXIT_TASK_EXIST_ERROR;
                  }
                  if (TFS_SUCCESS != ret)
                    get_task_manager().dump(TBSYS_LOG_LEVEL_DEBUG, "REISTATE DUMP TASK INFOMRATION,");
                }
              }
            }
            if (TFS_SUCCESS == ret)
            {
              ret = members[family_aid_info_index].server_ == INVALID_SERVER_ID ? EXIT_DATASERVER_NOT_FOUND: TFS_SUCCESS;
            }
            if (TFS_SUCCESS == ret)
            {
              int32_t family_aid_info = family->get_family_aid_info();
              SET_MASTER_INDEX(family_aid_info, family_aid_info_index);
              ret = get_task_manager().add(family->get_family_id(), family_aid_info, PLAN_TYPE_EC_REINSTATE,
                helper.get_array_index(), members, now);
            }
          }
        }
      }
      return TFS_SUCCESS == ret;
    }

    bool LayoutManager::build_dissolve_task_(int64_t& need, const FamilyCollect* family,
          const common::ArrayHelper<FamilyMemberInfo>& reinstate_members, const time_t now)
    {
      int32_t ret = ((NULL != family)
                  && (plan_run_flag_ & PLAN_RUN_FALG_DISSOLVE) && need > 0) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        const int32_t DATA_MEMBER_NUM = GET_DATA_MEMBER_NUM(family->get_family_aid_info());
        const int32_t CHECK_MEMBER_NUM = GET_CHECK_MEMBER_NUM(family->get_family_aid_info());
        const int32_t MEMBER_NUM = DATA_MEMBER_NUM + CHECK_MEMBER_NUM;
        const int32_t MAX_FAMILY_MEMBER_INFO = MEMBER_NUM * 2;
        FamilyMemberInfo members[MAX_FAMILY_MEMBER_INFO];
        ArrayHelper<FamilyMemberInfo> helper(MAX_FAMILY_MEMBER_INFO, members);
        ret = get_family_manager().get_members(helper, reinstate_members, family->get_family_id());
        if (TFS_SUCCESS == ret)
        {
          ret = MEMBER_NUM == helper.get_array_index() ? TFS_SUCCESS : EXIT_FAMILY_MEMBER_INFO_ERROR;
          if (TFS_SUCCESS == ret)
          {
            std::pair<uint64_t, uint64_t> targets[MEMBER_NUM];
            ArrayHelper<std::pair<uint64_t, uint64_t> > results(MEMBER_NUM, targets);
            ret = get_family_manager().dissolve_family_choose_member_targets_server(results, family->get_family_id(), family->get_family_aid_info());
            if (TFS_SUCCESS == ret)
            {
              ret = MEMBER_NUM == results.get_array_index() ? TFS_SUCCESS : EXIT_CHOOSE_TARGET_SERVER_INSUFFICIENT_ERROR;
              if (TFS_SUCCESS == ret)
              {
                int32_t index = 0;
                int32_t master_index = 0;
                int32_t next_index = 0;
                for (; index < DATA_MEMBER_NUM && TFS_SUCCESS == ret; ++index)
                {
                  std::pair<uint64_t, uint64_t>* item = results.at(index);
                  next_index = index + MEMBER_NUM;
                  members[next_index].server_ = item->first;
                  members[next_index].block_  = members[index].block_;
                  members[next_index].status_ = members[index].status_;
                  members[next_index].version_= members[index].version_;
                  if (INVALID_SERVER_ID != members[next_index].server_
                      && INVALID_SERVER_ID != members[index].server_
                      && INVALID_BLOCK_ID != members[index].block_)
                  {
                    bool target = get_task_manager().is_target(members[index], index, PLAN_TYPE_EC_DISSOLVE, family->get_family_aid_info());
                    bool next_target = get_task_manager().is_target(members[next_index], next_index, PLAN_TYPE_EC_DISSOLVE, family->get_family_aid_info());
                    ret = ((!get_task_manager().exist_block(members[index].block_))
                          &&(!get_task_manager().exist_server(members[index].server_))
                          &&(!get_task_manager().exist_server(members[next_index].server_))
                          &&(get_task_manager().has_space_do_task_in_machine(members[index].server_, target))
                          &&(get_task_manager().has_space_do_task_in_machine(members[next_index].server_, next_target))) ? TFS_SUCCESS : EXIT_TASK_EXIST_ERROR;
                    if (0 == master_index && INVALID_SERVER_ID != members[index].server_ && FAMILY_MEMBER_STATUS_NORMAL == members[index].status_)
                      master_index = index;
                    if (TFS_SUCCESS != ret)
                      get_task_manager().dump(TBSYS_LOG_LEVEL_DEBUG, "DISOLVE DUMP TASK INFORMATION,");
                  }
                }
                if (TFS_SUCCESS == ret)
                {
                  for (; index < MEMBER_NUM; ++index)
                  {
                    next_index = index + MEMBER_NUM;
                    //members[index].status_ = FAMILY_MEMBER_STATUS_ABNORMAL;
                    //members[index].version_= INVALID_VERSION;
                    members[next_index].server_ = INVALID_SERVER_ID;
                    members[next_index].block_  = members[index].block_;
                    members[next_index].status_ = members[index].status_;
                    members[next_index].version_= members[index].version_;
                    if (0 == master_index && INVALID_SERVER_ID != members[index].server_)
                      master_index = index;
                  }
                }
                if (TFS_SUCCESS == ret)
                {
                  ret = members[master_index].server_ == INVALID_SERVER_ID ? EXIT_DATASERVER_NOT_FOUND: TFS_SUCCESS;
                  if (TFS_SUCCESS != ret)
                    TBSYS_LOG(WARN, "all members in family : %"PRI64_PREFIX"d,are lost", family->get_family_id());
                }
                if (TFS_SUCCESS == ret)
                {
                  int32_t family_aid_info = family->get_family_aid_info();
                  SET_DATA_MEMBER_NUM(family_aid_info, DATA_MEMBER_NUM * 2);
                  SET_CHECK_MEMBER_NUM(family_aid_info, CHECK_MEMBER_NUM * 2);
                  SET_MASTER_INDEX(family_aid_info, master_index);
                  ret = get_task_manager().add(family->get_family_id(), family_aid_info, PLAN_TYPE_EC_DISSOLVE,
                    MAX_FAMILY_MEMBER_INFO, members, now);
                }
              }
            }
          }
        }
      }
      return TFS_SUCCESS == ret;
    }

    bool LayoutManager::build_redundant_(int64_t& need, const time_t now)
    {
      UNUSED(now);
      bool ret = false;
      std::pair<uint64_t, uint64_t> output;
      while (need-- > 0 && get_block_manager().pop_from_delete_queue(output))
      {
        BlockCollect* block = get_block_manager().get(output.second);
        ServerCollect* server = get_server_manager().get(output.first);
        ret = ((NULL != block) && (NULL != server));
        if (ret)
        {
          relieve_relation(block, server, now, false);//这里有可能误报，因为有一些地方会先解除关系后再加入到删除列表
          ret = (TFS_SUCCESS == get_task_manager().remove_block_from_dataserver(server->id(), block->id(), now));
        }
      }
      return true;
    }

    int LayoutManager::build_marshalling_(int64_t& need, const time_t now)
    {
      int32_t ret = need > 0 ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        //get_family_manager().dump_marshalling_queue(TBSYS_LOG_LEVEL_INFO);
        const int32_t DATA_MEMBER_NUM  = SYSPARAM_NAMESERVER.max_data_member_num_;
        const int32_t CHECK_MEMBER_NUM = SYSPARAM_NAMESERVER.max_check_member_num_;
        const int32_t MEMBER_NUM = DATA_MEMBER_NUM + CHECK_MEMBER_NUM;
        ret = CHECK_MEMBER_NUM_V2(DATA_MEMBER_NUM, CHECK_MEMBER_NUM) ? TFS_SUCCESS : EXIT_FAMILY_MEMBER_NUM_ERROR;
        if (TFS_SUCCESS == ret)
        {
          uint64_t servers[MEMBER_NUM];
          std::pair<uint64_t, uint64_t> members[MEMBER_NUM];
          common::ArrayHelper<uint64_t> helper(MEMBER_NUM, servers);
          common::ArrayHelper<std::pair<uint64_t, uint64_t> > member_helper(MEMBER_NUM, members);
          ret = get_family_manager().create_family_choose_data_members(member_helper, DATA_MEMBER_NUM);
          if (TFS_SUCCESS == ret)
          {
            ret = DATA_MEMBER_NUM == member_helper.get_array_index() ?
              TFS_SUCCESS : EXIT_CHOOSE_TARGET_SERVER_INSUFFICIENT_ERROR;
            if (TFS_SUCCESS == ret)
            {
              for (int64_t index = 0; index < member_helper.get_array_index(); ++index)
              {
                std::pair<uint64_t, uint64_t> item = *member_helper.at(index);
                ServerCollect* server = get_server_manager().get(item.first);
                if (NULL != server)
                  helper.push_back(server->id());
              }
              ret = helper.get_array_index() == member_helper.get_array_index() ? TFS_SUCCESS :
                EXIT_CHOOSE_TARGET_SERVER_INSUFFICIENT_ERROR;
            }

            if (TFS_SUCCESS == ret)
            {
              ret = get_family_manager().create_family_choose_check_members(member_helper, helper, CHECK_MEMBER_NUM);
              if (TFS_SUCCESS == ret)
              {
                ret = CHECK_MEMBER_NUM == (member_helper.get_array_index() - DATA_MEMBER_NUM) ?
                  TFS_SUCCESS : EXIT_CHOOSE_TARGET_SERVER_INSUFFICIENT_ERROR;
              }
            }

            if (TFS_SUCCESS == ret)
            {
              int32_t family_aid_info;
              FamilyMemberInfo fminfo[MEMBER_NUM];
              SET_DATA_MEMBER_NUM(family_aid_info,DATA_MEMBER_NUM);
              SET_CHECK_MEMBER_NUM(family_aid_info,CHECK_MEMBER_NUM);
              SET_MASTER_INDEX(family_aid_info, DATA_MEMBER_NUM);
              SET_MARSHALLING_TYPE(family_aid_info, SYSPARAM_NAMESERVER.marshalling_type_);
              for (int64_t index = 0; index < member_helper.get_array_index() && TFS_SUCCESS == ret; ++index)
              {
                std::pair<uint64_t, uint64_t>* item = member_helper.at(index);
                fminfo[index].block_ = item->second;
                fminfo[index].server_ = item->first;
                fminfo[index].status_ = FAMILY_MEMBER_STATUS_NORMAL;
                fminfo[index].version_= 0;
                ret = (INVALID_BLOCK_ID != fminfo[index].block_) ? TFS_SUCCESS : EXIT_BLOCK_ID_INVALID_ERROR;
                if (TFS_SUCCESS == ret)
                  ret = get_task_manager().exist_block(fminfo[index].block_) ? EXIT_TASK_EXIST_ERROR : TFS_SUCCESS;
                if (TFS_SUCCESS == ret)
                  ret = (INVALID_SERVER_ID != fminfo[index].server_) ? TFS_SUCCESS : EXIT_SERVER_ID_INVALID_ERROR;
                if (TFS_SUCCESS == ret)
                {
                  bool target = get_task_manager().is_target(fminfo[index], index, PLAN_TYPE_EC_MARSHALLING, family_aid_info);
                  ret = ((!get_task_manager().exist_block(fminfo[index].block_))
                      && (!get_task_manager().exist_server(fminfo[index].server_))
                      && (get_task_manager().has_space_do_task_in_machine(fminfo[index].server_, target))) ? TFS_SUCCESS :  EXIT_TASK_EXIST_ERROR;
                  if (TFS_SUCCESS != ret)
                     get_task_manager().dump(TBSYS_LOG_LEVEL_DEBUG, "MARSHALLING DUMP TASK INFORMATION,");
                }
              }
              int64_t family_id = INVALID_FAMILY_ID;
              if (TFS_SUCCESS == ret)
              {
                ret = get_oplog_sync_mgr().create_family_id(family_id);
                ret = (TFS_SUCCESS == ret && INVALID_FAMILY_ID != family_id) ? TFS_SUCCESS : EXIT_CREATE_FAMILY_ID_ERROR;
              }

              if (TFS_SUCCESS == ret)
              {
                ret = get_task_manager().add(family_id, family_aid_info,PLAN_TYPE_EC_MARSHALLING,
                    MEMBER_NUM, fminfo, now);
              }
            }
          }
        }
      }
      return ret;
    }

    bool LayoutManager::build_adjust_copies_location_task_(common::ArrayHelper<uint64_t>& copies_location, BlockCollect* block, const time_t now)
    {
      bool ret = copies_location.get_array_index() > 0 && NULL != block;
      if (ret)
      {
        bool result = false;
        uint64_t server = INVALID_SERVER_ID;
        for (int64_t index = 0; index < copies_location.get_array_index(); ++index)
        {
          server = *copies_location.at(index);
          result = (relieve_relation(block, server, now));
          if (result)
          {
            ret = true;
            result = get_block_manager().push_to_emergency_replicate_queue(block);
            if (result)
              get_block_manager().push_to_delete_queue(block->id(), server);
          }
        }
      }
      return ret;
    }

    int64_t LayoutManager::has_space_in_task_queue_() const
    {
      return server_manager_.size() - task_manager_.get_running_server_size();
    }

    bool LayoutManager::scan_block_(ArrayHelper<BlockCollect*>& results, int64_t& need, uint64_t& start, int64_t& max_compact_task_count, const int32_t max_query_block_num,
          const time_t now, const bool compact_time, const bool marshalling_time, const bool adjust_copies_location_time, const bool report_time)
    {
      results.clear();
      bool ret  = false;
      BlockCollect* block = NULL;
      uint64_t copies[SYSPARAM_NAMESERVER.max_replication_];
      ArrayHelper<uint64_t> copies_location(SYSPARAM_NAMESERVER.max_replication_, copies);
      bool over = get_block_manager().scan(results, start, max_query_block_num);
      for (int64_t index = 0; index < results.get_array_index() && need > 0; ++index)
      {
        copies_location.clear();
        block = *results.at(index);
        assert(NULL != block);
        ret = get_block_manager().need_replicate(block, now);
        if ((ret) && (ret = get_block_manager().push_to_emergency_replicate_queue(block)))
          --need;
        ret = (!ret && plan_run_flag_ && PLAN_RUN_FLAG_RESLOVE_VERSION_CONFLICT
            && get_block_manager().check_version_conflict(block, now));
        if ((ret) && (ret = build_resolve_block_conflict_(block, now)))
          --need;
        ret = (!ret && adjust_copies_location_time && (plan_run_flag_ & PLAN_RUN_FLAG_ADJUST_COPIES_LOCATION)
            && get_block_manager().need_adjust_copies_location(copies_location,block,now));
        ret = ((ret) && (ret == build_adjust_copies_location_task_(copies_location, block, now)));
        ret = (!ret && compact_time && (plan_run_flag_ & PLAN_RUN_FLAG_COMPACT) && max_compact_task_count > 0
            && get_block_manager().need_compact(block,now));
        if ((ret) && (ret = build_compact_task_(block, now)))
        {
          --need;
          --max_compact_task_count;
        }
        ret = (!ret && marshalling_time && !report_time && (plan_run_flag_ & PLAN_RUN_FALG_MARSHALLING)
            && get_block_manager().need_marshalling(block, now));
        if (ret)
          ret = get_family_manager().push_block_to_marshalling_queues(block, now);
      }
      return over;
    }

    bool LayoutManager::scan_family_(common::ArrayHelper<FamilyCollect*>& results, int64_t& need, int64_t& start,
          const int32_t max_query_family_num, const time_t now, const bool compact_time)
    {
      UNUSED(need);
      results.clear();
      bool ret  = false;
      FamilyCollect* family = NULL;
      FamilyMemberInfo members[MAX_MARSHALLING_NUM];
      ArrayHelper<FamilyMemberInfo> helper(MAX_MARSHALLING_NUM, members);
      bool over = get_family_manager().scan(results, start, max_query_family_num);
      for (int64_t index = 0; index < results.get_array_index(); ++index)
      {
        helper.clear();
        family = *results.at(index);
        assert(NULL != family);
        ret = get_family_manager().check_need_reinstate(helper, family, now);
        if ((ret) && (ret = get_family_manager().push_to_reinstate_or_dissolve_queue(family, PLAN_TYPE_EC_REINSTATE)))
        {

        }
        ret = ((!ret) && get_family_manager().check_need_dissolve(family, helper, now));
        if ((ret) && (ret = get_family_manager().push_to_reinstate_or_dissolve_queue(family, PLAN_TYPE_EC_DISSOLVE)))
        {

        }
        ret = ((!ret) && compact_time && get_family_manager().check_need_compact(family, now));
        if ((ret) && (ret = get_family_manager().push_to_reinstate_or_dissolve_queue(family, PLAN_TYPE_EC_DISSOLVE)))
        {

        }
      }
      return over;
    }

    void LayoutManager::BuildPlanThreadHelper::run()
    {
      try
      {
        manager_.build_();
      }
      catch(std::exception& e)
      {
        TBSYS_LOG(ERROR, "catch exception: %s", e.what());
      }
      catch(...)
      {
        TBSYS_LOG(ERROR, "%s", "catch exception, unknow message");
      }
    }

    void LayoutManager::RunPlanThreadHelper::run()
    {
      try
      {
        manager_.get_task_manager().run();
      }
      catch(std::exception& e)
      {
        TBSYS_LOG(ERROR, "catch exception: %s", e.what());
      }
      catch(...)
      {
        TBSYS_LOG(ERROR, "%s", "catch exception, unknow message");
      }
    }

    void LayoutManager::CheckDataServerThreadHelper::run()
    {
      try
      {
        manager_.check_all_server_lease_timeout_();
      }
      catch(std::exception& e)
      {
        TBSYS_LOG(ERROR, "catch exception: %s", e.what());
      }
      catch(...)
      {
        TBSYS_LOG(ERROR, "%s", "catch exception, unknow message");
      }
    }

    void LayoutManager::AddBlockInAllServerThreadHelper::run()
    {
      try
      {
        manager_.regular_create_block_for_servers();
      }
      catch(std::exception& e)
      {
        TBSYS_LOG(ERROR, "catch exception: %s", e.what());
      }
      catch(...)
      {
        TBSYS_LOG(ERROR, "%s", "catch exception, unknow message");
      }
    }

    void LayoutManager::BuildBalanceThreadHelper::run()
    {
      try
      {
        manager_.balance_();
      }
      catch(std::exception& e)
      {
        TBSYS_LOG(ERROR, "catch exception: %s", e.what());
      }
      catch(...)
      {
        TBSYS_LOG(ERROR, "%s", "catch exception, unknow message");
      }
    }

    void LayoutManager::TimeoutThreadHelper::run()
    {
      try
      {
        manager_.timeout_();
      }
      catch(std::exception& e)
      {
        TBSYS_LOG(ERROR, "catch exception: %s", e.what());
      }
      catch(...)
      {
        TBSYS_LOG(ERROR, "%s", "catch exception, unknow message");
      }
    }

    void LayoutManager::RedundantThreadHelper::run()
    {
      try
      {
        manager_.redundant_();
      }
      catch(std::exception& e)
      {
        TBSYS_LOG(ERROR, "catch exception: %s", e.what());
      }
      catch(...)
      {
        TBSYS_LOG(ERROR, "%s", "catch exception, unknow message");
      }
    }

    void LayoutManager::LoadFamilyInfoThreadHelper::run()
    {
      try
      {
        manager_.load_family_info_();
      }
      catch(std::exception& e)
      {
        TBSYS_LOG(ERROR, "catch exception: %s", e.what());
      }
      catch(...)
      {
        TBSYS_LOG(ERROR, "%s", "catch exception, unknow message");
      }
    }
  } /** nameserver **/
}/** tfs **/
