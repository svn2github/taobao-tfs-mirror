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
 *   duanfei <duanfei@taobao.com>
 *      -modify interface-2012.03.15
 *
 */
#include <tbsys.h>
#include <tbnetutil.h>
#include "global_factory.h"
#include "common/status_message.h"
#include "common/base_service.h"
#include "message/client_cmd_message.h"
#include "nameserver.h"
#include "layout_manager.h"

namespace tfs
{
  namespace nameserver
  {
    using namespace common;
    using namespace tbsys;
    using namespace message;
    ClientRequestServer::ClientRequestServer(LayoutManager& manager)
      :ref_count_(0),
      manager_(manager)
    {

    }

    int ClientRequestServer::apply(common::DataServerStatInfo& info, int32_t& expire_time, int32_t& next_renew_time, int32_t& renew_retry_times, int32_t& renew_retry_timeout)
    {
      const time_t now = Func::get_monotonic_time();
      int32_t ret = manager_.get_server_manager().apply(info, now,SYSPARAM_NAMESERVER.between_ns_and_ds_lease_expire_time_);
      if (TFS_SUCCESS == ret)
      {
        calc_lease_expire_time_(expire_time, next_renew_time, renew_retry_times, renew_retry_timeout);
      }
      TBSYS_LOG(INFO, "dataserver: %s apply lease %s, ret: %d: use capacity: %" PRI64_PREFIX "u, total capacity: %" PRI64_PREFIX "u,lease_expired_time: %d, next_renew_time: %d, retry_times: %d",
        CNetUtil::addrToString(info.id_).c_str(),TFS_SUCCESS == ret ? "successful" : "failed", ret, info.use_capacity_, info.total_capacity_,
        expire_time, next_renew_time, renew_retry_times);
      return ret;
    }

    int ClientRequestServer::renew(const common::ArrayHelper<BlockInfoV2>& input,
          common::DataServerStatInfo& info, common::ArrayHelper<common::BlockLease>& output,
          int32_t& expire_time, int32_t& next_renew_time, int32_t& renew_retry_times, int32_t& renew_retry_timeout)
    {
      const time_t now = Func::get_monotonic_time();
      ServerManager& server_manager = manager_.get_server_manager();
      int32_t ret = server_manager.renew(info, now, SYSPARAM_NAMESERVER.between_ns_and_ds_lease_expire_time_);
      if (TFS_SUCCESS == ret)
      {
        calc_lease_expire_time_(expire_time, next_renew_time, renew_retry_times, renew_retry_timeout);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = server_manager.renew_block(info.id_,input, output);
      }
      TBSYS_LOG(INFO, "dataserver: %s renew lease %s, ret: %d: use capacity: %" PRI64_PREFIX "u, total capacity: %" PRI64_PREFIX "u,lease_expired_time: %d, next_renew_time: %d, retry_times: %d",
        CNetUtil::addrToString(info.id_).c_str(),TFS_SUCCESS == ret ? "successful" : "failed", ret, info.use_capacity_, info.total_capacity_,
        expire_time, next_renew_time, renew_retry_times);
      return ret;
    }

    int ClientRequestServer::giveup(const common::ArrayHelper<common::BlockInfoV2>& input,common::DataServerStatInfo& info)
    {
      BlockLease lease_array[1024];
      ArrayHelper<BlockLease> output(1024, lease_array);
      const time_t now = Func::get_monotonic_time();
      ServerManager& server_manager = manager_.get_server_manager();
      int32_t ret = server_manager.giveup_block(info.id_, input, output);
      if (TFS_SUCCESS == ret)
      {
        ret = server_manager.giveup(now, info.id_);
      }
      TBSYS_LOG(INFO, "dataserver: %s giveup lease %s, ret: %d: use capacity: %" PRI64_PREFIX "u, total capacity: %" PRI64_PREFIX "u",
        CNetUtil::addrToString(info.id_).c_str(),TFS_SUCCESS == ret ? "successful" : "failed", ret, info.use_capacity_, info.total_capacity_);
      return ret;
    }

    int ClientRequestServer::apply_block(const uint64_t server, common::ArrayHelper<common::BlockLease>& output)
    {
      ServerManager& server_manager = manager_.get_server_manager();
      return server_manager.apply_block(server, output);
    }

    int ClientRequestServer::apply_block_for_update(const uint64_t server, common::ArrayHelper<common::BlockLease>& output)
    {
      ServerManager& server_manager = manager_.get_server_manager();
      return server_manager.apply_block_for_update(server, output);
    }

    int ClientRequestServer::giveup_block(const uint64_t server, const common::ArrayHelper<common::BlockInfoV2>& input, common::ArrayHelper<common::BlockLease>& output)
    {
      ServerManager& server_manager = manager_.get_server_manager();
      return server_manager.giveup_block(server, input, output);
    }

    int ClientRequestServer::report_block(std::vector<uint64_t>& expires, const uint64_t server, const time_t now,
        const ArrayHelper<common::BlockInfoV2>& blocks)
    {
      int32_t ret = TFS_ERROR;
      ServerCollect* pserver = manager_.get_server_manager().get(server);
      ret = NULL == pserver ? EIXT_SERVER_OBJECT_NOT_FOUND : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        //update all relations of blocks belongs to it
        ret = manager_.update_relation(expires, pserver, blocks, now);
        if (TFS_SUCCESS == ret)
        {
          pserver = manager_.get_server_manager().get(server);
          ret = (NULL == pserver) ? EIXT_SERVER_OBJECT_NOT_FOUND : TFS_SUCCESS;
          if (TFS_SUCCESS == ret)
          {
            pserver->set_report_block_status(REPORT_BLOCK_STATUS_COMPLETE);
            pserver->set_next_report_block_time(now, random() % 0xFFFFFFF, false);
          }
        }
      }
      return ret;
    }

    int ClientRequestServer::open(const uint64_t block_id, const int32_t mode, const int32_t flag, const time_t now, uint64_t& lease_id,
          common::ArrayHelper<uint64_t>& servers, FamilyInfoExt& family_info)
    {
      servers.clear();
      BlockCollect* block = NULL;
      family_info.family_id_ = INVALID_FAMILY_ID;
      int32_t ret = (INVALID_BLOCK_ID == block_id) ? EXIT_BLOCK_ID_INVALID_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        ret = (mode & T_NEWBLK || mode & T_READ) ? TFS_SUCCESS : EXIT_ACCESS_MODE_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        block = manager_.get_block_manager().get(block_id);
        ret = (NULL != block) ? TFS_SUCCESS : EXIT_BLOCK_NOT_FOUND;
      }

      if (TFS_SUCCESS != ret)
      {
        if (mode & T_NEWBLK)
        {
          NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
          ret = ngi.in_discard_newblk_safe_mode_time(now) || is_discard() ? EXIT_DISCARD_NEWBLK_ERROR: TFS_SUCCESS;
        }
      }

      if (EXIT_BLOCK_NOT_FOUND == ret)
      {
        if (mode & T_NEWBLK)
        {
          ret = manager_.open_helper_create_new_block_by_id(block_id);
          if (TFS_SUCCESS != ret)
            TBSYS_LOG(INFO, "create new block by block id: %"PRI64_PREFIX"u failed, ret: %d", block_id, ret);
          else
            block =  manager_.get_block_manager().get(block_id);
        }
      }

      if (TFS_SUCCESS == ret)
      {
        ret = (NULL != block) ? TFS_SUCCESS : EXIT_BLOCK_NOT_FOUND;
      }

      if (TFS_SUCCESS == ret)
      {
        ret = manager_.get_block_manager().get_servers(servers, block);
      }

      if (TFS_SUCCESS == ret)
      {
        if (block->has_lease())
        {
          lease_id = *servers.at(0);
        }
      }
      bool read_family_info = (mode & T_READ) ? ((NULL != block) && (TFS_SUCCESS != ret || flag & F_FAMILY_INFO))
                                    : (mode & T_NEWBLK) ? (NULL != block) : false;
      if (read_family_info)
      {
        int64_t family_id = block->get_family_id();
        if (INVALID_FAMILY_ID != family_id)
        {
          common::ArrayHelper<std::pair<uint64_t, uint64_t> > helper(MAX_MARSHALLING_NUM, family_info.members_);
          ret = open(family_id, T_READ, family_info.family_aid_info_, helper);
          if (TFS_SUCCESS == ret)
          {
            int32_t index = 0;
            for (int64_t i = 0; i < helper.get_array_index(); ++i)
            {
              std::pair<uint64_t, uint64_t>* item = helper.at(i);
              if (item->second != INVALID_SERVER_ID)
                ++index;
            }

            const int32_t DATA_MEMBER = GET_DATA_MEMBER_NUM(family_info.family_aid_info_);
            ret = index >= DATA_MEMBER ? TFS_SUCCESS: EXIT_BLOCK_CANNOT_REINSTATE;
            if (TFS_SUCCESS == ret)
            {
              family_info.family_id_ = family_id;
            }
          }
        }
      }
      std::vector<stat_int_t> stat(4,0);
      mode & T_READ ? ret == TFS_SUCCESS ? stat[0] = 1 : stat[1] = 1
        : ret == TFS_SUCCESS ? stat[2] = 1 : stat[3] = 1;
      GFactory::get_stat_mgr().update_entry(GFactory::tfs_ns_stat_, stat);
      return ret;
    }

    int ClientRequestServer::batch_open(const int32_t mode, const int32_t flag, common::ArrayHelper<BlockMeta>& out)
    {
      time_t now = Func::get_monotonic_time();
      for (int64_t index = 0; index < out.get_array_index(); ++index)
      {
        uint64_t lease_id = INVALID_LEASE_ID;
        BlockMeta* meta = out.at(index);
        assert(NULL != meta);
        common::ArrayHelper<uint64_t> servers(MAX_REPLICATION_NUM, meta->ds_);
        meta->result_ = open(meta->block_id_,mode, flag, now, lease_id, servers, meta->family_info_);
        if (TFS_SUCCESS == meta->result_)
        {
          meta->lease_id_ = lease_id;
          meta->size_     = servers.get_array_index();
        }
      }
      return TFS_SUCCESS;
    }

    int ClientRequestServer::handle_control_load_block(const time_t now, const common::ClientCmdInformation& info, common::BasePacket* message, const int64_t buf_length, char* buf)
    {
      TBSYS_LOG(INFO, "handle control load block: %"PRI64_PREFIX"u, server: %s",
          info.value3_, CNetUtil::addrToString(info.value1_).c_str());
      int32_t ret = ((NULL != buf) && (buf_length > 0)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        bool new_create_block_collect  = false;
        BlockCollect* block = manager_.get_block_manager().get(info.value3_);
        ret  = NULL == block ? EXIT_NO_BLOCK : TFS_SUCCESS;
        if (TFS_SUCCESS != ret)
        {
          new_create_block_collect = true;
          block = manager_.get_block_manager().insert(info.value3_, now, true);
        }
        ret  = NULL == block ? EXIT_NO_BLOCK : TFS_SUCCESS;
        if (TFS_SUCCESS != ret)
          snprintf(buf, buf_length, " block: %"PRI64_PREFIX"u no exist, ret: %d", info.value3_, ret);

        if (TFS_SUCCESS == ret)
        {
          ServerCollect* server = manager_.get_server_manager().get(info.value1_);
          ret = NULL == server ? EIXT_SERVER_OBJECT_NOT_FOUND : TFS_SUCCESS;
          if (TFS_SUCCESS != ret)
            snprintf(buf, buf_length, " server: %s no exist, ret: %d", CNetUtil::addrToString(info.value1_).c_str(), ret);
          if (TFS_SUCCESS == ret)
          {
            int32_t status = STATUS_MESSAGE_ERROR;
            ret = send_msg_to_server(info.value1_, message, status);
            if ((STATUS_MESSAGE_OK != status)
                || (TFS_SUCCESS != ret))
            {
              snprintf(buf, buf_length, "send load block: %"PRI64_PREFIX"u  message to server: %s failed, ret: %d, status: %d",
                  info.value3_, CNetUtil::addrToString(info.value1_).c_str(), ret, status);
            }
            if (TFS_SUCCESS == ret)
            {
              ret = manager_.build_relation(block, server, now, true);
            }
          }
          if (block->get_servers_size() <= 0 && new_create_block_collect)
            manager_.get_block_manager().remove(block, info.value3_);
          manager_.get_gc_manager().insert(block, now);
        }
      }
      return ret;
    }

    int ClientRequestServer::handle_control_delete_block(const time_t now, const common::ClientCmdInformation& info,const int64_t buf_length, char* buf)
    {
      TBSYS_LOG(INFO, "handle control remove block: %"PRI64_PREFIX"u, flag: %"PRI64_PREFIX"u, server: %s",
          info.value3_, info.value4_, CNetUtil::addrToString(info.value1_).c_str());
      int32_t ret = ((NULL != buf) && (buf_length > 0)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      BlockCollect* pblock = NULL;
      if (TFS_SUCCESS == ret)
      {
        pblock = manager_.get_block_manager().get(info.value3_);
        ret = NULL == pblock ? EXIT_BLOCK_NOT_FOUND : TFS_SUCCESS;
        if (TFS_SUCCESS != ret)
        {
          snprintf(buf, buf_length, " block: %"PRI64_PREFIX"u no exist, ret: %d", info.value3_, ret);
        }
      }

      if (TFS_SUCCESS == ret)
      {
        BlockCollect* pobject = NULL;
        // admintool工具删除block不提供标志位混合的功能
        if (info.value4_ == HANDLE_DELETE_BLOCK_FLAG_BOTH || info.value4_ == HANDLE_DELETE_BLOCK_FLAG_ONLY_RELATION)
        {
          uint64_t servers[MAX_REPLICATION_NUM];
          ArrayHelper<uint64_t> helper(MAX_REPLICATION_NUM, servers);
          ret = manager_.get_block_manager().get_servers(helper, pblock);
          if (TFS_SUCCESS != ret)// EXIT_NO_DATASERVER
          {
            manager_.get_block_manager().remove(pobject, info.value3_);// just need to remove empty block object
            snprintf(buf, buf_length, " block: %"PRI64_PREFIX"u's dataserver not found, ret: %d", info.value3_, ret);
          }
          else
          {
            for (int64_t index = 0; index < helper.get_array_index() && TFS_SUCCESS == ret; ++index)
            {
              uint64_t server = *helper.at(index);
              ret = manager_.relieve_relation(pblock, server, now) ? TFS_SUCCESS : EXIT_RELIEVE_RELATION_ERROR;
              if (TFS_SUCCESS == ret && info.value4_ == HANDLE_DELETE_BLOCK_FLAG_BOTH)
              {
                ret = manager_.get_task_manager().remove_block_from_dataserver(server, info.value3_, now);
              }
            }
            if (TFS_SUCCESS != ret)
            {
              snprintf(buf, buf_length, " block: %"PRI64_PREFIX"u's relieve relation failed, ret: %d", info.value3_, ret);
            }
            else
            {
              manager_.get_block_manager().remove(pobject, info.value3_);
            }
          }
        }
        else if (info.value4_ == HANDLE_DELETE_BLOCK_FLAG_ONLY_DS)
        {
          uint64_t servers[MAX_REPLICATION_NUM];
          ArrayHelper<uint64_t> helper(MAX_REPLICATION_NUM, servers);
          ret = manager_.get_block_manager().get_servers(helper, pblock);
          if (TFS_SUCCESS != ret)
          {
            snprintf(buf, buf_length, " block: %"PRI64_PREFIX"u's dataserver not found, ret: %d", info.value3_, ret);// need to do nothing
          }
          else
          {
            for (int64_t index = 0; index < helper.get_array_index() && TFS_SUCCESS == ret; ++index)
            {
              uint64_t server = *helper.at(index);
              ret = manager_.relieve_relation(pblock, server, now) ? TFS_SUCCESS : EXIT_RELIEVE_RELATION_ERROR;
              if (TFS_SUCCESS == ret)
              {
                ret = manager_.get_task_manager().remove_block_from_dataserver(server, info.value3_, now);
              }
            }
            if (TFS_SUCCESS != ret)
            {
              snprintf(buf, buf_length, " block: %"PRI64_PREFIX"u's relieve relation failed, ret: %d", info.value3_, ret);
            }
            // else will keep empty block ds list object in ns
          }
        }
        else// relieve relation between specific ds and block
        {
          ServerCollect* server = manager_.get_server_manager().get(info.value1_);
          ret = NULL != server ? TFS_SUCCESS : EIXT_SERVER_OBJECT_NOT_FOUND;
          if (TFS_SUCCESS != ret)
          {
            snprintf(buf, buf_length, "dataserver server: %s no exist in nameserver or is not alive, block: %"PRI64_PREFIX"u ret: %d", CNetUtil::addrToString(info.value1_).c_str(), info.value3_, ret);
          }
          else
          {
            ret = manager_.relieve_relation(pblock, server, now) ? TFS_SUCCESS : EXIT_RELIEVE_RELATION_ERROR;
          }
          if (pblock->get_servers_size() <= 0)
          {
            manager_.get_block_manager().remove(pobject, info.value3_);
          }
        }
        manager_.get_gc_manager().insert(pobject, now);
      }
      return ret;
    }

    int ClientRequestServer::handle_control_compact_block(const time_t now, const common::ClientCmdInformation& info, const int64_t buf_length, char* buf)
    {
      TBSYS_LOG(INFO, "handle control compact block: %"PRI64_PREFIX"u", info.value3_);
      int32_t ret = ((NULL == buf) || (buf_length <= 0)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        uint64_t servers[MAX_REPLICATION_NUM];
        ArrayHelper<uint64_t> helper(MAX_REPLICATION_NUM, servers);
        ret = manager_.get_block_manager().get_servers(helper, info.value3_);
        if (TFS_SUCCESS != ret)
          snprintf(buf, buf_length, " block: %"PRI64_PREFIX"u no exist or dataserver not found, ret: %d", info.value3_, ret);
        if (TFS_SUCCESS == ret)
        {
          ret = manager_.get_task_manager().add(info.value3_, helper, PLAN_TYPE_COMPACT, now);
          if (TFS_SUCCESS != ret)
            snprintf(buf, buf_length, " add task(compact) failed, block: %"PRI64_PREFIX"u, ret: %d", info.value3_, ret);
        }
      }
      return ret;
    }

    int ClientRequestServer::handle_control_immediately_replicate_block(const time_t now, const common::ClientCmdInformation& info, const int64_t buf_length, char* buf)
    {
      TBSYS_LOG(INFO, "handle control %s block: %"PRI64_PREFIX"u, source: %s, target: %s",
          REPLICATE_BLOCK_MOVE_FLAG_NO == info.value4_ ? "replicate" : "move", info.value3_,
          CNetUtil::addrToString(info.value1_).c_str(), CNetUtil::addrToString(info.value2_).c_str());
      int32_t ret = ((NULL == buf) || (buf_length <= 0)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        bool new_create_block_collect = false;
        BlockCollect* block = manager_.get_block_manager().get(info.value3_);
        ret = NULL == block ? EXIT_NO_BLOCK : TFS_SUCCESS;
        if (TFS_SUCCESS != ret)
        {
          ret = INVALID_BLOCK_ID == info.value3_ ? EXIT_BLOCK_ID_INVALID_ERROR : TFS_SUCCESS;
          if (TFS_SUCCESS == ret)
          {
            new_create_block_collect = true;
            block = manager_.get_block_manager().insert(info.value3_, now, true);
          }
          ret = NULL == block ? EXIT_NO_BLOCK : TFS_SUCCESS;
        }
        if (TFS_SUCCESS == ret)
        {
          ServerCollect* source = NULL;
          ServerCollect* target = NULL;
          uint64_t servers[MAX_REPLICATION_NUM+ 1];
          ArrayHelper<uint64_t> helper(MAX_REPLICATION_NUM+ 1, servers);
          manager_.get_block_manager().get_servers(helper, info.value3_);
          if (INVALID_SERVER_ID != info.value1_)
            source = manager_.get_server_manager().get(info.value1_);
          else
            manager_.get_server_manager().choose_replicate_source_server(source, helper);
          ret = NULL != source ? TFS_SUCCESS : EXIT_NO_DATASERVER;
          if (TFS_SUCCESS != ret)
          {
            snprintf(buf, buf_length, "immediately %s block: %"PRI64_PREFIX"u fail, cannot found source dataserver",
                info.value4_ == REPLICATE_BLOCK_MOVE_FLAG_NO ? "replicate" : "move", info.value3_);
          }
          if (TFS_SUCCESS == ret)
          {
            if (!helper.exist(source->id()))
              helper.push_back(source->id());
            if (INVALID_SERVER_ID != info.value2_)
              target = manager_.get_server_manager().get(info.value2_);
            else//这里复制和均衡都采用同样的策略，如果迁移时直接采用迁移策略代价太大
              manager_.get_server_manager().choose_replicate_target_server(target, helper);
            ret = NULL != target ? TFS_SUCCESS : EXIT_NO_DATASERVER;
            if (TFS_SUCCESS != ret)
            {
              snprintf(buf, buf_length, "immediately %s block: %"PRI64_PREFIX"u fail, cannot found target dataserver",
                  info.value4_ == REPLICATE_BLOCK_MOVE_FLAG_NO ? "replicate" : "move", info.value3_);
            }
          }
          if (TFS_SUCCESS == ret)//NULL != source && NULL != target
          {
            uint32_t lan = Func::get_lan(source->id(), SYSPARAM_NAMESERVER.group_mask_);
            uint32_t lan2= Func::get_lan(target->id(), SYSPARAM_NAMESERVER.group_mask_);
            ret = (lan != lan2) ? TFS_SUCCESS : EXIT_CHOOSE_RACK_ERROR;
            if (TFS_SUCCESS != ret)
            {
              snprintf(buf, buf_length, "immediately %s block: %"PRI64_PREFIX"u fail, choose rack error: %u == %u",
                  info.value4_ == REPLICATE_BLOCK_MOVE_FLAG_NO ? "replicate" : "move", info.value3_, lan, lan2);
            }
          }
          if (TFS_SUCCESS == ret)
          {
            helper.clear();
            helper.push_back(source->id());
            helper.push_back(target->id());
            PlanType type = (info.value4_ == REPLICATE_BLOCK_MOVE_FLAG_NO) ? PLAN_TYPE_REPLICATE : PLAN_TYPE_MOVE;
            ret = manager_.get_task_manager().add(info.value3_, helper, type, now);
            if (TFS_SUCCESS != ret)
            {
              snprintf(buf, buf_length, "add task(%s) failed, block: %"PRI64_PREFIX"u",
                  info.value4_ == REPLICATE_BLOCK_MOVE_FLAG_NO ? "replicate" : "move", info.value3_);
            }
          }
          BlockCollect* pobject = NULL;
          if (block->get_servers_size() <= 0 && new_create_block_collect)
            manager_.get_block_manager().remove(pobject, info.value3_);
          manager_.get_gc_manager().insert(pobject, now);
        }
      }
      return ret;
    }

    int ClientRequestServer::handle_control_rotate_log(void)
    {
      BaseService* service = dynamic_cast<BaseService*>(BaseService::instance());
      TBSYS_LOGGER.rotateLog(service->get_log_path());
      return TFS_SUCCESS;
    }

    int ClientRequestServer::handle_control_set_runtime_param(const common::ClientCmdInformation& info, const int64_t buf_length, char* buf)
    {
      return manager_.set_runtime_param(info.value3_, info.value4_, buf_length, buf);
    }

    int ClientRequestServer::handle_control_get_balance_percent(const int64_t buf_length, char* buf)
    {
      snprintf(buf, buf_length, "%.6f", SYSPARAM_NAMESERVER.balance_percent_);
      return TFS_SUCCESS;
    }

    int ClientRequestServer::handle_control_set_balance_percent(const common::ClientCmdInformation& info, const int64_t buf_length, char* buf)
    {
      int32_t ret = (info.value3_ > 1 || info.value4_ < 0) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS != ret)
        snprintf(buf, buf_length, "parameter is invalid, value3: %"PRI64_PREFIX"u, value4: %"PRI64_PREFIX"d", info.value3_, info.value4_);
      if (TFS_SUCCESS == ret)
      {
        char data[32] = {'\0'};
        snprintf(data, 32, "%"PRI64_PREFIX"d.%06"PRI64_PREFIX"d", info.value3_, info.value4_);
        SYSPARAM_NAMESERVER.balance_percent_ = strtod(data, NULL);
      }
      return ret;
    }

    int ClientRequestServer::handle_control_clear_system_table(const common::ClientCmdInformation& info, const int64_t buf_length, char* buf)
    {
      int32_t ret = (info.value3_ <= 0) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS != ret)
        snprintf(buf, buf_length, "parameter is invalid, value3: %"PRI64_PREFIX"u", info.value3_);
      if (TFS_SUCCESS == ret)
      {
        if (info.value3_ & CLEAR_SYSTEM_TABLE_FLAG_TASK)
            manager_.get_task_manager().clear();
        if (info.value3_ & CLEAR_SYSTEM_TABLE_FLAG_DELETE_QUEUE)
            manager_.get_block_manager().clear_delete_queue();
      }
      return ret;
    }

    // parameter value3 ==> family id
    // remove family from db, clear family id
    int ClientRequestServer::handle_control_delete_family(const common::ClientCmdInformation& info, const int64_t buf_length, char* buf)
    {
      int32_t ret = (info.value3_ <= 0) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS != ret)
        snprintf(buf, buf_length, "parameter is invalid, value3: %"PRI64_PREFIX"u", info.value3_);
      if (TFS_SUCCESS == ret)
      {
        ret = manager_.get_oplog_sync_mgr().del_family(info.value3_);
        if (TFS_SUCCESS == ret)
        {
          std::pair<uint64_t, int32_t> members[MAX_MARSHALLING_NUM];
          common::ArrayHelper<std::pair<uint64_t, int32_t> > helper(MAX_MARSHALLING_NUM, members);
          ret = manager_.get_family_manager().get_members(helper, info.value3_);
          if (TFS_SUCCESS == ret)
          {
            for (int64_t index = 0; index < helper.get_array_index(); index++)
            {
              manager_.get_block_manager().set_family_id(helper.at(index)->first, INVALID_FAMILY_ID);
            }
            FamilyCollect* family = NULL;
            manager_.get_family_manager().remove(family, info.value3_);
            manager_.get_gc_manager().insert(family, Func::get_monotonic_time());
          }
        }
      }

      if (TFS_SUCCESS != ret)
      {
        snprintf(buf, buf_length, "del family %"PRI64_PREFIX"d fail, ret: %d", info.value3_, ret);
      }

      return ret;
    }

    int ClientRequestServer::handle_control_cmd(const ClientCmdInformation& info, common::BasePacket* msg, const int64_t buf_length, char* buf)
    {
      time_t now = Func::get_monotonic_time();
      int32_t ret = TFS_ERROR;
      switch (info.cmd_)
      {
        case CLIENT_CMD_LOADBLK:
          ret = handle_control_load_block(now, info, msg, buf_length, buf);
          break;
        case CLIENT_CMD_EXPBLK:
          ret = handle_control_delete_block(now, info, buf_length, buf);
          break;
        case CLIENT_CMD_COMPACT:
          ret = handle_control_compact_block(now, info, buf_length, buf);
          break;
        case CLIENT_CMD_IMMEDIATELY_REPL:  //immediately replicate
          ret = handle_control_immediately_replicate_block(now, info, buf_length, buf);
          break;
        case CLIENT_CMD_REPAIR_GROUP:
          //TODO
          break;
        case CLIENT_CMD_SET_PARAM:
          ret = handle_control_set_runtime_param(info, buf_length, buf);
          break;
        case CLIENT_CMD_ROTATE_LOG:
          ret = handle_control_rotate_log();
          break;
        case CLIENT_CMD_GET_BALANCE_PERCENT:
          ret = handle_control_get_balance_percent(buf_length, buf);
          break;
        case CLIENT_CMD_SET_BALANCE_PERCENT:
          ret = handle_control_set_balance_percent(info, buf_length, buf);
          break;
        case CLIENT_CMD_CLEAR_SYSTEM_TABLE:
          ret = handle_control_clear_system_table(info, buf_length, buf);
          break;
        case CLIENT_CMD_DELETE_FAMILY:
          ret = handle_control_delete_family(info, buf_length, buf);
          break;
        default:
          snprintf(buf, buf_length, "unknow client cmd: %d", info.cmd_);
          ret = EXIT_UNKNOWN_MSGTYPE;
          break;
      }
      return ret;
    }

    int ClientRequestServer::handle(common::BasePacket* msg)
    {
      int32_t ret = (NULL != msg) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        int32_t pcode = msg->getPCode();
        switch (pcode)
        {
          case REPLICATE_BLOCK_MESSAGE:
          case BLOCK_COMPACT_COMPLETE_MESSAGE:
          case REQ_EC_MARSHALLING_COMMIT_MESSAGE:
          case REQ_EC_REINSTATE_COMMIT_MESSAGE:
          case REQ_EC_DISSOLVE_COMMIT_MESSAGE:
            ret = manager_.handle_task_complete(msg);
            break;
          default:
            TBSYS_LOG(WARN, "unkonw message PCode = %d", pcode);
            break;
        }
      }
      return ret;
    }


    int ClientRequestServer::resolve_block_version_conflict(const uint64_t block_id, const common::ArrayHelper<std::pair<uint64_t, common::BlockInfoV2> >& members)
    {
      int32_t ret = (INVALID_BLOCK_ID != block_id &&  !members.empty()) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BlockCollect* block = manager_.get_block_manager().get(block_id);
        ret = (NULL != block) ? TFS_SUCCESS : EXIT_NO_BLOCK;
        if (TFS_SUCCESS == ret)
        {
          int64_t index = 0;
          bool update_last_time = false;
          time_t now = Func::get_monotonic_time();
          ServerCollect* server = NULL;
          common::BlockInfoV2 info;
          info.version_ = INVALID_VERSION;
          uint64_t servers[MAX_REPLICATION_NUM];
          ArrayHelper<uint64_t> helper(MAX_REPLICATION_NUM, servers);
          for (index = 0; index < members.get_array_index(); ++index)
          {
            std::pair<uint64_t, common::BlockInfoV2>* item = members.at(index);
            TBSYS_LOG(INFO, "resolve block version conflict: current block: %"PRI64_PREFIX"u, server: %s, version: %d",
              block->id(), tbsys::CNetUtil::addrToString(item->first).c_str(), item->second.version_);
            if (item->second.version_ >= info.version_)
            {
              server = manager_.get_server_manager().get(item->first);
              bool exist = manager_.get_block_manager().exist(block, server);
              if (item->second.version_ > info.version_ && exist)
                helper.clear();
              if (exist)
              {
                info = item->second;
                helper.push_back(item->first);
              }
            }
          }
          for (index = 0; index < members.get_array_index(); ++index)
          {
            std::pair<uint64_t, common::BlockInfoV2>* item = members.at(index);
            if (!helper.exist(item->first)
                && manager_.get_block_manager().get_servers_size(block_id) > 1)
            {
              //解除关系失败可以暂时不管
              update_last_time = true;
              server = manager_.get_server_manager().get(item->first);
              manager_.relieve_relation(block, server, now);
              manager_.get_block_manager().push_to_delete_queue(block_id, item->first);
              TBSYS_LOG(INFO, "resolve block version conflict: relieve relation block: %"PRI64_PREFIX"u, server: %s, version: %d",
                block_id, tbsys::CNetUtil::addrToString(item->first).c_str(), item->second.version_);
            }
          }
          if (block->version() < info.version_)
          {
            block->update(info);
          }
          if (update_last_time)
          {
            block->set(now, 0);
          }
        }
      }
      return ret;
    }


    int ClientRequestServer::open(const int64_t family_id, const int32_t mode, int32_t& family_aid_info, common::ArrayHelper<std::pair<uint64_t, uint64_t> >& members) const
    {
      UNUSED(mode);
      return manager_.get_family_manager().get_members(members, family_aid_info, family_id);
    }

    void ClientRequestServer::dump_plan(tbnet::DataBuffer& output)
    {
      return manager_.get_task_manager().dump(output);
    }

    bool ClientRequestServer::is_discard(void)
    {
      bool ret = SYSPARAM_NAMESERVER.discard_max_count_ > 0;
      if (ret)
      {
        ret = (atomic_inc(&ref_count_) >= static_cast<uint32_t>(SYSPARAM_NAMESERVER.discard_max_count_));
        if (ret)
        {
          atomic_exchange(&ref_count_, 0);
        }
      }
      return ret;
    }

    void ClientRequestServer::calc_lease_expire_time_(int32_t& expire_time, int32_t& next_renew_time, int32_t& renew_retry_times, int32_t& renew_retry_timeout) const
    {
      renew_retry_timeout = SYSPARAM_NAMESERVER.between_ns_and_ds_lease_retry_expire_time_;
      renew_retry_times = SYSPARAM_NAMESERVER.between_ns_and_ds_lease_retry_expire_time_;
      expire_time = SYSPARAM_NAMESERVER.between_ns_and_ds_lease_expire_time_ - SYSPARAM_NAMESERVER.between_ns_and_ds_lease_safe_time_;
      next_renew_time = expire_time - SYSPARAM_NAMESERVER.between_ns_and_ds_lease_retry_times_ * SYSPARAM_NAMESERVER.between_ns_and_ds_lease_retry_expire_time_;
    }
  }/** nameserver **/
}/** tfs **/
