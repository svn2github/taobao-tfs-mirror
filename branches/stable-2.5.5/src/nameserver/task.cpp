/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: task.cpp 2139 2011-03-28 09:15:26Z duanfei $
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
#include <tbsys.h>
#include <bitset>
#include "common/client_manager.h"
#include "message/compact_block_message.h"
#include "common/status_message.h"
#include "message/replicate_block_message.h"
#include "message/block_info_message.h"
#include "message/erasure_code_message.h"

#include "task.h"
#include "task_manager.h"
#include "global_factory.h"
#include "server_collect.h"
#include "block_manager.h"
#include "server_manager.h"
#include "common/array_helper.h"
#include "layout_manager.h"

using namespace tfs::common;
using namespace tfs::message;

namespace tfs
{
  namespace nameserver
  {
    Task::Task(TaskManager& manager, const common::PlanType type):
      BaseObject<LayoutManager>(0xFFFFFFFF),
      manager_(manager),
      seqno_(0),
      type_(type),
      status_(PLAN_STATUS_NONE)
    {

    }

    bool Task::operator < (const Task& task) const
    {
      if (type_ < task.type_)
        return true;
      if (type_ > task.type_)
        return false;
      return seqno_ < task.seqno_;
    }

    /*bool Task::timeout(const time_t now) const
    {
      return now > last_update_time_;
    }*/

    int Task::send_msg_to_server(const uint64_t server, common::BasePacket* msg)
    {
      int32_t ret = (INVALID_SERVER_ID != server && NULL != msg) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        std::stringstream info;
        dump(info);
        int32_t status = STATUS_MESSAGE_ERROR;
        ret = tfs::common::send_msg_to_server(server, msg, status);
        if ((TFS_SUCCESS == ret) && (STATUS_MESSAGE_OK == status))
        {
          TBSYS_LOG(INFO, "send %s command to %s successful, information: %s, ret: %d", transform_type_to_str(), tbsys::CNetUtil::addrToString(server).c_str(), info.str().c_str(), ret);
        }
        else
        {
          TBSYS_LOG(WARN, "send %s command to %s failed, information: %s, ret: %d, status: %d", transform_type_to_str(), tbsys::CNetUtil::addrToString(server).c_str(), info.str().c_str(), ret, status);
          ret = EXIT_SENDMSG_ERROR;
        }
      }
      return ret;
    }

    const char* Task::transform_type_to_str() const
    {
      return plan_type_to_str(type_);
    }

    const char* Task::transform_status_to_str(const int8_t status) const
    {
      return status == PLAN_STATUS_NONE ? "add" : status == PLAN_STATUS_BEGIN ? "begin" : status == PLAN_STATUS_TIMEOUT ? "timeout" :
          status == PLAN_STATUS_END ? "finish" : status == PLAN_STATUS_FAILURE ? "failure": status == PLAN_STATUS_PART_END ? "part-end": "unknow";
    }

    void Task::dump(tbnet::DataBuffer& stream)
    {
      stream.writeInt8(type_);
      stream.writeInt8(status_);
      stream.writeInt64(get());
      stream.writeInt64(seqno_);
    }

    void Task::runTimerTask()
    {
      status_ = PLAN_STATUS_TIMEOUT;
      dump(TBSYS_LOG_LEVEL(INFO), "task expired");
    }

    int Task::log(const int32_t type, common::BasePacket* msg)
    {
      int32_t ret = (type > OPLOG_TYPE_BLOCK_OP && type <= OPLOG_TYPE_DISSOLVE_MSG && NULL != msg) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        if (GFactory::get_runtime_info().is_master())
        {
          LayoutManager& lm = manager_.get_layout_manager();
          common::Stream stream(msg->length());
          ret = msg->serialize(stream);
          assert(TFS_SUCCESS == ret);
          int32_t result = lm.get_oplog_sync_mgr().log(
                      type, stream.get_data(), stream.get_data_length(), Func::get_monotonic_time());
          if (TFS_SUCCESS == ret)
            dump(TBSYS_LOG_LEVEL(INFO), "write %s oplog %s, ret: %d", transform_type_to_str(), TFS_SUCCESS == result ? "successful": "failed", result);
        }
      }
      return ret;
    }

    ReplicateTask::ReplicateTask(TaskManager& manager, const uint64_t block, const int8_t server_num, const uint64_t* servers,
      const common::PlanType type):
      Task(manager, type),
      servers_(NULL),
      block_(block),
      server_num_(server_num)
    {
      assert(server_num_ > 0);
      servers_ = new (std::nothrow) uint64_t[server_num_];
      assert(servers_);
      memcpy(servers_, servers, server_num_ * INT64_SIZE);
    }

    ReplicateTask::~ReplicateTask()
    {
      tbsys::gDeleteA(servers_);
    }

    int ReplicateTask::handle()
    {
      const int64_t now = Func::get_monotonic_time();
      int32_t ret = (INVALID_BLOCK_ID != block_ && NULL != servers_) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        create_msg_ref(ReplicateBlockMessage, msg);
        ReplBlock block;
        block.block_id_ = block_;
        int32_t source_index = 0;
        for (int32_t index = 0; index < server_num_; ++index)
        {
          if (1 != index)
            block.source_id_[source_index++] = servers_[index];
        }
        block.destination_id_ = servers_[1];
        block.start_time_ = Func::get_monotonic_time();
        block.is_move_ = PLAN_TYPE_MOVE  == type_ ? REPLICATE_BLOCK_MOVE_FLAG_YES : REPLICATE_BLOCK_MOVE_FLAG_NO;
        block.source_num_ = (server_num_ - 1);
        msg.set_repl_block(block);
        msg.set_status(PLAN_STATUS_BEGIN);
        msg.set_seqno(seqno_);
        msg.set_expire_time(SYSPARAM_NAMESERVER.move_task_expired_time_ - MAX_TASK_RESERVE_TIME);
        ret = send_msg_to_server(block.source_id_[0], &msg);
        status_ = PLAN_STATUS_BEGIN;
      }
      if (TFS_SUCCESS == ret)
      {
        BlockManager& bm =  manager_.get_layout_manager().get_block_manager();
        bm.set_task_expired_time(block_, now, SYSPARAM_NAMESERVER.move_task_expired_time_);
      }
      set(now, SYSPARAM_NAMESERVER.move_task_expired_time_);
      return ret;
    }

    void ReplicateTask::dump(tbnet::DataBuffer& stream)
    {
      Task::dump(stream);
      stream.writeInt64(block_);
      stream.writeInt32(server_num_);
      for (int8_t index = 0; index < server_num_ && NULL != servers_; ++index)
      {
        stream.writeInt64(servers_[index]);
      }
    }

    void ReplicateTask::dump(std::stringstream& stream)
    {
      const char* helper_str = PLAN_TYPE_COMPACT == type_ ? " " : " ==> ";
      stream << "block: " << block_ << " seqno: " << seqno_;
      for (int8_t index = 0; index < server_num_ && NULL != servers_;)
      {
        stream << " " << tbsys::CNetUtil::addrToString(servers_[index]);
        if (++index < server_num_)
          stream << helper_str;
      }
    }

    void ReplicateTask::dump(const int32_t level, const char* file, const int32_t line,
         const char* function, pthread_t thid, const char* format, ...)
    {
      if (level <= TBSYS_LOGGER._level)
      {
        char msgstr[256] = {'\0'};/** include '\0'*/
        va_list ap;
        va_start(ap, format);
        vsnprintf(msgstr, 256, NULL == format ? "" : format, ap);
        va_end(ap);
        std::stringstream str;
        for (int8_t index = 0; index < server_num_ && NULL != servers_; ++index)
        {
          str << " " << tbsys::CNetUtil::addrToString(servers_[index]) << "/";
        }
        TBSYS_LOGGER.logMessage(level, file, line, function, thid,"%s seqno: %"PRI64_PREFIX"d, type: %s, status: %s, block: %"PRI64_PREFIX"u, expired_time: %"PRI64_PREFIX"d, servers: %s",
            msgstr, seqno_, transform_type_to_str(),
            transform_status_to_str(status_), block_, get(), str.str().c_str());
      }
    }

    void ReplicateTask::dump_block(const int32_t level, const char* file, const int32_t line,
         const char* function, pthread_t thid, tbsys::CLogger& log)
    {
      if (level <= log._level)
      {
        std::stringstream str;
        for (int8_t index = 0; index < server_num_ && NULL != servers_; ++index)
        {
          str << " " << tbsys::CNetUtil::addrToString(servers_[index]) << "/";
        }
        log.logMessage(level, file, line, function, thid, "%s block-%"PRI64_PREFIX"u, seqno:%"PRI64_PREFIX"d, status:%s, servers :%s",
            transform_type_to_str(), block_, seqno_, transform_status_to_str(status_), str.str().c_str());
      }
    }

    int ReplicateTask::handle_complete(common::BasePacket* msg)
    {
      int32_t ret = (NULL != msg) ? STATUS_MESSAGE_OK : STATUS_MESSAGE_ERROR;
      if (STATUS_MESSAGE_OK == ret)
      {
        ret = (msg->getPCode() == REPLICATE_BLOCK_MESSAGE) ? STATUS_MESSAGE_OK : STATUS_MESSAGE_ERROR;
        if (STATUS_MESSAGE_OK == ret)
        {
          BlockManager& bm = manager_.get_layout_manager().get_block_manager();
          ServerManager& sm = manager_.get_layout_manager().get_server_manager();
          LayoutManager& lm = manager_.get_layout_manager();
          time_t now = Func::get_monotonic_time();
          ReplicateBlockMessage* message = dynamic_cast<ReplicateBlockMessage*>(msg);
          const ReplBlock& blocks = message->get_repl_block();
          BlockInfoV2 info = message->get_block_info();
          status_ = static_cast<PlanStatus>(message->get_status());
          if (status_ == PLAN_STATUS_END)
          {
            BlockCollect* block = bm.get(blocks.block_id_);
            ServerCollect* dest   = sm.get(blocks.destination_id_);// find destination dataserver
            ServerCollect* source = sm.get(blocks.source_id_[0]);// find source dataserver
            if ((NULL != block) && (NULL != dest))
            {
              const int32_t version = info.version_;
              info.version_ -= VERSION_INC_STEP_REPLICATE;
              if (blocks.is_move_ == REPLICATE_BLOCK_MOVE_FLAG_YES)
              {
                ret = STATUS_MESSAGE_ERROR;
                int32_t result = lm.build_relation(block, dest, &info, now, false);

                if (TFS_SUCCESS == result)
                {
                  ret = STATUS_MESSAGE_OK;
                  const int32_t size =  bm.get_servers_size(block);
                  if (size > 1 && (NULL != source) && block->exist(source->id()))
                  {
                    result = lm.relieve_relation(block, source, now, true);
                    ret = (TFS_SUCCESS == result) ? STATUS_MESSAGE_REMOVE : STATUS_MESSAGE_OK;
                  }
                }
              }
              else
              {
                //build relation between block and dest dataserver
                ret = TFS_SUCCESS == lm.build_relation(block, dest, &info, now, false)
                  ? STATUS_MESSAGE_OK: STATUS_MESSAGE_ERROR;
              }

              if (STATUS_MESSAGE_OK == ret || STATUS_MESSAGE_REMOVE == ret)
              {
                uint64_t array[MAX_REPLICATION_NUM];
                common::ArrayHelper<uint64_t> helper(MAX_REPLICATION_NUM, array);
                bm.get_servers(helper, block);
                if (blocks.is_move_ == REPLICATE_BLOCK_MOVE_FLAG_YES)
                {
                  bm.update_version(helper, info.block_id_, version, VERSION_INC_STEP_REPLICATE, info);
                }
                else
                {
                  bm.update_version(helper, info.block_id_, version, info);
                }
              }
            }
            this->log(OPLOG_TYPE_REPLICATE_MSG, message);
          }
          else
          {
            dump(TBSYS_LOG_LEVEL(INFO), "%s block: %"PRI64_PREFIX"u complete, but status: %s error",
              transform_type_to_str(), blocks.block_id_, transform_status_to_str(status_));
          }
        }
        if (GFactory::get_runtime_info().is_master())
          msg->reply(new StatusMessage(ret));
      }
      return (ret == STATUS_MESSAGE_OK || ret == STATUS_MESSAGE_REMOVE) ? TFS_SUCCESS : ret;
    }

    MoveTask::MoveTask(TaskManager& manager, const uint64_t block, const int8_t server_num, const uint64_t* servers):
      ReplicateTask(manager, block, server_num, servers, PLAN_TYPE_MOVE)
    {

    }

    CompactTask::CompactTask(TaskManager& manager, const uint64_t block, const int8_t server_num, const uint64_t* servers):
      ReplicateTask(manager, block, server_num, servers, PLAN_TYPE_COMPACT)
    {

    }

    int CompactTask::handle()
    {
      int64_t now = Func::get_monotonic_time();
      int32_t ret = (INVALID_BLOCK_ID != block_ && NULL != servers_) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        create_msg_ref(NsRequestCompactBlockMessage, msg);
        msg.set_seqno(seqno_);
        msg.set_block_id(block_);
        msg.set_expire_time(SYSPARAM_NAMESERVER.compact_task_expired_time_ - MAX_TASK_RESERVE_TIME);
        for (int8_t index = 0; index < server_num_; ++index)
          msg.get_servers().push_back(servers_[index]);
        ret = send_msg_to_server(servers_[0], &msg);
        status_ = PLAN_STATUS_BEGIN;
      }
      if (TFS_SUCCESS == ret)
      {
        BlockManager& bm =  manager_.get_layout_manager().get_block_manager();
        bm.set_task_expired_time(block_,now, SYSPARAM_NAMESERVER.compact_task_expired_time_);
      }
      set(now, SYSPARAM_NAMESERVER.compact_task_expired_time_);
      return ret;
    }

    int CompactTask::handle_complete(common::BasePacket* msg)
    {
      status_ = PLAN_STATUS_FAILURE;
      int32_t ret = (NULL != msg) && (msg->getPCode() == BLOCK_COMPACT_COMPLETE_MESSAGE) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BlockManager& bm = manager_.get_layout_manager().get_block_manager();
        ServerManager& sm = manager_.get_layout_manager().get_server_manager();
        DsCommitCompactBlockCompleteToNsMessage* message = dynamic_cast<DsCommitCompactBlockCompleteToNsMessage*>(msg);
        BlockInfoV2 info = message->get_block_info();
        BlockCollect* block = bm.get(info.block_id_);
        ret = (NULL != block) ? TFS_SUCCESS : EXIT_NO_BLOCK;
        if (TFS_SUCCESS == ret)
        {
          uint64_t array[MAX_REPLICATION_NUM], success[MAX_REPLICATION_NUM];
          common::ArrayHelper<uint64_t> helper(MAX_REPLICATION_NUM, array);
          common::ArrayHelper<uint64_t> success_helper(MAX_REPLICATION_NUM, success);
          const std::vector<std::pair<uint64_t, int8_t> >& result = message->get_result();
          std::vector<std::pair<uint64_t, int8_t> >::const_iterator iter = result.begin();
          for (; iter != result.end(); ++iter)
          {
            ServerCollect* pserver = sm.get(iter->first);
            ret = (NULL != pserver) ? TFS_SUCCESS : EXIT_NO_DATASERVER;
            if (TFS_SUCCESS == ret)
            {
              if (PLAN_STATUS_END == iter->second)
                success_helper.push_back(iter->first);
              else
                helper.push_back(iter->first);
            }
          }
          const int64_t total = result.size();
          const int64_t count = success_helper.get_array_index();
          status_ = total == count ? PLAN_STATUS_END : count > 0 ? PLAN_STATUS_PART_END : PLAN_STATUS_FAILURE;
          if (PLAN_STATUS_FAILURE != status_)
          {
            int64_t index = 0;
            bm.update_version(success_helper, info.block_id_, info.version_, VERSION_INC_STEP_COMPACT, info);
            for (index = 0; index < success_helper.get_array_index(); ++index)
            {
              uint64_t server = *success_helper.at(index);
              ServerCollect* pserver = sm.get(server);
              if (NULL != pserver)
                pserver->add_writable(block->id(), block->is_full());
            }
            ret = this->log(OPLOG_TYPE_COMPACT_MSG, message);
          }
        }
        if ( GFactory::get_runtime_info().is_master())
          message->reply(new StatusMessage(STATUS_MESSAGE_OK));
      }
      return ret;
    }

    ECMarshallingTask::ECMarshallingTask(TaskManager& manager, const int64_t family_id, const int32_t family_aid_info,
            const int32_t member_num, const common::FamilyMemberInfo* members, const common::PlanType type):
      Task(manager, type),
      family_id_(family_id),
      family_aid_info_(family_aid_info)
    {
      assert(member_num > 0);
      assert(members);
      family_members_ = new (std::nothrow) FamilyMemberInfo[member_num];
      memcpy(family_members_, members, sizeof(FamilyMemberInfo) * member_num);
    }

    ECMarshallingTask::~ECMarshallingTask()
    {
      tbsys::gDeleteA(family_members_);
    }

    int ECMarshallingTask::handle()
    {
      const int32_t index = GET_MASTER_INDEX(family_aid_info_);
      const int32_t MEMBER_NUM = GET_DATA_MEMBER_NUM(family_aid_info_) + GET_CHECK_MEMBER_NUM(family_aid_info_);
      int32_t ret = (NULL != family_members_ && CHECK_MEMBER_NUM(MEMBER_NUM) && index >=0 && index <=MAX_MARSHALLING_NUM)
              ? common::TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        create_msg_ref(ECMarshallingMessage, msg);
        msg.set_seqno(seqno_);
        msg.set_family_id(family_id_);
        msg.set_family_member_info(family_members_, family_aid_info_);
        msg.set_expire_time(SYSPARAM_NAMESERVER.marshalling_task_expired_time_ - MAX_TASK_RESERVE_TIME);
        const uint64_t server = family_members_[index].server_;
        ret = send_msg_to_server(server, &msg);
        status_ = PLAN_STATUS_BEGIN;
      }
      set(Func::get_monotonic_time(), SYSPARAM_NAMESERVER.marshalling_task_expired_time_);
      return ret;
    }

    int ECMarshallingTask::handle_complete(common::BasePacket* msg)
    {
      int32_t ret = (NULL != msg) && (msg->getPCode() == REQ_EC_MARSHALLING_COMMIT_MESSAGE) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BlockManager& bm = manager_.get_layout_manager().get_block_manager();
        ServerManager& sm = manager_.get_layout_manager().get_server_manager();
        FamilyManager& fm = manager_.get_layout_manager().get_family_manager();
        LayoutManager& lm = manager_.get_layout_manager();
        ECMarshallingCommitMessage* packet = dynamic_cast<ECMarshallingCommitMessage*>(msg);
        status_ = static_cast<common::PlanStatus>(packet->get_status());
        const bool forward = packet->get_forward();
        if (PLAN_STATUS_END == status_)
        {
          FamilyInfo family_info;
          family_info.family_id_ = packet->get_family_id();
          family_info.family_aid_info_ = packet->get_family_aid_info();
          const int32_t MEMBER_NUM = GET_DATA_MEMBER_NUM(family_info.family_aid_info_) + GET_CHECK_MEMBER_NUM(family_info.family_aid_info_);
          FamilyMemberInfo* base_info = packet->get_family_member_info();
          ret = (CHECK_MEMBER_NUM(MEMBER_NUM) && NULL != base_info) ? TFS_SUCCESS : EXIT_EXECUTE_TASK_ERROR;
          if (TFS_SUCCESS == ret)
          {
            const time_t now = common::Func::get_monotonic_time();
            std::pair<uint64_t, int32_t> members[MEMBER_NUM];
            common::ArrayHelper<std::pair<uint64_t, int32_t> > helper(MEMBER_NUM, members);
            for (int32_t index = 0; index < MEMBER_NUM; ++index)
            {
              family_info.family_member_.push_back(std::make_pair(base_info[index].block_, base_info[index].version_));
              helper.push_back(std::make_pair(base_info[index].block_, base_info[index].version_));
            }
            FamilyCollect* family = fm.get(family_info.family_id_);
            ret = (NULL == family) ? TFS_SUCCESS : EXIT_FAMILY_EXISTED;
            if (TFS_SUCCESS == ret)
            {
              ret = fm.insert(family_info.family_id_, family_info.family_aid_info_,helper, now);
              if (TFS_SUCCESS != ret)
                TBSYS_LOG(INFO, "add new family in memory failed, ret: %d, family id: %"PRI64_PREFIX"d", ret, family_info.family_id_);
            }
            if (TFS_SUCCESS == ret && !forward)
            {
              ret = lm.get_oplog_sync_mgr().create_family(family_info);
              if (TFS_SUCCESS != ret)
                TBSYS_LOG(INFO, "add new family in db failed, ret: %d, family id: %"PRI64_PREFIX"d", ret, family_info.family_id_);
            }
            if (TFS_SUCCESS == ret)
            {
              int32_t index = 0, del_index = 0;
              BlockCollect * pblock  = NULL;
              ServerCollect* pserver = NULL;
              const int32_t CHECK_MEMBER_NUM = GET_CHECK_MEMBER_NUM(family_info.family_aid_info_);
              const int32_t DATA_MEMBER_NUM  = GET_DATA_MEMBER_NUM(family_info.family_aid_info_);
              const int32_t MEMBER_NUM = CHECK_MEMBER_NUM + DATA_MEMBER_NUM;
              const int32_t MAX_DELETE_BLOCK_ARRAY_SIZE = MEMBER_NUM * SYSPARAM_NAMESERVER.max_replication_;
              std::pair<ServerCollect*, BlockCollect*> del_items[MAX_DELETE_BLOCK_ARRAY_SIZE];
              uint64_t servers[MAX_REPLICATION_NUM];
              ArrayHelper<uint64_t> helper2(MAX_REPLICATION_NUM, servers);
              for (; index < DATA_MEMBER_NUM && TFS_SUCCESS == ret; ++index)
              {
                helper2.clear();
                uint64_t block = base_info[index].block_;
                uint64_t server = base_info[index].server_;
                pblock = bm.get(block);
                ret = (NULL != pblock) ? TFS_SUCCESS : EXIT_BLOCK_NOT_FOUND;
                if (TFS_SUCCESS == ret)
                {
                  ret = bm.get_servers(helper2, pblock);
                }
                if (TFS_SUCCESS == ret)
                {
                  pserver = sm.get(server);
                  ret  = (NULL != pserver) ? TFS_SUCCESS : EIXT_SERVER_OBJECT_NOT_FOUND;
                }
                if (TFS_SUCCESS == ret)
                {
                  ret = bm.set_family_id(block, server, family_info.family_id_);
                }
                if (TFS_SUCCESS == ret)
                {
                  base_info[index].status_ = FAMILY_MEMBER_STATUS_OTHER;
                  helper2.remove(server);
                  for (int64_t i = 0; i < helper2.get_array_index(); ++i)
                  {
                    uint64_t server = *helper2.at(i);
                    pserver = sm.get(server);
                    assert(NULL != pserver);
                    std::pair<ServerCollect*, BlockCollect*>* item = &del_items[del_index++];
                    item->first = pserver;
                    item->second= pblock;
                  }
                }
              }

              for (; index < MEMBER_NUM && TFS_SUCCESS == ret; ++index)
              {
                uint64_t block = base_info[index].block_;
                uint64_t server = base_info[index].server_;
                pblock = bm.insert(block, now, false);
                assert(NULL != pblock);
                ret = (NULL != (pserver = sm.get(server))) ? TFS_SUCCESS : EIXT_SERVER_OBJECT_NOT_FOUND;
                if (TFS_SUCCESS == ret)
                  ret =  lm.build_relation(pblock, pserver, NULL, now, false);
                if (TFS_SUCCESS == ret)
                  ret = bm.set_family_id(block, server, family_info.family_id_);
                if (TFS_SUCCESS == ret)
                  base_info[index].status_ = FAMILY_MEMBER_STATUS_OTHER;
              }

              //build relation failed, we'll rollback
              if (TFS_SUCCESS != ret)
              {
                family = NULL;
                fm.remove(family, family_info.family_id_);
                lm.get_gc_manager().insert(family, now);
                if (!forward)
                  ret = lm.get_oplog_sync_mgr().del_family(family_info.family_id_);
                for (index = 0; index < MEMBER_NUM && TFS_SUCCESS == ret; ++index)
                {
                  if (FAMILY_MEMBER_STATUS_OTHER == base_info[index].status_)
                    ret = bm.set_family_id(base_info[index].block_, base_info[index].server_, INVALID_FAMILY_ID);
                }
                for (index = DATA_MEMBER_NUM; index < MEMBER_NUM && TFS_SUCCESS == ret; ++index)
                {
                  if (FAMILY_MEMBER_STATUS_OTHER == base_info[index].status_)
                    ret = lm.relieve_relation(base_info[index].block_, base_info[index].server_, now, true);
                  if (TFS_SUCCESS == ret)
                  {
                    bm.remove(pblock, base_info[index].block_);
                    lm.get_gc_manager().insert(pblock, now);// remove invalid check block from dataserver after next report
                  }
                }
              }
              else
              {
                for (index = 0; index < del_index; ++index)
                {
                  std::pair<ServerCollect*, BlockCollect*>* item = &del_items[index];
                  ServerItem si;
                  si.server_ = item->first->id();
                  si.family_id_ = INVALID_FAMILY_ID;
                  si.version_   = 0;
                  lm.relieve_relation(item->second,item->first, now, true);
                  bm.push_to_delete_queue(item->second->id(), si,GFactory::get_runtime_info().is_master());
                }
                //if (!forward)
                //  this->log(OPLOG_TYPE_MARSHALLING_MSG, msg);
              }
            }
          }
        }
        if (TFS_SUCCESS != ret)
          dump(TBSYS_LOG_LEVEL(INFO), "handle marshalling task failed, ret: %d,", ret);

        if (GFactory::get_runtime_info().is_master())
        {
          StatusMessage* reply_msg = new StatusMessage((PLAN_STATUS_END == status_ && TFS_SUCCESS == ret)
              ? STATUS_MESSAGE_OK : STATUS_MESSAGE_ERROR);
          msg->reply(reply_msg);
        }
      }
      return ret;
    }

    void ECMarshallingTask::dump(tbnet::DataBuffer& stream)
    {
      Task::dump(stream);
      stream.writeInt64(family_id_);
      stream.writeInt32(family_aid_info_);
      const int32_t MEMBER_NUM = GET_DATA_MEMBER_NUM(family_aid_info_) + GET_CHECK_MEMBER_NUM(family_aid_info_);
      for (int32_t index =  0; index < MEMBER_NUM; ++index)
      {
        stream.writeInt64(family_members_[index].server_);
        stream.writeInt64(family_members_[index].block_);
        stream.writeInt32(family_members_[index].version_);
        stream.writeInt32(family_members_[index].status_);
      }
    }

    void ECMarshallingTask::dump(std::stringstream& stream)
    {
      const int32_t MEMBER_NUM = GET_DATA_MEMBER_NUM(family_aid_info_) + GET_CHECK_MEMBER_NUM(family_aid_info_);
      stream << " seqno: " << seqno_ << " family_id: " <<family_id_ <<" master index: " <<GET_MASTER_INDEX(family_aid_info_)
              << " data_member_num: " << GET_DATA_MEMBER_NUM(family_aid_info_)
              << " check_member_num: " <<  GET_CHECK_MEMBER_NUM(family_aid_info_);
      for (int32_t index =  0; index < MEMBER_NUM ; ++index)
      {
        stream << "| server: " << tbsys::CNetUtil::addrToString(family_members_[index].server_) << " block: " << family_members_[index].block_
               << " version: " << family_members_[index].version_ << " status: " << family_members_[index].status_ ;
      }
    }

    void ECMarshallingTask::dump(const int32_t level, const char* file, const int32_t line,
         const char* function, pthread_t thid, const char* format, ...)
    {
      if (level <= TBSYS_LOGGER._level)
      {
        char msgstr[256] = {'\0'};/** include '\0'*/
        va_list ap;
        va_start(ap, format);
        vsnprintf(msgstr, 256, NULL == format ? "" : format, ap);
        va_end(ap);
        std::stringstream str;
        dump(str);
        TBSYS_LOGGER.logMessage(level, file, line, function, thid, "%s type: %s, status: %s, expired_time: %"PRI64_PREFIX"d, infomations: %s",
          msgstr, transform_type_to_str(), transform_status_to_str(status_), get(), str.str().c_str());
      }
    }

    void ECMarshallingTask::dump_block(const int32_t level, const char* file, const int32_t line,
         const char* function, pthread_t thid, tbsys::CLogger& log)
    {
      if (level <= log._level)
      {
        std::stringstream stream;
        const int32_t MEMBER_NUM = GET_DATA_MEMBER_NUM(family_aid_info_) + GET_CHECK_MEMBER_NUM(family_aid_info_);
        stream << "master_index:" << GET_MASTER_INDEX(family_aid_info_)
                << ", " << GET_DATA_MEMBER_NUM(family_aid_info_)
                << "+" <<  GET_CHECK_MEMBER_NUM(family_aid_info_);
        for (int32_t index =  0; index < MEMBER_NUM ; ++index)
        {
          stream << " | " << tbsys::CNetUtil::addrToString(family_members_[index].server_) << ", block-" << family_members_[index].block_
                 << ", status:" << family_members_[index].status_ ;
        }
        log.logMessage(level, file, line, function, thid, "%s family-%"PRI64_PREFIX"u, seqno:%"PRI64_PREFIX"d, status:%s, %s",
            transform_type_to_str(), family_id_, seqno_, transform_status_to_str(status_), stream.str().c_str());
      }
    }

    ECReinstateTask::ECReinstateTask(TaskManager& manager, const int64_t family_id, const int32_t family_aid_info,
      const int32_t all_member_num , const common::FamilyMemberInfo* members):
      ECMarshallingTask(manager, family_id, family_aid_info, all_member_num, members, PLAN_TYPE_EC_REINSTATE)
    {

    }

    ECReinstateTask::~ECReinstateTask()
    {

    }

    int ECReinstateTask::handle()
    {
      const int64_t now = Func::get_monotonic_time();
      const int32_t index = GET_MASTER_INDEX(family_aid_info_);
      const int32_t MEMBER_NUM = GET_DATA_MEMBER_NUM(family_aid_info_) + GET_CHECK_MEMBER_NUM(family_aid_info_);
      int32_t ret = (NULL != family_members_ && CHECK_MEMBER_NUM(MEMBER_NUM)
                    && index >= 0 && index < MAX_MARSHALLING_NUM) ? common::TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        create_msg_ref(ECReinstateMessage, msg);
        msg.set_seqno(seqno_);
        msg.set_family_id(family_id_);
        msg.set_family_member_info(family_members_, family_aid_info_);
        msg.set_expire_time(SYSPARAM_NAMESERVER.reinstate_task_expired_time_ - MAX_TASK_RESERVE_TIME);
        const uint64_t server = family_members_[index].server_;
        ret = send_msg_to_server(server, &msg);
        status_ = PLAN_STATUS_BEGIN;
      }
      if (TFS_SUCCESS == ret)
      {
        FamilyManager& fm = manager_.get_layout_manager().get_family_manager();
        fm.set_task_expired_time(family_id_, now, SYSPARAM_NAMESERVER.reinstate_task_expired_time_);
      }
      set(now,  SYSPARAM_NAMESERVER.reinstate_task_expired_time_);
      return ret;
    }

    BlockInfoV2* ECReinstateTask::query_(BlockInfoV2* infos, const int32_t num, const uint64_t block)
    {
      BlockInfoV2* result = NULL;
      if (NULL != infos && INVALID_BLOCK_ID != block)
      {
        for (int32_t index = 0; index < num && NULL == result; ++index)
        {
          if (infos[index].block_id_ == block)
            result = &infos[index];
        }
      }
      return result;
    }

    int ECReinstateTask::handle_complete(common::BasePacket* msg)
    {
      int32_t ret = (NULL != msg) && (msg->getPCode() == REQ_EC_REINSTATE_COMMIT_MESSAGE) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BlockManager& bm = manager_.get_layout_manager().get_block_manager();
        ServerManager& sm = manager_.get_layout_manager().get_server_manager();
        FamilyManager& fm = manager_.get_layout_manager().get_family_manager();
        LayoutManager& lm = manager_.get_layout_manager();
        ECReinstateCommitMessage* packet = dynamic_cast<ECReinstateCommitMessage*>(msg);
        status_ = static_cast<common::PlanStatus>(packet->get_status());
        //const bool forward = packet->get_forward();

        if (PLAN_STATUS_END == status_)
        {
          FamilyInfo family_info;
          family_info.family_id_ = packet->get_family_id();
          family_info.family_aid_info_ = packet->get_family_aid_info();
          const int32_t MEMBER_NUM = GET_DATA_MEMBER_NUM(family_info.family_aid_info_)
                + GET_CHECK_MEMBER_NUM(family_info.family_aid_info_);
          const FamilyMemberInfo* member_info = packet->get_family_member_info();
          ret = (INVALID_FAMILY_ID != family_info.family_id_ && CHECK_MEMBER_NUM(MEMBER_NUM) && NULL != member_info) ? TFS_SUCCESS : EXIT_EXECUTE_TASK_ERROR;
          if (TFS_SUCCESS == ret)
          {
            FamilyCollect* family = fm.get(family_info.family_id_);
            ret = (NULL != family) ? TFS_SUCCESS : EXIT_NO_FAMILY;
          }
          if (TFS_SUCCESS == ret)
          {
            BlockCollect* block = NULL;
            ServerCollect* server = NULL;
            const time_t now = Func::get_monotonic_time();
            const int32_t REINSTATE_NUM = packet->get_reinstate_num();
            common::BlockInfoV2* blockinfos = packet->get_reinstate_block_info();
            assert(NULL != blockinfos);
            for (int32_t index = 0; index < MEMBER_NUM && TFS_SUCCESS == ret; ++index)
            {
              if (FAMILY_MEMBER_STATUS_ABNORMAL == member_info[index].status_)
              {
                block  = bm.get(member_info[index].block_);
                ret = (NULL != block) ? TFS_SUCCESS : EXIT_BLOCK_NOT_FOUND;
                if (TFS_SUCCESS != ret)
                {
                  block = bm.insert(member_info[index].block_, now, false);
                  ret = (NULL != block) ? TFS_SUCCESS : EXIT_BLOCK_NOT_FOUND;
                  if (TFS_SUCCESS == ret)
                    ret = bm.set_family_id(member_info[index].block_, member_info[index].server_, family_info.family_id_);
                }
                if (TFS_SUCCESS == ret)
                {
                  server = sm.get(member_info[index].server_);
                  ret = (NULL != server) ? TFS_SUCCESS : EIXT_SERVER_OBJECT_NOT_FOUND;
                }
                if (TFS_SUCCESS == ret)
                {
                  BlockInfoV2* info = query_(blockinfos, REINSTATE_NUM, member_info[index].block_);
                  assert(NULL != info);
                  ret = lm.build_relation(block, server, info,now, false);
                }
              }
            }

            //if (TFS_SUCCESS == ret && !forward)
            //  this->log(OPLOG_TYPE_REINSTATE_MSG, msg);
          }
        }
        if (TFS_SUCCESS != ret)
          dump(TBSYS_LOG_LEVEL(INFO), "handle reistate task failed, ret: %d,", ret);
        if (GFactory::get_runtime_info().is_master())
        {
          StatusMessage* reply_msg = new StatusMessage((PLAN_STATUS_END == status_ && TFS_SUCCESS == ret)
              ? STATUS_MESSAGE_OK : STATUS_MESSAGE_ERROR);
          ret = msg->reply(reply_msg);
        }
      }
     return ret;
    }

    ECDissolveTask::ECDissolveTask(TaskManager& manager, const int64_t family_id, const int32_t family_aid_info,
      const int32_t all_member_num , const common::FamilyMemberInfo* members):
      ECMarshallingTask(manager, family_id, family_aid_info, all_member_num, members, PLAN_TYPE_EC_DISSOLVE)
    {

    }

    ECDissolveTask::~ECDissolveTask()
    {

    }

    int ECDissolveTask::handle()
    {
      const int64_t now = Func::get_monotonic_time();
      const int32_t MEMBER_NUM = GET_DATA_MEMBER_NUM(family_aid_info_) + GET_CHECK_MEMBER_NUM(family_aid_info_);
      int32_t ret = (NULL != family_members_ && MEMBER_NUM > 0 && MEMBER_NUM <= (MAX_MARSHALLING_NUM * 2))
                      ? common::TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        create_msg_ref(ECDissolveMessage, msg);
        msg.set_seqno(seqno_);
        msg.set_family_id(family_id_);
        msg.set_family_member_info(family_members_, family_aid_info_);
        msg.set_expire_time(SYSPARAM_NAMESERVER.dissolve_task_expired_time_ - MAX_TASK_RESERVE_TIME);
        const int32_t index = GET_MASTER_INDEX(family_aid_info_);
        ret = send_msg_to_server(family_members_[index].server_, &msg);
        status_ = PLAN_STATUS_BEGIN;
      }
      if (TFS_SUCCESS == ret)
      {
        FamilyManager& fm = manager_.get_layout_manager().get_family_manager();
        fm.set_task_expired_time(family_id_, now, SYSPARAM_NAMESERVER.dissolve_task_expired_time_);
      }
      set(now, SYSPARAM_NAMESERVER.dissolve_task_expired_time_);
      return ret;
    }

    int ECDissolveTask::handle_complete(common::BasePacket* msg)
    {
      int32_t ret = (NULL != msg) && (msg->getPCode() == REQ_EC_DISSOLVE_COMMIT_MESSAGE) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BlockManager& bm = manager_.get_layout_manager().get_block_manager();
        ServerManager& sm = manager_.get_layout_manager().get_server_manager();
        FamilyManager& fm = manager_.get_layout_manager().get_family_manager();
        LayoutManager& lm = manager_.get_layout_manager();
        ECDissolveCommitMessage* packet = dynamic_cast<ECDissolveCommitMessage*>(msg);
        status_ = static_cast<common::PlanStatus>(packet->get_status());
        const bool forward = packet->get_forward();
        if (PLAN_STATUS_END == status_)
        {
          FamilyInfo family_info;
          family_info.family_id_ = packet->get_family_id();
          family_info.family_aid_info_ = packet->get_family_aid_info();
          const int32_t DATA_MEMBER_NUM  = GET_DATA_MEMBER_NUM(family_info.family_aid_info_);
          const int32_t CHECK_MEMBER_NUM = GET_CHECK_MEMBER_NUM(family_info.family_aid_info_);
          const int32_t MEMBER_NUM       = DATA_MEMBER_NUM + CHECK_MEMBER_NUM;
          const FamilyMemberInfo* member_info = packet->get_family_member_info();
          ret = (INVALID_FAMILY_ID != family_info.family_id_ && MEMBER_NUM > 0 && MEMBER_NUM <= (MAX_MARSHALLING_NUM * 2) && NULL != member_info) ? TFS_SUCCESS : EXIT_EXECUTE_TASK_ERROR;
          if (TFS_SUCCESS == ret)
          {
            FamilyCollect* family = fm.get(family_info.family_id_);
            ret = (NULL != family) ? TFS_SUCCESS : EXIT_NO_FAMILY;
          }
          if (TFS_SUCCESS == ret)
          {
            if (!forward)
              ret = lm.get_oplog_sync_mgr().del_family(family_info.family_id_);
            if (TFS_SUCCESS == ret)
            {
              int32_t index = 0;
              BlockCollect* block = NULL;
              ServerCollect* server = NULL;
              const time_t now = Func::get_monotonic_time();
              const int32_t MAX_LOOP = MEMBER_NUM - (CHECK_MEMBER_NUM / 2);
              for (index = MEMBER_NUM / 2; index < MAX_LOOP && TFS_SUCCESS == ret; ++index)
              {
                if (INVALID_BLOCK_ID != member_info[index].block_)
                {
                  block  = bm.get(member_info[index].block_);
                  ret = (NULL != block) ? TFS_SUCCESS : EXIT_BLOCK_NOT_FOUND;
                  if (TFS_SUCCESS != ret)
                  {
                    block = bm.insert(member_info[index].block_, now, false);
                    ret = (NULL != block) ? TFS_SUCCESS : EXIT_BLOCK_NOT_FOUND;
                  }
                  if (TFS_SUCCESS == ret)
                  {
                    ret = bm.set_family_id(member_info[index].block_, member_info[index-MEMBER_NUM/2].server_, INVALID_FAMILY_ID);
                  }
                  if (TFS_SUCCESS == ret)
                  {
                    if (FAMILY_MEMBER_STATUS_NORMAL == member_info[index].status_
                        && INVALID_SERVER_ID != member_info[index].server_)
                    {
                      BlockInfoV2 info = block->get_block_info();
                      server = sm.get(member_info[index].server_);
                      ret = (NULL != server) ? TFS_SUCCESS : EIXT_SERVER_OBJECT_NOT_FOUND;
                      if (TFS_SUCCESS == ret)
                        lm.build_relation(block, server, &info, now, false);
                      if (TFS_SUCCESS == ret)
                      {
                        uint64_t array[MAX_REPLICATION_NUM];
                        common::ArrayHelper<uint64_t> helper(MAX_REPLICATION_NUM, array);
                        bm.get_servers(helper, block);
                        bm.update_version(helper, info.block_id_, info.version_, VERSION_INC_STEP_REPLICATE, info);
                      }
                    }
                  }
                }
              }
              for (block = NULL, index = DATA_MEMBER_NUM / 2; index <  MEMBER_NUM / 2; ++index, block = NULL)
              {
                if (INVALID_BLOCK_ID != member_info[index].block_)
                {
                  bm.remove(block, member_info[index].block_);
                  lm.get_gc_manager().insert(block, now);
                }
              }
              FamilyCollect* family = NULL;
              fm.remove(family, family_info.family_id_);
              lm.get_gc_manager().insert(family, now);
            }

            //if (TFS_SUCCESS == ret && !forward)
            //  this->log(OPLOG_TYPE_DISSOLVE_MSG, msg);
          }
        }
        if (TFS_SUCCESS != ret)
          dump(TBSYS_LOG_LEVEL(INFO), "handle dissolve task failed, ret: %d,", ret);
        if (GFactory::get_runtime_info().is_master())
        {
          StatusMessage* reply_msg = new StatusMessage((PLAN_STATUS_END == status_ && TFS_SUCCESS == ret)
              ? STATUS_MESSAGE_OK : STATUS_MESSAGE_ERROR);
          ret = msg->reply(reply_msg);
        }
      }
      return ret;
    }
  }/** end namespace nameserver **/
}/** end namespace tfs **/

