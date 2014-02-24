/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: task.cpp 390 2012-08-06 10:11:49Z linqing.zyd@taobao.com $
 *
 * Authors:
 *   linqing.zyd@taobao.com
 *      - initial release
 *
 */

#include <Memory.hpp>
#include "common/status_message.h"
#include "message/replicate_block_message.h"
#include "message/compact_block_message.h"
#include "message/write_data_message.h"
#include "message/dataserver_task_message.h"
#include "common/new_client.h"
#include "common/client_manager.h"
#include "task.h"
#include "data_helper.h"
#include "block_manager.h"
#include "dataservice.h"
#include "erasure_code.h"

namespace tfs
{
  namespace dataserver
  {
    using namespace common;
    using namespace message;
    using namespace tbutil;
    using namespace std;

    Task::Task(DataService& service, const int64_t seqno,
        const uint64_t source_id, const int32_t expire_time):
        GCObject(common::Func::get_monotonic_time()),
      service_(service), seqno_(seqno), source_id_(source_id), expire_time_(expire_time)
    {
      start_time_ = common::Func::get_monotonic_time();
      task_from_ds_ = false;
    }

    Task::~Task()
    {
    }

    inline DataHelper& Task::get_data_helper()
    {
      return service_.get_data_helper();
    }

    inline BlockManager& Task::get_block_manager()
    {
      return service_.get_block_manager();
    }

    const char* Task::get_type_str() const
    {
      const char* typestr = NULL;
      if (type_ <= PLAN_TYPE_EC_MARSHALLING)
      {
        typestr = planstr[type_];
      }
      return typestr;
    }

    string Task::dump() const
    {
      std::stringstream tmp_stream;
      const char* delim = ", ";

      tmp_stream << "dump " << get_type_str() << " task. ";
      tmp_stream << "seqno: " << seqno_ << delim;
      tmp_stream << "task source: " << tbsys::CNetUtil::addrToString(source_id_) << delim;
      tmp_stream << "expire time: " << expire_time_ << delim;
      return tmp_stream.str();
    }

    CompactTask::CompactTask(DataService& service, const int64_t seqno,
        const uint64_t source_id, const int32_t expire_time, const uint32_t block_id):
      Task(service, seqno, source_id, expire_time)
    {
      type_ = PLAN_TYPE_COMPACT;
      block_id_ = block_id;
    }

    CompactTask::~CompactTask()
    {

    }

    bool CompactTask::is_completed() const
    {
      bool ret = true;
      if (!task_from_ds())
      {
        for (uint32_t i = 0; i < result_.size(); i++)
        {
          if (PLAN_STATUS_TIMEOUT == result_[i].second)
          {
            ret = false;  // this subtask not finish yet
            break;
          }
        }
      }
      return ret;
    }

    int CompactTask::handle()
    {
      int ret = TFS_SUCCESS;
      if (task_from_ds())
      {
        ret = do_compact();
        int status = translate_status(ret);
        ret = report_to_ds(status);
      }
      else
      {
        // initialize status
        info_.block_id_ = block_id_;
        for (uint32_t i = 0; i < servers_.size(); i++)
        {
          result_.push_back(std::make_pair(servers_[i], common::PLAN_STATUS_TIMEOUT));
        }
        ret = dispatch_sub_task();
      }

      return ret;
    }

    int CompactTask::handle_complete(BasePacket* packet)
    {
      int ret = TFS_SUCCESS;
      if (RESP_DS_COMPACT_BLOCK_MESSAGE == packet->getPCode())
      {
        RespDsCompactBlockMessage* resp_cpt_msg = dynamic_cast<RespDsCompactBlockMessage*>(packet);
        int status = resp_cpt_msg->get_status();
        add_response(resp_cpt_msg->get_ds_id(), status, *(resp_cpt_msg->get_block_info()));

        resp_cpt_msg->reply(new StatusMessage(STATUS_MESSAGE_OK));

        if (is_completed())
        {
          ret = report_to_ns(status); // status is notused here
        }

        TBSYS_LOG(INFO, "handle complete compact task, "
            "seqno: %"PRI64_PREFIX"d, server: %s, status: %d, ret: %d\n",
            seqno_, tbsys::CNetUtil::addrToString(resp_cpt_msg->get_ds_id()).c_str(), status, ret);
      }
      return ret;  // no need return here
    }

    string CompactTask::dump() const
    {
      const char* delim = ", ";
      std::stringstream tmp_stream;
      tmp_stream << Task::dump();
      tmp_stream << "block id: " << block_id_ << delim;
      for (uint32_t i = 0; i < servers_.size(); i++)
      {
        tmp_stream << "server: " << tbsys::CNetUtil::addrToString(servers_[i]) << delim;
      }
      return tmp_stream.str();
    }

    bool CompactTask::get_involved_blocks(common::ArrayHelper<uint64_t>& blocks) const
    {
      bool ret = blocks.get_array_size() >= 1;
      if (ret)
      {
        blocks.clear();
        blocks.push_back(block_id_);
      }
      return ret;
    }

    void CompactTask::add_response(const uint64_t server, const int status,
        const common::BlockInfoV2& info)
    {
      for (uint32_t i = 0; i < result_.size(); i++)
      {
        if (result_[i].first == server)
        {
          result_[i].second = status;
        }

        if (PLAN_STATUS_END == status)
        {
          info_ = info;
        }
      }
    }

    int CompactTask::report_to_ns(const int status)
    {
      UNUSED(status);
      int ret = TFS_SUCCESS;
      DsCommitCompactBlockCompleteToNsMessage cmit_cpt_msg;
      cmit_cpt_msg.set_seqno(seqno_);
      cmit_cpt_msg.set_block_info(info_);
      cmit_cpt_msg.set_result(result_);
      ret = get_data_helper().send_simple_request(source_id_, &cmit_cpt_msg);

      TBSYS_LOG(INFO, "compact report to ns. seqno: %"PRI64_PREFIX"d, "
          "blockid: %"PRI64_PREFIX"u, status: %d, source: %s, ret: %d",
          seqno_, block_id_, status, tbsys::CNetUtil::addrToString(source_id_).c_str(), ret);

      return ret;
    }

    int CompactTask::report_to_ds(const int status)
    {
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      RespDsCompactBlockMessage resp_cpt_msg;
      resp_cpt_msg.set_seqno(seqno_);
      resp_cpt_msg.set_ds_id(ds_info.information_.id_);
      resp_cpt_msg.set_status(status);

      if (PLAN_STATUS_END == status)
      {
        BlockInfoV2 info;
        int ret = get_block_manager().get_block_info(info, block_id_);
        if (TFS_SUCCESS != ret)
        {
          resp_cpt_msg.set_status(PLAN_STATUS_FAILURE);
          TBSYS_LOG(ERROR, "compact get block failed. blockid: %"PRI64_PREFIX"u, ret: %d",
              block_id_, ret);
        }
        else
        {
          resp_cpt_msg.set_block_info(info);
        }
      }

      // post repsonse to master ds, won't care result
      //int32_t rret = post_msg_to_server(source_id_, &resp_cpt_msg, Task::ds_task_callback);
      post_msg_to_server(source_id_, &resp_cpt_msg, Task::ds_task_callback);

      service_.get_task_manager().remove_block(this);

      TBSYS_LOG(INFO, "compact report to ds. seqno: %"PRI64_PREFIX"d, "
          "blockid: %"PRI64_PREFIX"u, status: %d, source: %s",
        seqno_, block_id_, status, tbsys::CNetUtil::addrToString(source_id_).c_str());

      return TFS_SUCCESS;
    }

    int CompactTask::dispatch_sub_task()
    {
      int ret = TFS_SUCCESS;
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      DsCompactBlockMessage req_cpt_msg;
      req_cpt_msg.set_seqno(seqno_);
      req_cpt_msg.set_block_id(block_id_);
      req_cpt_msg.set_source_id(ds_info.information_.id_);
      req_cpt_msg.set_expire_time(expire_time_);
      for (uint32_t i = 0; (TFS_SUCCESS == ret) && (i < servers_.size()); i++)
      {
        ret = get_data_helper().send_simple_request(servers_[i], &req_cpt_msg);
        TBSYS_LOG(DEBUG, "task seqno(%"PRI64_PREFIX"d) request %s to compact, ret: %d",
            seqno_, tbsys::CNetUtil::addrToString(servers_[i]).c_str(), ret);
      }
      return ret;
    }

    int CompactTask::do_compact()
    {
      uint64_t block_id = block_id_;
      BaseLogicBlock* src = NULL;
      BaseLogicBlock* dest = NULL;

      // create temp block
      int ret = get_block_manager().new_block(block_id, true);
      if (TFS_SUCCESS == ret)
      {
        src = get_block_manager().get(block_id);
        ret = (NULL != src) ? TFS_SUCCESS : EXIT_NO_LOGICBLOCK_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        dest = get_block_manager().get(block_id, true);
        ret = (NULL != dest) ? TFS_SUCCESS : EXIT_NO_LOGICBLOCK_ERROR;
      }

      // do compact work
      if (TFS_SUCCESS == ret)
      {
        ret = real_compact(src, dest);
      }

      // switch block
      if (TFS_SUCCESS == ret)
      {
        ret = get_block_manager().switch_logic_block(block_id, true);
        if (TFS_SUCCESS == ret)
        {
          ret = get_block_manager().del_block(block_id, true);
        }
      }

      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "Run task %s, ret: %d", dump().c_str(), ret);
      }

      return ret;
    }

    int CompactTask::real_compact(BaseLogicBlock* src, BaseLogicBlock* dest)
    {
      LogicBlock* tmpsrc = dynamic_cast<LogicBlock* >(src);
      LogicBlock::Iterator* iter = new (std::nothrow) LogicBlock::Iterator(tmpsrc);
      assert(NULL != iter);

      char* buffer = new (std::nothrow) char[MAX_SINGLE_FILE_SIZE];
      assert(NULL != buffer);

      int32_t new_offset = 0;  // offset to write new file
      int32_t inner_offset = 0; // offset in compact buffer
      IndexHeaderV2 header, old_header;
      FileInfoV2* finfo = NULL;
      vector<FileInfoV2> finfos_vec;
      int32_t ret = dest->get_index_header(header);
      if (TFS_SUCCESS == ret)
      {
        ret = src->get_index_header(old_header);
      }
      while ((TFS_SUCCESS == ret) && (TFS_SUCCESS == (ret = iter->next(finfo))))
      {
        if (finfo->status_ & FI_DELETED)
        {
          continue;  // ignore deleted file
        }

        TBSYS_LOG(DEBUG, "COMPACT: %lu, file_id: %lu, offset: %d, crc: %u, next: %d",
            src->id(), finfo->id_, finfo->offset_, finfo->crc_, finfo->next_);

        if (inner_offset + finfo->size_ > MAX_SINGLE_FILE_SIZE)
        {
          if (inner_offset > 0)  // buffer not empty, flush first
          {
            ret = get_block_manager().pwrite(buffer, inner_offset, new_offset, dest->id(), true);
            ret = (ret > 0) ? TFS_SUCCESS : ret;
            if (TFS_SUCCESS == ret)
            {
              new_offset += inner_offset;
              inner_offset = 0;
              dest->get_used_offset(header.used_offset_);
              assert(new_offset == header.used_offset_);
            }
          }
        }

        if (TFS_SUCCESS == ret)
        {
          // special process big file
          if (is_big_file(finfo->size_))
          {
            ret = write_big_file(src, dest, *finfo, new_offset);
            if (TFS_SUCCESS == ret)
            {
              finfo->offset_ = new_offset; // update fileinfo offset
              finfos_vec.push_back(*finfo);
              header.info_.file_count_++;
              header.info_.size_ += finfo->size_;
              new_offset += finfo->size_;
            }
          }
          else  // the tmp buffer can contains current file, just memcopy it
          {
            const char* file_data = iter->get_data(finfo->offset_, finfo->size_);
            assert(NULL != file_data);
            memcpy(buffer + inner_offset, file_data, finfo->size_);
            uint32_t crc = 0;
            crc = Func::crc(crc, (buffer + inner_offset + 4), (finfo->size_ - 4));
            if (crc != finfo->crc_)
            {
              Func::hex_dump(file_data, 10, true, TBSYS_LOG_LEVEL_INFO);//TODO
            }
            assert(crc == finfo->crc_);
            finfo->offset_ = new_offset + inner_offset; // update fileinfo offset
            finfos_vec.push_back(*finfo);
            header.info_.file_count_++;
            header.info_.size_ += finfo->size_;
            inner_offset += finfo->size_;
          }
        }
      }

      if (EXIT_BLOCK_NO_DATA == ret)
        ret = TFS_SUCCESS;

      // still has data in buffer, flush it
      if (TFS_SUCCESS == ret && inner_offset > 0)
      {
        ret = get_block_manager().pwrite(buffer, inner_offset, new_offset, dest->id(), true);
        ret = (ret > 0) ? TFS_SUCCESS : ret;
        if (TFS_SUCCESS == ret)
        {
          new_offset += inner_offset;
          dest->get_used_offset(header.used_offset_);
          assert(new_offset == header.used_offset_);
        }
      }

      // write new header and index to dest
      if (TFS_SUCCESS == ret)
      {
        header.info_.version_ = old_header.info_.version_;
        header.info_.update_size_ = old_header.info_.update_size_;
        header.info_.update_file_count_ = old_header.info_.update_file_count_;
        memcpy(&header.throughput_, &old_header.throughput_, sizeof(header.throughput_));
        header.info_.version_ = old_header.info_.version_;
        header.seq_no_ = old_header.seq_no_;
        dest->get_marshalling_offset(header.marshalling_offset_);
        DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
        ret = dest->write_file_infos(header, finfos_vec, block_id_, false, ds_info.verify_index_reserved_space_ratio_);
        if (TFS_SUCCESS == ret)
        {
          dest->update_block_version(VERSION_INC_STEP_COMPACT);
        }
      }
      tbsys::gDeleteA(buffer);
      tbsys::gDelete(iter);

      return ret;
    }

    int CompactTask::write_big_file(BaseLogicBlock* src, BaseLogicBlock* dest,
        const FileInfoV2& finfo, const int32_t new_offset)
    {
      int offset = 0;
      int length = 0;
      int ret = TFS_SUCCESS;
      const int32_t MAX_READ_SIZE = 2 * 1024 * 1024;
      char *buffer = new (std::nothrow) char[MAX_READ_SIZE];
      assert(NULL != buffer);
      while ((TFS_SUCCESS == ret) && (offset < finfo.size_))
      {
        length = std::min(finfo.size_ - offset, MAX_READ_SIZE);
        ret = src->pread(buffer, length, finfo.offset_ + offset);
        ret = (ret >= 0) ? TFS_SUCCESS : ret;
        if (TFS_SUCCESS == ret)
        {
          ret = get_block_manager().pwrite(buffer, length, new_offset + offset, dest->id(), true);
          ret = (ret >= 0) ? TFS_SUCCESS : ret;
          if (TFS_SUCCESS == ret)
          {
            offset += length;
          }
        }
      }
      tbsys::gDeleteA(buffer);
      return ret;
    }

    bool CompactTask::is_big_file(const int32_t size) const
    {
      return size > MAX_SINGLE_FILE_SIZE;
    }

    ReplicateTask::ReplicateTask(DataService& service, const int64_t seqno,
        const uint64_t source_id, const int32_t expire_time, const common::ReplBlock& repl_info):
      Task(service, seqno, source_id, expire_time)
    {
      type_ = PLAN_TYPE_REPLICATE;
      repl_info_ = repl_info;
    }

    ReplicateTask::~ReplicateTask()
    {

    }

    int ReplicateTask::handle()
    {
      int ret = do_replicate();
      int status = translate_status(ret);
      if (task_from_ds())
      {
        ret = report_to_ds(status);
      }
      else
      {
        ret = report_to_ns(status);
      }

      return ret;
    }

    string ReplicateTask::dump() const
    {
      const char* delim = ", ";
      std::stringstream tmp_stream;
      tmp_stream << Task::dump();
      tmp_stream << "block id: " << repl_info_.block_id_ << delim;
      tmp_stream << "source id: ";
      for (int i = 0; i < repl_info_.source_num_; i++)
      {
        tmp_stream << tbsys::CNetUtil::addrToString(repl_info_.source_id_[i]) << " ";
      }
      tmp_stream << "dest id: " << tbsys::CNetUtil::addrToString(repl_info_.destination_id_) << delim;
      tmp_stream << "move flag: " << (REPLICATE_BLOCK_MOVE_FLAG_NO == repl_info_.is_move_ ? "no": "yes");
      return tmp_stream.str();
    }

    bool ReplicateTask::get_involved_blocks(common::ArrayHelper<uint64_t>& blocks) const
    {
      bool ret = blocks.get_array_size() >= 1;
      if (ret)
      {
        blocks.clear();
        blocks.push_back(repl_info_.block_id_);
      }
      return ret;
    }

    int ReplicateTask::report_to_ns(const int status)
    {
      ReplicateBlockMessage req_rb_msg;
      int ret = TFS_SUCCESS;

      req_rb_msg.set_seqno(seqno_);
      req_rb_msg.set_repl_block(&repl_info_);
      req_rb_msg.set_status(status);

      bool need_remove = false;
      NewClient* client = NewClientManager::get_instance().create_client();
      if (NULL == client)
      {
        ret = EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
      }
      else
      {
        tbnet::Packet* rsp_msg = NULL;
        ret = send_msg_to_server(source_id_, client, &req_rb_msg, rsp_msg);
        if (TFS_SUCCESS == ret)
        {
          if (STATUS_MESSAGE == rsp_msg->getPCode())
          {
            StatusMessage* sm = dynamic_cast<StatusMessage*> (rsp_msg);
            if ((REPLICATE_BLOCK_MOVE_FLAG_YES == repl_info_.is_move_) &&
                (STATUS_MESSAGE_REMOVE == sm->get_status()))
            {
              need_remove = true;
              ret = TFS_SUCCESS;
            }
            else if (STATUS_MESSAGE_OK == sm->get_status())
            {
              ret = TFS_SUCCESS;
            }
            else
            {
              ret = sm->get_status();
            }
          }
          else
          {
            ret = EXIT_UNKNOWN_MSGTYPE;
          }
        }
        NewClientManager::get_instance().destroy_client(client);
      }

      if (need_remove)
      {
        int rm_ret = get_block_manager().del_block(repl_info_.block_id_);
        TBSYS_LOG(INFO, "send repl block complete info: del blockid: %"PRI64_PREFIX"u, ret: %d\n",
            repl_info_.block_id_, rm_ret);
      }

      service_.get_task_manager().remove_block(this);

      TBSYS_LOG(INFO, "replicate report to ns. seqno: %"PRI64_PREFIX"d, "
          "blockid: %"PRI64_PREFIX"u, status: %d, source: %s, ret: %d",
          seqno_, repl_info_.block_id_, status, tbsys::CNetUtil::addrToString(source_id_).c_str(), ret);

      return ret;
    }

    int ReplicateTask::report_to_ds(const int status)
    {
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      RespDsReplicateBlockMessage resp_repl_msg;
      resp_repl_msg.set_seqno(seqno_);
      resp_repl_msg.set_ds_id(ds_info.information_.id_);
      resp_repl_msg.set_status(status);

      TBSYS_LOG(INFO, "replicate report to ds. seqno: %"PRI64_PREFIX"d, "
          "blockid: %"PRI64_PREFIX"u, status: %d, source: %s",
          seqno_, repl_info_.block_id_, status, tbsys::CNetUtil::addrToString(source_id_).c_str());

      //int32_t ret = post_msg_to_server(source_id_,  &resp_repl_msg, Task::ds_task_callback);
      post_msg_to_server(source_id_,  &resp_repl_msg, Task::ds_task_callback);

      service_.get_task_manager().remove_block(this);
      return TFS_SUCCESS;
    }

    int ReplicateTask::replicate_data(const int32_t block_size)
    {
      int32_t length = 0;
      int32_t offset = 0;
      uint64_t block_id = repl_info_.block_id_;
      uint64_t source_id = repl_info_.source_id_[0];
      uint64_t dest_id = repl_info_.destination_id_;
      char data[MAX_READ_SIZE];  // just use 1M stack space
      int ret = TFS_SUCCESS;
      while ((TFS_SUCCESS == ret) && (offset < block_size))
      {
        length = std::min(block_size - offset, MAX_READ_SIZE);
        ret = get_data_helper().read_raw_data(source_id, block_id, data, length, offset);
        if (TFS_SUCCESS == ret)
        {
          ret = get_data_helper().write_raw_data(dest_id, block_id, data, length, offset, true);
          if (TFS_SUCCESS == ret)
          {
            offset += length;
          }
        }
      }

      return ret;
    }

    int ReplicateTask::replicate_index()
    {
      uint64_t block_id = repl_info_.block_id_;
      uint64_t dest_id = repl_info_.destination_id_;
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();

      uint64_t helper[MAX_DATA_MEMBER_NUM];
      ArrayHelper<uint64_t> attach_blocks(MAX_DATA_MEMBER_NUM, helper);
      int ret = get_block_manager().get_attach_blocks(attach_blocks, block_id);
      for (int i = 0; (TFS_SUCCESS == ret) && (i < attach_blocks.get_array_index()); i++)
      {
        IndexDataV2 index_data;
        ret = get_data_helper().read_index(ds_info.information_.id_,
            block_id, *attach_blocks.at(i), index_data);
        if (TFS_SUCCESS == ret)
        {
          ret = get_data_helper().write_index(dest_id,
              block_id, *attach_blocks.at(i), index_data, true);
        }
      }

      return ret;
    }

    int ReplicateTask::do_replicate()
    {
      uint64_t block_id = repl_info_.block_id_;
      uint64_t dest_id = repl_info_.destination_id_;
      int64_t family_id = 0;
      int32_t block_size = 0;
      int32_t index_num = 0;

      int ret = get_block_manager().get_family_id(family_id, block_id);
      if (TFS_SUCCESS == ret)
      {
        ret = get_block_manager().get_used_offset(block_size, block_id);
        if ((TFS_SUCCESS == ret) && IS_VERFIFY_BLOCK(block_id))
        {
          ret = get_block_manager().get_index_num(index_num, block_id);
        }
      }

      // new remote temp block
      if (TFS_SUCCESS == ret)
      {
        ret = get_data_helper().new_remote_block(dest_id,
            block_id, true, family_id, index_num, expire_time_);
      }

      // replicate data
      if (TFS_SUCCESS == ret)
      {
        ret = replicate_data(block_size);
      }

      // replicate index
      if (TFS_SUCCESS == ret)
      {
        ret = replicate_index();
      }

      ECMeta ec_meta;
      // update source block version
      if ((TFS_SUCCESS == ret) && (!repl_info_.is_move_))
      {
        ec_meta.version_step_ = VERSION_INC_STEP_REPLICATE;
        for (int i = 0;  (i < repl_info_.source_num_) && (TFS_SUCCESS == ret); i++)
        {
          ret = get_data_helper().commit_ec_meta(repl_info_.source_id_[i],
              block_id, ec_meta, SWITCH_BLOCK_NO);
        }
      }

      // when move parity block, marshalling offset should be set to target
      if (INVALID_FAMILY_ID != family_id)
      {
        ret = get_block_manager().get_marshalling_offset(ec_meta.mars_offset_, block_id);
      }

      // commit block, update version and switch
      if (TFS_SUCCESS == ret)
      {
        ret = get_data_helper().commit_ec_meta(dest_id, block_id, ec_meta, SWITCH_BLOCK_YES);
      }

      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "Run task %s, ret: %d", dump().c_str(), ret);
      }

      return ret;
    }

    MarshallingTask::MarshallingTask(DataService& service, const int64_t seqno,
        const uint64_t source_id, const int32_t expire_time, const int64_t family_id) :
      Task(service, seqno, source_id, expire_time)
    {
      type_ = PLAN_TYPE_EC_MARSHALLING;
      family_id_ = family_id;
    }

    MarshallingTask::~MarshallingTask()
    {
      tbsys::gDeleteA(family_members_);
    }

    int MarshallingTask::set_family_info(const FamilyMemberInfo* members,
        const int32_t family_aid_info)
    {
      const int32_t MEMBER_NUM = GET_DATA_MEMBER_NUM(family_aid_info) +
        GET_CHECK_MEMBER_NUM(family_aid_info);
      int32_t ret = TFS_SUCCESS;
      if ((NULL == members) || (MEMBER_NUM <= 0) || (MEMBER_NUM) > (MAX_MARSHALLING_NUM))
      {
        ret = EXIT_PARAMETER_ERROR;
      }
      else
      {
        family_aid_info_ = family_aid_info;
        family_members_ = new (std::nothrow)FamilyMemberInfo[MEMBER_NUM];
        assert(family_members_);
        memcpy(family_members_, members, MEMBER_NUM * sizeof(FamilyMemberInfo));
      }
      return ret;
    }

    string MarshallingTask::dump() const
    {
      const int32_t DATA_NUM = GET_DATA_MEMBER_NUM(family_aid_info_);
      const int32_t CHECK_NUM = GET_CHECK_MEMBER_NUM(family_aid_info_);
      const char* delim = ", ";
      const char* nf = " | ";
      std::stringstream tmp_stream;
      tmp_stream << Task::dump();
      tmp_stream << "family id: " << family_id_ << delim;
      tmp_stream << "data num: " << DATA_NUM << delim;
      tmp_stream << "check num: " << CHECK_NUM << nf;

      for (int32_t i = 0; i < DATA_NUM + CHECK_NUM; i++)
      {
        tmp_stream << " server: " << tbsys::CNetUtil::addrToString(family_members_[i].server_);
        tmp_stream << " blockid: " << family_members_[i].block_;
        tmp_stream << " version: " << family_members_[i].version_;
        tmp_stream << " status: " << family_members_[i].status_;
        tmp_stream << nf;
      }
      return tmp_stream.str();
    }

    bool MarshallingTask::get_involved_blocks(common::ArrayHelper<uint64_t>& blocks) const
    {
      const int32_t DATA_NUM = GET_DATA_MEMBER_NUM(family_aid_info_);
      const int32_t CHECK_NUM = GET_CHECK_MEMBER_NUM(family_aid_info_);
      bool ret = blocks.get_array_size() >= DATA_NUM + CHECK_NUM;
      if (ret)
      {
        blocks.clear();
        for (int32_t index = 0; index < DATA_NUM + CHECK_NUM; index++)
        {
          blocks.push_back(family_members_[index].block_);
        }
      }
      return ret;
    }

    int MarshallingTask::handle()
    {
      int ret = do_marshalling();
      int status = translate_status(ret);
      return report_to_ns(status);
    }

    int MarshallingTask::report_to_ns(const int status)
    {
      int ret = TFS_SUCCESS;
      ECMarshallingCommitMessage cmit_msg;
      cmit_msg.set_seqno(seqno_);
      cmit_msg.set_status(status);
      cmit_msg.set_family_id(family_id_);
      ret = cmit_msg.set_family_member_info(family_members_, family_aid_info_);
      if (TFS_SUCCESS == ret)
      {
        int32_t status = TFS_ERROR;
        ret = send_msg_to_server(source_id_, &cmit_msg, status);
        ret = (ret < 0) ? ret : status;
      }

      TBSYS_LOG(INFO, "marshalling report to ns. seqno: %"PRI64_PREFIX"d, status: %d, source: %s, ret: %d",
          seqno_, status, tbsys::CNetUtil::addrToString(source_id_).c_str(), ret);

      return ret;
    }

    // for marshalling, ECMeta array size is DATA NUM
    int MarshallingTask::encode_data(ECMeta* ec_metas, int32_t& marshalling_len)
    {
      const int32_t data_num = GET_DATA_MEMBER_NUM(family_aid_info_);
      const int32_t check_num = GET_CHECK_MEMBER_NUM(family_aid_info_);
      const int32_t member_num = data_num + check_num;

      ErasureCode encoder;
      int32_t offset = 0;
      int32_t length = 0;
      int32_t read_len = 0;
      char* data[member_num];
      memset(data, 0, member_num * sizeof(char*));

      // get the element with max used_offset
      ECMeta* max_ele = max_element(ec_metas, ec_metas + data_num, ECMeta::u_compare);
      marshalling_len = max_ele->used_offset_;
      // if not align, rollup for encode
      int unit = ErasureCode::ws_ * ErasureCode::ps_;
      if (0 != (marshalling_len % unit))
      {
        marshalling_len = (marshalling_len / unit + 1) * unit;
      }

      // config encoder parameter, alloc buffer
      int ret = encoder.config(data_num, check_num);
      if (TFS_SUCCESS == ret)
      {
        for (int32_t i = 0; i < member_num; i++)
        {
          data[i] = (char*)malloc(MAX_READ_SIZE * sizeof(char));
          assert(NULL != data[i]);
        }
        encoder.bind(data, member_num, MAX_READ_SIZE);
      }

      // process block data
      while ((TFS_SUCCESS == ret) && (offset < marshalling_len))
      {
        length = std::min(marshalling_len - offset, MAX_READ_SIZE);

        // read data from data node
        for (int32_t i = 0; (TFS_SUCCESS == ret) && (i < data_num); i++)
        {
          read_len = length;
          if (offset < ec_metas[i].used_offset_)
          {
            // read part of buffer, need memset first to ensure data zero
            // and truncate read length to avoid read invalid data
            if (offset + length > ec_metas[i].used_offset_)
            {
              memset(data[i], 0, length * sizeof(char));
              read_len = ec_metas[i].used_offset_ - offset;
            }

            ret = get_data_helper().read_raw_data(family_members_[i].server_,
                family_members_[i].block_, data[i], read_len, offset);
          }
          else
          {
            memset(data[i], 0, length * sizeof(char));
          }
        }

        // encode data to buffer
        if (TFS_SUCCESS == ret)
        {
          ret = encoder.encode(length);
        }

        // write data to check node
        for (int32_t i = data_num; (TFS_SUCCESS == ret) && (i < member_num); i++)
        {
          ret = get_data_helper().write_raw_data(family_members_[i].server_,
              family_members_[i].block_, data[i], length, offset, true);
        }

        // one turn success, update offset
        if (TFS_SUCCESS == ret)
        {
          offset += length;
        }
      }

      for (int32_t i = 0; i < member_num; i++)
      {
        tbsys::gFree(data[i]);
      }

      return ret;
    }

    int MarshallingTask::backup_index()
    {
      int ret = TFS_SUCCESS;
      const int32_t data_num = GET_DATA_MEMBER_NUM(family_aid_info_);
      const int32_t check_num = GET_CHECK_MEMBER_NUM(family_aid_info_);
      const int32_t member_num = data_num + check_num;

      for (int i = 0; (TFS_SUCCESS == ret) && (i < data_num); i++)
      {
        IndexDataV2 index_data;
        ret = get_data_helper().read_index(family_members_[i].server_,
            family_members_[i].block_, family_members_[i].block_, index_data);
        if (TFS_SUCCESS == ret)
        {
          // we need record data block's marshalling len in check block
          index_data.header_.marshalling_offset_ = index_data.header_.used_offset_;
          index_data.header_.info_.family_id_ = family_id_;
        }
        // backup every data block's index to all check blocks
        for (int j = data_num; (TFS_SUCCESS == ret) && (j < member_num); j++)
        {
          ret = get_data_helper().write_index(family_members_[j].server_,
              family_members_[j].block_, family_members_[i].block_, index_data, true);
        }
      }
      return ret;
    }

    int MarshallingTask::do_marshalling()
    {
      const int32_t data_num = GET_DATA_MEMBER_NUM(family_aid_info_);
      const int32_t check_num = GET_CHECK_MEMBER_NUM(family_aid_info_);
      const int32_t member_num = data_num + check_num;
      int32_t marshalling_len = 0;
      ECMeta ec_metas[data_num];

      int ret = TFS_SUCCESS;
      bool need_unlock[data_num];
      for (int i = 0; i < data_num; i++)
      {
        need_unlock[i] = false;
      }

      for (int i = 0; (TFS_SUCCESS == ret) && (i < data_num); i++)
      {
        // marshalling will lock all data blocks on query
        ret = get_data_helper().query_ec_meta(family_members_[i].server_,
            family_members_[i].block_, ec_metas[i], expire_time_);
        need_unlock[i] = (TFS_SUCCESS == ret);
      }

      // create parity block
      for (int i = data_num; (TFS_SUCCESS == ret) && (i < member_num); i++)
      {
        ret = get_data_helper().new_remote_block(family_members_[i].server_,
            family_members_[i].block_, true, family_id_, data_num, expire_time_);
      }

      // encode data & write to parity block
      if (TFS_SUCCESS == ret)
      {
        ret = encode_data(ec_metas, marshalling_len);
      }

      // backup index to parity block's index
      if (TFS_SUCCESS == ret)
      {
        ret = backup_index();
      }

      // commit family id & other infos
      if (TFS_SUCCESS == ret)
      {
        for (int i = 0; (TFS_SUCCESS == ret) && (i < data_num); i++)
        {
          ec_metas[i].family_id_ = family_id_;
          // set used_offset as marshalling_offset for data node
          // marshalling will unlock all data blocks on commit
          ec_metas[i].mars_offset_ = ec_metas[i].used_offset_;
          ret = get_data_helper().commit_ec_meta(family_members_[i].server_,
            family_members_[i].block_, ec_metas[i], SWITCH_BLOCK_NO, UNLOCK_BLOCK_YES);
        }

        ECMeta ec_meta;
        ec_meta.family_id_ = family_id_;
        ec_meta.mars_offset_ = marshalling_len;
        for (int i = data_num; (TFS_SUCCESS == ret) && (i < member_num); i++)
        {
          ret = get_data_helper().commit_ec_meta(family_members_[i].server_,
            family_members_[i].block_, ec_meta, SWITCH_BLOCK_YES);
        }
      }

      if (TFS_SUCCESS != ret)
      {
        // just unlock all data blocks, ignore return value
        // if fail, blocks will be expired by background thread
        ECMeta zero;
        for (int i = 0; i < data_num; i++)
        {
          if (need_unlock[i])
          {
            get_data_helper().commit_ec_meta(family_members_[i].server_,
              family_members_[i].block_, zero, SWITCH_BLOCK_NO, UNLOCK_BLOCK_YES);
          }
        }
      }

      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "Run task %s, ret: %d", dump().c_str(), ret);
      }

      return ret;
    }

    ReinstateTask::ReinstateTask(DataService& service, const int64_t seqno,
        const uint64_t source_id, const int32_t expire_time, const int64_t family_id) :
      MarshallingTask(service, seqno, source_id, expire_time, family_id)
    {
      type_ = PLAN_TYPE_EC_REINSTATE;
      reinstate_num_ = 0;
    }

    ReinstateTask::~ReinstateTask()
    {

    }

    int ReinstateTask::set_family_info(const FamilyMemberInfo* members,
        const int32_t family_aid_info, const int* erased)
    {
      int ret = MarshallingTask::set_family_info(members, family_aid_info);
      if (TFS_SUCCESS == ret)
      {
        ret = (NULL == erased) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          const int32_t MEMBER_NUM = GET_DATA_MEMBER_NUM(family_aid_info) +
            GET_CHECK_MEMBER_NUM(family_aid_info);
          memcpy(erased_, erased, MEMBER_NUM * sizeof(int));
        }
      }
      return ret;
    }

    int ReinstateTask::handle()
    {
      int ret = do_reinstate();
      int status = translate_status(ret);
      return report_to_ns(status);
    }

    int ReinstateTask::do_reinstate()
    {
      const int32_t data_num = GET_DATA_MEMBER_NUM(family_aid_info_);
      const int32_t check_num = GET_CHECK_MEMBER_NUM(family_aid_info_);
      const int32_t member_num = data_num + check_num;
      int32_t marshalling_len = 0;
      ECMeta ec_metas[member_num];

      int ret = TFS_SUCCESS;
      bool lost_data = false;
      bool lost_check = false;
      bool need_unlock[member_num];
      for (int i = 0; i < member_num; i++)
      {
        need_unlock[i] = false;
      }
      for (int i = 0; (TFS_SUCCESS == ret) && (i < member_num); i++)
      {
        if (ErasureCode::NODE_ALIVE == erased_[i])
        {
          // reinstate will lock all alive blocks used for decode
          ret = get_data_helper().query_ec_meta(family_members_[i].server_,
              family_members_[i].block_, ec_metas[i], expire_time_);
          need_unlock[i] = (TFS_SUCCESS == ret);
        }
        else if (ErasureCode::NODE_DEAD == erased_[i])
        {
          if (i < data_num)
          {
            lost_data = true;
          }
          else
          {
            lost_check = true;
          }
        }
      }

      // create lost block
      for (int i = 0; (TFS_SUCCESS == ret) && (i < member_num); i++)
      {
        if (ErasureCode::NODE_DEAD != erased_[i])
        {
          continue;  // only query dead nodes
        }

        if (i < data_num)  // create data block
        {
          ret = get_data_helper().new_remote_block(family_members_[i].server_,
            family_members_[i].block_, true, INVALID_FAMILY_ID, 0, expire_time_);
        }
        else // create parity block
        {
          ret = get_data_helper().new_remote_block(family_members_[i].server_,
              family_members_[i].block_, true, family_id_, data_num, expire_time_);
        }
      }

      // decode data & write to recover target
      if (TFS_SUCCESS == ret)
      {
        ret = decode_data(ec_metas, marshalling_len);
      }

      // recover data index
      if ((TFS_SUCCESS == ret) && lost_data)
      {
        ret = recover_data_index(ec_metas);
      }

      // recover check index
      if ((TFS_SUCCESS == ret) && lost_check)
      {
        ret = recover_check_index(ec_metas, marshalling_len);
      }

      // unlock all alive blocks used for decode
      ECMeta zero;
      for (int32_t i = 0; i < member_num; i++)
      {
        if (ErasureCode::NODE_ALIVE == erased_[i] && need_unlock[i])
        {
          get_data_helper().commit_ec_meta(family_members_[i].server_,
              family_members_[i].block_, zero, SWITCH_BLOCK_NO, UNLOCK_BLOCK_YES);
        }
      }

      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "Run task %s, ret: %d", dump().c_str(), ret);
      }

      return ret;
    }

    // for reinstate, ECMeta array size is MEMBER NUM
    int ReinstateTask::decode_data(ECMeta* ec_metas, int32_t& marshalling_len)
    {
      const int32_t data_num = GET_DATA_MEMBER_NUM(family_aid_info_);
      const int32_t check_num = GET_CHECK_MEMBER_NUM(family_aid_info_);
      const int32_t member_num = data_num + check_num;

      ErasureCode decoder;
      int32_t offset = 0;
      int32_t length = 0;
      int32_t read_len = 0;
      char* data[member_num];
      memset(data, 0, member_num * sizeof(char*));

      // get the element with max marshalling_offset
      ECMeta* max_ele = max_element(ec_metas, ec_metas + member_num, ECMeta::m_compare);
      marshalling_len = max_ele->mars_offset_;
      // if not align, rollup for encode
      int unit = ErasureCode::ws_ * ErasureCode::ps_;
      if (0 != (marshalling_len % unit))
      {
        marshalling_len = (marshalling_len / unit + 1) * unit;
      }

      // config decoder parameter, alloc buffer
      int ret = decoder.config(data_num, check_num, erased_);
      if (TFS_SUCCESS == ret)
      {
        for (int32_t i = 0; i < member_num; i++)
        {
          data[i] = (char*)malloc(MAX_READ_SIZE * sizeof(char));
          assert(NULL != data[i]);
        }
        decoder.bind(data, member_num, MAX_READ_SIZE);
      }

      while ((TFS_SUCCESS == ret) && (offset < marshalling_len))
      {
        length = std::min(marshalling_len - offset, MAX_READ_SIZE);

        for (int32_t i = 0; i < member_num; i++)
        {
          if (ErasureCode::NODE_ALIVE != erased_[i])
          {
            continue; // not alive, just continue
          }

          read_len = length;
          if (offset < ec_metas[i].mars_offset_)
          {
            // read part of buffer, need memset first to ensure data zero
            // and truncate read length to avoid read invalid data
            if (length + offset > ec_metas[i].mars_offset_)
            {
              memset(data[i], 0, length * sizeof(char));
              read_len = ec_metas[i].mars_offset_ - offset;
            }

            ret = get_data_helper().read_raw_data(family_members_[i].server_,
                family_members_[i].block_, data[i], read_len, offset);
          }
          else
          {
            // no data to read in this block, just set all data to zero
            memset(data[i], 0, length * sizeof(char));
          }
        }

        // decode data to buffer
        if (TFS_SUCCESS == ret)
        {
          ret = decoder.decode(length);
        }

        // recover data
        for (int32_t i = 0; (TFS_SUCCESS == ret) && (i < member_num); i++)
        {
          if (ErasureCode::NODE_DEAD != erased_[i])
          {
            continue;  // we want to find dead node
          }

          ret = get_data_helper().write_raw_data(family_members_[i].server_,
              family_members_[i].block_, data[i], length, offset, true);
        }

        // all success, update offset
        if (TFS_SUCCESS == ret)
        {
          offset += length;
        }
      }

      for (int32_t i = 0; i < member_num; i++)
      {
        tbsys::gFree(data[i]);
      }

      return ret;
    }

    int ReinstateTask::recover_data_index(ECMeta* ec_metas)
    {
      int ret = TFS_SUCCESS;
      const int32_t data_num = GET_DATA_MEMBER_NUM(family_aid_info_);
      const int32_t check_num = GET_CHECK_MEMBER_NUM(family_aid_info_);
      const int32_t member_num = data_num + check_num;

      int32_t pi = -1;
      for (int i = data_num; i < member_num; i++)
      {
        if (ErasureCode::NODE_ALIVE == erased_[i])
        {
          pi = i;  // find a alive parity block's index
          break;
        }
      }

      ret = (pi > 0) ? TFS_SUCCESS: EXIT_NO_ENOUGH_DATA;
      for (int i = 0; (TFS_SUCCESS == ret) && (i < data_num); i++)
      {
        if (ErasureCode::NODE_DEAD != erased_[i])
        {
          continue; // we need find dead node to recover
        }

        IndexDataV2 index_data;
        ret = get_data_helper().read_index(family_members_[pi].server_,
            family_members_[pi].block_, family_members_[i].block_, index_data);
        if (TFS_SUCCESS == ret)
        {
          // update lost data node's marshalling len
          // it's needed when recover check block
          ec_metas[i].mars_offset_ = index_data.header_.marshalling_offset_;
          ret = get_data_helper().write_index(family_members_[i].server_,
              family_members_[i].block_, family_members_[i].block_, index_data, true);
        }

        if (TFS_SUCCESS == ret)
        {
          ret = recover_updated_files(index_data, ec_metas[pi].mars_offset_,
              family_members_[i].block_, pi, i);
        }

        if (TFS_SUCCESS == ret)
        {
          block_infos_[reinstate_num_++] = index_data.header_.info_;
        }
      }

      for (int i = 0; (TFS_SUCCESS == ret) && (i < data_num); i++)
      {
        if (ErasureCode::NODE_DEAD != erased_[i])
        {
          continue; // we need find dead node to recover
        }

        ECMeta ec_meta;
        ec_meta.family_id_ = family_id_;
        ret = get_data_helper().commit_ec_meta(family_members_[i].server_,
            family_members_[i].block_, ec_meta, SWITCH_BLOCK_YES);
      }

      return ret;
    }

    int ReinstateTask::recover_check_index(common::ECMeta* ec_metas, const int32_t marshalling_len)
    {
      int ret = TFS_SUCCESS;
      const int32_t data_num = GET_DATA_MEMBER_NUM(family_aid_info_);
      const int32_t check_num = GET_CHECK_MEMBER_NUM(family_aid_info_);
      const int32_t member_num = data_num + check_num;

      // all data nodes are alive now, recover check nodes from all data nodes
      for (int i = 0; (TFS_SUCCESS == ret) && (i < data_num); i++)
      {
        IndexDataV2 index_data;
        ret = get_data_helper().read_index(family_members_[i].server_,
            family_members_[i].block_, family_members_[i].block_, index_data);
        for (int j = data_num; (TFS_SUCCESS == ret) && (j < member_num); j++)
        {
          if (ErasureCode::NODE_DEAD != erased_[j])
          {
            continue; // we need find dead node to recove
          }
          ret = get_data_helper().write_index(family_members_[j].server_,
              family_members_[j].block_, family_members_[i].block_, index_data, true);
          if (TFS_SUCCESS == ret)
          {
            ret = recover_updated_files(index_data, ec_metas[i].mars_offset_,
                family_members_[i].block_, i, j);
          }
        }
      }

      // commit every verify block
      for (int i = data_num; (TFS_SUCCESS == ret) && (i < member_num); i++)
      {
        if (ErasureCode::NODE_DEAD != erased_[i])
        {
          continue; // we need find dead node to recove
        }

        ECMeta ec_meta;
        ec_meta.family_id_ = family_id_;
        ec_meta.mars_offset_ = marshalling_len;
        ret = get_data_helper().commit_ec_meta(family_members_[i].server_,
            family_members_[i].block_, ec_meta, SWITCH_BLOCK_YES);
      }

      return ret;
    }


    int ReinstateTask::recover_updated_files(const common::IndexDataV2& index_data,
      const int32_t marshalling_len, const uint64_t block_id, const int32_t src, const int32_t dest)
    {
      int ret = TFS_SUCCESS;
      bool updated = false;
      const vector<FileInfoV2>& finfos = index_data.finfos_;
      for (uint32_t i = 0; (TFS_SUCCESS == ret) && (i < finfos.size()); i++)
      {
        if (finfos[i].offset_ < marshalling_len)
        {
          continue;  // we need find updated files
        }

        TBSYS_LOG(DEBUG, "recover updated file. blockid: %"PRI64_PREFIX"u, fileid: "
            "%"PRI64_PREFIX"u, offset: %d", block_id, finfos[i].id_, finfos[i].offset_);

        updated = true;
        int32_t offset = FILEINFO_EXT_SIZE;  // every file has a header, ignore it
        int32_t length = finfos[i].size_ - offset; // filesize should be limited
        char *data = new (std::nothrow) char[length];
        assert(NULL != data);
        ret = get_data_helper().read_file(family_members_[src].server_,
            family_members_[src].block_, block_id,
            finfos[i].id_, data, length, offset, READ_DATA_OPTION_FLAG_FORCE);
        if (TFS_SUCCESS == ret)
        {
          ret = get_data_helper().write_file(family_members_[dest].server_,
              family_members_[dest].block_, block_id,
              finfos[i].id_, data, length, finfos[i].status_, true);  // write to a temp block
        }
        tbsys::gDeleteA(data);
      }

      if ((TFS_SUCCESS == ret) && updated)
      {
        // we just need update header here
        // because after recover updateed files, block version are changed
        // so here we set partial flag true
        IndexDataV2 only_header;
        only_header.header_ = index_data.header_;
        ret = get_data_helper().write_index(family_members_[dest].server_,
            family_members_[dest].block_, family_members_[dest].block_, only_header, true, true);
      }

      return ret;
    }

    int ReinstateTask::report_to_ns(const int status)
    {
      int ret = TFS_SUCCESS;
      ECReinstateCommitMessage cmit_msg;
      cmit_msg.set_seqno(seqno_);
      cmit_msg.set_status(status);
      cmit_msg.set_family_id(family_id_);
      ret = cmit_msg.set_family_member_info(family_members_, family_aid_info_);
      if (TFS_SUCCESS == ret)
      {
        ret = cmit_msg.set_reinstate_block_info(block_infos_, reinstate_num_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = get_data_helper().send_simple_request(source_id_, &cmit_msg);
      }

      TBSYS_LOG(INFO, "reinstate report to ns. seqno: %"PRI64_PREFIX"d, status: %d, source: %s, ret: %d",
          seqno_, status, tbsys::CNetUtil::addrToString(source_id_).c_str(), ret);

      return ret;
    }

    DissolveTask::DissolveTask(DataService& service, const int64_t seqno,
          const uint64_t source_id, const int expire_time, const int64_t family_id) :
      MarshallingTask(service, seqno, source_id, expire_time, family_id)
    {
      type_ = PLAN_TYPE_EC_DISSOLVE;
    }

    DissolveTask::~DissolveTask()
    {

    }

    bool DissolveTask::is_completed() const
    {
      int ret = true;
      for (uint32_t i = 0; i < result_.size(); i++)
      {
        if (PLAN_STATUS_TIMEOUT == result_[i].second)
        {
          ret = false;
          break;
        }
      }
      return ret;
    }

    int DissolveTask::handle()
    {
      int ret = TFS_SUCCESS;
      int32_t data_num = GET_DATA_MEMBER_NUM(family_aid_info_) / 2;

      // initialize stutus
      for (int32_t i = 0; i < data_num; i++)
      {
        if (FAMILY_MEMBER_STATUS_NORMAL == family_members_[i].status_)
        {
          result_.push_back(make_pair(family_members_[i].server_, PLAN_STATUS_TIMEOUT));
        }
      }

      // no data block need to replicate
      if (0 == result_.size())
      {
        ret = report_to_ns(PLAN_STATUS_END);
      }
      else
      {
        ret = do_dissolve();
      }

      return ret;
    }

    int DissolveTask::do_dissolve()
    {
      int ret = replicate_data_blocks();
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "Run task %s, ret: %d", dump().c_str(), ret);
      }
      return ret;
    }

    int DissolveTask::handle_complete(BasePacket* packet)
    {
      int ret = TFS_SUCCESS;
      if (RESP_DS_REPLICATE_BLOCK_MESSAGE == packet->getPCode())
      {
        RespDsReplicateBlockMessage* resp_msg =
          dynamic_cast<RespDsReplicateBlockMessage*> (packet);
        int status = resp_msg->get_status();
        uint64_t server = resp_msg->get_ds_id();

        for (uint32_t i = 0; i < result_.size(); i++)
        {
          if (result_[i].first == server)
          {
            result_[i].second = status;
            break;
          }
        }

        resp_msg->reply(new StatusMessage(STATUS_MESSAGE_OK));

        if (is_completed())
        {
          ret = report_to_ns(status);
        }

        TBSYS_LOG(INFO, "handle complete dissolve task. "
            "seqno: %"PRI64_PREFIX"d, server: %s, status: %d, ret: %d\n",
            seqno_, tbsys::CNetUtil::addrToString(resp_msg->get_ds_id()).c_str(), status, ret);
      }
      return ret;
    }

    int DissolveTask::report_to_ns(const int status)
    {
      int final_status = status;
      if (PLAN_STATUS_TIMEOUT != final_status) // maybe expired by task manager
      {
        final_status = PLAN_STATUS_END;
        for (uint32_t i = 0; i < result_.size(); i++)
        {
          if (PLAN_STATUS_END != result_[i].second)
          {
            final_status = PLAN_STATUS_FAILURE;
            break;
          }
        }
      }

      ECDissolveCommitMessage cmit_msg;
      cmit_msg.set_seqno(seqno_);
      cmit_msg.set_status(final_status);
      cmit_msg.set_family_id(family_id_);
      int ret = cmit_msg.set_family_member_info(family_members_, family_aid_info_);
      if (TFS_SUCCESS == ret)
      {
        ret = get_data_helper().send_simple_request(source_id_, &cmit_msg);
      }

      if ((PLAN_STATUS_END == final_status) && (TFS_SUCCESS == ret))
      {
        if (TFS_SUCCESS == ret)
        {
          // success, do clear work, ignore return value
          clear_family_id();
          delete_parity_blocks();
        }
      }

      TBSYS_LOG(INFO, "dissolve report to ns. seqno: %"PRI64_PREFIX"d, status: %d, source: %s, ret: %d",
          seqno_, final_status, tbsys::CNetUtil::addrToString(source_id_).c_str(), ret);

      return ret;
    }

    int DissolveTask::replicate_data_blocks()
    {
      int ret = TFS_SUCCESS;
      int32_t data_num = GET_DATA_MEMBER_NUM(family_aid_info_) / 2;
      int32_t check_num = GET_CHECK_MEMBER_NUM(family_aid_info_) / 2;
      int32_t total_num = data_num + check_num;

      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      for (int32_t i = 0; (TFS_SUCCESS == ret) && (i < data_num); i++)
      {
        if (FAMILY_MEMBER_STATUS_NORMAL != family_members_[i].status_)
        {
          continue;  // lost block, can't to replicate
        }

        DsReplicateBlockMessage repl_msg;
        ReplBlock repl_block;
        repl_block.block_id_ = family_members_[i].block_;
        repl_block.source_id_[0] = family_members_[i].server_;
        repl_block.source_num_ = 1;  // when dissolve happens, there will be only one source
        repl_block.destination_id_ = family_members_[i+total_num].server_;
        repl_block.is_move_ = REPLICATE_BLOCK_MOVE_FLAG_NO;

        repl_msg.set_seqno(seqno_);
        repl_msg.set_expire_time(expire_time_);
        repl_msg.set_source_id(ds_info.information_.id_);
        repl_msg.set_repl_info(repl_block);
        ret = get_data_helper().send_simple_request(family_members_[i].server_, &repl_msg);

        TBSYS_LOG(DEBUG, "task seqno(%"PRI64_PREFIX"d) request %s to replicate, ret: %d",
            seqno_, tbsys::CNetUtil::addrToString(family_members_[i].server_).c_str(), ret);

      }

      return ret;
    }

    int DissolveTask::clear_family_id()
    {
      int ret = TFS_SUCCESS;
      int32_t data_num = GET_DATA_MEMBER_NUM(family_aid_info_) / 2;
      int32_t check_num = GET_CHECK_MEMBER_NUM(family_aid_info_) / 2;
      int32_t total_num = data_num + check_num;

      for (int32_t i = 0; (TFS_SUCCESS == ret) && (i < data_num); i++)
      {
        if (FAMILY_MEMBER_STATUS_NORMAL != family_members_[i].status_)
        {
          continue;  // lost block, needn't clear
        }

        // ignore return value
        // if fail, family id will be cleared in next report
        ECMeta ec_meta;
        ec_meta.family_id_ = INVALID_FAMILY_ID;
        get_data_helper().commit_ec_meta(family_members_[i].server_,
            family_members_[i].block_, ec_meta, SWITCH_BLOCK_NO);
        get_data_helper().commit_ec_meta(family_members_[i+total_num].server_,
            family_members_[i+total_num].block_, ec_meta, SWITCH_BLOCK_NO);

        TBSYS_LOG(DEBUG, "task seqno(%"PRI64_PREFIX"d) request %s to clear family id, ret: %d",
            seqno_, tbsys::CNetUtil::addrToString(family_members_[i].server_).c_str(), ret);
      }

      return ret;
    }

    int DissolveTask::delete_parity_blocks()
    {
      int ret = TFS_SUCCESS;
      const int32_t data_num = GET_DATA_MEMBER_NUM(family_aid_info_) / 2;
      const int32_t check_num = GET_CHECK_MEMBER_NUM(family_aid_info_) / 2;

      for (int32_t i = data_num; i < data_num + check_num; i++)
      {
        if (FAMILY_MEMBER_STATUS_NORMAL != family_members_[i].status_)
        {
          continue;  // lost block, no need to delete
        }
        ret = get_data_helper().delete_remote_block(family_members_[i].server_,
            family_members_[i].block_);

        TBSYS_LOG(DEBUG, "task seqno(%"PRI64_PREFIX"d) request %s to delete, ret: %d",
            seqno_, tbsys::CNetUtil::addrToString(family_members_[i].server_).c_str(), ret);

      }

      return ret;
    }

    ResolveVersionConflictTask::ResolveVersionConflictTask(DataService& service, const int64_t seqno,
        const uint64_t source_id, const int32_t expire_time, const uint32_t block_id):
      Task(service, seqno, source_id, expire_time)
    {
      type_ = PLAN_TYPE_RESOLVE_VERSION_CONFLICT;
      block_id_ = block_id;
      size_  = 0;
    }

    ResolveVersionConflictTask::~ResolveVersionConflictTask()
    {

    }

    int ResolveVersionConflictTask::handle()
    {
      int ret = do_resolve();
      int status = translate_status(ret);
      return report_to_ns(status);
    }

    int ResolveVersionConflictTask::report_to_ns(const int32_t status)
    {
      UNUSED(status);
      ResolveBlockVersionConflictMessage req_msg;
      req_msg.set_seqno(get_seqno());
      int ret = req_msg.set_members(members_, size_);
      if (TFS_SUCCESS == ret)
      {
        tbnet::Packet* ret_msg = NULL;
        NewClient* client = NewClientManager::get_instance().create_client();
        ret = (NULL != client) ? TFS_SUCCESS : EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = send_msg_to_server(source_id_, client, &req_msg, ret_msg);
          if (TFS_SUCCESS == ret)
          {
            if (RSP_RESOLVE_BLOCK_VERSION_CONFLICT_MESSAGE == ret_msg->getPCode())
            {
              ResolveBlockVersionConflictResponseMessage* msg =
                dynamic_cast<ResolveBlockVersionConflictResponseMessage*>(ret_msg);
              ret = msg->get_status();
            }
            else
            {
              ret = EXIT_RESOLVE_BLOCK_VERSION_CONFLICT_ERROR;
            }
            NewClientManager::get_instance().destroy_client(client);
          }
        }
      }

      return ret;
    }

    string ResolveVersionConflictTask::dump() const
    {
      const char* delim = ", ";
      std::stringstream tmp_stream;
      tmp_stream << Task::dump();
      tmp_stream << "block id: " << block_id_ << delim;
      for (int32_t i = 0; i < size_; i++)
      {
        tmp_stream << "server: " << tbsys::CNetUtil::addrToString(servers_[i]) << delim;
      }
      return tmp_stream.str();
    }

    int ResolveVersionConflictTask::set_servers(const uint64_t* servers, const int32_t size)
    {
      int ret = NULL != servers && size > 0 && size <= MAX_REPLICATION_NUM ?
        TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        size_ = size;
        for (int32_t index = 0; index < size; index++)
        {
          servers_[index] = servers[index];
          members_[index].first = servers[index];
          members_[index].second.block_id_ = block_id_;
          members_[index].second.version_ = -1;
        }
      }
      return ret;
    }

    int ResolveVersionConflictTask::do_resolve()
    {
      // get every replica's block info
      for (int32_t index = 0; index < size_; index++)
      {
        get_data_helper().get_block_info(servers_[index],
            block_id_, members_[index].second);
      }
      return TFS_SUCCESS;
    }

  }
}
