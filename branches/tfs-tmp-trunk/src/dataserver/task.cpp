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
      service_(service), seqno_(seqno), source_id_(source_id), expire_time_(expire_time)
    {
      start_time_ = common::Func::get_monotonic_time();
      task_from_ds_ = false;
    }

    Task::~Task()
    {
    }

    inline DataHelper& Task::data_helper()
    {
      return service_.data_helper();
    }

    inline BlockManager& Task::block_manager()
    {
      return service_.block_manager();
    }

    int Task::send_simple_request(uint64_t server_id, common::BasePacket* message)
    {
      int32_t status = TFS_ERROR;
      int ret = send_msg_to_server(server_id, message, status);
      return (ret < 0) ? ret : status;
    }

    string Task::dump() const
    {
      std::stringstream tmp_stream;
      std::string type;
      const char* delim = ", ";

      switch (type_)
      {
        case  PLAN_TYPE_REPLICATE:
          type = "replicate";
          break;
        case  PLAN_TYPE_COMPACT:
          type = "compact";
          break;
        case PLAN_TYPE_EC_MARSHALLING:
          type = "marshalling";
          break;
        case PLAN_TYPE_EC_REINSTATE:
          type = "reinstate";
          break;
        case PLAN_TYPE_EC_DISSOLVE:
          type = "dissolve";
          break;
        default:
          type = "unknown";
          break;
      }

      tmp_stream << "dump " << type << " task. ";
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
            ret = false;
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
        ret = do_compact(block_id_);
        int status = translate_status(ret);
        ret = report_to_ds(status);
      }
      else
      {
        // initialize status
        for (uint32_t i = 0; i < servers_.size(); i++)
        {
          result_.push_back(std::make_pair(servers_[i], common::PLAN_STATUS_TIMEOUT));
        }
        ret = compact_peer_blocks();
      }

      TBSYS_LOG(INFO, "handle compact task, seqno: %"PRI64_PREFIX"d, ret: %d\n", seqno_, ret );

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

    void CompactTask::add_response(const uint64_t server, const int status, const common::BlockInfo& info)
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

    int CompactTask::do_compact(const uint32_t block_id)
    {
      UNUSED(block_id);
      return 0;
    }

    int CompactTask::report_to_ns(const int status)
    {
      UNUSED(status);
      int ret = TFS_SUCCESS;
      DsCommitCompactBlockCompleteToNsMessage cmit_cpt_msg;
      cmit_cpt_msg.set_seqno(seqno_);
      cmit_cpt_msg.set_block_info(info_);
      cmit_cpt_msg.set_result(result_);
      ret = send_simple_request(source_id_, &cmit_cpt_msg);

      TBSYS_LOG(INFO, "compact report to ns. seqno: %"PRI64_PREFIX"d, blockid: %u, status: %d, source: %s, ret: %d",
          seqno_, block_id_, status, tbsys::CNetUtil::addrToString(source_id_).c_str(), ret);

      return ret;
    }

    int CompactTask::report_to_ds(const int status)
    {
      RespDsCompactBlockMessage resp_cpt_msg;
      resp_cpt_msg.set_seqno(seqno_);
      resp_cpt_msg.set_ds_id(service_.get_ds_ipport());
      resp_cpt_msg.set_status(status);

      if (PLAN_STATUS_END == status)
      {
        /*
        LogicBlock* LogicBlock = BlockFileManager::get_instance()->get_logic_block(block_id_);
        if (NULL == LogicBlock)
        {
          resp_cpt_msg.set_status(PLAN_STATUS_FAILURE);
          TBSYS_LOG(ERROR, "get block failed. blockid: %u\n", block_id_);
        }
        else
        {
          BlockInfo* blk = LogicBlock->get_block_info();
          resp_cpt_msg.set_block_info(*blk);
        }
        */
      }

      TBSYS_LOG(INFO, "compact report to ds. seqno: %"PRI64_PREFIX"d, blockid: %u, status: %d, source: %s",
        seqno_, block_id_, status, tbsys::CNetUtil::addrToString(source_id_).c_str());

      NewClient* client = NewClientManager::get_instance().create_client();
      post_msg_to_server(source_id_, client, &resp_cpt_msg, Task::ds_task_callback);
      // NewClientManager::get_instance().destroy_client(client);
      return TFS_SUCCESS;
    }

    int CompactTask::compact_peer_blocks()
    {
      int ret = TFS_SUCCESS;
      for (uint32_t i = 0; (TFS_SUCCESS == ret) && (i < servers_.size()); i++)
      {
        DsCompactBlockMessage req_cpt_msg;
        req_cpt_msg.set_seqno(seqno_);
        req_cpt_msg.set_block_id(block_id_);
        req_cpt_msg.set_source_id(service_.get_ds_ipport());
        int32_t status = TFS_ERROR;
        ret = send_msg_to_server(source_id_, &req_cpt_msg, status);
        ret = (ret < 0) ? ret : status;

        TBSYS_LOG(DEBUG, "task seqno(%"PRI64_PREFIX"d) request %s to compact, ret: %d",
            seqno_, tbsys::CNetUtil::addrToString(servers_[i]).c_str(), ret);
      }
      return ret;
    }

    /*
    int CompactTask::real_compact(LogicBlock* src, LogicBlock* dest)
    {
      assert(NULL != src && NULL != dest);

      BlockInfo dest_blk;
      BlockInfo* src_blk = src->get_block_info();
      dest_blk.block_id_ = src_blk->block_id_;
      dest_blk.seq_no_ = src_blk->seq_no_;
      dest_blk.version_ = src_blk->version_;
      dest_blk.file_count_ = 0;
      dest_blk.size_ = 0;
      dest_blk.del_file_count_ = 0;
      dest_blk.del_size_ = 0;

      dest->set_last_update(time(NULL));
      TBSYS_LOG(DEBUG, "compact block set last update. blockid: %u\n", dest->get_logic_block_id());

      char* dest_buf = new char[MAX_COMPACT_READ_SIZE];
      int32_t write_offset = 0, data_len = 0;
      int32_t w_file_offset = 0;
      RawMetaVec dest_metas;
      FileIterator* fit = new FileIterator(src);

      int ret = TFS_SUCCESS;
      while (fit->has_next())
      {
        ret = fit->next();
        if (TFS_SUCCESS != ret)
        {
          tbsys::gDeleteA(dest_buf);
          tbsys::gDelete(fit);
          return ret;
        }

        const FileInfo* pfinfo = fit->current_file_info();
        if (pfinfo->flag_ & (FI_DELETED | FI_INVALID))
        {
          continue;
        }

        FileInfo dfinfo = *pfinfo;
        dfinfo.offset_ = w_file_offset;
        // the size returned by FileIterator.current_file_info->size is
        // the size of file content!!!
        dfinfo.size_ = pfinfo->size_ + sizeof(FileInfo);
        dfinfo.usize_ = pfinfo->size_ + sizeof(FileInfo);
        w_file_offset += dfinfo.size_;

        dest_blk.file_count_++;
        dest_blk.size_ += dfinfo.size_;

        RawMeta tmp_meta;
        tmp_meta.set_file_id(dfinfo.id_);
        tmp_meta.set_size(dfinfo.size_);
        tmp_meta.set_offset(dfinfo.offset_);
        dest_metas.push_back(tmp_meta);

        // need flush write buffer
        if ((0 != data_len) && (fit->is_big_file() || data_len + dfinfo.size_ > MAX_COMPACT_READ_SIZE))
        {
          TBSYS_LOG(DEBUG, "write one, blockid: %u, write offset: %d\n", dest->get_logic_block_id(),
              write_offset);
          ret = dest->write_raw_data(dest_buf, data_len, write_offset);
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "write raw data fail, blockid: %u, offset %d, readinglen: %d, ret :%d",
                dest->get_logic_block_id(), write_offset, data_len, ret);
            tbsys::gDeleteA(dest_buf);
            tbsys::gDelete(fit);
            return ret;
          }
          write_offset += data_len;
          data_len = 0;
        }

        if (fit->is_big_file())
        {
          ret = write_big_file(src, dest, *pfinfo, dfinfo, write_offset);
          write_offset += dfinfo.size_;
        }
        else
        {
          memcpy(dest_buf + data_len, &dfinfo, sizeof(FileInfo));
          int left_len = MAX_COMPACT_READ_SIZE - data_len;
          ret = fit->read_buffer(dest_buf + data_len + sizeof(FileInfo), left_len);
          data_len += dfinfo.size_;
        }
        if (TFS_SUCCESS != ret)
        {
          tbsys::gDeleteA(dest_buf);
          tbsys::gDelete(fit);
          return ret;
        }

      } // end of iterate

      if (0 != data_len) // flush the last buffer
      {
        TBSYS_LOG(DEBUG, "write one, blockid: %u, write offset: %d\n", dest->get_logic_block_id(), write_offset);
        ret = dest->write_raw_data(dest_buf, data_len, write_offset);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "write raw data fail, blockid: %u, offset %d, readinglen: %d, ret :%d",
              dest->get_logic_block_id(), write_offset, data_len, ret);
          tbsys::gDeleteA(dest_buf);
          tbsys::gDelete(fit);
          return ret;
        }
      }

      tbsys::gDeleteA(dest_buf);
      tbsys::gDelete(fit);
      TBSYS_LOG(DEBUG, "compact write complete. blockid: %u\n", dest->get_logic_block_id());

      ret = dest->batch_write_meta(&dest_blk, &dest_metas, VERSION_INC_STEP_DEFAULT);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "compact write segment meta failed. blockid: %u, meta size %zd\n", dest->get_logic_block_id(),
            dest_metas.size());
        return ret;
      }

      TBSYS_LOG(DEBUG, "compact set dirty flag. blockid: %u\n", dest->get_logic_block_id());
      ret = dest->set_block_dirty_type(C_DATA_CLEAN);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "compact blockid: %u set dirty flag fail. ret: %d\n", dest->get_logic_block_id(), ret);
        return ret;
      }

      return TFS_SUCCESS;
    }

    int CompactTask::write_big_file(LogicBlock* src, LogicBlock* dest, const FileInfo& src_info,
        const FileInfo& dest_info, int32_t woffset)
    {
      int32_t rsize = src_info.size_;
      int32_t roffset = src_info.offset_ + sizeof(FileInfo);
      char* buf = new char[MAX_COMPACT_READ_SIZE];
      int32_t read_len = 0;
      int ret = TFS_SUCCESS;

      memcpy(buf, &dest_info, sizeof(FileInfo));
      int32_t data_len = sizeof(FileInfo);
      while (read_len < rsize)
      {
        int32_t cur_read = MAX_COMPACT_READ_SIZE - data_len;
        if (cur_read > rsize - read_len)
          cur_read = rsize - read_len;
        ret = src->read_raw_data(buf + data_len, cur_read, roffset);
        if (TFS_SUCCESS != ret)
          break;
        data_len += cur_read;
        read_len += cur_read;
        roffset += cur_read;

        ret = dest->write_raw_data(buf, data_len, woffset);
        if (TFS_SUCCESS != ret)
          break;
        woffset += data_len;

        data_len = 0;
      }

      tbsys::gDeleteA(buf);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "write big file error, blockid: %u, ret: %d", dest->get_logic_block_id(), ret);
      }

      return ret;
    }
    */

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

      TBSYS_LOG(INFO, "handle replicate task, seqno: %"PRI64_PREFIX"d, ret: %d\n", seqno_, ret );

      return ret;
    }

    string ReplicateTask::dump() const
    {
      const char* delim = ", ";
      std::stringstream tmp_stream;
      tmp_stream << Task::dump();
      tmp_stream << "source id: " << tbsys::CNetUtil::addrToString(repl_info_.source_id_) << delim;
      tmp_stream << "dest id: " << tbsys::CNetUtil::addrToString(repl_info_.destination_id_) << delim;
      tmp_stream << "block id: " << repl_info_.block_id_ << delim;
      tmp_stream << "move flag: " << (0 == repl_info_.is_move_? "no": "yes");
      return tmp_stream.str();
    }

    int ReplicateTask::report_to_ns(const int status)
    {
      ReplicateBlockMessage req_rb_msg;
      int ret = TFS_ERROR;

      req_rb_msg.set_seqno(seqno_);
      req_rb_msg.set_repl_block(&repl_info_);
      req_rb_msg.set_status(status);

      bool need_remove = false;
      NewClient* client = NewClientManager::get_instance().create_client();
      if (NULL != client)
      {
        tbnet::Packet* rsp_msg = NULL;
        if (TFS_SUCCESS == send_msg_to_server(source_id_, client, &req_rb_msg, rsp_msg))
        {
          if (STATUS_MESSAGE != rsp_msg->getPCode())
          {
            TBSYS_LOG(ERROR, "unknow packet pcode: %d", rsp_msg->getPCode());
          }
          else
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
              TBSYS_LOG(ERROR, "send repl block complete info: %s\n", sm->get_error());
            }
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "send repl block complete info to ns error, ret: %d", ret);
        }
        NewClientManager::get_instance().destroy_client(client);
      }
      else
      {
        TBSYS_LOG(ERROR, "create client error");
      }

      if (need_remove)
      {
        int rm_ret = block_manager().del_block(repl_info_.block_id_);
        TBSYS_LOG(INFO, "send repl block complete info: del blockid: %u, ret: %d\n",
            repl_info_.block_id_, rm_ret);
      }

      TBSYS_LOG(INFO,
          "replicate report to ns. seqno: %"PRI64_PREFIX"d, blockid: %u, status: %d, source: %s, ret: %d",
          seqno_, repl_info_.block_id_, status, tbsys::CNetUtil::addrToString(source_id_).c_str(), ret);

      return ret;
    }

    int ReplicateTask::report_to_ds(const int status)
    {
      RespDsReplicateBlockMessage resp_repl_msg;
      resp_repl_msg.set_seqno(seqno_);
      resp_repl_msg.set_ds_id(service_.get_ds_ipport());
      resp_repl_msg.set_status(status);

      TBSYS_LOG(INFO, "replicate report to ds. seqno: %"PRI64_PREFIX"d, blockid: %u, status: %d, source: %s",
          seqno_, repl_info_.block_id_, status, tbsys::CNetUtil::addrToString(source_id_).c_str());

      NewClient* client = NewClientManager::get_instance().create_client();
      post_msg_to_server(source_id_, client, &resp_repl_msg, Task::ds_task_callback);
      // NewClientManager::get_instance().destroy_client(client);
      return TFS_SUCCESS;
    }

    int ReplicateTask::replicate_data(const int32_t block_size)
    {
      int32_t length = 0;
      int32_t offset = 0;
      uint64_t block_id = repl_info_.block_id_;
      uint64_t source_id = repl_info_.source_id_;
      uint64_t dest_id = repl_info_.destination_id_;
      char buffer[MAX_READ_SIZE];  // just use 1M stack space
      int ret = TFS_SUCCESS;
      while ((TFS_SUCCESS == ret) && (offset < block_size))
      {
        length = std::min(block_size - offset, MAX_READ_SIZE);
        ret = data_helper().read_raw_data(source_id, block_id, buffer, length, offset);
        if (TFS_SUCCESS == ret)
        {
          ret = data_helper().write_raw_data(dest_id, block_id, buffer, length, offset);
        }

        if (TFS_SUCCESS == ret)
        {
          offset += length;
        }
      }

      return ret;
    }

    int ReplicateTask::replicate_index()
    {
      uint64_t block_id = repl_info_.block_id_;
      uint64_t dest_id = repl_info_.destination_id_;

      // TODO: update block version first

      uint64_t helper[MAX_DATA_MEMBER_NUM];
      ArrayHelper<uint64_t> attach_blocks(MAX_DATA_MEMBER_NUM, helper);
      int ret = block_manager().get_attach_blocks(attach_blocks, block_id);
      if (TFS_SUCCESS == ret)
      {
        for (int i = 0; i < attach_blocks.get_array_index(); i++)
        {
          IndexDataV2 index_data;
          ret = data_helper().read_index(service_.get_ds_ipport(),
              block_id, *attach_blocks.at(i), index_data);
          if (TFS_SUCCESS == ret)
          {
            ret = data_helper().write_index(dest_id,
                block_id, *attach_blocks.at(i), index_data);
          }
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

      int ret = block_manager().get_family_id(family_id, block_id);
      if (TFS_SUCCESS == ret)
      {
        ret = block_manager().get_used_offset(block_size, block_id);
        if ((TFS_SUCCESS == ret) && IS_VERFIFY_BLOCK(block_id))
        {
          // index_num = block_manager().get_index_num();
        }
      }

      // new remote temp block
      if (TFS_SUCCESS == ret)
      {
        ret = data_helper().new_remote_block(dest_id,
            block_id, true, family_id, index_num);
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

      // commit block
      if (TFS_SUCCESS == ret)
      {
        ECMeta ec_meta;
        ec_meta.family_id_ = -1; // not update family id
        ret = data_helper().commit_ec_meta(dest_id, block_id, ec_meta, SWITCH_BLOCK_YES);
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
      tbsys::gDelete(family_members_);
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
      const char* nf = "\n";
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
          // read part of buffer, need memset first to ensure data zero
          if (offset + length > ec_metas[i].used_offset_)
          {
            memset(data[i], 0, length * sizeof(char));
          }

          if (offset < ec_metas[i].used_offset_)
          {
            ret = data_helper().read_raw_data(family_members_[i].server_,
                family_members_[i].block_, data[i], length, offset);
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
          ret = data_helper().write_raw_data(family_members_[i].server_,
              family_members_[i].block_, data[i], length, offset);
        }

        // one turn success, update offset
        if (TFS_SUCCESS == ret)
        {
          offset += length;
        }
      }

      for (int32_t i = 0; i < member_num; i++)
      {
        tbsys::gDelete(data[i]);
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
        ret = data_helper().read_index(family_members_[i].server_,
            family_members_[i].block_, family_members_[i].block_, index_data);
        for (int j = data_num; (TFS_SUCCESS == ret)  && (j < member_num); j++)
        {
          ret = data_helper().write_index(family_members_[j].server_,
              family_members_[j].block_, family_members_[i].block_, index_data);
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
      for (int i = 0; (TFS_SUCCESS == ret) && (i < data_num); i++)
      {
        ret = data_helper().query_ec_meta(family_members_[i].server_,
            family_members_[i].block_, ec_metas[i]);
      }

      // create parity block
      for (int i = data_num; (TFS_SUCCESS == ret) && (i < member_num); i++)
      {
        ret = data_helper().new_remote_block(family_members_[i].server_,
            family_members_[i].block_, true, family_id_, data_num);
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
          ec_metas[i].mars_offset_ = ec_metas[i].used_offset_;
          ret = data_helper().commit_ec_meta(family_members_[i].server_,
            family_members_[i].block_, ec_metas[i]);
        }

        ECMeta ec_meta;
        ec_meta.family_id_ = family_id_;
        ec_meta.mars_offset_ = marshalling_len;
        for (int i = data_num; (TFS_SUCCESS == ret) && (i < member_num); i++)
        {
          ret = data_helper().commit_ec_meta(family_members_[i].server_,
            family_members_[i].block_, ec_meta, SWITCH_BLOCK_YES);
        }
      }

      return ret;
    }

    ReinstateTask::ReinstateTask(DataService& service, const int64_t seqno,
        const uint64_t source_id, const int32_t expire_time, const int64_t family_id) :
      MarshallingTask(service, seqno, source_id, expire_time, family_id)
    {
      type_ = PLAN_TYPE_EC_REINSTATE;
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
          const int32_t DATA_NUM = GET_DATA_MEMBER_NUM(family_aid_info);
            GET_CHECK_MEMBER_NUM(family_aid_info);
          memcpy(erased_, erased, DATA_NUM * sizeof(int));
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
      for (int i = 0; (TFS_SUCCESS == ret) && (i < member_num); i++)
      {
        if (ErasureCode::NODE_ALIVE != family_members_[i].status_)
        {
          continue;  // only query alive nodes
        }
        ret = data_helper().query_ec_meta(family_members_[i].server_,
            family_members_[i].block_, ec_metas[i]);
      }

      // create lost block
      for (int i = 0; (TFS_SUCCESS == ret) && (i < member_num); i++)
      {
        if (ErasureCode::NODE_DEAD != family_members_[i].status_)
        {
          continue;  // only query dead nodes
        }

        if (i < data_num)  // create data block
        {
          ret = data_helper().new_remote_block(family_members_[i].server_,
            family_members_[i].block_, true);
        }
        else // create parity block
        {
          ret = data_helper().new_remote_block(family_members_[i].server_,
              family_members_[i].block_, true, family_id_, data_num);
        }
      }

      // decode data & write to recovery target
      if (TFS_SUCCESS == ret)
      {
        ret = decode_data(ec_metas, marshalling_len);
      }

      // recovery data index
      if (TFS_SUCCESS == ret)
      {
        ret = recovery_data_index(marshalling_len);
      }

      // recovery check index
      if (TFS_SUCCESS == ret)
      {
        ret = recovery_check_index(marshalling_len);
      }

      // commit family id & other infos
      if (TFS_SUCCESS == ret)
      {
        ECMeta ec_meta;
        ec_meta.family_id_ = family_id_;
        ec_meta.mars_offset_ = marshalling_len;
        ec_meta.used_offset_ = 0;  // denotes not update used_offset

        for (int i = 0; (TFS_SUCCESS == ret) && (i < member_num); i++)
        {
          if (ErasureCode::NODE_DEAD != family_members_[i].status_)
          {
            continue;  // only query alive nodes
          }
          ret = data_helper().commit_ec_meta(family_members_[i].server_,
            family_members_[i].block_, ec_meta, true);
        }
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
      char* data[member_num];
      memset(data, 0, member_num * sizeof(char*));

      // get the element with max marshalling_offset
      ECMeta* max_ele = max_element(ec_metas, ec_metas + member_num, ECMeta::m_compare);
      marshalling_len = max_ele->mars_offset_;

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

          // read part of buffer, need memset first to ensure data zero
          if (length + offset > ec_metas[i].mars_offset_)
          {
            memset(data[i], 0, length);
          }

          if (offset < ec_metas[i].mars_offset_)
          {
            ret = data_helper().read_raw_data(family_members_[i].server_,
                family_members_[i].block_, data[i], length, offset);
          }
        }

        // decode data to buffer
        if (TFS_SUCCESS == ret)
        {
          ret = decoder.decode(length);
        }

        // recovery data
        for (int32_t i = 0; (TFS_SUCCESS == ret) && (i < member_num); i++)
        {
          if (ErasureCode::NODE_DEAD != erased_[i])
          {
            continue;  // we want to find dead server
          }

          ret = data_helper().write_raw_data(family_members_[i].server_,
              family_members_[i].block_, data[i], length, offset);
        }

        // all success, update offset
        if (TFS_SUCCESS == ret)
        {
          offset += length;
        }
      }

      return ret;
    }

    int ReinstateTask::recovery_data_index(const int32_t marshalling_len)
    {
      int ret = TFS_SUCCESS;
      const int32_t data_num = GET_DATA_MEMBER_NUM(family_aid_info_);
      const int32_t check_num = GET_CHECK_MEMBER_NUM(family_aid_info_);
      const int32_t member_num = data_num + check_num;

      int32_t src = -1;
      for (int i = 0; (TFS_SUCCESS == ret) && (i < data_num); i++)
      {
        if (ErasureCode::NODE_DEAD != erased_[i])
        {
          continue; // we need find dead node to recovery
        }

        IndexDataV2 index_data;
        for (int j = data_num; j < member_num; j++)
        {
          if (ErasureCode::NODE_ALIVE != erased_[i])
          {
            continue; // we need find alive node
          }

          ret = data_helper().read_index(family_members_[j].server_,
              family_members_[j].block_, family_members_[i].block_, index_data);
          if (TFS_SUCCESS == ret)
          {
            src = j;
            break; // try all alive check nodes until success
          }
        }

        if (TFS_SUCCESS == ret)
        {
          ret = data_helper().write_index(family_members_[i].server_,
              family_members_[i].block_, family_members_[i].block_, index_data);
        }

        if (TFS_SUCCESS == ret)
        {
          ret = replicate_files(index_data, marshalling_len,
              family_members_[i].block_, src, i);
          if ((TFS_SUCCESS == ret) && index_data.finfos_.size() > 0)
          {
            // we just need update header here
            index_data.finfos_.clear();
            ret = data_helper().write_index(family_members_[i].server_,
                family_members_[i].block_, family_members_[i].block_, index_data);
          }
        }
      }

      return ret;
    }

    int ReinstateTask::recovery_check_index(const int32_t marshalling_len)
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
          pi = i;
          break;
        }
      }

      if (pi > 0)  // recovery parity index from other parity block
      {
        ret = recovery_check_index_from_cnodes(marshalling_len, pi);
      }
      else
      {
        ret = recovery_check_index_from_dnodes(marshalling_len);
      }

      return ret;
    }


    int ReinstateTask::recovery_check_index_from_dnodes(const int32_t marshalling_len)
    {
      int ret = TFS_SUCCESS;
      const int32_t data_num = GET_DATA_MEMBER_NUM(family_aid_info_);
      const int32_t check_num = GET_CHECK_MEMBER_NUM(family_aid_info_);
      const int32_t member_num = data_num + check_num;

      for (int i = data_num; (TFS_SUCCESS == ret) && (i < member_num); i++)
      {
        if (ErasureCode::NODE_DEAD != erased_[i])
        {
          continue; // we need find dead node to recovery
        }

        for (int j = 0; (TFS_SUCCESS == ret) && (j < data_num); j++)
        {
          IndexDataV2 index_data;
          ret = data_helper().read_index(family_members_[j].server_,
              family_members_[j].block_, family_members_[j].block_, index_data);
          if (TFS_SUCCESS == ret)
          {
            ret = data_helper().write_index(family_members_[i].server_,
                family_members_[i].block_, family_members_[j].block_, index_data);
          }
          if (TFS_SUCCESS == ret)
          {
            ret = replicate_files(index_data, marshalling_len,
                family_members_[j].block_, j, i);
            if ((TFS_SUCCESS == ret) && index_data.finfos_.size() > 0)
            {
              // we just need update header here
              index_data.finfos_.clear();
              ret = data_helper().write_index(family_members_[i].server_,
                  family_members_[i].block_, family_members_[j].block_, index_data);
            }
          }
        }
      }

      return ret;
    }

    int ReinstateTask::recovery_check_index_from_cnodes(const int32_t marshalling_len,
        const int32_t pi)
    {
      int ret = TFS_SUCCESS;
      const int32_t data_num = GET_DATA_MEMBER_NUM(family_aid_info_);
      const int32_t check_num = GET_CHECK_MEMBER_NUM(family_aid_info_);
      const int32_t member_num = data_num + check_num;

      for (int i = data_num; (TFS_SUCCESS == ret) && (i < member_num); i++)
      {
        if (ErasureCode::NODE_DEAD != erased_[i])
        {
          continue; // we need find dead node to recovery
        }

        for (int j = 0; (TFS_SUCCESS == ret) && (j < data_num); j++)
        {
          IndexDataV2 index_data;
          ret = data_helper().read_index(family_members_[pi].server_,
              family_members_[pi].block_, family_members_[j].block_, index_data);
          if (TFS_SUCCESS == ret)
          {
            ret = data_helper().write_index(family_members_[i].server_,
                family_members_[i].block_, family_members_[j].block_, index_data);
          }
          if (TFS_SUCCESS == ret)
          {
            ret = replicate_files(index_data, marshalling_len,
                family_members_[j].block_, pi, i);
            if ((TFS_SUCCESS == ret) && index_data.finfos_.size() > 0)
            {
              // we just need update header here
              index_data.finfos_.clear();
              ret = data_helper().write_index(family_members_[i].server_,
                  family_members_[i].block_, family_members_[j].block_, index_data);
            }
          }
        }
      }

      return ret;
    }

    int ReinstateTask::replicate_files(const common::IndexDataV2& index_data,
      const int32_t marshalling_len, const uint64_t block_id, const int32_t src, const int32_t dest)
    {
      int ret = TFS_SUCCESS;
      const vector<FileInfoV2>& finfos = index_data.finfos_;
      for (uint32_t i = 0; (TFS_SUCCESS == ret) && (i < finfos.size()); i++)
      {
        if (finfos[i].offset_ < marshalling_len)
        {
          continue;  // we need find updated files
        }

        char *data = new (std::nothrow) char[finfos[i].size_];
        assert(NULL != data);
        int32_t length = finfos[i].size_;
        ret = data_helper().read_file(family_members_[src].server_,
            family_members_[src].block_, block_id,
            finfos[i].id_, data, length);
        if (TFS_SUCCESS == ret)
        {
          ret = data_helper().write_file(family_members_[dest].server_,
              family_members_[dest].block_, block_id,
              finfos[i].id_, data, length);
        }
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
        int32_t status = TFS_ERROR;
        ret = send_msg_to_server(source_id_, &cmit_msg, status);
        ret = (ret < 0) ? ret : status;
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
      int32_t data_num = GET_DATA_MEMBER_NUM(family_aid_info_) / 2;

      // initialize stutus
      for (int32_t i = 0; i < data_num; i++)
      {
        if (FAMILY_MEMBER_STATUS_NORMAL == family_members_[i].status_)
        {
          result_.push_back(make_pair(family_members_[i].server_, PLAN_STATUS_TIMEOUT));
        }
      }

      return do_dissolve();
    }

    int DissolveTask::do_dissolve()
    {
      return replicate_data_blocks();
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
          ret = report_to_ns(status); // status is not used here
          if (TFS_SUCCESS == ret)
          {
            // success, do clear work, ignore return value
            delete_parity_blocks();
          }
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
        ret = send_simple_request(source_id_, &cmit_msg);
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

      for (int32_t i = 0; (TFS_SUCCESS == ret) && (i < data_num); i++)
      {
        if (FAMILY_MEMBER_STATUS_NORMAL != family_members_[i].status_)
        {
          continue;  // lost block, can't to replicate
        }

        DsReplicateBlockMessage repl_msg;
        ReplBlock repl_block;
        memset(&repl_block, 0, sizeof(ReplBlock));
        repl_block.block_id_ = family_members_[i].block_;
        repl_block.source_id_ = family_members_[i].server_;
        repl_block.destination_id_ = family_members_[i+total_num].server_;

        repl_msg.set_seqno(seqno_);
        repl_msg.set_expire_time(expire_time_);
        repl_msg.set_source_id(service_.get_ds_ipport());
        repl_msg.set_repl_info(repl_block);
        ret = send_simple_request(family_members_[i].server_, &repl_msg);

        TBSYS_LOG(DEBUG, "task seqno(%"PRI64_PREFIX"d) request %s to replicate, ret: %d",
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
        ret = data_helper().delete_remote_block(family_members_[i].server_,
            family_members_[i].block_);

        TBSYS_LOG(DEBUG, "task seqno(%"PRI64_PREFIX"d) request %s to delete, ret: %d",
            seqno_, tbsys::CNetUtil::addrToString(family_members_[i].server_).c_str(), ret);

      }

      return ret;
    }
  }
}
