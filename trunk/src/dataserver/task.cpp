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
#include "task_manager.h"
#include "erasure_code.h"

namespace tfs
{
  namespace dataserver
  {
    using namespace common;
    using namespace message;
    using namespace tbutil;
    using namespace std;

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

    int Task::send_simple_request(uint64_t server_id, common::BasePacket* message)
    {
      int ret = TFS_SUCCESS;
      int status = 0;
      ret = send_msg_to_server(server_id, message, status);
      TBSYS_LOG(DEBUG, "send simple request, ret: %d, status: %d", ret, status);
      if (TFS_SUCCESS == ret && STATUS_MESSAGE_OK != status)
      {
        ret = TFS_ERROR;
      }
      return ret;
    }

    int Task::write_raw_data(const uint64_t server_id, const uint32_t block_id,
        const char* data, const int32_t length, const int32_t offset, const RawDataType type)
    {
      int ret = TFS_SUCCESS;
      WriteRawDataMessage req_wrd_msg;
      req_wrd_msg.set_block_id(block_id);
      req_wrd_msg.set_offset(offset);
      req_wrd_msg.set_length(length);
      req_wrd_msg.set_data(data);

      //new block		
      if (0 == offset)
      {
        req_wrd_msg.set_new_block(type);
      }

      ret = send_simple_request(server_id, &req_wrd_msg);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "write raw data to %s fail, blockid: %u, offset: %u, length: %d, ret: %d",
            tbsys::CNetUtil::addrToString(server_id).c_str(), block_id, offset, length, ret);
      }

      TBSYS_LOG(DEBUG, "write raw data to %s, blockid: %u, offset: %u, length: %d",
          tbsys::CNetUtil::addrToString(server_id).c_str(), block_id, offset, length);

      return ret;
    }

    int Task::read_raw_data(const uint64_t server_id, const uint32_t block_id,
      char* data, const int32_t length, const int32_t offset, int32_t& data_file_size)
    {
      int ret = TFS_SUCCESS;
      ReadRawDataMessage req_rrd_msg;
      req_rrd_msg.set_block_id(block_id);
      req_rrd_msg.set_offset(offset);
      req_rrd_msg.set_length(length);

      NewClient* client = NewClientManager::get_instance().create_client();
      if (NULL == client)
      {
        ret = TFS_ERROR;
        TBSYS_LOG(ERROR, "create client error");
      }
      else
      {
        tbnet::Packet* rsp_msg = NULL;
        if (TFS_SUCCESS == send_msg_to_server(server_id, client, &req_rrd_msg, rsp_msg))
        {
          if (rsp_msg->getPCode() == RESP_READ_RAW_DATA_MESSAGE)
          {
            RespReadRawDataMessage* message = dynamic_cast<RespReadRawDataMessage*> (rsp_msg);
            int read_len = message->get_length();
            if (read_len >= 0)
            {
              memcpy(data, message->get_data(), read_len);
              data_file_size = message->get_data_file_size();
            }
            else
            {
              ret = read_len;  // error info stored in length
            }
          }
          else
          {
            ret = TFS_ERROR;
            TBSYS_LOG(ERROR, "read raw data from %s fail, blockid: %u, offset: %u, length: %d",
                tbsys::CNetUtil::addrToString(server_id).c_str(), block_id, offset, length);
          }
        }
        else
        {
          ret = TFS_ERROR;
          TBSYS_LOG(ERROR, "read raw data from %s fail, blockid: %u, offset: %u, length: %d",
              tbsys::CNetUtil::addrToString(server_id).c_str(), block_id, offset, length);
        }
        NewClientManager::get_instance().destroy_client(client);
      }

      TBSYS_LOG(DEBUG, "read raw data from %s, blockid: %u, offset: %u, length: %d, ret: %d",
          tbsys::CNetUtil::addrToString(server_id).c_str(), block_id, offset, length, ret);

      return ret;
    }

    int Task::batch_write_index(const uint64_t server_id, const uint32_t block_id)
    {
      int ret = TFS_SUCCESS;
      RawMetaVec raw_meta_vec;
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        ret = EXIT_NO_LOGICBLOCK_ERROR;
        TBSYS_LOG(ERROR, "block is not exist. blockid: %u\n", block_id);
      }
      else
      {
        ret = logic_block->get_meta_infos(raw_meta_vec);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "get meta info fail, server: %s, blockid: %u, ret: %d",
            tbsys::CNetUtil::addrToString(server_id).c_str(), block_id, ret);
          ret = TFS_ERROR;
        }
        else
        {
          WriteInfoBatchMessage req_wib_msg;
          req_wib_msg.set_block_id(block_id);
          req_wib_msg.set_offset(0);
          req_wib_msg.set_length(raw_meta_vec.size());
          req_wib_msg.set_raw_meta_list(&raw_meta_vec);
          req_wib_msg.set_block_info(logic_block->get_block_info());

          ret = send_simple_request(server_id, &req_wib_msg);
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "write meta info to %s fail, blockid: %u, ret: %d",
                tbsys::CNetUtil::addrToString(server_id).c_str(), block_id, ret);
          }
        }
      }

      TBSYS_LOG(DEBUG, "batch write index. blockid: %u, meta info size: %zd, ret: %d",
          block_id, raw_meta_vec.size(), ret);

      return ret;
    }

    int Task::write_raw_index(const uint64_t server_id, const uint32_t block_id,
        const int64_t family_id, const RawIndexOp index_op, const RawIndexVec& index_vec)
    {
      WriteRawIndexMessage wri_msg;
      wri_msg.set_block_id(block_id);
      wri_msg.set_family_id(family_id);
      wri_msg.set_index_op(index_op);
      wri_msg.set_index_vec(index_vec);

      int ret = TFS_SUCCESS;
      ret = send_simple_request(server_id, &wri_msg);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "write raw index to %s fail, blockid: %u, index_op: %d, ret: %d",
            tbsys::CNetUtil::addrToString(server_id).c_str(), block_id, index_op, ret);
      }

      TBSYS_LOG(DEBUG, "write raw index to %s, blockid: %u, index op: %d, vec size: %zd, ret: %d",
          tbsys::CNetUtil::addrToString(server_id).c_str(), block_id, index_op, index_vec.size(), ret);

      return ret;
    }

    int Task::read_raw_index(const uint64_t server_id, const uint32_t block_id,
        const RawIndexOp index_op, const uint32_t index_id, char* & data, int32_t& length)
    {
      int ret = TFS_SUCCESS;
      ReadRawIndexMessage rri_msg;
      rri_msg.set_block_id(block_id);
      rri_msg.set_index_op(index_op);
      rri_msg.set_index_id(index_id);

      NewClient* client = NewClientManager::get_instance().create_client();
      if (NULL == client)
      {
        ret = TFS_ERROR;
        TBSYS_LOG(ERROR, "create client error");
      }
      else
      {
        tbnet::Packet* rsp_msg = NULL;
        if (TFS_SUCCESS == send_msg_to_server(server_id, client, &rri_msg, rsp_msg))
        {
          if (rsp_msg->getPCode() == RSP_READ_RAW_INDEX_MESSAGE)
          {
            RespReadRawIndexMessage* message = dynamic_cast<RespReadRawIndexMessage*> (rsp_msg);
            length = message->get_length();
            if (length >= 0)
            {
              data = (char*)malloc(length * sizeof(char));
              assert (NULL != data);
              memcpy(data, message->get_data(), length);
            }
            else
            {
              ret = length;
            }
          }
          else
          {
            ret = TFS_ERROR;
            TBSYS_LOG(ERROR, "read raw index from %s fail, blockid: %u, index_op: %d",
                tbsys::CNetUtil::addrToString(server_id).c_str(), block_id, index_op);
          }
        }
        else
        {
          ret = TFS_ERROR;
          TBSYS_LOG(ERROR, "read raw index from %s fail, blockid: %u, index_op: %d",
              tbsys::CNetUtil::addrToString(server_id).c_str(), block_id, index_op);
        }
        NewClientManager::get_instance().destroy_client(client);
      }

      TBSYS_LOG(DEBUG, "read raw index from %s, blockid: %u, index_op: %d, ret: %d",
          tbsys::CNetUtil::addrToString(server_id).c_str(), block_id, index_op, ret);

      return ret;
    }

    CompactTask::CompactTask(TaskManager& manager, const int64_t seqno,
        const uint64_t source_id, const int32_t expire_time, const uint32_t block_id):
      Task(manager, seqno, source_id, expire_time)
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
        ret = request_ds_to_compact();
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
      TBSYS_LOG(DEBUG, "compact start blockid: %u\n", block_id);
      int ret = TFS_SUCCESS;
      LogicBlock* src_logic_block = NULL;
      LogicBlock* dest_logic_block = NULL;

      src_logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == src_logic_block)
      {
        TBSYS_LOG(ERROR, "src block is not exist. blockid: %u\n", block_id);
        ret = EXIT_NO_LOGICBLOCK_ERROR;
      }
      else
      {
        BlockInfo* src_blk = src_logic_block->get_block_info();
        assert(src_blk->block_id_ == block_id);

        // create the dest block
        uint32_t physical_block_id = 0;
        ret = BlockFileManager::get_instance()->new_block(block_id, physical_block_id, C_COMPACT_BLOCK);
        TBSYS_LOG(DEBUG, "compact new block blockid: %u, physical blockid: %d\n", block_id, physical_block_id);
      }

      if (TFS_SUCCESS == ret)
      {
        dest_logic_block = BlockFileManager::get_instance()->get_logic_block(block_id, C_COMPACT_BLOCK);
        if (NULL == dest_logic_block)
        {
          TBSYS_LOG(ERROR, "get compact dest block fail. blockid: %u\n", block_id);
          ret = EXIT_NO_LOGICBLOCK_ERROR;
        }
        else
        {
          ret = real_compact(src_logic_block, dest_logic_block);
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "inner real compact blockid: %u fail. ret: %d\n", block_id, ret);
          }
          else
          {
            ret = dest_logic_block->update_block_version(VERSION_INC_STEP_DEFAULT);
            if (TFS_SUCCESS == ret)
            {
              TBSYS_LOG(DEBUG, "compact blockid : %u, switch compact blk\n", block_id);
              BlockFileManager::get_instance()->switch_compact_blk(block_id);
              if (TFS_SUCCESS != ret)
              {
                TBSYS_LOG(ERROR, "compact blockid: %u, switch compact blk fail. ret: %d\n", block_id, ret);
              }
            }
          }

          /** must remove one block here */

          TBSYS_LOG(DEBUG, "compact del old blockid: %u\n", block_id);
          // del serve block
          ret = BlockFileManager::get_instance()->del_block(block_id, C_COMPACT_BLOCK);
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "compact blockid: %u after switch, del old block fail. ret: %d\n", block_id, ret);
          }
        }
      }

      return ret;
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
      resp_cpt_msg.set_ds_id(manager_.get_ds_id());
      resp_cpt_msg.set_status(status);

      if (PLAN_STATUS_END == status)
      {
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
      }

      TBSYS_LOG(INFO, "compact report to ds. seqno: %"PRI64_PREFIX"d, blockid: %u, status: %d, source: %s",
        seqno_, block_id_, status, tbsys::CNetUtil::addrToString(source_id_).c_str());

      NewClient* client = NewClientManager::get_instance().create_client();
      post_msg_to_server(source_id_, client, &resp_cpt_msg, Task::ds_task_callback);
      // NewClientManager::get_instance().destroy_client(client);
      return TFS_SUCCESS;
    }

    int CompactTask::request_ds_to_compact()
    {
      int ret = TFS_SUCCESS;
      for (uint32_t i = 0; i < servers_.size() && TFS_SUCCESS == ret; i++)
      {
        DsCompactBlockMessage req_cpt_msg;
        req_cpt_msg.set_seqno(seqno_);
        req_cpt_msg.set_block_id(block_id_);
        req_cpt_msg.set_source_id(manager_.get_ds_id());
        ret = send_simple_request(servers_[i], &req_cpt_msg);
        TBSYS_LOG(DEBUG, "task seqno(%"PRI64_PREFIX"d) request %s to compact, ret: %d",
            seqno_, tbsys::CNetUtil::addrToString(servers_[i]).c_str(), ret);

        if (TFS_SUCCESS != ret)
        {
          break;
        }
      }

      return ret;
    }

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

      ret = dest->batch_write_meta(&dest_blk, &dest_metas);
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

    ReplicateTask::ReplicateTask(TaskManager& manager, const int64_t seqno,
        const uint64_t source_id, const int32_t expire_time, const common::ReplBlock& repl_info):
      Task(manager, seqno, source_id, expire_time)
    {
      type_ = PLAN_TYPE_REPLICATE;
      repl_info_ = repl_info;
    }

    ReplicateTask::~ReplicateTask()
    {

    }

    int ReplicateTask::handle()
    {
      int ret = do_replicate(repl_info_);
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
            if (repl_info_.is_move_ == REPLICATE_BLOCK_MOVE_FLAG_YES && STATUS_MESSAGE_REMOVE == sm->get_status())
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
        int rm_ret = BlockFileManager::get_instance()->del_block(repl_info_.block_id_);
        TBSYS_LOG(INFO, "send repl block complete info: del blockid: %u, result: %d\n", repl_info_.block_id_, rm_ret);
      }

      TBSYS_LOG(INFO, "replicate report to ns. seqno: %"PRI64_PREFIX"d, blockid: %u, status: %d, source: %s, ret: %d",
          seqno_, repl_info_.block_id_, status, tbsys::CNetUtil::addrToString(source_id_).c_str(), ret);

      return ret;
    }

    int ReplicateTask::report_to_ds(const int status)
    {
      RespDsReplicateBlockMessage resp_repl_msg;
      resp_repl_msg.set_seqno(seqno_);
      resp_repl_msg.set_ds_id(manager_.get_ds_id());
      resp_repl_msg.set_status(status);

      TBSYS_LOG(INFO, "replicate report to ds. seqno: %"PRI64_PREFIX"d, blockid: %u, status: %d, source: %s",
          seqno_, repl_info_.block_id_, status, tbsys::CNetUtil::addrToString(source_id_).c_str());

      NewClient* client = NewClientManager::get_instance().create_client();
      post_msg_to_server(source_id_, client, &resp_repl_msg, Task::ds_task_callback);
      // NewClientManager::get_instance().destroy_client(client);
      return TFS_SUCCESS;
    }

    int ReplicateTask::do_replicate(const ReplBlock& repl_block)
    {
      uint64_t ds_ip = repl_block.destination_id_;
      uint32_t block_id = repl_block.block_id_;

      TBSYS_LOG(INFO, "replicating now, seqno: %"PRI64_PREFIX"d, blockid: %u, %s = >%s\n",
          seqno_, block_id, tbsys::CNetUtil::addrToString(repl_block.source_id_).c_str(),
          tbsys::CNetUtil::addrToString(ds_ip).c_str());

      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        TBSYS_LOG(ERROR, "block is not exist, blockid: %u, %s=>%s\n", repl_block.block_id_,
            tbsys::CNetUtil::addrToString(repl_block.source_id_).c_str(),
            tbsys::CNetUtil::addrToString(ds_ip).c_str());
        return TFS_ERROR;
      }

      //replicate block file
      int32_t len = 0, offset = 0;
      int ret = TFS_SUCCESS;
      char tmp_data_buf[MAX_READ_SIZE];

      //this block will not be write or update now, locked by ns
      int32_t total_len = logic_block->get_data_file_size();
      do  // use do while to process empty block
      {
        int32_t read_len = MAX_READ_SIZE;
        if (total_len - offset < MAX_READ_SIZE)
        {
          read_len = total_len - offset;
        }
        ret = logic_block->read_raw_data(tmp_data_buf, read_len, offset);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "read raw data fail, ip: %s, blockid: %u, offset: %d, reading len: %d, ret: %d",
              tbsys::CNetUtil::addrToString(ds_ip).c_str(), block_id, offset, read_len, ret);
          break;
        }
        len = read_len;

        TBSYS_LOG(DEBUG, "replicate raw data blockid: %u, offset: %d, read len: %d, total len: %d\n", block_id,
            offset, len, total_len);

        ret = Task::write_raw_data(ds_ip, block_id, tmp_data_buf, len, offset);
        if (TFS_SUCCESS != ret)
        {
          break;
        }

        offset += len;
      } while (offset < total_len);

      if (TFS_SUCCESS == ret)
      {
        ret = batch_write_index(ds_ip, block_id);
      }

      // update block info local
      if (TFS_SUCCESS == ret)
      {
        ret = logic_block->update_block_version(VERSION_INC_STEP_REPLICATE);
      }
      return ret;
    }

    MarshallingTask::MarshallingTask(TaskManager& manager, const int64_t seqno,
        const uint64_t source_id, const int32_t expire_time, const int64_t family_id) :
      Task(manager, seqno, source_id, expire_time)
    {
      type_ = PLAN_TYPE_EC_MARSHALLING;
      family_id_ = family_id;
    }

    MarshallingTask::~MarshallingTask()
    {
      tbsys::gDelete(family_members_);
    }

    int MarshallingTask::set_family_member_info(const FamilyMemberInfo* members, const int32_t family_aid_info)
    {
      const int32_t MEMBER_NUM = GET_DATA_MEMBER_NUM(family_aid_info) + GET_CHECK_MEMBER_NUM(family_aid_info);
      int32_t ret = TFS_SUCCESS;
      if (NULL == members || MEMBER_NUM <= 0 || MEMBER_NUM > MAX_MARSHALLING_NUM)
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
        ret = send_simple_request(source_id_, &cmit_msg);
      }

      TBSYS_LOG(INFO, "marshalling report to ns. seqno: %"PRI64_PREFIX"d, status: %d, source: %s, ret: %d",
          seqno_, status, tbsys::CNetUtil::addrToString(source_id_).c_str(), ret);

      return ret;
    }

    int MarshallingTask::do_marshalling()
    {
      int ret = TFS_SUCCESS;
      const int32_t data_num = GET_DATA_MEMBER_NUM(family_aid_info_);
      const int32_t check_num = GET_CHECK_MEMBER_NUM(family_aid_info_);
      const int32_t member_num = data_num + check_num;

      // check if args valid
      if (data_num > MAX_DATA_MEMBER_NUM || check_num > MAX_CHECK_MEMBER_NUM)
      {
        return EXIT_INVALID_ARGU_ERROR;
      }

      // check if all data ok
      int normal_count = 0;
      for (int32_t i = 0; i < data_num; i++)
      {
        // just need data_num nodes to recovery
        if (family_members_[i].status_ == FAMILY_MEMBER_STATUS_NORMAL)
        {
          normal_count++;
        }
      }

      if (normal_count != data_num)
      {
        TBSYS_LOG(ERROR, "no enough normal node to recovery, normal count: %d", normal_count);
        return EXIT_NO_ENOUGH_DATA;
      }

      ErasureCode encoder;
      int32_t encode_total_len = -1;
      int32_t encode_offset = 0;
      int32_t encode_len = 0;
      int32_t block_len[member_num];
      char* data[member_num];
      char* index_data[member_num];
      memset(block_len, 0, member_num * sizeof(int32_t));
      memset(data, 0, member_num * sizeof(char*));
      memset(index_data, 0, member_num * sizeof(char*));

      // config encoder parameter, alloc buffer
      if (TFS_SUCCESS == ret)
      {
        ret = encoder.config(data_num, check_num);
        if (TFS_SUCCESS == ret)
        {
          for (int32_t i = 0; i < member_num; i++)
          {
            data[i] = (char*)malloc(MAX_READ_SIZE * sizeof(char));
            assert(NULL != data[i]);
          }
          encoder.bind(data, member_num, MAX_READ_SIZE);
        }
      }

      // process block data
      if (TFS_SUCCESS == ret)
      {
        do
        {
          encode_len = MAX_READ_SIZE;
          if (encode_total_len > 0 && encode_total_len - encode_offset < MAX_READ_SIZE)
          {
            encode_len = encode_total_len - encode_offset;
          }

          // read data from data node
          for (int32_t i = 0; i < data_num; i++)
          {
            memset(data[i], 0, encode_len * sizeof(char));
            uint64_t server_id = family_members_[i].server_;
            uint32_t block_id = family_members_[i].block_;
            int32_t data_file_size = 0;

            if  (0 == encode_offset || encode_offset < block_len[i])
            {
              ret = read_raw_data(server_id, block_id, data[i], encode_len, encode_offset, data_file_size);
            }

            if (TFS_SUCCESS == ret)
            {
              block_len[i] = data_file_size;
              // get total len on first read
              if (0 == encode_offset && data_file_size > encode_total_len)
              {
                encode_total_len = data_file_size;

                // rollup for encode
                int unit = ErasureCode::ws_ * ErasureCode::ps_;
                if (0 != (encode_total_len % unit))
                {
                  encode_total_len = (encode_total_len / unit + 1) * unit;
                }
              }
            }
            else
            {
              break;
            }
          }

          if (TFS_SUCCESS != ret)
          {
            break;
          }

          ret = encoder.encode(encode_len);
          if (TFS_SUCCESS != ret)
          {
            break;
          }

          // write data to check node
          for (int32_t i = data_num; i < member_num; i++)
          {
            uint64_t server_id = family_members_[i].server_;
            uint32_t block_id = family_members_[i].block_;
            ret = write_raw_data(server_id, block_id, data[i], encode_len, encode_offset, PARITY_DATA);
            if (TFS_SUCCESS != ret)
            {
              break;
            }
          }

          if (TFS_SUCCESS != ret)
          {
            break;
          }

          // update offset
          encode_offset += encode_len;
        } while (encode_offset < encode_total_len);
      }

      // process block index
      if (TFS_SUCCESS == ret)
      {
        RawIndexVec index_vec;

        for (int i = 0; i < data_num; i++)
        {
          uint64_t server_id = family_members_[i].server_;
          uint32_t block_id = family_members_[i].block_;
          int32_t length = 0;
          ret = read_raw_index(server_id, block_id, READ_DATA_INDEX, 0, index_data[i], length);
          if (TFS_SUCCESS == ret)
          {
            TBSYS_LOG(DEBUG, "index info, server_id: %s, block_id: %u, length: %d",
              tbsys::CNetUtil::addrToString(server_id).c_str(), block_id, length);
            RawIndex raw_index(block_id, index_data[i], length);
            index_vec.push_back(raw_index);
          }
          else
          {
            break;
          }
        }

        if (TFS_SUCCESS == ret)
        {
          for (int i = 0; i < check_num; i++)
          {
            uint64_t server_id = family_members_[data_num+i].server_;
            uint32_t block_id = family_members_[data_num+i].block_;
            ret = write_raw_index(server_id, block_id, family_id_, WRITE_PARITY_INDEX, index_vec);
            if (TFS_SUCCESS != ret)
            {
              break;
            }
          }
        }

        // just write family id to data block
        if (TFS_SUCCESS == ret)
        {
          RawIndexVec empty_index_vec;
          for (int i = 0; i < data_num; i++)
          {
            uint64_t server_id = family_members_[i].server_;
            uint32_t block_id = family_members_[i].block_;
            ret = write_raw_index(server_id, block_id, family_id_, WRITE_DATA_INDEX, empty_index_vec);
            if (TFS_SUCCESS != ret)
            {
              break;
            }
          }
        }
      }

      for (int32_t i = 0; i < member_num; i++)
      {
        tbsys::gDelete(data[i]);
        tbsys::gDelete(index_data[i]);
      }

      return ret;
    }

    ReinstateTask::ReinstateTask(TaskManager& manager, const int64_t seqno,
        const uint64_t source_id, const int32_t expire_time, const int64_t family_id) :
      MarshallingTask(manager, seqno, source_id, expire_time, family_id)
    {
      type_ = PLAN_TYPE_EC_REINSTATE;
    }

    ReinstateTask::~ReinstateTask()
    {

    }

    int ReinstateTask::handle()
    {
      int ret = do_reinstate();
      int status = translate_status(ret);
      return report_to_ns(status);
    }

    int ReinstateTask::do_reinstate()
    {
      int ret = TFS_SUCCESS;
      const int32_t data_num = GET_DATA_MEMBER_NUM(family_aid_info_);
      const int32_t check_num = GET_CHECK_MEMBER_NUM(family_aid_info_);
      const int32_t member_num = data_num + check_num;

      // check if args valid
      if (data_num > MAX_DATA_MEMBER_NUM || check_num > MAX_CHECK_MEMBER_NUM)
      {
        return EXIT_INVALID_ARGU_ERROR;
      }

      ErasureCode decoder;
      int32_t decode_total_len = -1;
      int32_t decode_offset = 0;
      int32_t decode_len = 0;
      int32_t block_len[member_num];
      char* data[member_num];
      char* index_data[member_num];
      int erased[member_num];
      memset(block_len, 0, member_num * sizeof(int32_t));
      memset(data, 0, member_num * sizeof(char*));
      memset(index_data, 0, member_num * sizeof(char*));
      memset(erased, 0, member_num * sizeof(int));

      bool need_recovery = false;
      int normal_count = 0;
      for (int32_t i = 0; i < member_num; i++)
      {
        // just need data_num nodes to recovery
        if (family_members_[i].status_ == FAMILY_MEMBER_STATUS_NORMAL)
        {
          if (normal_count < data_num)
          {
            erased[i] = 0;  // alive
            normal_count++;
          }
          else
          {
            erased[i] = -1; // normal but not used
          }
        }
        else
        {
          erased[i] = 1;   // need to recovey
          need_recovery = true;
        }
      }

      if (normal_count != data_num)
      {
        TBSYS_LOG(ERROR, "no enough normal node to recovery, normal count: %d", normal_count);
        return EXIT_NO_ENOUGH_DATA;
      }

      // all node ok, no need to recovery, just return
      if (!need_recovery)
      {
        TBSYS_LOG(INFO, "all nodes are normal, no need do recovery");
        return TFS_SUCCESS;
      }

      // config encoder parameter, alloc buffer
      if (TFS_SUCCESS == ret)
      {
        ret = decoder.config(data_num, check_num, erased);
        if (TFS_SUCCESS == ret)
        {
          for (int32_t i = 0; i < member_num; i++)
          {
            data[i] = (char*)malloc(MAX_READ_SIZE * sizeof(char));
            assert(NULL != data[i]);
          }
          decoder.bind(data, member_num, MAX_READ_SIZE);
        }
      }

      // process block data
      if (TFS_SUCCESS == ret)
      {
        do
        {
          decode_len = MAX_READ_SIZE;
          if (decode_total_len > 0 && decode_total_len - decode_offset < MAX_READ_SIZE)
          {
            decode_len = decode_total_len - decode_offset;
          }

          // read data from data node
          for (int32_t i = 0; i < member_num; i++)
          {
            if (0 != erased[i])
            {
              continue;
            }
            memset(data[i], 0, decode_len * sizeof(char));
            uint64_t server_id = family_members_[i].server_;
            uint32_t block_id = family_members_[i].block_;
            int32_t data_file_size = 0;

            if (0 == decode_offset || decode_offset < block_len[i])
            {
              ret = read_raw_data(server_id, block_id, data[i], decode_len, decode_offset, data_file_size);
            }

            if (TFS_SUCCESS == ret)
            {
              block_len[i] = data_file_size;
              // get total len on first read
              if (0 == decode_offset && data_file_size > decode_total_len)
              {
                decode_total_len = data_file_size;

                // rollup for encode
                int unit = ErasureCode::ws_ * ErasureCode::ps_;
                if (0 != (decode_total_len % unit))
                {
                  decode_total_len = (decode_total_len / unit + 1) * unit;
                }
              }
            }
            else
            {
              break;
            }
          }

          if (TFS_SUCCESS != ret)
          {
            break;
          }

          ret = decoder.decode(decode_len);
          if (TFS_SUCCESS != ret)
          {
            break;
          }

          // write normal data
          for (int32_t i = 0; i < data_num; i++)
          {
            if (1 != erased[i])
            {
              continue;
            }
            uint64_t server_id = family_members_[i].server_;
            uint32_t block_id = family_members_[i].block_;
            ret = write_raw_data(server_id, block_id, data[i], decode_len, decode_offset);
            if (TFS_SUCCESS != ret)
            {
              break;
            }
          }

          if (TFS_SUCCESS != ret)
          {
            break;
          }

          // write parity data
          for (int32_t i = data_num; i < member_num; i++)
          {
            if (1 != erased[i])
            {
              continue;
            }
            uint64_t server_id = family_members_[i].server_;
            uint32_t block_id = family_members_[i].block_;
            ret = write_raw_data(server_id, block_id, data[i], decode_len, decode_offset, PARITY_DATA);
            if (TFS_SUCCESS != ret)
            {
              break;
            }
          }

          if (TFS_SUCCESS != ret)
          {
            break;
          }

          // update offset
          decode_offset += decode_len;

        } while (decode_offset < decode_total_len);
      }

      // recovery data index
      if (TFS_SUCCESS == ret)
      {
        for (int i = 0; i < data_num; i++)
        {
          if (1 != erased[i])
          {
            continue;
          }

          char* target_index = NULL;
          int32_t length = 0;
          uint32_t target_block = family_members_[i].block_;
          uint64_t target_server = family_members_[i].server_;

          for (int j = data_num; j < member_num; j++)
          {
            if (0 == erased[j])
            {
              uint64_t server_id = family_members_[j].server_;
              uint32_t block_id = family_members_[j].block_;
              ret = read_raw_index(server_id, block_id, READ_PARITY_INDEX, target_block, target_index, length);
              if (TFS_SUCCESS == ret)
              {
                break;
              }
            }
          }

          if (TFS_SUCCESS == ret)
          {
            RawIndexVec index_vec;
            RawIndex raw_index(target_block, target_index, length);
            index_vec.push_back(raw_index);
            ret = write_raw_index(target_server, target_block, family_id_, WRITE_DATA_INDEX, index_vec);
          }

          tbsys::gDelete(target_index);

          if (TFS_SUCCESS != ret)
          {
            break;
          }
        }
      }

      // recovery parity index
      bool miss_parity = false;
      if (TFS_SUCCESS == ret)
      {
        for (int i = data_num; i < member_num; i++)
        {
          if (1 == erased[i])
          {
            miss_parity = true;
            break;
          }
        }
      }

      if (TFS_SUCCESS == ret && miss_parity)
      {
        RawIndexVec index_vec;
        for (int i = 0; i < data_num; i++)
        {
          uint64_t server_id = family_members_[i].server_;
          uint32_t block_id = family_members_[i].block_;
          int32_t length = 0;
          ret = read_raw_index(server_id, block_id, READ_DATA_INDEX, 0, index_data[i], length);
          if (TFS_SUCCESS == ret)
          {
            TBSYS_LOG(DEBUG, "index info, server_id: %s, block_id: %u, length: %d",
                tbsys::CNetUtil::addrToString(server_id).c_str(), block_id, length);
            RawIndex raw_index(block_id, index_data[i], length);
            index_vec.push_back(raw_index);
          }
          else
          {
            break;
          }
        }

        if (TFS_SUCCESS == ret)
        {
          for (int i = data_num; i < member_num; i++)
          {
            if (1 != erased[i])
            {
              continue;
            }

            uint64_t server_id = family_members_[i].server_;
            uint32_t block_id = family_members_[i].block_;
            ret = write_raw_index(server_id, block_id, family_id_, WRITE_PARITY_INDEX, index_vec);
            if (TFS_SUCCESS != ret)
            {
              break;
            }
          }
        }
      }

      for (int32_t i = 0; i < member_num; i++)
      {
        tbsys::gDelete(data[i]);
        tbsys::gDelete(index_data[i]);
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
        ret = send_simple_request(source_id_, &cmit_msg);
      }

      TBSYS_LOG(INFO, "reinstate report to ns. seqno: %"PRI64_PREFIX"d, status: %d, source: %s, ret: %d",
          seqno_, status, tbsys::CNetUtil::addrToString(source_id_).c_str(), ret);

      return ret;
    }

    DissolveTask::DissolveTask(TaskManager& manager, const int64_t seqno,
          const uint64_t source_id, const int expire_time, const int64_t family_id) :
      MarshallingTask(manager, seqno, source_id, expire_time, family_id)
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

      return request_ds_to_replicate();
    }

    int DissolveTask::handle_complete(BasePacket* packet)
    {
      int ret = TFS_SUCCESS;
      if (RESP_DS_REPLICATE_BLOCK_MESSAGE == packet->getPCode())
      {
        RespDsReplicateBlockMessage* resp_msg = dynamic_cast<RespDsReplicateBlockMessage*> (packet);
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
            request_to_clear_family_id();
            request_ds_to_delete();
          }
        }

        TBSYS_LOG(INFO, "handle complete dissolve task, "
            "seqno: %"PRI64_PREFIX"d, server: %s, status: %d, ret: %d\n",
            seqno_, tbsys::CNetUtil::addrToString(resp_msg->get_ds_id()).c_str(), status, ret);
      }
      return ret;
    }

    int DissolveTask::report_to_ns(const int status)
    {
      int ret = TFS_SUCCESS;
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

      ret = cmit_msg.set_family_member_info(family_members_, family_aid_info_);
      if (TFS_SUCCESS == ret)
      {
        ret = send_simple_request(source_id_, &cmit_msg);
      }

      TBSYS_LOG(INFO, "dissolve report to ns. seqno: %"PRI64_PREFIX"d, status: %d, source: %s, ret: %d",
          seqno_, final_status, tbsys::CNetUtil::addrToString(source_id_).c_str(), ret);

      return ret;
    }

    int DissolveTask::request_ds_to_replicate()
    {
      int ret = TFS_SUCCESS;
      int32_t data_num = GET_DATA_MEMBER_NUM(family_aid_info_) / 2;
      int32_t check_num = GET_CHECK_MEMBER_NUM(family_aid_info_) / 2;
      int32_t total_num = data_num + check_num;

      for (int32_t i = 0; i < data_num; i++)
      {
        if (FAMILY_MEMBER_STATUS_NORMAL != family_members_[i].status_)
        {
          continue;
        }

        DsReplicateBlockMessage repl_msg;
        ReplBlock repl_block;
        memset(&repl_block, 0, sizeof(ReplBlock));
        repl_block.block_id_ = family_members_[i].block_;
        repl_block.source_id_ = family_members_[i].server_;
        repl_block.destination_id_ = family_members_[i+total_num].server_;

        repl_msg.set_seqno(seqno_);
        repl_msg.set_expire_time(expire_time_);
        repl_msg.set_source_id(manager_.get_ds_id());
        repl_msg.set_repl_info(repl_block);

        ret = send_simple_request(family_members_[i].server_, &repl_msg);
        TBSYS_LOG(DEBUG, "task seqno(%"PRI64_PREFIX"d) request %s to replicate, ret: %d",
            seqno_, tbsys::CNetUtil::addrToString(family_members_[i].server_).c_str(), ret);

        if (TFS_SUCCESS != ret)
        {
          break;
        }
      }

      return ret;
    }

    int DissolveTask::request_ds_to_delete()
    {
      int ret = TFS_SUCCESS;
      const int32_t data_num = GET_DATA_MEMBER_NUM(family_aid_info_) / 2;
      const int32_t check_num = GET_CHECK_MEMBER_NUM(family_aid_info_) / 2;

      for (int32_t i = data_num; i < data_num + check_num; i++)
      {
        if (FAMILY_MEMBER_STATUS_NORMAL != family_members_[i].status_)
        {
          continue;
        }

        RemoveBlockMessage del_msg;
        del_msg.set(family_members_[i].block_);

        ret = send_simple_request(family_members_[i].server_, &del_msg);
        TBSYS_LOG(DEBUG, "task seqno(%"PRI64_PREFIX"d) request %s to delete, ret: %d",
            seqno_, tbsys::CNetUtil::addrToString(family_members_[i].server_).c_str(), ret);

      }

      return ret;
    }

    int DissolveTask::request_to_clear_family_id()
    {
      int ret = TFS_SUCCESS;
      int32_t data_num = GET_DATA_MEMBER_NUM(family_aid_info_) / 2;

      for (int32_t i = 0; i < data_num; i++)
      {
        if (FAMILY_MEMBER_STATUS_NORMAL != family_members_[i].status_)
        {
          continue;
        }

        RawIndexVec empty_vec;
        ret = write_raw_index(family_members_[i].server_, family_members_[i].block_,
            0, WRITE_DATA_INDEX, empty_vec);
        TBSYS_LOG(DEBUG, "task seqno(%"PRI64_PREFIX"d) request %s to clear family id, ret: %d",
            seqno_, tbsys::CNetUtil::addrToString(family_members_[i].server_).c_str(), ret);

      }

      return ret;
    }

  }
}
