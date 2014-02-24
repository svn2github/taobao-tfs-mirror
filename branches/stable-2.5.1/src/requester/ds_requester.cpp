/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *  linqing <linqing.zyd@taobao.com>
 *      - initial release
 *
 */

#include "common/func.h"
#include "common/client_manager.h"
#include "common/base_packet.h"
#include "common/status_message.h"
#include "message/message_factory.h"
#include "ds_requester.h"

using namespace tfs::common;
using namespace tfs::message;
using namespace std;

namespace tfs
{
  namespace requester
  {
    int DsRequester::read_block_index(const uint64_t ds_id,
        const uint64_t block_id, const uint64_t attach_block_id,
        IndexDataV2& index_data)
    {
      int ret = INVALID_SERVER_ID != ds_id ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ReadIndexMessageV2 req_msg;
        tbnet::Packet* ret_msg = NULL;

        req_msg.set_block_id(block_id);
        req_msg.set_attach_block_id(attach_block_id);

        NewClient* new_client = NewClientManager::get_instance().create_client();
        ret = (NULL != new_client) ? TFS_SUCCESS : EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = send_msg_to_server(ds_id, new_client, &req_msg, ret_msg);
        }

        if (TFS_SUCCESS == ret)
        {
          if (READ_INDEX_RESP_MESSAGE_V2 == ret_msg->getPCode())
          {
            ReadIndexRespMessageV2* resp_msg = dynamic_cast<ReadIndexRespMessageV2* >(ret_msg);
            index_data = resp_msg->get_index_data();
          }
          else if (STATUS_MESSAGE == ret_msg->getPCode())
          {
            StatusMessage* resp_msg = dynamic_cast<StatusMessage*>(ret_msg);
            ret = resp_msg->get_status();
          }
          else
          {
            ret = EXIT_UNKNOWN_MSGTYPE;
          }
        }
        NewClientManager::get_instance().destroy_client(new_client);
      }
      return ret;
    }

    int DsRequester::read_raw_data(const uint64_t server, const uint64_t block,const int64_t traffic, const int64_t total_size, tbnet::DataBuffer& data)
    {
      const int64_t RETRY_TIMES = 2;
      const int64_t TRAN_BUFFER_SIZE = 8 * 1024 * 1024;
      bool eof_flag = false;
      int ret = TFS_SUCCESS;
      int32_t cur_offset = 0;
      int64_t read_size = std::min(traffic, TRAN_BUFFER_SIZE);
      int64_t micro_sec = 1000 * 1000;

      data.clear();
      while (!eof_flag)
      {
        int64_t remainder_retrys = RETRY_TIMES;
        TIMER_START();
        while (remainder_retrys > 0)
        {
          ReadRawdataMessageV2 rrd_msg;
          rrd_msg.set_block_id(block);
          rrd_msg.set_offset(cur_offset);
          rrd_msg.set_length(read_size);
          //rrd_msg.set_degrade_flag(false); //不考虑degrade read
          NewClient* client = NewClientManager::get_instance().create_client();
          tbnet::Packet* rsp = NULL;
          ret = send_msg_to_server(server, client, &rrd_msg, rsp);
          if (TFS_SUCCESS != ret)//只有发送的读消息没有收到回应才需要重试
          {
            --remainder_retrys;//need to retry
            TBSYS_LOG(WARN, "read raw data from ds: %s failed, blockid: %"PRI64_PREFIX"u, offset: %d, remainder_retrys: %"PRI64_PREFIX"d, ret: %d",
                tbsys::CNetUtil::addrToString(server).c_str(), block, cur_offset, remainder_retrys, ret);
          }
          else
          {
            remainder_retrys = 0;//retry end
            assert(NULL != rsp);
            if (READ_RAWDATA_RESP_MESSAGE_V2 == rsp->getPCode())
            {
              ReadRawdataRespMessageV2* rsp_rrd_msg = dynamic_cast<ReadRawdataRespMessageV2*>(rsp);
              int len = rsp_rrd_msg->get_length();
              assert(len > 0);
              data.writeBytes(rsp_rrd_msg->get_data(), len);
              cur_offset += len;
              if (len < read_size || cur_offset == total_size)
              {
                eof_flag = true;
                TBSYS_LOG(INFO, "read raw data from ds: %s finish, blockid: %"PRI64_PREFIX"u, offset: %d, remainder_retrys: %"PRI64_PREFIX"d, ret: %d, read_size:%ld, len: %d",
                    tbsys::CNetUtil::addrToString(server).c_str(), block, cur_offset, remainder_retrys, ret, read_size, len);
              }
              else
              {
                TBSYS_LOG(DEBUG, "read raw data from ds: %s succ, blockid: %"PRI64_PREFIX"u, offset: %d, len: %d, data: %p",
                    tbsys::CNetUtil::addrToString(server).c_str(), block, cur_offset, len, rsp_rrd_msg->get_data());
              }
            }
            else if (STATUS_MESSAGE == rsp->getPCode())
            {
              StatusMessage* sm = dynamic_cast<StatusMessage*>(rsp);
              TBSYS_LOG(ERROR, "read raw data from ds: %s failed, blockid: %"PRI64_PREFIX"u, offset: %d, remainder_retrys: %"PRI64_PREFIX"d, error msg:%s, ret: %d",
                  tbsys::CNetUtil::addrToString(server).c_str(), block, cur_offset, remainder_retrys, sm->get_error(), sm->get_status());
              ret = sm->get_status();
            }
            else //unknow type
            {
              TBSYS_LOG(ERROR, "read raw data from ds: %s failed, blockid: %"PRI64_PREFIX"u, offset: %d, remainder_retrys: %"PRI64_PREFIX"d, unknow msg type: %d",
                  tbsys::CNetUtil::addrToString(server).c_str(), block, cur_offset, remainder_retrys, rsp->getPCode());
              ret = EXIT_READ_FILE_ERROR;
            }
          }
          NewClientManager::get_instance().destroy_client(client);
        }
        TIMER_END();

        if (TFS_SUCCESS != ret)
        {
          break;
        }

        // restrict speed
        int64_t speed = 0;
        int64_t d_value = micro_sec - TIMER_DURATION();
        if (d_value > 0)
        {
          usleep(d_value);
          speed = read_size * 1000 * 1000 / TIMER_DURATION();
        }

        TBSYS_LOG(DEBUG, "read data, cost time: %"PRI64_PREFIX"d, read size: %"PRI64_PREFIX"d, speed: %"PRI64_PREFIX"d byte/s, need sleep time: %"PRI64_PREFIX"d",
            TIMER_DURATION(), read_size, speed, d_value);
      }
      return ret;
    }


    int DsRequester::stat_file(const uint64_t ds_id, const uint64_t block_id, const uint64_t attach_block_id,
        uint64_t file_id, TfsFileStat& file_stat, const int32_t flag)
    {
      int32_t ret = TFS_SUCCESS;
      StatFileMessageV2 sf_msg;
      sf_msg.set_block_id(block_id);
      sf_msg.set_attach_block_id(attach_block_id);
      sf_msg.set_file_id(file_id);
      sf_msg.set_flag(flag);

      tbnet::Packet* ret_msg = NULL;
      NewClient* new_client = NewClientManager::get_instance().create_client();
      ret = (NULL != new_client) ? TFS_SUCCESS : EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = send_msg_to_server(ds_id, new_client, &sf_msg, ret_msg);
      }

      if (TFS_SUCCESS == ret)
      {
        if (STAT_FILE_RESP_MESSAGE_V2 == ret_msg->getPCode())
        {
          StatFileRespMessageV2* resp_msg = dynamic_cast<StatFileRespMessageV2*>(ret_msg);
          const FileInfoV2& file_info = resp_msg->get_file_info();
          FileInfoV2ToFileStat(file_info, file_stat);
        }
        else if (STATUS_MESSAGE == ret_msg->getPCode())
        {
          StatusMessage* resp_msg = dynamic_cast<StatusMessage*>(ret_msg);
          ret = resp_msg->get_status();
        }
        else
        {
          ret = EXIT_UNKNOWN_MSGTYPE;
        }
      }
      if (NULL != new_client)
      {
        NewClientManager::get_instance().destroy_client(new_client);
      }
      return ret;
    }

    int DsRequester::read_file(const uint64_t ds_id, const uint64_t block_id, const uint64_t attach_block_id,
        const uint64_t file_id, char* data, const int32_t offset, const int32_t length, const int32_t flag)
    {
      int32_t ret = TFS_SUCCESS;
      ReadFileMessageV2 rd_msg;
      rd_msg.set_block_id(block_id);
      rd_msg.set_attach_block_id(attach_block_id);
      rd_msg.set_file_id(file_id);
      rd_msg.set_offset(offset);
      rd_msg.set_length(length);
      rd_msg.set_flag(flag);

      tbnet::Packet* ret_msg = NULL;
      NewClient* new_client = NewClientManager::get_instance().create_client();
      ret = (NULL != new_client) ? TFS_SUCCESS : EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = send_msg_to_server(ds_id, new_client, &rd_msg, ret_msg);
      }

      if (TFS_SUCCESS == ret)
      {
        if (READ_FILE_RESP_MESSAGE_V2 == ret_msg->getPCode())
        {
          ReadFileRespMessageV2* resp_msg = dynamic_cast<ReadFileRespMessageV2*>(ret_msg);
          ret = resp_msg->get_length();// return real size when read success
          assert(ret >= 0);
          memcpy(data, resp_msg->get_data(), resp_msg->get_length());
        }
        else if (STATUS_MESSAGE == ret_msg->getPCode())
        {
          StatusMessage* resp_msg = dynamic_cast<StatusMessage*>(ret_msg);
          ret = resp_msg->get_status();
        }
        else
        {
          ret = EXIT_UNKNOWN_MSGTYPE;
        }
      }
      if (NULL != new_client)
      {
        NewClientManager::get_instance().destroy_client(new_client);
      }

      return ret;
    }

    int DsRequester::list_block(const uint64_t ds_id, vector<BlockInfoV2>& block_infos)
    {
      int ret = TFS_SUCCESS;
      ListBlockMessage req_lb_msg;
      req_lb_msg.set_block_type(LB_INFOS);

      NewClient* client = NewClientManager::get_instance().create_client();
      if (NULL != client)
      {
        tbnet::Packet* ret_msg= NULL;
        ret = send_msg_to_server(ds_id, client, &req_lb_msg, ret_msg);
        if (TFS_SUCCESS == ret)
        {
          if (ret_msg->getPCode() == RESP_LIST_BLOCK_MESSAGE)
          {
            RespListBlockMessage* resp_lb_msg = dynamic_cast<RespListBlockMessage*> (ret_msg);
//            block_vec = *(resp_lb_msg->get_blocks());
            block_infos = *(resp_lb_msg->get_infos());
          }
          else
          {
            ret = EXIT_RECVMSG_ERROR;
            TBSYS_LOG(ERROR, "get block list reply msg fail, pcode: %d, ds: %s", ret_msg->getPCode(), tbsys::CNetUtil::addrToString(ds_id).c_str());
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "list block message fail, ds: %s, ret: %d", tbsys::CNetUtil::addrToString(ds_id).c_str(), ret);
        }
        NewClientManager::get_instance().destroy_client(client);
      }
      else
      {
        ret = EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
      }
      return ret;
    }

    int DsRequester::recombine_raw_data(const common::IndexDataV2& sindex, tbnet::DataBuffer& sbuf,
        common::IndexDataV2& dindex, tbnet::DataBuffer& dbuf, std::vector<FileInfoV2>& nosync_files)
    {
      std::vector<FileInfoV2>::const_iterator iter = sindex.finfos_.begin();
      for (; iter != sindex.finfos_.end(); ++iter)
      {
        const FileInfoV2& finfo = (*iter);
        if (0 != (finfo.status_ & (FILE_STATUS_DELETE | FILE_STATUS_INVALID)))//skip deleted file
        {
          nosync_files.push_back(finfo);
          continue;
        }

        if (INVALID_FILE_ID == finfo.id_)
        {
          TBSYS_LOG(WARN, "file id illegal, skip it. block: %"PRI64_PREFIX"u, statu: %d, size: %d, offset: %d",
            sindex.header_.info_.block_id_, finfo.status_, finfo.size_, finfo.offset_);
          nosync_files.push_back(finfo);
          continue;
        }

        FileInfoV2 dfinfo = finfo;
        dfinfo.offset_ = dbuf.getDataLen();
        char* data = (sbuf.getData() + finfo.offset_);
        dbuf.writeBytes(data, finfo.size_);
        dindex.finfos_.push_back(dfinfo);
      }

      memset(&dindex.header_, 0, sizeof(IndexHeaderV2));
      dindex.header_.info_.block_id_ = sindex.header_.info_.block_id_;
      dindex.header_.info_.version_ = dindex.finfos_.size();
      dindex.header_.info_.file_count_ = dindex.finfos_.size();
      dindex.header_.info_.size_ = dbuf.getDataLen();
      dindex.header_.info_.del_file_count_ = 0;
      dindex.header_.info_.del_size_ = 0;
      dindex.header_.info_.update_file_count_ = 0;
      dindex.header_.info_.update_size_ = 0;
      dindex.header_.seq_no_ = sindex.header_.seq_no_;//last file id sequence
      dindex.header_.used_file_info_bucket_size_ = dindex.finfos_.size();
      dindex.header_.file_info_bucket_size_ = dindex.finfos_.size();
      TBSYS_LOG(INFO, "recombine raw data success. blockid: %"PRI64_PREFIX"u, file count: %d, block size: %d",
                  sindex.header_.info_.block_id_, dindex.header_.info_.file_count_, dindex.header_.info_.size_);
      return TFS_SUCCESS;
    }

    int DsRequester::write_raw_data(tbnet::DataBuffer& buf, const uint64_t block, const uint64_t server, const int32_t traffic)
    {
      int32_t ret = TFS_SUCCESS;
      const int32_t RETRY_TIMES = 2;
      int32_t offset = 0, length = 0, total_len = buf.getDataLen();
      while (offset < total_len && TFS_SUCCESS == ret)
      {
        int32_t retry = RETRY_TIMES;
        do
        {
          length = std::min(total_len, traffic);
          WriteRawdataMessageV2 req;
          req.set_block_id(block);
          req.set_offset(offset);
          req.set_length(length);
          req.set_data(buf.getData());
          if (0 == offset)
            req.set_new_flag(true);
          tbnet::Packet* rsp = NULL;
          NewClient* client = NewClientManager::get_instance().create_client();
          ret = (NULL != client) ? TFS_SUCCESS : EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
          if (TFS_SUCCESS == ret)
          {
            ret = send_msg_to_server(server, client, &req, rsp);
            if (TFS_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "write raw data to %s failed, ret: %d, block: %"PRI64_PREFIX"u, offset: %d, length: %d",
                  tbsys::CNetUtil::addrToString(server).c_str(),ret, block, offset, length);
            }
          }
          if (TFS_SUCCESS == ret)
          {
            ret = STATUS_MESSAGE == rsp->getPCode() ? TFS_SUCCESS : EXIT_UNKNOWN_MSGTYPE;
            if (TFS_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "write raw data to %s failed, ret: %d, block: %"PRI64_PREFIX"u, offset: %d, length: %d",
                  tbsys::CNetUtil::addrToString(server).c_str(),ret, block, offset, length);
            }
          }

          if (TFS_SUCCESS == ret)
          {
            StatusMessage* sm = dynamic_cast<StatusMessage*>(rsp);
            assert(NULL != sm);
            if (STATUS_MESSAGE_OK != sm->get_status())
            {
              ret =  sm->get_status();
              TBSYS_LOG(WARN, "write raw data to %s failed, ret: %d, block: %"PRI64_PREFIX"u, offset: %d, length: %d",
                  tbsys::CNetUtil::addrToString(server).c_str(),ret, block, offset, length);
            }
            else
            {
              buf.drainData(length);
              offset += length;
            }
          }
          NewClientManager::get_instance().destroy_client(client);
        }
        while (retry-- > 0 && TFS_SUCCESS != ret);
      }
      return ret;
    }

    int DsRequester::write_raw_index(const common::IndexDataV2& index_data, const uint64_t block, const uint64_t server)
    {
      WriteIndexMessageV2 req;
      req.set_block_id(index_data.header_.info_.block_id_);
      req.set_attach_block_id(index_data.header_.info_.block_id_);
      req.set_index_data(index_data);
      req.set_cluster_flag(true);

      tbnet::Packet* rsp = NULL;
      NewClient* client = NewClientManager::get_instance().create_client();
      int32_t ret = (NULL != client) ? TFS_SUCCESS : EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = send_msg_to_server(server, client, &req, rsp);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "write raw index to %s failed, ret: %d, block: %"PRI64_PREFIX"u, files: %zd",
              tbsys::CNetUtil::addrToString(server).c_str(),ret, block, index_data.finfos_.size());
        }
      }
      if (TFS_SUCCESS == ret)
      {
        ret = STATUS_MESSAGE == rsp->getPCode() ? TFS_SUCCESS : EXIT_UNKNOWN_MSGTYPE;
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "write raw index to %s failed, ret: %d, block: %"PRI64_PREFIX"u, files: %zd",
              tbsys::CNetUtil::addrToString(server).c_str(),ret, block, index_data.finfos_.size());
        }
      }

      if (TFS_SUCCESS == ret)
      {
        StatusMessage* sm = dynamic_cast<StatusMessage*>(rsp);
        assert(NULL != sm);
        if (STATUS_MESSAGE_OK != sm->get_status())
        {
          ret =  sm->get_status();
          TBSYS_LOG(WARN, "write raw index to %s failed, ret: %d, block: %"PRI64_PREFIX"u, files: %zd",
              tbsys::CNetUtil::addrToString(server).c_str(),ret, block, index_data.finfos_.size());
        }
      }
      NewClientManager::get_instance().destroy_client(client);
      return ret;
    }

    int DsRequester::remove_block(const uint64_t block, const std::string& addr, const bool tmp)
    {
      RemoveBlockMessageV2 req;
      req.set_block_id(block);
      req.set_tmp_flag(tmp);

      tbnet::Packet* rsp = NULL;
      NewClient* client = NewClientManager::get_instance().create_client();
      int32_t ret = send_msg_to_server(Func::get_host_ip(addr.c_str()), client, &req, rsp);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(INFO, "send remove block: %"PRI64_PREFIX"u command to %s failed,ret: %d", block, addr.c_str(), ret);
      }
      else
      {
        assert(NULL != rsp);
        if (STATUS_MESSAGE == rsp->getPCode())
        {
          StatusMessage* sm = dynamic_cast<StatusMessage*>(rsp);
          if (STATUS_MESSAGE_OK == sm->get_status())
          {
            TBSYS_LOG(INFO, "remove block: %"PRI64_PREFIX"u from %s successful", block, addr.c_str());
          }
          else
          {
            ret = sm->get_status();
            TBSYS_LOG(INFO, "remove block: %"PRI64_PREFIX"u from %s fail, ret: %d", block, addr.c_str(), ret);
          }
        }
        else
        {
          ret = EXIT_UNKNOWN_MSGTYPE;
          TBSYS_LOG(INFO, "remove block: %"PRI64_PREFIX"u from %s fail, ret: %d", block, addr.c_str(), ret);
        }
      }
      NewClientManager::get_instance().destroy_client(client);
      return ret;
    }
  }/** end namespace requester **/
}/** end namespace tfs **/

