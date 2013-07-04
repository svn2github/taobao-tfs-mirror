/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *   linqing <linqing.zyd@taobao.com>
 *      - initial release
 *
 */

#include "common/parameter.h"
#include "common/status_message.h"
#include "default_server_helper.h"

using namespace __gnu_cxx;
using namespace tbsys;
using namespace tfs::message;
using namespace tfs::common;
using namespace std;

namespace tfs
{
  namespace checkserver
  {
    ServerStat::ServerStat():
      id_(0), use_capacity_(0), total_capacity_(0), current_load_(0), block_count_(0),
      last_update_time_(0), startup_time_(0), current_time_(0)
    {
      memset(&total_tp_, 0, sizeof(total_tp_));
      memset(&last_tp_, 0, sizeof(last_tp_));
    }

    ServerStat::~ServerStat()
    {
    }

    int ServerStat::deserialize(tbnet::DataBuffer& input, const int32_t length, int32_t& offset)
    {
      if (input.getDataLen() <= 0 || offset >= length)
      {
        return TFS_ERROR;
      }
      int32_t len = input.getDataLen();
      id_ = input.readInt64();
      use_capacity_ = input.readInt64();
      total_capacity_ = input.readInt64();
      current_load_ = input.readInt32();
      block_count_  = input.readInt32();
      last_update_time_ = input.readInt64();
      startup_time_ = input.readInt64();
      total_tp_.write_byte_ = input.readInt64();
      total_tp_.read_byte_ = input.readInt64();
      total_tp_.write_file_count_ = input.readInt64();
      total_tp_.read_file_count_ = input.readInt64();
      total_tp_.unlink_file_count_ = input.readInt64();
      total_tp_.fail_write_byte_ = input.readInt64();
      total_tp_.fail_read_byte_ = input.readInt64();
      total_tp_.fail_write_file_count_ = input.readInt64();
      total_tp_.fail_read_file_count_ = input.readInt64();
      total_tp_.fail_unlink_file_count_ = input.readInt64();
      current_time_ = input.readInt64();
      status_ = (DataServerLiveStatus)input.readInt32();
      offset += (len - input.getDataLen());

      return TFS_SUCCESS;
    }

    DefaultServerHelper::DefaultServerHelper()
    {
    }

    DefaultServerHelper::~DefaultServerHelper()
    {
    }

    int DefaultServerHelper::get_all_ds(const uint64_t ns_id, VUINT64& servers)
    {
      ShowServerInformationMessage msg;
      SSMScanParameter& param = msg.get_param();
      param.type_ = SSM_TYPE_SERVER;
      param.child_type_ = SSM_CHILD_SERVER_TYPE_INFO;
      param.start_next_position_ = 0x0;
      param.should_actual_count_= (100 << 16);  // get 100 ds every turn
      param.end_flag_ = SSM_SCAN_CUTOVER_FLAG_YES;

      if (false == NewClientManager::get_instance().is_init())
      {
        TBSYS_LOG(ERROR, "new client manager not init.");
        return TFS_ERROR;
      }

      while (!((param.end_flag_ >> 4) & SSM_SCAN_END_FLAG_YES))
      {
        param.data_.clear();
        tbnet::Packet* ret_msg = NULL;
        NewClient* client = NewClientManager::get_instance().create_client();
        int ret = send_msg_to_server(ns_id, client, &msg, ret_msg);
        if (TFS_SUCCESS != ret || ret_msg == NULL)
        {
          TBSYS_LOG(ERROR, "get server info error, ret: %d", ret);
          NewClientManager::get_instance().destroy_client(client);
          return TFS_ERROR;
        }
        if(ret_msg->getPCode() != SHOW_SERVER_INFORMATION_MESSAGE)
        {
          TBSYS_LOG(ERROR, "get invalid message type, pcode: %d", ret_msg->getPCode());
          NewClientManager::get_instance().destroy_client(client);
          return TFS_ERROR;
        }
        ShowServerInformationMessage* message = dynamic_cast<ShowServerInformationMessage*>(ret_msg);
        SSMScanParameter& ret_param = message->get_param();

        int32_t data_len = ret_param.data_.getDataLen();
        int32_t offset = 0;
        while (data_len > offset)
        {
          ServerStat server;
          if (TFS_SUCCESS == server.deserialize(ret_param.data_, data_len, offset))
          {
            servers.push_back(server.id_);
            std::string ip_port = Func::addr_to_str(server.id_, true);
          }
        }
        param.addition_param1_ = ret_param.addition_param1_;
        param.addition_param2_ = ret_param.addition_param2_;
        param.end_flag_ = ret_param.end_flag_;
        NewClientManager::get_instance().destroy_client(client);
      }

      return TFS_SUCCESS;
    }

    int DefaultServerHelper::get_block_replicas(const uint64_t ns_id, const uint64_t block_id, VUINT64& servers)
    {
      int ret = ((INVALID_SERVER_ID != ns_id) && (INVALID_BLOCK_ID != block_id)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        tbnet::Packet* resp_msg = NULL;
        NewClient* client = NewClientManager::get_instance().create_client();
        if (NULL != client)
        {
          GetBlockInfoMessageV2 msg;
          msg.set_block_id(block_id);
          msg.set_mode(T_READ);
          ret = send_msg_to_server(ns_id, client, &msg, resp_msg);
          if (TFS_SUCCESS == ret)
          {
            if (GET_BLOCK_INFO_RESP_MESSAGE_V2 == resp_msg->getPCode())
            {
              GetBlockInfoRespMessageV2* response = dynamic_cast<GetBlockInfoRespMessageV2*>(resp_msg);
              BlockMeta& meta = response->get_block_meta();
              for (int32_t i = 0; i < meta.size_; i++)
              {
                servers.push_back(meta.ds_[i]);
              }
            }
            else if (STATUS_MESSAGE == resp_msg->getPCode())
            {
              StatusMessage* smsg = dynamic_cast<StatusMessage*>(resp_msg);
              ret = smsg->get_status();
            }
            else
            {
              ret = EXIT_UNKNOWN_MSGTYPE;
            }
          }
          NewClientManager::get_instance().destroy_client(client);
        }
        else
        {
          ret = EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
        }
      }
      return ret;
    }

    int DefaultServerHelper::fetch_check_blocks(const uint64_t ds_id, const TimeRange& range, const int32_t group_count, const int32_t group_seq, common::VUINT64& blocks)
    {
      int ret = (INVALID_SERVER_ID != ds_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        tbnet::Packet* resp_msg = NULL;
        NewClient* client = NewClientManager::get_instance().create_client();
        if (NULL != client)
        {
          CheckBlockRequestMessage msg;
          msg.set_time_range(range);
          msg.set_group_count(group_count);
          msg.set_group_seq(group_seq);
          ret = send_msg_to_server(ds_id, client, &msg, resp_msg);
          if (TFS_SUCCESS == ret)
          {
            if (RSP_CHECK_BLOCK_MESSAGE == resp_msg->getPCode())
            {
              CheckBlockResponseMessage* response = dynamic_cast<CheckBlockResponseMessage*>(resp_msg);
              VUINT64 check_blocks = response->get_blocks();
              VUINT64::iterator iter = check_blocks.begin();
              for ( ; iter != check_blocks.end(); iter++)
              {
                blocks.push_back(*iter);
              }
            }
            else if (STATUS_MESSAGE == resp_msg->getPCode())
            {
              StatusMessage* smsg = dynamic_cast<StatusMessage*>(resp_msg);
              ret = smsg->get_status();
            }
            else
            {
              ret = EXIT_UNKNOWN_MSGTYPE;
            }
          }
          NewClientManager::get_instance().destroy_client(client);
        }
        else
        {
          ret = EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
        }
      }
      return ret;
    }

    int DefaultServerHelper::dispatch_check_blocks(const uint64_t ds_id, const int64_t seqno, const common::VUINT64& blocks)
    {
      int ret = (INVALID_SERVER_ID != ds_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        tbnet::Packet* resp_msg = NULL;
        NewClient* client = NewClientManager::get_instance().create_client();
        if (NULL != client)
        {
          ReportCheckBlockMessage msg;
          msg.set_seqno(seqno);
          msg.set_blocks(blocks);
          msg.set_server_id(SYSPARAM_CHECKSERVER.self_id_);
          ret = send_msg_to_server(ds_id, client, &msg, resp_msg);
          if (TFS_SUCCESS == ret)
          {
            if (STATUS_MESSAGE == resp_msg->getPCode())
            {
              StatusMessage* smsg = dynamic_cast<StatusMessage*>(resp_msg);
              ret = smsg->get_status();
            }
            else
            {
              ret = EXIT_UNKNOWN_MSGTYPE;
            }
          }
          NewClientManager::get_instance().destroy_client(client);
        }
        else
        {
          ret = EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
        }
      }
      return ret;
    }

    int DefaultServerHelper::get_group_info(const uint64_t ns_id, int32_t& group_count, int32_t& group_seq)
    {
      int ret = (INVALID_SERVER_ID != ns_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        string value;
        ret = get_param_from_ns(ns_id, 22, value);
        if (TFS_SUCCESS == ret)
        {
          group_count = atoi(value.c_str());
        }
      }

      if (TFS_SUCCESS == ret)
      {
        string value;
        ret = get_param_from_ns(ns_id, 23, value);
        if (TFS_SUCCESS == ret)
        {
          group_seq = atoi(value.c_str());
        }
      }

      return ret;
    }

    int DefaultServerHelper::get_param_from_ns(const uint64_t ns_id,
        const int32_t param_index, string& param_value)
    {
      int ret = (INVALID_SERVER_ID != ns_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        tbnet::Packet* resp_msg = NULL;
        NewClient* client = NewClientManager::get_instance().create_client();
        if (NULL != client)
        {
          ClientCmdMessage msg;
          msg.set_cmd(CLIENT_CMD_SET_PARAM);
          msg.set_value3(param_index);
          msg.set_value4(0);
          ret = send_msg_to_server(ns_id, client, &msg, resp_msg);
          if (TFS_SUCCESS == ret)
          {
            if (STATUS_MESSAGE == resp_msg->getPCode())
            {
              StatusMessage* smsg = dynamic_cast<StatusMessage*>(resp_msg);
              ret = smsg->get_status();
              if (STATUS_MESSAGE_OK == ret)
              {
                param_value = smsg->get_error();
              }
            }
            else
            {
              ret = EXIT_UNKNOWN_MSGTYPE;
            }
          }
          NewClientManager::get_instance().destroy_client(client);
        }
        else
        {
          ret = EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
        }
      }
      return ret;
    }

  }
}


