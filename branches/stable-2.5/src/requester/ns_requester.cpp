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
#include "ns_requester.h"

using namespace tfs::common;
using namespace tfs::message;
using namespace std;

namespace tfs
{
  namespace requester
  {
    int NsRequester::get_block_replicas(const uint64_t ns_id, const uint64_t block_id, VUINT64& replicas)
    {
      int ret = INVALID_SERVER_ID != ns_id && INVALID_BLOCK_ID != block_id ?
        TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        replicas.clear();
        GetBlockInfoMessageV2 gbi_message;
        gbi_message.set_block_id(block_id);
        gbi_message.set_mode(T_READ);

        tbnet::Packet* rsp = NULL;
        NewClient* client = NewClientManager::get_instance().create_client();
        ret = (NULL != client) ? TFS_SUCCESS : EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = send_msg_to_server(ns_id, client, &gbi_message, rsp);
        }

        if (TFS_SUCCESS == ret)
        {
          if (rsp->getPCode() == GET_BLOCK_INFO_RESP_MESSAGE_V2)
          {

            GetBlockInfoRespMessageV2* msg = dynamic_cast<GetBlockInfoRespMessageV2* >(rsp);
            BlockMeta& block_meta = msg->get_block_meta();
            for (int i = 0; i < block_meta.size_; i++)
            {
              replicas.push_back(block_meta.ds_[i]);
            }
          }
          else if (rsp->getPCode() == STATUS_MESSAGE)
          {
            ret = dynamic_cast<StatusMessage*>(rsp)->get_status();
          }
          else
          {
            ret = EXIT_UNKNOWN_MSGTYPE;
          }
        }

        NewClientManager::get_instance().destroy_client(client);
      }
      return ret;
    }

    int NsRequester::get_cluster_id(const uint64_t ns_id, int32_t& cluster_id)
    {
      std::string value;
      int ret = get_ns_param(ns_id, "cluster_index", value);
      if (TFS_SUCCESS == ret)
      {
        cluster_id = atoi(value.c_str()) - '0';
      }
      return ret;
    }

    int NsRequester::get_group_count(const uint64_t ns_id, int32_t& group_count)
    {
      std::string value;
      int ret = get_ns_param(ns_id, "group_count", value);
      if (TFS_SUCCESS == ret)
      {
        group_count = atoi(value.c_str());
      }
      return ret;
    }

    int NsRequester::get_group_seq(const uint64_t ns_id, int32_t& group_seq)
    {
      std::string value;
      int ret = get_ns_param(ns_id, "group_seq", value);
      if (TFS_SUCCESS == ret)
      {
        group_seq = atoi(value.c_str());
      }
      return ret;
    }

    int NsRequester::get_ns_param(const uint64_t ns_id, const std::string& key, std::string& value)
    {
      int ret = (INVALID_SERVER_ID != ns_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      int32_t key_index = -1;

      // find key's index in param table
      if (TFS_SUCCESS == ret)
      {
        ret = EXIT_PARAMETER_ERROR;
        int32_t count = sizeof(dynamic_parameter_str) / sizeof(dynamic_parameter_str[0]);
        for (int i = 0; i < count; i++)
        {
          if (!strcmp(key.c_str(), dynamic_parameter_str[i]))
          {
            key_index = i + 1;
            ret = TFS_SUCCESS;
            break;
          }
        }
      }

      if (TFS_SUCCESS == ret)
      {
        ClientCmdMessage msg;
        msg.set_cmd(CLIENT_CMD_SET_PARAM);
        msg.set_value3(key_index);
        msg.set_value4(0);

        tbnet::Packet* resp_msg = NULL;
        NewClient* client = NewClientManager::get_instance().create_client();
        ret = (NULL != client) ? TFS_SUCCESS : EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = send_msg_to_server(ns_id, client, &msg, resp_msg);
        }

        if (TFS_SUCCESS == ret)
        {
          if (STATUS_MESSAGE == resp_msg->getPCode())
          {
            StatusMessage* smsg = dynamic_cast<StatusMessage*>(resp_msg);
            ret = smsg->get_status();
            if (STATUS_MESSAGE_OK == ret)
            {
              value = smsg->get_error();
            }
          }
          else
          {
            ret = EXIT_UNKNOWN_MSGTYPE;
          }
        }

        NewClientManager::get_instance().destroy_client(client);
      }

      return ret;
    }

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

    int NsRequester::get_ds_list(const uint64_t ns_id, common::VUINT64& ds_list)
    {
      ds_list.clear();
      int ret = TFS_SUCCESS;
      ShowServerInformationMessage msg;
      SSMScanParameter& param = msg.get_param();
      param.type_ = SSM_TYPE_SERVER;
      param.child_type_ = SSM_CHILD_SERVER_TYPE_INFO;
      param.start_next_position_ = 0x0;
      param.should_actual_count_= (100 << 16);  // get 100 ds every turn
      param.end_flag_ = SSM_SCAN_CUTOVER_FLAG_YES;

      while (TFS_SUCCESS == ret && !((param.end_flag_ >> 4) & SSM_SCAN_END_FLAG_YES))
      {
        param.data_.clear();
        tbnet::Packet* ret_msg = NULL;
        NewClient* client = NewClientManager::get_instance().create_client();
        ret = send_msg_to_server(ns_id, client, &msg, ret_msg);
        if (TFS_SUCCESS == ret)
        {
          if (ret_msg->getPCode() != SHOW_SERVER_INFORMATION_MESSAGE)
          {
            if (ret_msg->getPCode() == STATUS_MESSAGE)
            {
              ret = dynamic_cast<StatusMessage*>(ret_msg)->get_status();
            }
            else
            {
              ret = EXIT_UNKNOWN_MSGTYPE;
            }
          }
        }

        if (TFS_SUCCESS == ret)
        {
          ShowServerInformationMessage* message = dynamic_cast<ShowServerInformationMessage*>(ret_msg);
          SSMScanParameter& ret_param = message->get_param();

          int32_t data_len = ret_param.data_.getDataLen();
          int32_t offset = 0;
          while (data_len > offset)
          {
            ServerStat server;
            if (TFS_SUCCESS == server.deserialize(ret_param.data_, data_len, offset))
            {
              ds_list.push_back(server.id_);
              std::string ip_port = Func::addr_to_str(server.id_, true);
            }
          }
          param.addition_param1_ = ret_param.addition_param1_;
          param.addition_param2_ = ret_param.addition_param2_;
          param.end_flag_ = ret_param.end_flag_;
        }
        NewClientManager::get_instance().destroy_client(client);
      }

      return ret;
    }
  }
}

