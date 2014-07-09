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
#include "requester/ns_requester.h"
#include "default_server_helper.h"

using namespace __gnu_cxx;
using namespace tbsys;
using namespace tfs::message;
using namespace tfs::common;
using namespace tfs::requester;
using namespace std;

namespace tfs
{
  namespace checkserver
  {
    DefaultServerHelper::DefaultServerHelper()
    {
    }

    DefaultServerHelper::~DefaultServerHelper()
    {
    }

    int DefaultServerHelper::get_all_ds(const uint64_t ns_id, VUINT64& servers)
    {
      return NsRequester::get_ds_list(ns_id, servers);
    }

    int DefaultServerHelper::get_block_replicas(const uint64_t ns_id, const uint64_t block_id, VUINT64& servers)
    {
      return NsRequester::get_block_replicas(ns_id, block_id, servers);
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

    int DefaultServerHelper::dispatch_check_blocks(const uint64_t ds_id, const common::CheckParam& param)
    {
      int ret = (INVALID_SERVER_ID != ds_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        tbnet::Packet* resp_msg = NULL;
        NewClient* client = NewClientManager::get_instance().create_client();
        if (NULL != client)
        {
          ReportCheckBlockMessage msg;
          msg.set_param(param);
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


