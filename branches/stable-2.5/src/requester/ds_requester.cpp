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
  }
}

