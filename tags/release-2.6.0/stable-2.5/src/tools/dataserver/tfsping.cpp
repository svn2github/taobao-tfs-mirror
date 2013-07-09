/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfsping.cpp 413 2011-06-03 00:52:46Z daoan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include <stdio.h>
#include "common/func.h"
#include "common/new_client.h"
#include "common/client_manager.h"
#include "common/status_message.h"
#include "message/message_factory.h"
#include "message/replicate_block_message.h"
#include "message/compact_block_message.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::message;

int send_simple_request(uint64_t server_id, BasePacket* message)
{
  int ret = TFS_SUCCESS;
  if (0 == server_id || NULL == message)
  {
    ret = EXIT_PARAMETER_ERROR;
  }
  else
  {
    MessageFactory factory;
    BasePacketStreamer streamer(&factory);
    int ret = NewClientManager::get_instance().initialize(&factory, &streamer);
    assert(TFS_SUCCESS == ret);
    NewClient* client = NewClientManager::get_instance().create_client();
    if (NULL == client)
    {
      TBSYS_LOG(ERROR, "create client error");
    }
    else
    {
      tbnet::Packet* rsp_msg = NULL;
      if (TFS_SUCCESS == send_msg_to_server(server_id, client, message, rsp_msg))
      {
        if (rsp_msg->getPCode() == STATUS_MESSAGE)
        {
          StatusMessage* sm = dynamic_cast<StatusMessage*> (rsp_msg);
          if (STATUS_MESSAGE_OK != sm->get_status())
          {
            ret = TFS_ERROR;
          }
        }
      }
      else
      {
        ret = TFS_ERROR;
      }
      NewClientManager::get_instance().destroy_client(client);
    }
  }
  return ret;
}

int main(int argc, char* argv[])
{
  if (argc != 7)
  {
    printf("%s seqno blockid source dest\n\n", argv[0]);
    return TFS_ERROR;
  }

/*
  ReplicateBlockMessage rb_msg;
  rb_msg.set_seqno(atoi(argv[1]));
  ReplBlock repl_block;
  repl_block.block_id_ = atoi(argv[2]);
  repl_block.source_id_ = Func::str_to_addr(argv[3], atoi(argv[4]));
  repl_block.destination_id_= Func::str_to_addr(argv[5], atoi(argv[6]));
  rb_msg.set_repl_block(&repl_block);
  rb_msg.set_expire_time(240);
*/
  NsRequestCompactBlockMessage cb_msg;
  cb_msg.set_seqno(atoi(argv[1]));
  cb_msg.set_block_id(atoi(argv[2]));
  vector<uint64_t> servers;
  uint64_t server1 = Func::str_to_addr(argv[3], atoi(argv[4]));
  uint64_t server2 = Func::str_to_addr(argv[5], atoi(argv[6]));
  servers.push_back(server1);
  servers.push_back(server2);
  cb_msg.set_servers(servers);
  cb_msg.set_expire_time(240);

  send_simple_request(server1, &cb_msg);

//  send_simple_request(repl_block.source_id_, &rb_msg);

  return 0;
}
