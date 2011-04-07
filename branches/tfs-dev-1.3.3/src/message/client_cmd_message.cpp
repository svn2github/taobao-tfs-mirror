/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include "client_cmd_message.h"

using namespace tfs::common;

namespace tfs
{
  namespace message
  {
    // ClientCmdMessage 
    ClientCmdMessage::ClientCmdMessage()
    {
      _packetHeader._pcode = CLIENT_CMD_MESSAGE;
      memset(&info_, 0, sizeof(ClientCmdInformation));
    }

    ClientCmdMessage::~ClientCmdMessage()
    {

    }

    int ClientCmdMessage::parse(char *data, int32_t len)
    {
      if (get_object_copy(&data, &len, reinterpret_cast<void*> (&info_), sizeof(ClientCmdInformation)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      return TFS_SUCCESS;
    }

    int32_t ClientCmdMessage::message_length()
    {
      return sizeof(ClientCmdInformation);
    }

    int ClientCmdMessage::build(char *data, int32_t len)
    {
      if (set_object(&data, &len, &info_, sizeof(ClientCmdInformation)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      return TFS_SUCCESS;
    }

    char *ClientCmdMessage::get_name()
    {
      return "clientcmdmessage";
    }

    Message* ClientCmdMessage::create(const int32_t type)
    {
      ClientCmdMessage *req_cc_msg = new ClientCmdMessage();
      req_cc_msg->set_message_type(type);
      return req_cc_msg;
    }
  }
}
