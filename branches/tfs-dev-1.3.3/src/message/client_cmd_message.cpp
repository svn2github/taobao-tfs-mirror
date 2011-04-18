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
      int32_t iret = NULL != data ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        iret = get_int32(&data, &len, &info_.cmd_);
      }
      if (TFS_SUCCESS == iret)
      {
        iret = get_uint64(&data, &len, &info_.value1_);
      }
      if (TFS_SUCCESS == iret)
      {
        iret = get_uint32(&data, &len, &info_.value3_);
      }
      if (TFS_SUCCESS == iret)
      {
        iret = get_uint32(&data, &len, &info_.value4_);
      }
      if (TFS_SUCCESS == iret)
      {
        iret = get_uint64(&data, &len, &info_.value2_);
      }
      return iret;
    }

    int32_t ClientCmdMessage::message_length()
    {
      return INT_SIZE * 3 + INT64_SIZE * 2;
    }

    int ClientCmdMessage::build(char *data, int32_t len)
    {
      int32_t iret = NULL != data ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        iret = set_int32(&data, &len, info_.cmd_);
      }
      if (TFS_SUCCESS == iret)
      {
        iret = set_int64(&data, &len, info_.value1_);
      }
      if (TFS_SUCCESS == iret)
      {
        iret = set_int32(&data, &len, info_.value3_);
      }
      if (TFS_SUCCESS == iret)
      {
        iret = set_int32(&data, &len, info_.value4_);
      }
      if (TFS_SUCCESS == iret)
      {
        iret = set_int64(&data, &len, info_.value2_);
      }
      return iret;
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
