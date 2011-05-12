/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: dump_plan_message.cpp 1990 2010-12-24 08:41:05Z duanfei $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include "dump_plan_message.h"

using namespace tfs::common;

namespace tfs
{
  namespace message
  {
    DumpPlanMessage::DumpPlanMessage():
      reserve_(0)
    {
      _packetHeader._pcode = DUMP_PLAN_MESSAGE;
    }

    DumpPlanMessage::~DumpPlanMessage()
    {
    }

    int DumpPlanMessage::parse(char* data, int32_t len)
    {
      int32_t iret = (NULL != data && len >= message_length()) ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        iret = get_int32(&data, &len, &reserve_);
      }
      return iret;
    }

    int32_t DumpPlanMessage::message_length()
    {
      return INT_SIZE;
    }

    int DumpPlanMessage::build(char* data, int32_t len)
    {
      int32_t iret = (NULL != data && len >= message_length()) ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        iret = set_int32(&data, &len, reserve_);
      }
      return iret;
    }

    char* DumpPlanMessage::get_name()
    {
      return "dumpplanmessage";
    }

    Message* DumpPlanMessage::create(const int32_t type)
    {
      DumpPlanMessage* msg= new DumpPlanMessage();
      msg->set_message_type(type);
      return msg;
    }

    DumpPlanResponseMessage::DumpPlanResponseMessage()
    {
      _packetHeader._pcode = DUMP_PLAN_RESPONSE_MESSAGE;
    }

    DumpPlanResponseMessage::~DumpPlanResponseMessage()
    {
    }

    int DumpPlanResponseMessage::parse(char* data, int32_t len)
    {
      if (len > 0)
      {
        data_.writeBytes(data, len);
      }
      return TFS_SUCCESS;
    }

    int32_t DumpPlanResponseMessage::message_length()
    {
      return data_.getDataLen();
    }

    int DumpPlanResponseMessage::build(char* data, int32_t len)
    {
      if (data_.getDataLen() > 0)
      {
        memcpy(data, data_.getData(), data_.getDataLen());
      }
      return TFS_SUCCESS;
    }

    char* DumpPlanResponseMessage::get_name()
    {
      return "dumpplanresponsemessage";
    }

    Message* DumpPlanResponseMessage::create(const int32_t type)
    {
      DumpPlanResponseMessage* msg = new DumpPlanResponseMessage();
      msg->set_message_type(type);
      return msg;
    }
 }
}
