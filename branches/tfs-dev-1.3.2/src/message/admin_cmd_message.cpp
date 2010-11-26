/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: adminserver.cpp 18 2010-10-12 09:45:55Z nayan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   nayan<nayan@taobao.com>
 *      - modify 2009-03-27
 *
 */
#include "common/define.h"
#include "admin_cmd_message.h"

using namespace tfs::common;
namespace tfs
{
  namespace message
  {
    AdminCmdMessage::AdminCmdMessage() : type_(ADMIN_CMD_NONE)
    {
      _packetHeader._pcode = ADMIN_CMD_MESSAGE;
    }

    AdminCmdMessage::AdminCmdMessage(int32_t cmd_type) : type_(cmd_type)
    {
      _packetHeader._pcode = ADMIN_CMD_MESSAGE;
    }

    AdminCmdMessage::~AdminCmdMessage()
    {
    }

    int AdminCmdMessage::parse(char *data, int32_t len)
    {
      if (TFS_ERROR == get_int32(&data, &len, &type_))
      {
        return TFS_ERROR;
      }

      // request message only set index vector
      // response message only set status vector,

      int32_t count = 0;
      if (TFS_ERROR == get_int32(&data, &len, &count))
      {
        return TFS_ERROR;
      }

      if (ADMIN_CMD_RESP != type_) // request message
      {
        char* tmp = NULL;
        // get index
        for (int32_t i = 0; i < count; i++)
        {
          if (TFS_ERROR == get_string(&data, &len, &tmp))
          {
            return TFS_ERROR;
          }
          index_.push_back(tmp);
        }
      }
      else                      // response message
      {
        MonitorStatus* m_status = NULL;
        // get status
        for (int32_t i = 0; i < count; i++)
        {
          // not copy
          if (TFS_ERROR == get_object(&data, &len, reinterpret_cast<void**>(&m_status), sizeof(MonitorStatus)))
          {
            return TFS_ERROR;
          }
          monitor_status_.push_back(m_status);
        }
      }
      return TFS_SUCCESS;
    }

    int AdminCmdMessage::build(char *data, int32_t len)
    {
      if (set_int32(&data, &len, type_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      int32_t count = (ADMIN_CMD_RESP != type_) ? index_.size() : monitor_status_.size();
      if (set_int32(&data, &len, count) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      if (ADMIN_CMD_RESP != type_)
      {
        // set index
        for (int32_t i = 0; i < count; i++)
        {
          if (TFS_ERROR == set_string(&data, &len, const_cast<char*>(index_[i].c_str())))
          {
            return TFS_ERROR;
          }
        }
      }
      else                            // response message
      {
        // set status
        for (int32_t i = 0; i < count; i++)
        {
          if (set_object(&data, &len, monitor_status_[i], sizeof(MonitorStatus)) == TFS_ERROR)
          {
            return TFS_ERROR;
          }
        }
      }

      return TFS_SUCCESS;
    }

    int32_t AdminCmdMessage::message_length()
    {
      int32_t size = common::INT_SIZE * 2; // type_ and vector count

      if (type_ != ADMIN_CMD_RESP)
      {
        for (size_t i = 0; i < index_.size(); i++)
        {
          size += get_string_len(const_cast<char*>(index_[i].c_str()));
        }
      }
      else
      {
        size += sizeof(MonitorStatus)*monitor_status_.size();
      }
      return size;
    }

    char* AdminCmdMessage::get_name()
    {
      return "admincmdmessage";
    }

    Message* AdminCmdMessage::create(const int32_t type)
    {
      AdminCmdMessage *msg = new AdminCmdMessage();
      msg->set_message_type(type);
      return msg;
    }

  }
}
