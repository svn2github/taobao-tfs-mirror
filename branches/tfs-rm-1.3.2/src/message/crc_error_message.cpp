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
#include "crc_error_message.h"

using namespace tfs::common;
namespace tfs
{
  namespace message
  {
    CrcErrorMessage::CrcErrorMessage() :
      block_id_(0), file_id_(0), crc_(0), error_flag_(CRC_DS_PATIAL_ERROR)
    {
      _packetHeader._pcode = CRC_ERROR_MESSAGE;
    }

    CrcErrorMessage::~CrcErrorMessage()
    {
    }

    int CrcErrorMessage::parse(char* data, int32_t len)
    {
      if (get_int32(&data, &len, reinterpret_cast<int32_t*> (&block_id_)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_int64(&data, &len, reinterpret_cast<int64_t*> (&file_id_)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_int32(&data, &len, reinterpret_cast<int32_t*> (&crc_)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_int32(&data, &len, reinterpret_cast<int32_t*> (&error_flag_)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      get_vint64(&data, &len, fail_server_);
      return TFS_SUCCESS;
    }

    int32_t CrcErrorMessage::message_length()
    {
      int32_t len = INT_SIZE * 3 + INT64_SIZE + get_vint64_len(fail_server_);
      return len;
    }

    int CrcErrorMessage::build(char* data, int32_t len)
    {
      if (set_int32(&data, &len, block_id_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_int64(&data, &len, file_id_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_int32(&data, &len, crc_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_int32(&data, &len, error_flag_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_vint64(&data, &len, fail_server_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      return TFS_SUCCESS;
    }

    char* CrcErrorMessage::get_name()
    {
      return "crcerrormessage";
    }

    Message* CrcErrorMessage::create(const int32_t type)
    {
      CrcErrorMessage* req_ce_msg = new CrcErrorMessage();
      req_ce_msg->set_message_type(type);
      return req_ce_msg;
    }
  }
}
