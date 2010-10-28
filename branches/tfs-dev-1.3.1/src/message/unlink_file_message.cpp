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
#include "unlink_file_message.h"

using namespace tfs::common;

namespace tfs
{
  namespace message
  {
    UnlinkFileMessage::UnlinkFileMessage() :
      option_flag_(0), version_(0), lease_(0), has_lease_(false)
    {
      _packetHeader._pcode = UNLINK_FILE_MESSAGE;
      memset(&unlink_file_info_, 0, sizeof(UnlinkFileInfo));
    }

    UnlinkFileMessage::~UnlinkFileMessage()
    {
    }

    int UnlinkFileMessage::parse(char* data, int32_t len)
    {
      if (get_object_copy(&data, &len, reinterpret_cast<void*> (&unlink_file_info_), sizeof(UnlinkFileInfo)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_vint64(&data, &len, dataservers_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      get_int32(&data, &len, &option_flag_);
      has_lease_ = parse_special_ds(dataservers_, version_, lease_);
      return TFS_SUCCESS;
    }

    int32_t UnlinkFileMessage::message_length()
    {
      int32_t len = sizeof(UnlinkFileInfo) + get_vint64_len(dataservers_) + INT_SIZE;
      if (has_lease_ == true)
      {
        len += INT64_SIZE * 3;
      }
      return len;
    }

    int UnlinkFileMessage::build(char* data, int32_t len)
    {
      if (has_lease_ == true)
      {
        dataservers_.push_back(ULONG_LONG_MAX);
        dataservers_.push_back(static_cast<uint64_t> (version_));
        dataservers_.push_back(static_cast<uint64_t> (lease_));
      }

      if (set_object(&data, &len, &unlink_file_info_, sizeof(UnlinkFileInfo)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_vint64(&data, &len, dataservers_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_int32(&data, &len, option_flag_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      has_lease_ = parse_special_ds(dataservers_, version_, lease_);

      return TFS_SUCCESS;
    }

    char* UnlinkFileMessage::get_name()
    {
      return "unlinkfilemessage";
    }

    Message* UnlinkFileMessage::create(const int32_t type)
    {
      UnlinkFileMessage* req_uf_msg = new UnlinkFileMessage();
      req_uf_msg->set_message_type(type);
      return req_uf_msg;
    }
  }
}
