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
#include "rename_file_message.h"

using namespace tfs::common;

namespace tfs
{
  namespace message
  {
    RenameFileMessage::RenameFileMessage() :
      option_flag_(0)
    {
      _packetHeader._pcode = RENAME_FILE_MESSAGE;
      memset(&rename_file_info_, 0, sizeof(RenameFileInfo));
    }

    RenameFileMessage::~RenameFileMessage()
    {
    }

    int RenameFileMessage::parse(char* data, int32_t len)
    {
      if (get_object_copy(&data, &len, reinterpret_cast<void*>(&rename_file_info_), sizeof(RenameFileInfo)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_vint64(&data, &len, ds_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      get_int32(&data, &len, &option_flag_);
      has_lease_ = parse_special_ds(ds_, version_, lease_id_);

      return TFS_SUCCESS;
    }

    int32_t RenameFileMessage::message_length()
    {
      int32_t len = sizeof(RenameFileInfo) + get_vint64_len(ds_) + INT_SIZE;
      if (has_lease_ == true)
      {
        len += INT64_SIZE * 3;
      }
      return len;
    }

    int RenameFileMessage::build(char* data, int32_t len)
    {
      if (has_lease_ == true)
      {
        ds_.push_back(ULONG_LONG_MAX);
        ds_.push_back(static_cast<uint64_t> (version_));
        ds_.push_back(static_cast<uint64_t> (lease_id_));
      }

      if (set_object(&data, &len, &rename_file_info_, sizeof(RenameFileInfo)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_vint64(&data, &len, ds_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_int32(&data, &len, option_flag_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      has_lease_ = parse_special_ds(ds_, version_, lease_id_);

      return TFS_SUCCESS;
    }

    char* RenameFileMessage::get_name()
    {
      return "renamefilemessage";
    }

    Message* RenameFileMessage::create(const int32_t type)
    {
      RenameFileMessage* req_rf_msg = new RenameFileMessage();
      req_rf_msg->set_message_type(type);
      return req_rf_msg;
    }
  }
}
