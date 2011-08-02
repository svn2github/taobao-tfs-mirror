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
#include "meta_nameserver_client_message.h"

using namespace tfs::common;
using namespace tfs::namemetaserver;
namespace tfs
{
  namespace message
  {
    FilepathActionMessage::FilepathActionMessage() :
      app_id_(0), user_id_(0), file_path_(), new_file_path_(), action_(NON_ACTION)
    {
      _packetHeader._pcode = FILEPATH_ACTION_MESSAGE;
    }

    FilepathActionMessage::~FilepathActionMessage()
    {

    }

    int FilepathActionMessage::deserialize(Stream& input)
    {
      int ret = TFS_ERROR;

      TBSYS_LOG(INFO, "input_size: %d", input.get_data_length());
      ret = input.get_int64(&app_id_);
      TBSYS_LOG(INFO, "app_id: %ld", app_id_);
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int64(&user_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_string(file_path_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_string(new_file_path_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int8(reinterpret_cast<int8_t*>(&action_));
      }
      return ret;
    }

    int64_t FilepathActionMessage::length() const
    {
      return 2 * common::INT64_SIZE + common::Serialization::get_string_length(file_path_) + common::Serialization::get_string_length(new_file_path_) + common::INT8_SIZE;
    }

    int FilepathActionMessage::serialize(Stream& output) const
    {
      int ret = TFS_SUCCESS;
      ret = output.set_int64(app_id_);
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int64(user_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_string(file_path_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_string(new_file_path_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int8(action_);
      }
      return ret;
    }

    WriteFilepathMessage::WriteFilepathMessage() :
      app_id_(0), user_id_(0), file_path_()
    {
      _packetHeader._pcode = WRITE_FILEPATH_MESSAGE;
    }

    WriteFilepathMessage::~WriteFilepathMessage()
    {

    }

    int WriteFilepathMessage::deserialize(Stream& input)
    {
      int ret = TFS_ERROR;

      ret = input.get_int64(&app_id_);
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int64(&user_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_string(file_path_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = frag_info_.deserialize(input);
      }
      return ret;
    }

    int64_t WriteFilepathMessage::length() const
    {
      return 2 * INT64_SIZE + common::Serialization::get_string_length(file_path_) + frag_info_.get_length();
    }

    int WriteFilepathMessage::serialize(Stream& output) const
    {
      int ret = TFS_SUCCESS;
      ret = output.set_int64(app_id_);
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int64(user_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_string(file_path_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = frag_info_.serialize(output);
      }
      return ret;
    }


    ReadFilepathMessage::ReadFilepathMessage() :
      app_id_(0), user_id_(0), file_path_(""), offset_(0), size_(0)
    {
      _packetHeader._pcode = READ_FILEPATH_MESSAGE;
    }

    ReadFilepathMessage::~ReadFilepathMessage()
    {

    }

    int ReadFilepathMessage::deserialize(Stream& input)
    {
      int ret = TFS_ERROR;

      ret = input.get_int64(&app_id_);
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int64(&user_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_string(file_path_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int64(&offset_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int64(&size_);
      }
      return ret;
    }

    int64_t ReadFilepathMessage::length() const
    {
      return 4 * common::INT64_SIZE + common::Serialization::get_string_length(file_path_);
    }

    int ReadFilepathMessage::serialize(Stream& output) const
    {
      int ret = TFS_SUCCESS;
      ret = output.set_int64(app_id_);
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int64(user_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_string(file_path_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int64(offset_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int64(size_);
      }
      return ret;
    }

    RespReadFilepathMessage::RespReadFilepathMessage() :
      still_have_(0)
    {
      _packetHeader._pcode = RESP_READ_FILEPATH_MESSAGE;
    }

    RespReadFilepathMessage::~RespReadFilepathMessage()
    {

    }

    int RespReadFilepathMessage::deserialize(Stream& input)
    {
      int ret = TFS_ERROR;

      ret = input.get_int8(reinterpret_cast<int8_t*>(&still_have_));
     
      if (TFS_SUCCESS == ret)
      {
        ret = frag_info_.deserialize(input);
      }
      return ret;
    }

    int64_t RespReadFilepathMessage::length() const
    {
      return INT8_SIZE + frag_info_.get_length();
    }

    int RespReadFilepathMessage::serialize(Stream& output) const
    {
      int ret = TFS_SUCCESS;
      ret = output.set_int8(still_have_);
      if (TFS_SUCCESS == ret)
      {
        ret = frag_info_.serialize(output);
      }
      return ret;
    }
  }
}
