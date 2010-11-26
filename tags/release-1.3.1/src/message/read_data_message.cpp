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
#include "read_data_message.h"

using namespace tfs::common;
namespace tfs
{
  namespace message
  {
    ReadDataMessage::ReadDataMessage()
    {
      _packetHeader._pcode = READ_DATA_MESSAGE;
      memset(&read_data_info_, 0, sizeof(ReadDataInfo));
    }

    ReadDataMessage::~ReadDataMessage()
    {
    }

    int ReadDataMessage::parse(char* data, int32_t len)
    {
      if (get_object_copy(&data, &len, reinterpret_cast<void*>  (&read_data_info_), sizeof(ReadDataInfo)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      return TFS_SUCCESS;
    }

    int32_t ReadDataMessage::message_length()
    {
      int32_t len = sizeof(ReadDataInfo);
      return len;
    }

    int ReadDataMessage::build(char* data, int32_t len)
    {
      if (set_object(&data, &len, reinterpret_cast<void*> (&read_data_info_), sizeof(ReadDataInfo)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      return TFS_SUCCESS;
    }

    char* ReadDataMessage::get_name()
    {
      return "readdatamessage";
    }

    Message* ReadDataMessage::create(const int32_t type)
    {
      ReadDataMessage* req_rd_msg = new ReadDataMessage();
      req_rd_msg->set_message_type(type);
      return req_rd_msg;
    }

    RespReadDataMessage::RespReadDataMessage() :
      data_(NULL), length_(-1), alloc_(false)
    {
      _packetHeader._pcode = RESP_READ_DATA_MESSAGE;
    }

    RespReadDataMessage::~RespReadDataMessage()
    {
      if ((data_ != NULL) && (alloc_ == true))
      {
        ::free(data_);
        data_ = NULL;
      }
    }

    char* RespReadDataMessage::alloc_data(int32_t len)
    {
      if (len < 0)
      {
        return NULL;
      }
      if (len == 0)
      {
        length_ = len;
        return NULL;
      }
      if (data_ != NULL)
      {
        ::free(data_);
        data_ = NULL;
      }
      length_ = len;
      data_ = (char*) malloc(len);
      alloc_ = true;
      return data_;
    }

    int RespReadDataMessage::parse(char* data, int32_t len)
    {
      if (get_int32(&data, &len, &length_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (length_ > 0)
      {
        if (get_object(&data, &len, reinterpret_cast<void**> (&data_), length_) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }
      return TFS_SUCCESS;
    }

    int32_t RespReadDataMessage::message_length()
    {
      int32_t len = INT_SIZE;
      if ((length_ > 0) && (data_ != NULL))
      {
        len += length_;
      }
      return len;
    }

    int RespReadDataMessage::build(char* data, int32_t len)
    {
      if (set_int32(&data, &len, length_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if ((length_ > 0) && (data_ != NULL))
      {
        if (set_object(&data, &len, data_, length_) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }
      return TFS_SUCCESS;
    }

    char* RespReadDataMessage::get_name()
    {
      return "respreaddatamessage";
    }

    Message* RespReadDataMessage::create(const int32_t type)
    {
      RespReadDataMessage* resp_rd_msg = new RespReadDataMessage();
      resp_rd_msg->set_message_type(type);
      return resp_rd_msg;
    }

    ReadDataMessageV2::ReadDataMessageV2()
    {
      _packetHeader._pcode = READ_DATA_MESSAGE_V2;
      memset(&read_data_info_, 0, sizeof(ReadDataInfo));
    }

    ReadDataMessageV2::~ReadDataMessageV2()
    {
    }

    int ReadDataMessageV2::parse(char* data, int32_t len)
    {
      if (get_object_copy(&data, &len, reinterpret_cast<void*> (&read_data_info_), sizeof(ReadDataInfo)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      return TFS_SUCCESS;
    }

    int32_t ReadDataMessageV2::message_length()
    {
      int32_t len = sizeof(ReadDataInfo);
      return len;
    }

    int ReadDataMessageV2::build(char* data, int32_t len)
    {
      if (set_object(&data, &len, reinterpret_cast<void*> (&read_data_info_), sizeof(ReadDataInfo)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      return TFS_SUCCESS;
    }

    char* ReadDataMessageV2::get_name()
    {
      return "readdatamessagev2";
    }

    Message* ReadDataMessageV2::create(const int32_t type)
    {
      ReadDataMessageV2* req_rdv2_msg = new ReadDataMessageV2();
      req_rdv2_msg->set_message_type(type);
      return req_rdv2_msg;
    }

    RespReadDataMessageV2::RespReadDataMessageV2() :
      data_(NULL), length_(-1), alloc_(false), file_info_(NULL)
    {
      _packetHeader._pcode = RESP_READ_DATA_MESSAGE_V2;
    }

    RespReadDataMessageV2::~RespReadDataMessageV2()
    {
      if ((data_ != NULL) && (alloc_ == true))
      {
        ::free(data_);
        data_ = NULL;
      }
    }

    char* RespReadDataMessageV2::alloc_data(int32_t len)
    {
      if (len < 0)
      {
        return NULL;
      }
      if (len == 0)
      {
        length_ = len;
        return NULL;
      }
      if (data_ != NULL)
      {
        ::free(data_);
        data_ = NULL;
      }
      length_ = len;
      data_ = (char*) malloc(len);
      alloc_ = true;
      return data_;
    }

    int RespReadDataMessageV2::parse(char* data, int32_t len)
    {
      if (get_int32(&data, &len, &length_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (length_ > 0)
      {
        if (get_object(&data, &len, reinterpret_cast<void**> (&data_), length_) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }
      int32_t size;
      if (get_int32(&data, &len, &size) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (size > 0)
      {
        if (get_object(&data, &len, reinterpret_cast<void**> (&file_info_), FILEINFO_SIZE) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }
      return TFS_SUCCESS;
    }

    int32_t RespReadDataMessageV2::message_length()
    {
      int32_t len = INT_SIZE * 2;
      if ((length_ > 0) && (data_ != NULL))
      {
        len += length_;
      }
      if (file_info_ != NULL)
      {
        len += FILEINFO_SIZE;
      }
      return len;
    }

    int RespReadDataMessageV2::build(char* data, int32_t len)
    {
      if (set_int32(&data, &len, length_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if ((length_ > 0) && (data_ != NULL))
      {
        if (set_object(&data, &len, data_, length_) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }

      int32_t size = 0;
      if (file_info_ != NULL)
      {
        size = FILEINFO_SIZE;
      }
      if (set_int32(&data, &len, size) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (size > 0)
      {
        if (set_object(&data, &len, file_info_, size) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }
      return TFS_SUCCESS;
    }

    char* RespReadDataMessageV2::get_name()
    {
      return "respreaddatamessagev2";
    }

    Message* RespReadDataMessageV2::create(const int32_t type)
    {
      RespReadDataMessageV2* resp_rdv2_msg = new RespReadDataMessageV2();
      resp_rdv2_msg->set_message_type(type);
      return resp_rdv2_msg;
    }

    ReadRawDataMessage::ReadRawDataMessage()
    {
      _packetHeader._pcode = READ_RAW_DATA_MESSAGE;
      memset(&read_data_info_, 0, sizeof(ReadDataInfo));
    }

    ReadRawDataMessage::~ReadRawDataMessage()
    {

    }

    int ReadRawDataMessage::parse(char* data, int32_t len)
    {
      if (get_object_copy(&data, &len, reinterpret_cast<void*> (&read_data_info_), sizeof(ReadDataInfo)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      return TFS_SUCCESS;
    }

    int ReadRawDataMessage::build(char* data, int32_t len)
    {
      if (set_object(&data, &len, reinterpret_cast<void*> (&read_data_info_), sizeof(ReadDataInfo)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      return TFS_SUCCESS;
    }

    char* ReadRawDataMessage::get_name()
    {
      return "readdatabatchmessage";
    }

    int32_t ReadRawDataMessage::message_length()
    {
      int32_t len = sizeof(ReadDataInfo);
      return len;
    }

    Message* ReadRawDataMessage::create(const int32_t type)
    {
      ReadRawDataMessage* req_rrd_msg = new ReadRawDataMessage();
      req_rrd_msg->set_message_type(type);
      return req_rrd_msg;
    }

    RespReadRawDataMessage::RespReadRawDataMessage() :
      data_(NULL), length_(-1), alloc_(false)
    {
      _packetHeader._pcode = RESP_READ_RAW_DATA_MESSAGE;
    }

    RespReadRawDataMessage::~RespReadRawDataMessage()
    {
      if ((data_ != NULL) && (alloc_ == true))
      {
        ::free(data_);
        data_ = NULL;
      }
    }

    int RespReadRawDataMessage::parse(char* data, int32_t len)
    {
      if (get_int32(&data, &len, &length_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (length_ > 0)
      {
        if (get_object(&data, &len, reinterpret_cast<void**> (&data), length_) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }
      return TFS_SUCCESS;
    }

    int RespReadRawDataMessage::build(char* data, int32_t len)
    {
      if (set_int32(&data, &len, length_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if ((length_ > 0) && (data_ != NULL))
      {
        if (set_object(&data, &len, data_, length_) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }
      return TFS_SUCCESS;
    }

    int32_t RespReadRawDataMessage::message_length()
    {
      int32_t len = INT_SIZE;
      if ((length_ > 0) && (data_ != NULL))
      {
        len += length_;
      }
      return len;
    }

    char* RespReadRawDataMessage::alloc_data(int32_t len)
    {
      if (len < 0)
      {
        return NULL;
      }
      if (len == 0)
      {
        length_ = len;
        return NULL;
      }
      if (data_ != NULL)
      {
        ::free(data_);
        data_ = NULL;
      }
      length_ = len;
      data_ = (char*) malloc(len);
      alloc_ = true;
      return data_;
    }

    char* RespReadRawDataMessage::get_name()
    {
      return "respreaddatabatchmessage";
    }

    Message* RespReadRawDataMessage::create(const int32_t type)
    {
      RespReadRawDataMessage* resp_rdd_msg = new RespReadRawDataMessage();
      resp_rdd_msg->set_message_type(type);
      return resp_rdd_msg;
    }

    ReadScaleImageMessage::ReadScaleImageMessage()
    {
      // TODO modify _pcode
      _packetHeader._pcode = READ_SCALE_IMAGE_MESSAGE;
      memset(&zoom_, 0, sizeof(ZoomData));
    }

    ReadScaleImageMessage::~ReadScaleImageMessage()
    {
    }

    int32_t ReadScaleImageMessage::parse(char* data, int32_t len)
    {
      if (get_object_copy(&data, &len, reinterpret_cast<void*> (&read_data_info_), sizeof(ReadDataInfo)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_object_copy(&data, &len, reinterpret_cast<void*> (&zoom_), sizeof(ZoomData)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      return TFS_SUCCESS;
    }

    int32_t ReadScaleImageMessage::build(char* data, int32_t len)
    {
      if (set_object(&data, &len, reinterpret_cast<void*> (&read_data_info_), sizeof(ReadDataInfo)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_object(&data, &len, reinterpret_cast<void*> (&zoom_), sizeof(ZoomData)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      return TFS_SUCCESS;
    }

    int32_t ReadScaleImageMessage::message_length()
    {
      int32_t len = ReadDataMessageV2::message_length() + sizeof(ZoomData);
      return len;
    }

    char* ReadScaleImageMessage::get_name()
    {
      return "readscaleimagemessage";
    }

    Message* ReadScaleImageMessage::create(const int32_t type)
    {
      ReadScaleImageMessage* req_rsi_msg = new ReadScaleImageMessage();
      req_rsi_msg->set_message_type(type);
      return req_rsi_msg;
    }
  }
}
