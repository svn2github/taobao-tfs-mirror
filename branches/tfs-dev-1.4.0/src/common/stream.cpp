/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *
 */

#include "stream.h"
namespace tfs
{
  namespace common
  {
    int64_t Stream::get_vuint8_length(const std::vector<uint8_t>& value)
    {
      return INT_SIZE + value.size() * INT8_SIZE;
    }
    int64_t Stream::get_vuint16_length(const std::vector<uint16_t>& value)
    {
      return INT_SIZE + value.size() * INT16_SIZE;
    }
    int64_t Stream::get_vuint32_length(const std::vector<uint32_t>& value)
    {
      return INT_SIZE + value.size() * INT_SIZE;
    }
    int64_t Stream::get_vuint64_length(const std::vector<uint64_t>& value)
    {
      return INT_SIZE + value.size() * INT64_SIZE;
    }
    int64_t Stream::get_vint8_length(const std::vector<int8_t>& value)
    {
      return INT_SIZE + value.size() * INT8_SIZE;
    }
    int64_t Stream::get_vint16_length(const std::vector<int16_t>& value)
    {
      return INT_SIZE + value.size() * INT16_SIZE;
    }
    int64_t Stream::get_vint32_length(const std::vector<int32_t>& value)
    {
      return INT_SIZE + value.size() * INT_SIZE;
    }
    int64_t Stream::get_vint64_length(const std::vector<int64_t>& value)
    {
      return INT_SIZE + value.size() * INT64_SIZE;
    }
    int64_t Stream::get_string_length(const char* str)
    {
      return NULL == str ? INT_SIZE : strlen(str) + INT_SIZE;
    }
    int64_t Stream::get_string_length(const std::string& str)
    {
      return str.empty() ? INT_SIZE : str.length() + INT_SIZE;
    }

    Stream::Stream()
    {

    }

    Stream::Stream(const int64_t length)
    {
      if (length > 0)
      {
        buffer_.expand(length);
      }
    }

    Stream::~Stream()
    {

    }

    char* Stream::get_data() const
    {
      return buffer_.get_data();
    }

    int64_t Stream::get_data_length() const
    {
      return buffer_.get_data_length();
    }
    char* Stream::get_free() const
    {
      return buffer_.get_free();
    }
    int64_t Stream::get_free_length() const
    {
      return buffer_.get_free_length();
    }

    void Stream::expand(const int64_t length)
    {
      buffer_.expand(length);
    }

    void Stream::clear()
    {
      buffer_.clear();
    }

    int Stream::set_int8(const int8_t value)
    {
      int64_t pos = 0;
      if (buffer_.get_free_length() < INT8_SIZE)
      {
        buffer_.expand(INT8_SIZE);
      }
      int32_t iret = Serialization::set_int8(buffer_.get_free(), buffer_.get_free_length(), pos, value); 
      if (TFS_SUCCESS == iret)
      {
        buffer_.pour(INT8_SIZE);
      }
      return iret;
    }

    int Stream::set_int16(const int16_t value)
    {
      int64_t pos = 0;
      if (buffer_.get_free_length() < INT16_SIZE)
      {
        buffer_.expand(INT16_SIZE);
      }
      int32_t iret = Serialization::set_int16(buffer_.get_free(), buffer_.get_free_length(), pos, value); 
      if (TFS_SUCCESS == iret)
      {
        buffer_.pour(INT16_SIZE);
      }
      return iret;
    }

    int Stream::set_int32(const int32_t value)
    {
      int64_t pos = 0;
      if (buffer_.get_free_length() < INT_SIZE)
      {
        buffer_.expand(INT_SIZE);
      }
      int32_t iret = Serialization::set_int32(buffer_.get_free(), buffer_.get_free_length(), pos, value); 
      if (TFS_SUCCESS == iret)
      {
        buffer_.pour(INT_SIZE);
      }
      return iret;
    }

    int Stream::set_int64(const int64_t value)
    {
      int64_t pos = 0;
      if (buffer_.get_free_length() < INT64_SIZE)
      {
        buffer_.expand(INT64_SIZE);
      }
      int32_t iret = Serialization::set_int64(buffer_.get_free(), buffer_.get_free_length(), pos, value); 
      if (TFS_SUCCESS == iret)
        buffer_.pour(INT64_SIZE);
      return iret;
    }

    int Stream::set_bytes(const void* data, const int64_t length)
    {
      int64_t pos = 0;
      if (buffer_.get_free_length() < length)
      {
        buffer_.expand(length);
      }
      int32_t iret = Serialization::set_bytes(buffer_.get_free(), buffer_.get_free_length(), pos, data, length); 
      if (TFS_SUCCESS == iret)
      {
        buffer_.pour(length);
      }
      return iret;
    }

    int Stream::set_string(const char* str)
    {
      int64_t pos = 0;
      int64_t length = NULL == str ? 0 : strlen(str);
      if (buffer_.get_free_length() < length + INT_SIZE)
      {
        buffer_.expand(length + INT_SIZE);
      }
      int32_t iret = Serialization::set_string(buffer_.get_free(), buffer_.get_free_length(), pos, str); 
      if (TFS_SUCCESS == iret)
      {
        buffer_.pour(length);
      }
      return iret;
    }

    int Stream::set_vint32(const std::vector<uint32_t>& value)
    {
      int64_t pos = 0;
      int64_t size = value.size() * INT_SIZE + INT_SIZE;
      if (buffer_.get_free_length() < size)
      {
        buffer_.expand(size);
      }

      int32_t iret = Serialization::set_int32(buffer_.get_free(), buffer_.get_free_length(), pos, value.size());
      if (TFS_SUCCESS == iret)
      {
        std::vector<uint32_t>::const_iterator iter = value.begin();
        for (; iter != value.end(); ++iter)
        {
          iret = Serialization::set_int32(buffer_.get_free(), buffer_.get_free_length(), pos, (*iter));
          if (TFS_SUCCESS != iret)
          {
            break;
          }
        }
        if (TFS_SUCCESS == iret)
        {
          buffer_.pour(size);
        }
      }
      return iret;
    }

    int Stream::set_vint64(const std::vector<uint64_t>& value)
    {
      int64_t pos = 0;
      int64_t size = value.size() * INT64_SIZE + INT_SIZE;
      if (buffer_.get_free_length() < size)
      {
        buffer_.expand(size);
      }
      int32_t iret = Serialization::set_int32(buffer_.get_free(), buffer_.get_free_length(), pos, value.size());
      if (TFS_SUCCESS == iret)
      {
        std::vector<uint64_t>::const_iterator iter = value.begin();
        for (; iter != value.end(); ++iter)
        {
          iret = Serialization::set_int64(buffer_.get_free(), buffer_.get_free_length(), pos, (*iter));
          if (TFS_SUCCESS != iret)
          {
            break;
          }
        }
        if (TFS_SUCCESS == iret)
        {
          buffer_.pour(size);
        }
      }
      return iret;
    }

    int Stream::get_int8(int8_t* value)
    {
      int64_t pos = 0;
      int32_t iret = Serialization::get_int8(buffer_.get_data(), buffer_.get_data_length(), pos, value);
      if (TFS_SUCCESS == iret)
      {
        buffer_.drain(INT8_SIZE);
      }
      return iret;
    }

    int Stream::get_int16(int16_t* value)
    {
      int64_t pos = 0;
      int32_t iret = Serialization::get_int16(buffer_.get_data(), buffer_.get_data_length(), pos, value);
      if (TFS_SUCCESS == iret)
      {
        buffer_.drain(INT16_SIZE);
      }
      return iret;
    }

    int Stream::get_int32(int32_t* value)
    {
      int64_t pos = 0;
      int32_t iret = Serialization::get_int32(buffer_.get_data(), buffer_.get_data_length(), pos, value);
      if (TFS_SUCCESS == iret)
      {
        buffer_.drain(INT_SIZE);
      }
      return iret;
    }

    int Stream::get_int64(int64_t* value)
    {
      int64_t pos = 0;
      int32_t iret = Serialization::get_int64(buffer_.get_data(), buffer_.get_data_length(), pos, value);
      if (TFS_SUCCESS == iret)
      {
        buffer_.drain(INT64_SIZE);
      }
      return iret;
    }

    int Stream::get_bytes(void* data, const int64_t length)
    {
      int64_t pos = 0;
      int32_t iret = Serialization::get_bytes(buffer_.get_data(), buffer_.get_data_length(), pos, data, length);
      if (TFS_SUCCESS == iret)
      {
        buffer_.drain(length);
      }
      return iret;
    }

    int Stream::get_string(char* str, const int64_t length)
    {
      int64_t pos = 0;
      int32_t iret = Serialization::get_string(buffer_.get_data(), buffer_.get_data_length(), pos, str, length);
      if (TFS_SUCCESS == iret)
      {
        buffer_.drain(length);
      }
      return iret;
    }

    int Stream::get_vint32(std::vector<uint32_t>& value)
    {
      int64_t pos = 0;
      int32_t length = 0;
      int32_t iret = Serialization::get_int32(buffer_.get_data(), buffer_.get_data_length(), pos, &length);
      if (TFS_SUCCESS == iret)
      {
        int32_t value = 0;
        for (int32_t i = 0; i < length; ++i)
        {
          iret = Serialization::get_int32(buffer_.get_data(), buffer_.get_data_length(), pos, &value);
          if (TFS_SUCCESS != iret)
          {
            break;
          }
        }
        if (TFS_SUCCESS == iret)
        {
          buffer_.drain(length + INT_SIZE);
        }
      }
      return iret;
    }

    int Stream::get_vint64(std::vector<uint64_t>& value)
    {
      int64_t pos = 0;
      int32_t length = 0;
      int32_t iret = Serialization::get_int32(buffer_.get_data(), buffer_.get_data_length(), pos, &length);
      if (TFS_SUCCESS == iret)
      {
        int64_t value = 0;
        for (int32_t i = 0; i < length; ++i)
        {
          iret = Serialization::get_int64(buffer_.get_data(), buffer_.get_data_length(), pos, &value);
          if (TFS_SUCCESS != iret)
          {
            break;
          }
        }
        if (TFS_SUCCESS == iret)
        {
          buffer_.drain(length + INT_SIZE);
        }
      }
      return iret;
    }
}//end namespace comon
}//end namespace tfs

