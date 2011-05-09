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
 *   duolong <duolong@taobao.com>
 *
 */

#ifndef TFS_COMMON_SERIALIZATION_H_
#define TFS_COMMON_SERIALIZATION_H_ 
#include "define.h"

namespace tfs
{
  namespace common
  {
    struct Serialization 
    {
      static int get_int8(const char* data, const int64_t data_len, int64_t& pos, int8_t* value)
      {
        int32_t iret = NULL != value && NULL != data && data_len - pos >= INT8_SIZE &&  pos >= 0 ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          *value = data[pos++];
        }
        return iret;
      }

      static int get_int16(const char* data, const int64_t data_len, int64_t& pos, int16_t* value)
      {
        int32_t iret = NULL != value && NULL != data && data_len - pos >= INT16_SIZE  &&  pos >= 0 ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          int64_t tmp = pos += INT16_SIZE;
          *value = static_cast<unsigned char>(data[--tmp]);
          *value <<= 8;
          *value |= static_cast<unsigned char>(data[--tmp]);
        }
        return iret;
      }

      static int get_int32(const char* data, const int64_t data_len, int64_t& pos, int32_t* value)
      {
        int32_t iret = NULL != value && NULL != data && data_len - pos >= INT_SIZE &&  pos >= 0 ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          int64_t tmp = pos += INT_SIZE;
          *value = static_cast<unsigned char>(data[--tmp]);
          *value <<= 8;
          *value |= static_cast<unsigned char>(data[--tmp]);
          *value <<= 8;
          *value |= static_cast<unsigned char>(data[--tmp]);
          *value <<= 8;
          *value |= static_cast<unsigned char>(data[--tmp]);
        }
        return iret;
      }

      static int get_int64(const char* data, const int64_t data_len, int64_t& pos, int64_t* value)
      {
        int32_t iret = NULL != value && NULL != data && data_len - pos >= INT64_SIZE &&  pos >= 0 ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          int64_t tmp = pos += INT64_SIZE;
          *value = static_cast<unsigned char>(data[--tmp]);
          *value <<= 8;
          *value |= static_cast<unsigned char>(data[--tmp]);
          *value <<= 8;
          *value |= static_cast<unsigned char>(data[--tmp]);
          *value <<= 8;
          *value |= static_cast<unsigned char>(data[--tmp]);
          *value <<= 8;
          *value |= static_cast<unsigned char>(data[--tmp]);
          *value <<= 8;
          *value |= static_cast<unsigned char>(data[--tmp]);
          *value <<= 8;
          *value |= static_cast<unsigned char>(data[--tmp]);
          *value <<= 8;
          *value |= static_cast<unsigned char>(data[--tmp]);
        }
        return iret;
      }

      static int get_bytes(const char* data, const int64_t data_len, int64_t& pos, void* buf, const int64_t buf_length)
      {
        int32_t iret = NULL != data && NULL != buf && buf_length > 0 && data_len - pos > buf_length &&  pos >= 0 ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          memcpy(buf, (data+pos), buf_length);
          pos += buf_length;
        }
        return iret;
      }

      static int get_string(const char* data, const int64_t data_len, int64_t& pos, char* str, const int64_t str_buf_length)
      {
        int32_t iret = NULL != data &&  data_len - pos >= INT_SIZE  &&  pos >= 0 ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          int32_t length  = 0;
          iret = get_int32(data, data_len, pos, &length);
          if (TFS_SUCCESS == iret)
          {
            if (length > 0)
            {
              iret = length <= str_buf_length ? TFS_SUCCESS : TFS_ERROR;
              if (TFS_SUCCESS == iret)
              {
                if (NULL == str)
                {
                  iret = TFS_ERROR;
                }
                else
                {
                  iret = data_len - pos >= length ? TFS_SUCCESS : TFS_ERROR;
                  if (TFS_SUCCESS == iret)
                  {
                    memcpy(str, (data+pos), length);
                    pos += length;
                  }
                }
              }
              else
              {
                pos -= INT_SIZE;
              }
            }
          }
        }
        return iret;
      }

      static int set_int8(char* data, const int64_t data_len, int64_t& pos, const int8_t value)
      {
        int32_t iret = NULL != data && data_len - pos >= INT8_SIZE  &&  pos >= 0 ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          data[pos++] = value;
        }
        return iret;
      }

      static int set_int16(char* data, const int64_t data_len, int64_t& pos, const int16_t value)
      {
        int32_t iret = NULL != data && data_len - pos >= INT16_SIZE &&  pos >= 0 ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          data[pos++] = value & 0xFF;
          data[pos++]= (value >> 8) & 0xFF;
        }
        return iret;
      }

      static int set_int32(char* data, const int64_t data_len, int64_t& pos, const int32_t value)
      {
        int32_t iret = NULL != data && data_len - pos >= INT_SIZE &&  pos >= 0 ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          data[pos++] =  value & 0xFF;
          data[pos++] =  (value >> 8) & 0xFF;
          data[pos++] =  (value >> 16) & 0xFF;
          data[pos++] =  (value >> 24) & 0xFF;
        }
        return iret;
      }

      static int set_int64(char* data, const int64_t data_len, int64_t& pos, const int64_t value)
      {
        int32_t iret = NULL != data && data_len - pos >= INT64_SIZE &&  pos >= 0 ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          data[pos++] =  (value) & 0xFF;
          data[pos++] =  (value >> 8) & 0xFF;
          data[pos++] =  (value >> 16) & 0xFF;
          data[pos++] =  (value >> 24) & 0xFF;
          data[pos++] =  (value >> 32) & 0xFF;
          data[pos++] =  (value >> 40) & 0xFF;
          data[pos++] =  (value >> 48) & 0xFF;
          data[pos++] =  (value >> 56) & 0xFF;
        }
        return iret;
      }

      static int set_string(char* data, const int64_t data_len, int64_t& pos, const char* str)
      {
        int32_t iret = NULL != data &&  pos < data_len &&  pos >= 0 ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          int64_t length = NULL == str ? 0 : strlen(str);
          iret = data_len - pos >= (length + INT_SIZE) ? TFS_SUCCESS : TFS_ERROR;
          if (TFS_SUCCESS == iret)
          {
            iret = set_int32(data, data_len, pos, length);
            if (TFS_SUCCESS == iret)
            {
              memcpy((data+pos), str, length);
              pos += length;
            }
          }
        }
        return iret;
      }

      static int set_bytes(char* data, const int64_t data_len, int64_t& pos, const void* buf, const int64_t buf_length)
      {
        int32_t iret = NULL != data && buf_length > 0 && NULL != buf && data_len - pos >= buf_length  &&  pos >= 0 ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          memcpy((data+pos), buf, buf_length);
          pos += buf_length;
        }
        return iret;
      }
    };
  }//end namespace comon
}//end namespace tfs

#endif /** TFS_COMMON_SERIALIZATION_H_ */

