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

#ifndef TFS_COMMON_STREAM_H_
#define TFS_COMMON_STREAM_H_ 

#include "define.h"
#include "buffer.h"
#include "serialization.h"
namespace tfs
{
  namespace common
  {
    class BasePacket;
    class Stream 
    {
    friend class BasePacket;
    public:
      Stream();
      explicit Stream(const int64_t length);
      virtual ~Stream();

      char* get_data() const;
      int64_t get_data_length() const;

      Buffer& get_buffer() { return buffer_;}

      int set_int8(const int8_t value);
      int set_int16(const int16_t value);
      int set_int32(const int32_t value);
      int set_int64(const int64_t value);
      int set_bytes(const void* data, const int64_t length);
      int set_string(const char* str);
      int set_vint32(const std::vector<uint32_t>& value);
      int set_vint64(const std::vector<uint64_t>& value);

      int get_int8(int8_t* value);
      int get_int16(int16_t* value);
      int get_int32(int32_t* value);
      int get_int64(int64_t* value);
      int get_bytes(void* data, const int64_t length);
      int get_string(char* str, const int64_t length);
      int get_vint32(std::vector<uint32_t>& value);
      int get_vint64(std::vector<uint64_t>& value);
    private:
      void expand(const int64_t length);

      DISALLOW_COPY_AND_ASSIGN(Stream); 
      Buffer buffer_;
      void clear();
    };
  }//end namespace comon
}//end namespace tfs

#endif /*TFS_COMMON_STREAM_H_*/
