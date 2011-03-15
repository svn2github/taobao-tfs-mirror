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
#ifndef TFS_MESSAGE_FILEINFOMESSAGE_H_
#define TFS_MESSAGE_FILEINFOMESSAGE_H_

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string>
#include <errno.h>
#include "common/interval.h"
#include "message.h"

namespace tfs
{
  namespace message
  {
    class FileInfoMessage: public Message
    {
      public:
        FileInfoMessage();
        virtual ~FileInfoMessage();

        inline void set_block_id(const uint32_t block_id)
        {
          block_id_ = block_id;
        }
        inline uint32_t get_block_id() const
        {
          return block_id_;
        }
        inline void set_file_id(const uint64_t file_id)
        {
          file_id_ = file_id;
        }

        inline uint64_t get_file_id() const
        {
          return file_id_;
        }

        inline void set_mode(const int32_t mode)
        {
          mode_ = mode;
        }

        inline int32_t get_mode() const
        {
          return mode_;
        }

        virtual int parse(char* data, int32_t len);
        virtual int build(char* data, int32_t len);
        virtual int32_t message_length();
        virtual char* get_name();

        static Message* create(const int32_t type);
      protected:
        uint32_t block_id_;
        uint64_t file_id_;
        int32_t mode_;
    };

    class RespFileInfoMessage: public Message
    {
      public:
        RespFileInfoMessage();
        virtual ~RespFileInfoMessage();

        inline void set_file_info(common::FileInfo* const file_info)
        {
          file_info_ = file_info;
        }
        inline common::FileInfo* get_file_info() const
        {
          return file_info_;
        }

        virtual int parse(char* data, int32_t len);
        virtual int build(char* data, int32_t len);
        virtual int32_t message_length();
        virtual char* get_name();

        static Message* create(const int32_t type);
      protected:
        common::FileInfo* file_info_;
    };
  }
}
#endif
