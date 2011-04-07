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
#ifndef TFS_MESSAGE_ROLLBACKMESSAGE_H_
#define TFS_MESSAGE_ROLLBACKMESSAGE_H_

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string>
#include <errno.h>

#include "message.h"

namespace tfs
{
  namespace message
  {
    class RollbackMessage: public Message
    {
    public:
      RollbackMessage();
      virtual ~RollbackMessage();

      virtual int parse(char* data, int32_t len);
      virtual int build(char* data, int32_t len);
      virtual int32_t message_length();
      virtual char* get_name();

      static Message* create(const int32_t type);
    public:
      inline int32_t get_act_type() const
      {
        return act_type_;
      }

      inline void set_act_type(const int32_t type)
      {
        act_type_ = type;
      }

      inline common::BlockInfo* get_block_info() const
      {
        return block_info_;
      }

      inline void set_block_info(common::BlockInfo* const block_info)
      {
        block_info_ = block_info;
      }

      inline common::FileInfo* get_file_info() const
      {
        return file_info_;
      }

      inline void set_file_info(common::FileInfo* const file_info)
      {
        file_info_ = file_info;
      }

    private:
      int32_t act_type_;
      common::BlockInfo* block_info_;
      common::FileInfo* file_info_;
    };
  }
}
#endif
