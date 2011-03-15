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
#ifndef TFS_MESSAGE_SERVERMETAINFOMESSAGE_H_
#define TFS_MESSAGE_SERVERMETAINFOMESSAGE_H_

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
    class ServerMetaInfoMessage: public Message
    {
    public:
      ServerMetaInfoMessage();
      virtual ~ServerMetaInfoMessage();

      virtual int parse(char* data, int32_t len);
      virtual int build(char* data, int32_t len);
      virtual int32_t message_length();
      virtual char* get_name();
      static Message* create(const int32_t type);
    protected:
      int32_t status_;
      char* str_;
    };

    class RespServerMetaInfoMessage: public Message
    {
    public:
      RespServerMetaInfoMessage();
      virtual ~RespServerMetaInfoMessage();

      void alloc_data();
      inline void set_server_inode_info(common::ServerMetaInfo* const server_meta_info)
      {
        server_meta_info_ = server_meta_info;
      }
      inline common::ServerMetaInfo* get_server_inode_info() const
      {
        return server_meta_info_;
      }
      void set_data(const common::ServerMetaInfo* server_meta_info);

      virtual int parse(char* data, int32_t len);
      virtual int build(char* data, int32_t len);
      virtual int32_t message_length();
      virtual char* get_name();
      static Message* create(const int32_t type);
    protected:
      common::ServerMetaInfo* server_meta_info_;
      int32_t length_;
      bool alloc_;
    };
  }
}
#endif
