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
#ifndef TFS_MESSAGE_CLIENTCMDMESSAGE_H_
#define TFS_MESSAGE_CLIENTCMDMESSAGE_H_

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
#pragma pack(4)
    struct ClientCmdInfo
    {
      int32_t type_;
      uint64_t server_id_;
      uint32_t block_id_;
      uint32_t version_;
      uint64_t from_server_id_;
    };
#pragma pack()

    // Client Command
    enum ClientCmd
    {
      CLIENT_CMD_EXPBLK = 1,
      CLIENT_CMD_LOADBLK,
      CLIENT_CMD_COMPACT,
      CLIENT_CMD_IMMEDIATELY_REPL,
      CLIENT_CMD_REPAIR_GROUP,
      CLIENT_CMD_SET_PARAM,
      CLIENT_CMD_UNLOADBLK,
      CLIENT_CMD_GET_REPL_INFO,
      CLIENT_CMD_CLEAR_REPL_INFO
    };

    class ClientCmdMessage: public Message
    {
    public:
      ClientCmdMessage();
      virtual ~ClientCmdMessage();

      inline void set_type(const int32_t type)
      {
        client_cmd_info_.type_ = type;
      }

      inline int32_t get_type() const
      {
        return client_cmd_info_.type_;
      }

      inline void set_server_id(const uint64_t server_id)
      {
        client_cmd_info_.server_id_ = server_id;
      }

      inline uint64_t get_server_id() const
      {
        return client_cmd_info_.server_id_;
      }

      inline void set_block_id(const uint32_t block_id)
      {
        client_cmd_info_.block_id_ = block_id;
      }

      inline uint32_t get_block_id() const
      {
        return client_cmd_info_.block_id_;
      }

      inline void set_version(const uint32_t version)
      {
        client_cmd_info_.version_ = version;
      }

      inline uint32_t get_version() const
      {
        return client_cmd_info_.version_;
      }

      inline void set_from_server_id(const uint64_t from_server_id)
      {
        client_cmd_info_.from_server_id_ = from_server_id;
      }

      inline uint64_t get_from_server_id() const
      {
        return client_cmd_info_.from_server_id_;
      }

      virtual int parse(char *data, int32_t len);
      virtual int build(char *data, int32_t len);
      virtual int32_t message_length();
      virtual char *get_name();

      static Message* create(const int32_t type);
    protected:
      ClientCmdInfo client_cmd_info_;
    };
  }
}
#endif
