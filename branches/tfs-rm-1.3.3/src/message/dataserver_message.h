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
#ifndef TFS_MESSAGE_DATASERVERMESSAGE_H_
#define TFS_MESSAGE_DATASERVERMESSAGE_H_

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <vector>
#include <errno.h>
#include "message.h"

// the common::DataServerStatInfo information of block
// format:
// common::DataServerStatInfo, block_count, block_id, block_version, ...
namespace tfs
{
  namespace message
  {
    class SetDataserverMessage: public Message
    {
      public:
        SetDataserverMessage();
        virtual ~SetDataserverMessage();
        virtual int parse(char* data, int32_t len);
        virtual int build(char* data, int32_t len);
        virtual int32_t message_length();
        virtual char* get_name();

        void set_ds(common::DataServerStatInfo* ds);
        inline void set_has_block(const common::HasBlockFlag has_block)
        {
          has_block_ = has_block;
        }
        void add_block(common::BlockInfo* block_info);
        inline common::HasBlockFlag get_has_block() const
        {
          return has_block_;
        }
        inline common::DataServerStatInfo& get_ds()
        {
          return *ds_;
        }
        inline common::BLOCK_INFO_LIST& get_blocks()
        {
          return blocks_;
        }

        static Message* create(const int32_t type);

      protected:
        common::DataServerStatInfo* ds_;
        common::BLOCK_INFO_LIST blocks_;
        common::HasBlockFlag has_block_;
    };

    class SuspectDataserverMessage: public Message
    {
      public:
        SuspectDataserverMessage();
        virtual ~SuspectDataserverMessage();
        virtual int parse(char* data, int32_t len);
        virtual int build(char* data, int32_t len);
        virtual int32_t message_length();
        virtual char* get_name();
        static Message* create(const int32_t type);

        inline void set_server_id(const uint64_t server_id)
        {
          server_id_ = server_id;
        }
        inline uint64_t get_server_id() const
        {
          return server_id_;
        }
      protected:
        uint64_t server_id_;
    };
  }
}
#endif
