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
#ifndef TFS_MESSAGE_OPLOGSYNCMESSAGE_H_
#define TFS_MESSAGE_OPLOGSYNCMESSAGE_H_ 

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
    class OpLogSyncMessage: public Message
    {
      public:
        OpLogSyncMessage();
        virtual ~OpLogSyncMessage();
        virtual int parse(char* data, int32_t len);
        virtual int build(char* data, int32_t len);
        virtual int32_t message_length();
        virtual char* get_name();
        void set_data(const char* data, int32_t length);
        int32_t get_length() const
        {
          return length_;
        }
        const char* get_data() const
        {
          return data_;
        }

        static Message* create(const int32_t type);

      private:
        bool alloc_;
        int32_t length_;
        char* data_;
    };

    enum OpLogSyncMsgCompleteFlag
    {
      OPLOG_SYNC_MSG_COMPLETE_YES = 0x00,
      OPLOG_SYNC_MSG_COMPLETE_NO
    };

    class OpLogSyncResponeMessage: public Message
    {
      public:
        OpLogSyncResponeMessage();
        virtual ~OpLogSyncResponeMessage();
        virtual int parse(char* data, int32_t len);
        virtual int build(char* data, int32_t len);
        virtual int32_t message_length();
        virtual char* get_name();
        uint8_t get_complete_flag() const
        {
          return complete_flag_;
        }
        void set_complete_flag(uint8_t flag = OPLOG_SYNC_MSG_COMPLETE_YES)
        {
          complete_flag_ = flag;
        }

        static Message* create(const int32_t type);
      private:
        OpLogSyncResponeMessage(const OpLogSyncResponeMessage&);
        OpLogSyncResponeMessage operator=(const OpLogSyncResponeMessage&);
      private:
        uint8_t complete_flag_;
    };

  }
}
#endif
