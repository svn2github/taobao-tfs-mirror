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
#ifndef TFS_MESSAGE_STATUSMESSAGE_H_
#define TFS_MESSAGE_STATUSMESSAGE_H_

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
    class StatusMessage: public Message
    {
      public:
        StatusMessage();
        StatusMessage(const int32_t status, char* const str = NULL);
        void set_message(const int32_t status, char* const str = NULL);
        virtual ~StatusMessage();
        virtual int parse(char* data, int32_t len);
        virtual int build(char* data, int32_t len);
        virtual int32_t message_length();
        virtual char* get_name();
        char* get_error() const;
        int32_t get_status() const;

        static Message* create(const int32_t type);

      protected:
        int32_t status_;
        char* str_;
    };
  }
}

#endif
