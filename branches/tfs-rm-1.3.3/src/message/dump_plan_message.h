/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: dump_plan_message.h 2135 2011-03-25 06:57:01Z duanfei $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_MESSAGE_DUMP_PLAN_MESSAGE_H_
#define TFS_MESSAGE_DUMP_PLAN_MESSAGE_H_

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <vector>
#include <errno.h>
#include "message.h"

namespace tfs
{
  namespace message
  {
    class DumpPlanMessage: public Message
    {
      public:
        DumpPlanMessage();
        virtual ~DumpPlanMessage();
        virtual int parse(char* data, int32_t len);
        virtual int build(char* data, int32_t len);
        virtual int32_t message_length();
        virtual char* get_name();
        static Message* create(const int32_t type);
    };
    class DumpPlanResponseMessage: public Message
    {
      public:
        DumpPlanResponseMessage();
        virtual ~DumpPlanResponseMessage();
        virtual int parse(char* data, int32_t len);
        virtual int32_t message_length();
        virtual int build(char* data, int32_t len);
        virtual char* get_name();
        static Message* create(const int32_t type);
        inline tbnet::DataBuffer& get_data()
        {
          return data_;
        }
      private:
        tbnet::DataBuffer data_;
    };
  }
}

#endif
