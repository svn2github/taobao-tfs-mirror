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
#ifndef TFS_MESSAGE_RELOADMESSAGE_H_
#define TFS_MESSAGE_RELOADMESSAGE_H_

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
    class ReloadConfigMessage: public Message
    {
    public:
      ReloadConfigMessage();
      ReloadConfigMessage(const int32_t status);
      void set_message(const int32_t status);
      virtual ~ReloadConfigMessage();
      virtual int parse(char* data, int32_t len);
      virtual int build(char* data, int32_t len);
      virtual int32_t message_length();
      virtual char* get_name();
      char* get_error() const;
      void set_switch_cluster_flag(const int32_t flag);
      int32_t get_switch_cluster_flag() const;

      static Message* create(const int32_t type);

    protected:
      int32_t flag_;
    };

  }
}
#endif
