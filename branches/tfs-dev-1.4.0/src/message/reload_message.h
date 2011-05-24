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
#include "common/base_packet.h"

namespace tfs
{
  namespace message
  {
    class ReloadConfigMessage: public common::BasePacket 
    {
    public:
      ReloadConfigMessage();
      //ReloadConfigMessage(const int32_t status);
      //void set_message(const int32_t status);
      virtual ~ReloadConfigMessage();
      virtual int serialize(common::Stream& output);
      virtual int deserialize(common::Stream& input);
      virtual int64_t length() const;
      static common::BasePacket* create(const int32_t type);

      void set_switch_cluster_flag(const int32_t flag);
      int32_t get_switch_cluster_flag() const;
    protected:
      int32_t flag_;
    };
  }
}
#endif
