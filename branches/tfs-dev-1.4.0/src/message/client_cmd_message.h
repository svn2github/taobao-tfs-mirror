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
#include "common/base_packet.h"

namespace tfs
{
  namespace message
  {
#pragma pack(4)
    struct ClientCmdInformation
    {
      int64_t value1_;
      int64_t value2_;
      int32_t value3_;
      int32_t value4_;
      int32_t  cmd_;
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      int64_t length() const;
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
      CLIENT_CMD_FORCE_DATASERVER_REPORT
    };

    class ClientCmdMessage: public common::BasePacket 
    {
    public:
      ClientCmdMessage();
      virtual ~ClientCmdMessage();
      virtual int serialize(common::Stream& output) const ;
      virtual int deserialize(common::Stream& input);
      virtual int64_t length() const;
      inline void set_value1(const uint64_t value)
      {
        info_.value1_ = value;
      }
      inline uint64_t get_value1() const
      {
        return info_.value1_;
      }
      inline void set_value2(const uint64_t value)
      {
        info_.value2_ = value;
      }
      inline uint64_t get_value2() const
      {
        return info_.value2_;
      }
      inline void set_value3(const uint32_t value)
      {
        info_.value3_ = value;
      }
      inline uint32_t get_value3() const
      {
        return info_.value3_;
      }
      inline void set_value4(const uint32_t value)
      {
        info_.value4_ = value;
      }
      inline uint32_t get_value4() const
      {
        return info_.value4_;
      }
      inline void set_cmd(const int32_t cmd)
      {
        info_.cmd_ = cmd;
      }
      inline int32_t get_cmd() const
      {
        return info_.cmd_;
      }

    protected:
      ClientCmdInformation info_;
    };
  }
}
#endif
