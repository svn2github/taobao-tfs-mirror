/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *   linqing<linqing.zyd@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_MESSAGE_CLIENT_NS_KEEPALIVE_MESSAGE_H_
#define TFS_MESSAGE_CLIENT NS_KEEPALIVE_MESSAGE_H_

#include "common/base_packet.h"

namespace tfs
{
  namespace message
  {
    class ClientNsKeepaliveMessage: public common::BasePacket
    {
      public:
        ClientNsKeepaliveMessage();
        virtual ~ClientNsKeepaliveMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_flag(const int32_t flag)
        {
          flag_ = flag;
        }

        int32_t get_flag() const
        {
          return flag_;
        }

      private:
        int32_t flag_;
    };

    class ClientNsKeepaliveResponseMessage: public common::BasePacket
    {
      public:
        ClientNsKeepaliveResponseMessage();
        virtual ~ClientNsKeepaliveResponseMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline tbnet::DataBuffer& get_data()
        {
          return data_;
        }

        common::ClusterConfig& get_cluster_config()
        {
          return config_;
        }

        int32_t& get_interval()
        {
          return interval_;
        }

      private:
        common::ClusterConfig config_;
        int32_t interval_;
        mutable tbnet::DataBuffer data_; // server_id_array
    };
  }
}

#endif
