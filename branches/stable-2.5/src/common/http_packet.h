/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: base_packet.h 213 2011-04-22 16:22:51Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_COMMON_HTTP_PACKET_H_
#define TFS_COMMON_HTTP_PACKET_H_

#include <tbsys.h>
#include <tbnet.h>

#include "stream.h"
#include "serialization.h"

namespace tfs
{
  namespace  common
  {

    static const int32_t HTTP_REQUEST_MESSAGE = 1000;
    static const int32_t HTTP_RESPONSE_MESSAGE = 1001;
    class HttpPacket : public tbnet::Packet
    {
      public:
        HttpPacket();
        virtual ~HttpPacket();
        bool encode(tbnet::DataBuffer* output);
        bool decode(tbnet::DataBuffer* input, tbnet::PacketHeader* header);
        virtual int serialize(Stream& output) const = 0;
        virtual int deserialize(Stream& input) = 0;
        virtual int64_t length() const = 0;
      private:
        Stream stream_;
        tbnet::Connection* connection_;
    };
  }/** end namesapce common **/
}/** end namespace tfs **/

#endif
