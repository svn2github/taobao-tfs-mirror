/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: http_message_factory.h 388 2013-12-25 09:21:44Z nayan@taobao.com $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_COMMON_HTTP_PACKET_FACTORY_H_
#define TFS_COMMON_HTTP_PACKET_FACTORY_H_

#include <tbnet.h>

namespace tfs
{
  namespace common
  {
    class HttpMessageFactory : public tbnet::IPacketFactory
    {
      public:
        HttpMessageFactory(){};
        virtual ~HttpMessageFactory(){};
        virtual tbnet::Packet* createPacket(const int pcode);
    };
  } /** end namesapce common**/
}/** end namesapce tfs **/

#endif
