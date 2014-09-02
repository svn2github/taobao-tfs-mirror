/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: http_message_factory.cpp 388 2013-12-25 09:21:44Z nayan@taobao.com $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 *
 */
#include "http_packet.h"
#include "base_http_message.h"
#include "http_message_factory.h"

namespace tfs
{
  namespace common
  {
    tbnet::Packet* HttpMessageFactory::createPacket(const int pcode)
    {
      switch (pcode)
      {
        case HTTP_REQUEST_MESSAGE:
          return new (std::nothrow)HttpRequestMessage();
        case HTTP_RESPONSE_MESSAGE:
          return new (std::nothrow)HttpResponseMessage();
        default:
          TBSYS_LOG(WARN, "HttpMessageFactory wrong pcode =%d", pcode);
          return NULL;
      }
      return NULL;
    }
  } /** end namesapce common**/
}/** end namesapce tfs **/
