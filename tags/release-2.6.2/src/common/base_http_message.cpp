/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: base_http_message.cpp 388 2013-12-25 09:21:44Z nayan@taobao.com $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 *
 */

#include "tbnet.h"
#include "base_http_message.h"
#include "stream.h"

namespace tfs
{
  namespace common
  {
    using namespace common;
    HttpRequestMessage::HttpRequestMessage()
    {
      _packetHeader._pcode = HTTP_REQUEST_MESSAGE;
    }

    HttpRequestMessage::~HttpRequestMessage()
    {

    }

    int HttpRequestMessage::serialize(Stream& output) const
    {
      int32_t ret = request_line_.empty() ? EXIT_NO_HTTP_REQUEST_LINE : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        output.set_bytes(request_line_.c_str(), request_line_.length());
      }
      if (TFS_SUCCESS == ret && !request_header_.empty())
      {
        output.set_bytes(request_header_.c_str(), request_header_.length());
      }
      char buf[128] = {'\0'};
      if (TFS_SUCCESS == ret)
      {
        if (!request_body_.empty())
        {
          snprintf(buf, 128, "Content-Length: %zd\r\n\r\n",request_body_.length());
          output.set_bytes(buf, strlen(buf));
          output.set_bytes(request_body_.c_str(), request_body_.length());
        }
        else
        {
          snprintf(buf, 128, "\r\n");
          output.set_bytes(buf, strlen(buf));
        }
      }
      return ret;
    }

    int HttpRequestMessage::deserialize(Stream& input)
    {
      UNUSED(input);
      return  TFS_SUCCESS;;
    }

    int64_t HttpRequestMessage::length() const
    {
      return request_header_.length()
        + request_line_.length()
        + request_body_.length();
    }

    HttpResponseMessage::HttpResponseMessage()
    {
      _packetHeader._pcode = HTTP_RESPONSE_MESSAGE;
    }

    HttpResponseMessage::~HttpResponseMessage()
    {
    }

    int HttpResponseMessage::serialize(Stream& output) const
    {
      UNUSED(output);
      return TFS_SUCCESS;
    }

    int HttpResponseMessage::deserialize(Stream& input)
    {
      char buffer[128] = {'\0'};
      int32_t HTTP_RESPONSE_STATUS_LENGTH = 3;
      int32_t ret = (input.get_data_length() >= HTTP_PROTOCOL_LENGTH) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      /* get http protocl -- "HTTP/1.1" */
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_bytes(buffer, HTTP_PROTOCOL_LENGTH);
      }
      if (TFS_SUCCESS == ret)
      {
        buffer[HTTP_PROTOCOL_LENGTH] = '\0';
        input.drain(HTTP_BLANK_LENGTH);
        ret = strncmp(buffer, HTTP_PROTOCOL, HTTP_PROTOCOL_LENGTH) == 0 ? TFS_SUCCESS : EXIT_WRONG_HTTP_PROTOCOL;
      }

      /* get http response status */
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_bytes(buffer, HTTP_RESPONSE_STATUS_LENGTH);
      }

      if (TFS_SUCCESS == ret)
      {
        input.drain(HTTP_BLANK_LENGTH);
        buffer[HTTP_RESPONSE_STATUS_LENGTH] = '\0';
        status_ = atoi(buffer);
      }
      input.clear();
      return ret;
    }

    int64_t HttpResponseMessage::length() const
    {
      return request_header_.length()
        + request_line_.length()
        + request_body_.length();
    }
  }
}
