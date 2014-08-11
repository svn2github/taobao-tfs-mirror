/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: base_http_message.h 388 2013-12-25 09:21:44Z nayan@taobao.com $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 *
 */

#ifndef TFS_HTTP_MESSAGE_H
#define TFS_HTTP_MESSAGE_H

#include "http_packet.h"

namespace tfs
{
  namespace common
  {
    class HttpRequestMessage : public common::HttpPacket
    {
      public :
        HttpRequestMessage();
        virtual ~HttpRequestMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_requset_line(const std::string& method, const std::string& url)
        {
          request_line_ = method  + " " + url + " HTTP/1.1\r\n";
        }

        void set_request_header(const std::string& host, const std::string& header_options)
        {
          request_header_ = "Host: " + host + "\r\n" + TBNET_HTTP_KEEP_ALIVE + TBNET_HTTP_CONTENT_TYPE;
          if (!header_options.empty())
            request_header_ += header_options;
          request_header_ += "\r\n";
        }

        void set_request_body(const std::string& request_body)
        {
          request_body_ = request_body;
        }

      private:
        std::string request_line_;
        std::string request_header_;
        std::string request_body_;
    };

    class HttpResponseMessage : public common::HttpPacket
    {
      public :
        HttpResponseMessage();
        virtual ~HttpResponseMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        const int32_t get_status() const
        {
          return status_;
        }
      private:
        int32_t status_;
        std::string request_line_;
        std::string request_header_;
        std::string request_body_;
    };
  }
}

#endif

