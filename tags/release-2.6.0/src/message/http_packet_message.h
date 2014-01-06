
#ifndef TFS_HTTP_MESSAGE_H
#define TFS_HTTP_MESSAGE_H

#include "common/http_packet.h"

namespace tfs
{
  namespace message 
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

          void set_request_header(const std::string& header_options)
          {
            request_header_ = "Host: " + (header_host_.empty() ? "localhost" : header_host_) + "\r\n" + TBNET_HTTP_KEEP_ALIVE + TBNET_HTTP_CONTENT_TYPE + header_options + "\r\n\r\n";
          }

          void set_request_host(const std::string& header_host)
          {
            header_host_ = header_host;
          }

          void set_request_body(const std::string& request_body)
          {
            request_body_ = request_body;
          }

        private:
          std::string request_line_;
          std::string request_header_;
          std::string request_body_;
          std::string header_host_;
      };

      class HttpResponseMessage : public common::HttpPacket
    {
      public :
        HttpResponseMessage();
        virtual ~HttpResponseMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        const int32_t get_status()
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

