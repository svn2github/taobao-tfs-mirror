
#include "tbnet.h"
#include "http_packet_message.h"
#include "common/stream.h"

namespace tfs
{
  namespace message 
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
      int32_t ret = TFS_SUCCESS;
      int64_t length = 0;

      if ((length = request_line_.length()) > 0)
      {
        ret = output.set_bytes(request_line_.c_str(), length);
      }
      else
      {
        TBSYS_LOG(WARN, "HttpRequestMessage no request line");
        ret = EXIT_NO_HTTP_REQUEST_LINE;
      }

      if (TFS_SUCCESS == ret)
      {
        if ((length = request_header_.length()) > 0)
        {
          ret = output.set_bytes(request_header_.c_str(), length);
        }
        else
        {
          TBSYS_LOG(WARN, "HttpRequestMessage no request header");
          ret = EXIT_NO_HTTP_REQUEST_HEADER;
        }
      }

      if (TFS_SUCCESS == ret)
      {
        if ((length = request_body_.length()) > 0)
        {
          ret = output.set_bytes(request_body_.c_str(), length);
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
      TBSYS_LOG(DEBUG, "HttpResponseMessage::deserialize datalen = %ld", input.get_data_length());
      int64_t len = 0;
      char buffer[128] = {0};

      /* get http protocl -- "HTTP/1.1" */
      int32_t iret = input.get_bytes(buffer, strlen(HTTP_PROTOCOL));
      if (TFS_SUCCESS == iret)
      {
        buffer[HTTP_PROTOCOL_LENGTH] = '\0';
        len = HTTP_PROTOCOL_LENGTH;

        if (strncmp(buffer, HTTP_PROTOCOL, strlen(HTTP_PROTOCOL)) != 0)
        {
          TBSYS_LOG(WARN, "wrong protocol = %s", buffer);
          iret = EXIT_WRONG_HTTP_PROTOCOL;
        }
      }

      /* get http response status */
      if (TFS_SUCCESS == iret)
      {
        input.drain(HTTP_BLANK_LENGTH);
        len += HTTP_BLANK_LENGTH;

        iret = input.get_bytes(buffer,HTTP_RESPONSE_STATUS_LENGTH);
      }

      if (TFS_SUCCESS == iret)
      {
        buffer[HTTP_RESPONSE_STATUS_LENGTH] = '\0';
        len += HTTP_RESPONSE_STATUS_LENGTH;
        status_ = atoi(buffer);
        TBSYS_LOG(DEBUG, "status = %d", status_);
        input.drain(HTTP_BLANK_LENGTH);
        len += HTTP_BLANK_LENGTH;

        input.drain(input.get_data_length() - len);
      }

      return iret;
    }

    int64_t HttpResponseMessage::length() const
    {
      return request_header_.length()
        + request_line_.length()
        + request_body_.length();
    }

  }
}
