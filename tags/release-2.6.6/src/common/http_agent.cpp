#include "http_agent.h"

namespace tfs
{
  namespace common
  {
    HttpAgent::HttpAgent(const uint64_t server):
      server_(server),
      streamer_(&factory_),
      connmgr_(&transport_, &streamer_, this)
    {
      transport_.start();
      connmgr_.setDefaultQueueLimit(0, 256);
    }

    HttpAgent::~HttpAgent()
    {
      destroy();
    }

    void HttpAgent::destroy()
    {
      transport_.stop();
      transport_.wait();
    }

    tbnet::IPacketHandler::HPRetCode HttpAgent::handlePacket(tbnet::Packet *packet, void *args)
    {
      UNUSED(args);
      int32_t ret = (NULL != packet) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        TBSYS_LOG(WARN, "received wrong response packet: %d", packet->getPCode());
        if (!packet->isRegularPacket())
        {
          TBSYS_LOG(WARN, "received control packet %d ", dynamic_cast<tbnet::ControlPacket*>(packet)->getCommand());
        }
        else
        {
          TBSYS_LOG(WARN, "received wrong response packet: %d", packet->getPCode());
          if (packet->getPCode() != HTTP_RESPONSE_MESSAGE)
          {
            TBSYS_LOG(WARN, "received wrong response packet: %d", packet->getPCode());
          }
          else
          {
            HttpResponseMessage *http_rsp_msg = dynamic_cast<HttpResponseMessage*>(packet);
            int32_t status = http_rsp_msg->get_status();
            TBSYS_LOG(INFO, "get response status: %s, %d", HTTP_RESPONSE_OK == status ? "OK":"FAIL", status);
          }
          packet->free();
        }
      }
      return tbnet::IPacketHandler::FREE_CHANNEL;
    }

    int HttpAgent::send(const std::string& method, const std::string& host, const std::string& url, const std::string& header, const std::string& body)
    {
      int32_t ret = (!method.empty() && !url.empty() && !host.empty()) ? TFS_SUCCESS : EXIT_NO_HTTP_HEADER;
      if (TFS_SUCCESS == ret)
      {
        //parse header
      }
      if (TFS_SUCCESS == ret)
      {
        HttpRequestMessage* req = new (std::nothrow)HttpRequestMessage();
        assert(NULL != req);
        req->set_requset_line(method, url);
        req->set_request_header(host,header);
        req->set_request_body(body);
        ret = connmgr_.sendPacket(server_, req, NULL,NULL) ? TFS_SUCCESS : EXIT_SEND_HTTP_ERROR;
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "send http request to %s error,%s %s %s %s %s",
              tbsys::CNetUtil::addrToString(server_).c_str(), method.c_str(), host.c_str(), url.c_str(),
              header.c_str(), body.c_str());
          req->free();
        }
      }
      return ret;
    }
  }
}
