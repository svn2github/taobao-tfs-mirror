#include "http_agent.h"

namespace tfs
{
  namespace tools
  {
    using namespace common;
    using namespace message;

    HttpAgent::HttpAgent(const char* spec)
      :server_id_(0L),connmgr_(NULL), transport_(NULL) 
    {
      spec_ = strdup(spec);
    }

    HttpAgent::~HttpAgent()
    {
      destroy();
      if (spec_) {
        free(spec_);
        spec_ = NULL;
      }
    }

    void HttpAgent::destroy()
    {
      if (transport_)
      {
        transport_->stop();
        transport_->wait();
        tbsys::gDelete(transport_);
      }
      if (connmgr_)
      {
        tbsys::gDelete(connmgr_);
      }
    }

    tbnet::IPacketHandler::HPRetCode HttpAgent::handlePacket(tbnet::Packet *packet, void *args)
    {
      UNUSED(args);
      int32_t ret = packet ? TFS_SUCCESS : TFS_ERROR;

      TBSYS_LOG(DEBUG, "receive response packet");
      if (TFS_SUCCESS == ret)
      {
        if (packet->isRegularPacket())
        {
          if (packet->getPCode() != HTTP_RESPONSE_MESSAGE)
          {
            TBSYS_LOG(WARN, "received wrong response packet: %d", packet->getPCode());
          }
          else
          {
            HttpResponseMessage *http_rsp_msg = dynamic_cast<HttpResponseMessage*>(packet);
            int32_t status = http_rsp_msg->get_status();
            if (status != HTTP_RESPONSE_OK)
            {
              TBSYS_LOG(WARN, "http responsse error = %d", status);
            }
            else
            {
              TBSYS_LOG(DEBUG, "http responsse ok = %d", status);
            }
          }
          packet->free();
        }
        // control packet
        else
        {
          TBSYS_LOG(WARN, "received control packet %d ", dynamic_cast<tbnet::ControlPacket*>(packet)->getCommand());
        }
      }

      return tbnet::IPacketHandler::FREE_CHANNEL;
    }

    int HttpAgent::initialize()
    {
      int32_t ret = spec_ ? TFS_SUCCESS : TFS_ERROR;

      if (TFS_SUCCESS == ret) {
        server_id_ = common::Func::get_host_ip(spec_);
      }

      streamer_.set_packet_factory(&factory_);
      transport_ = new (std::nothrow)tbnet::Transport();
      assert(transport_!= NULL);
      transport_->start();

      connmgr_ = new (std::nothrow)tbnet::ConnectionManager(transport_, &streamer_, this);
      assert(connmgr_ != NULL);
      connmgr_->setDefaultQueueLimit(0, 256);

      return ret;
    }

    int HttpAgent::http_request(const char* method, const char* url, const char *headers)
    {
      int32_t ret = (method && url) ? TFS_SUCCESS : EXIT_NO_HTTP_HEADER;
      HttpRequestMessage *http_req_msg = NULL;

      if (TFS_SUCCESS == ret)
      {
        http_req_msg = new (std::nothrow)HttpRequestMessage();
        assert(http_req_msg != NULL);

        http_req_msg->set_requset_line(method, url);
        if (headers)
        {
          http_req_msg->set_request_header(headers);
        }

        TBSYS_LOG(DEBUG, "sendPacket ok, pcode:%d", http_req_msg->getPCode());
        bool bret = connmgr_->sendPacket(server_id_, http_req_msg, NULL,NULL);
        if (!bret)
        {
          TBSYS_LOG(WARN, "sendPacket fail");
          ret = EXIT_SEND_HTTP_ERROR;
          if (http_req_msg)
          {
            http_req_msg->free();
            http_req_msg = NULL;
          }
        }
      }
      return ret;
    }
  }
}
