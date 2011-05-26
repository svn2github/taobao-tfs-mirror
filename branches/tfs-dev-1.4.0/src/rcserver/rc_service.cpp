/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   zongdai <zongdai@taobao.com>
 *      - initial release
 *
 */
#include "rc_service.h"
#include "rc_param.h"
#include "resource_manager.h"
#include "session_manager.h"
#include "common/base_packet.h"
#include "common/status_message.h"
#include "message/message_factory.h"

namespace tfs
{
  namespace rcserver
  {
    using namespace common;
    using namespace message;
    using namespace std;

    RcService::RcService() :
      resource_manager_(NULL), session_manager_(NULL)
    {
      //Todo: replace factory
      factory_ = new common::BasePacketFactory();
      streamer_ = new common::BasePacketStreamer(factory_);
    }

    RcService::~RcService()
    {
      tbsys::gDelete(resource_manager_);
      tbsys::gDelete(session_manager_);
      tbsys::gDelete(streamer_);
      tbsys::gDelete(factory_);
    }

    const char* RcService::get_log_file_path()
    {
      return PARAM_RCSERVER.log_file_.c_str();
    }

    bool RcService::handlePacketQueue(tbnet::Packet *packet, void *args)
    {
      int ret = true;
      BasePacket* base_packet = NULL;
      if (!(ret = BaseService::handlePacketQueue(packet, args)))
      {
        TBSYS_LOG(ERROR, "call BaseService::handlePacketQueue fail. ret: %d", ret);
      }
      else
      {
        base_packet = dynamic_cast<BasePacket*>(packet);
        switch (base_packet->getPCode())
        {
          case REQ_RC_LOGIN_MESSAGE:
            ret = req_login(base_packet);
            break;
          case REQ_RC_KEEPALIVE_MESSAGE:
            ret = req_keep_alive(base_packet);
            break;
          case REQ_RC_LOGOUT_MESSAGE:
            ret = req_logout(base_packet);
            break;
          default:
            ret = EXIT_UNKNOWN_MSGTYPE;
            TBSYS_LOG(ERROR, "unknown msg type: %d", base_packet->getPCode());
            break;
        }
      }

      if (ret != TFS_SUCCESS && NULL != base_packet)
      {
        base_packet->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), ret, "execute message failed");
      }
      // always return true. packet will be freed by caller
      return true;
    }

    int RcService::initialize(int argc, char* argv[])
    {
      int ret = TFS_SUCCESS;
      resource_manager_ = new ResourceManager();
      if ((ret = resource_manager_->initialize()) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "call ResourceManager::initialize fail. ret: %d", ret);
      }
      else
      {
        session_manager_ = new SessionManager(resource_manager_, get_timer());
        if ((ret = session_manager_->initialize()) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "call SessionManager::initialize fail. ret: %d", ret);
        }
      }

      return ret;
    }

    int RcService::destroy_service()
    {
      int ret = TFS_SUCCESS;
      session_manager_->destroy();
      session_manager_->wait_for_shut_down();

      // Todo
      // resource_manager_->destroy();
      return ret;
    }

    int RcService::req_login(common::BasePacket* packet)
    {
      int ret = TFS_SUCCESS;
      if (NULL == packet)
      {
        ret = EXIT_INVALID_ARGU;
        TBSYS_LOG(ERROR, "RcService::req_login fail. input packet invaild. ret: %d", ret);
      }
      else
      {
        ReqRcLoginMessage* req_login_msg = dynamic_cast<ReqRcLoginMessage*>(packet);
        string session_id;
        BaseInfo base_info;
        if ((ret = session_manager_->login(req_login_msg->get_app_key(),
                req_login_msg->get_app_ip(), session_id, base_info)) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "call SessionManager::login fail. input packet invaild. ret: %d", ret);
        }
        else
        {
          RspRcLoginMessage* rsp_login_msg = new RspRcLoginMessage();
          if ((ret = rsp_login_msg->set_session_id(session_id.c_str())) != TFS_SUCCESS)
          {
            TBSYS_LOG(ERROR, "call RspRcLoginMessage::set_session_id fail. ret: %d", ret);
          }
          else
          {
            rsp_login_msg->set_base_info(base_info);
            req_login_msg->reply(rsp_login_msg);
          }
        }
      }
      return ret;
    }

    int RcService::req_keep_alive(common::BasePacket* packet)
    {
      int ret = TFS_SUCCESS;
      if (NULL == packet)
      {
        ret = EXIT_INVALID_ARGU;
        TBSYS_LOG(ERROR, "RcService::req_keep_alive fail. input packet invaild. ret: %d", ret);
      }
      else
      {
        ReqRcKeepAliveMessage* req_ka_msg = dynamic_cast<ReqRcKeepAliveMessage*>(packet);
        const KeepAliveInfo& ka_info = req_ka_msg->get_ka_info();
        bool update_flag = false;
        BaseInfo base_info;
        if ((ret = session_manager_->keep_alive(ka_info.s_base_info_.session_id_,
                ka_info, update_flag, base_info)) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "call SessionManager::keep_alive fail. session_id: %s, ret: %d",
              ka_info.s_base_info_.session_id_.c_str(), ret);
        }
        else
        {
          RspRcKeepAliveMessage* rsp_ka_msg = new RspRcKeepAliveMessage();
          rsp_ka_msg->set_update_flag(update_flag);
          if (update_flag)
          {
            rsp_ka_msg->set_base_info(base_info);
          }
          req_ka_msg->reply(rsp_ka_msg);
        }
      }
      return ret;
    }

    int RcService::req_logout(common::BasePacket* packet)
    {
      int ret = TFS_SUCCESS;
      if (NULL == packet)
      {
        ret = EXIT_INVALID_ARGU;
        TBSYS_LOG(ERROR, "RcService::req_logout fail. input packet invaild. ret: %d", ret);
      }
      else
      {
        ReqRcLogoutMessage* req_logout_msg = dynamic_cast<ReqRcLogoutMessage*>(packet);
        const KeepAliveInfo& ka_info = req_logout_msg->get_ka_info();
        if ((ret = session_manager_->logout(ka_info.s_base_info_.session_id_,
                ka_info)) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "call SessionManager::logout fail. session_id: %s, ret: %d",
              ka_info.s_base_info_.session_id_.c_str(), ret);
        }
        else
        {
          ret = req_logout_msg->reply(new StatusMessage());
        }
      }
      return ret;
    }

    int req_reload_config()
    {
      int ret = TFS_SUCCESS;
      return ret;
    }

    int req_reload_resource()
    {
      int ret = TFS_SUCCESS;
      return ret;
    }
  }
}
