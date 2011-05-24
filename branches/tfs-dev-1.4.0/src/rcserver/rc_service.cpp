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

namespace tfs
{
  namespace rcserver
  {
    using namespace common;

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
      if (!(ret = BaseService::handlePacketQueue(packet, args)))
      {
        TBSYS_LOG(ERROR, "call BaseService::handlePacketQueue fail. ret: %d", ret);
      }
      else
      {
        //Todo 
      }
      return ret;
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

    int req_login()
    {
      int ret = TFS_SUCCESS;
      return ret;
    }

    int req_keep_alive()
    {
      int ret = TFS_SUCCESS;
      return ret;
    }

    int req_logout()
    {
      int ret = TFS_SUCCESS;
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
