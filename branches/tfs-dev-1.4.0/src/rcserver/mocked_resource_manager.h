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
#ifndef TFS_RCSERVER_MOCKEDRESOURCEMANAGER_H_
#define TFS_RCSERVER_MOCKEDRESOURCEMANAGER_H_

#include "i_resource_manager.h"

namespace tfs
{
  namespace rcserver
  {
    class MockedResourceManager : public IResourceManager
    {
      public:
        MockedResourceManager() {}
        ~MockedResourceManager() {}

      public:
        int initialize()
        {
          return common::TFS_SUCCESS;
        }

        int load()
        {
          return common::TFS_SUCCESS;
        }

        int login(const std::string& app_key, const uint64_t session_ip, BaseInfo& base_info)
        {
          return common::TFS_SUCCESS;
        }

        int keep_alive(const std::string& session_id, const uint64_t modify_time, BaseInfo& base_info)
        {
          return common::TFS_SUCCESS;
        }

        int logout(const std::string& session_id)
        {
          return common::TFS_SUCCESS;
        }

      private:
        DISALLOW_COPY_AND_ASSIGN(MockedResourceManager);
    };
  }
}
#endif //TFS_RCSERVER_MOCKEDRESOURCEMANAGER_H_
