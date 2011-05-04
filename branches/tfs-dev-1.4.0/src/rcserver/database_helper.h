/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id 
 *
 *   
 *   Authors:
 *          daoan(daoan@taobao.com)
 *
 */

#ifndef TFS_RCSERVER_DATABASE_HELPER_H_
#define TFS_RCSERVER_DATABASE_HELPER_H_
#include "resource_server_data.h"
namespace tfs
{
  namespace rcserver
  {
    class DatabaseHelper
    {
      public:
        DatabaseHelper();
        virtual ~DatabaseHelper();

        bool is_connected() const;
        int set_conn_param(const char* conn_str, const char* user_name, const char* passwd);

        virtual int connect() = 0;
        virtual int close() = 0;

        //ResourceServerInfo 
        virtual int select(const ResourceServerInfo& inparam, ResourceServerInfo& outparam) = 0;
        virtual int update(const ResourceServerInfo& inparam) = 0;
        virtual int remove(const ResourceServerInfo& inparam) = 0;
        virtual int scan(const ResourceServerInfo& inparam, VResourceServerInfo& outparam) = 0;

        //AppInfo
        virtual int select(const AppInfo& inparam, AppInfo& outparam) = 0;
        virtual int update(const AppInfo& inparam) = 0;
        virtual int remove(const AppInfo& inparam) = 0;
        virtual int scan(const AppInfo& inparam, MIdAppInfo& outparam) = 0;

      protected:
        const int CONN_STR_LEN = 256;
        char conn_str_[CONN_STR_LEN];
        char user_name_[CONN_STR_LEN];
        char passwd_[CONN_STR_LEN];
        bool is_connected_;

      private:
        DISALLOW_COPY_AND_ASSIGN(DatabaseHelper);
    };
  }
}
#endif
