/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: database_helper.h 2140 2012-07-18 10:28:04Z duanfei $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_NAMESERVER_DATABASE_HELPER_H_
#define TFS_NAMESERVER_DATABASE_HELPER_H_

#include <mysql.h>
#include <Timer.h>
#include <Mutex.h>
#include "ns_define.h"

#ifdef TFS_GTEST
#include <gtest/gtest.h>
#endif

namespace tfs
{
  namespace nameserver
  {
    class DataBaseHelper
    {
      #ifdef TFS_GTEST
      friend class DataBaseHelperTest;
      FRIEND_TEST(DataBaseHelperTest, create_family);
      FRIEND_TEST(DataBaseHelperTest, del_family);
      FRIEND_TEST(DataBaseHelperTest, scan);
      #endif
    public:
      DataBaseHelper(std::string& connstr, std::string& username, std::string& password);
      virtual ~DataBaseHelper();
      int create_family_id(int64_t& family_id);
      int create_family(common::FamilyInfo& family_info, int64_t& mysql_ret);
      int del_family(int64_t& mysql_ret, const int64_t family_id);
      int scan(std::vector<common::FamilyInfo>& family_infos, const int64_t start_family_id = 0);
    private:
      int initialize_();
      int connect_();
      int disconnect_();
      int excute_stmt_(MYSQL_STMT* stmt);
    private:
      DISALLOW_COPY_AND_ASSIGN(DataBaseHelper);
      tbutil::Mutex mutex_;
      std::string connstr_;
      std::string username_;
      std::string password_;
      std::string hostname_;
      std::string dbname_;
      int32_t port_;
      MYSQL mysql_;
      bool is_connected_;
    };
  }/** end namespace nameserver **/
}/** end namespace tfs **/

#endif
