/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tair_helper.h 2140 2012-07-18 10:28:04Z duanfei $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_NAMESERVER_DATABASE_HELPER_H_
#define TFS_NAMESERVER_DATABASE_HELPER_H_

#include <Timer.h>
#include <Mutex.h>
#include "ns_define.h"
#include "tair_client_api.hpp"

#ifdef TFS_GTEST
#include <gtest/gtest.h>
#endif

namespace tfs
{
  namespace nameserver
  {
    class TairHelper
    {
      #ifdef TFS_GTEST
      friend class TairHelperTest;
      FRIEND_TEST(TairHelperTest, create_family);
      FRIEND_TEST(TairHelperTest, del_family);
      FRIEND_TEST(TairHelperTest, scan);
      #endif
    public:
      TairHelper(const std::string& key_prefix, const std::string& master_ipaddr, const std::string& slave_ipaddr,
        const std::string& group_name, const int32_t area);
      virtual ~TairHelper();
      int initialize();
      int destroy();
      int create_family_id(int64_t& family_id);
      int create_family(common::FamilyInfo& family_info);
      int query_family(common::FamilyInfo& family_info);
      int del_family(const int64_t family_id);
      int scan(std::vector<common::FamilyInfo>& family_infos, const int64_t start_family_id = 0);
    private:
      DISALLOW_COPY_AND_ASSIGN(TairHelper);
      tbutil::Mutex mutex_;
      tair::tair_client_api tair_client_;
      std::string key_prefix_;
      std::string master_ipaddr_;
      std::string slave_ipaddr_;
      std::string group_name_;
      std::string max_key_;
      int32_t area_;
    };
  }/** end namespace nameserver **/
}/** end namespace tfs **/

#endif
