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
#include "tair_mc_client_api.hpp"
#include "data_entry.hpp"

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
      TairHelper(const std::string& key_prefix, const std::string& config_id, const int32_t area);
      virtual ~TairHelper();
      int initialize();
      int destroy();
      int create_family_id(int64_t& family_id);
      int create_family(common::FamilyInfo& family_info);
      int del_family(const int64_t family_id, const bool del, const bool log, const uint64_t own_ipport);
      int query_family(common::FamilyInfo& family_info);
      int scan(std::vector<common::FamilyInfo>& family_infos, const int64_t start_family_id,
          const int32_t chunk, const bool del, const uint64_t peer_ipport, const int32_t limit);
    private:
      int put_(const char* pkey, const char* skey, const char* value, const int32_t value_len);
      int get_(const char* pkey, const char* skey, char* value, const int32_t value_len);
      int del_(const char* pkey, const char* skey);
      int incr_(const char* key, const int32_t step, int64_t& value);
      int insert_del_family_log_(const int64_t family_id, const uint64_t own_ipport);
      inline int32_t get_bucket(const int64_t family_id) const { return family_id % MAX_FAMILY_CHUNK_NUM;}
    private:
      DISALLOW_COPY_AND_ASSIGN(TairHelper);
      tbutil::Mutex mutex_;
      tair::tair_mc_client_api tair_client_;
      std::string key_prefix_;
      std::string config_id_;
      int32_t area_;
    };
  }/** end namespace nameserver **/
}/** end namespace tfs **/

#endif
