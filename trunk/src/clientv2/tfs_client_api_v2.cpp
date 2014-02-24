/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *   linqing <linqing.zyd@taobao.com>
 *      - initial release
 *
 */

#include "tfs_client_impl_v2.h"
#include "tfs_client_api_v2.h"

namespace tfs
{
  namespace clientv2
  {
    using namespace common;

    TfsClientV2::TfsClientV2()
    {
    }

    TfsClientV2::~TfsClientV2()
    {
    }

    int TfsClientV2::initialize(const char* ns_addr, const int32_t cache_time, const int32_t cache_items)
    {
      return TfsClientImplV2::Instance()->initialize(ns_addr, cache_time, cache_items);
    }

    int TfsClientV2::destroy()
    {
      return TfsClientImplV2::Instance()->destroy();
    }

    int TfsClientV2::stat_file(common::TfsFileStat* file_stat, const char* file_name, const char* suffix,
        const char* ns_addr)
    {
      return TfsClientImplV2::Instance()->stat_file(file_stat, file_name, suffix,
          common::NORMAL_STAT, ns_addr);
    }

    int64_t TfsClientV2::save_file(char* ret_tfs_name, const int32_t ret_tfs_name_len,
        const char* local_file, const int32_t mode, const char* suffix, const char* ns_addr)
    {
      return TfsClientImplV2::Instance()->save_file(ret_tfs_name, ret_tfs_name_len,
          local_file, mode, suffix, ns_addr);
    }

    int64_t TfsClientV2::save_file_update(const char* local_file, const int32_t mode,
        const char* tfs_name, const char* suffix, const char* ns_addr)
    {
      return TfsClientImplV2::Instance()->save_file_update(local_file, mode, tfs_name, suffix, ns_addr);
    }

    int TfsClientV2::fetch_file(const char* local_file, const char* file_name, const char* suffix, const char* ns_addr)
    {
      return TfsClientImplV2::Instance()->fetch_file(local_file, file_name, suffix, READ_DATA_OPTION_FLAG_NORMAL, ns_addr);
    }

    int TfsClientV2::unlink(int64_t& file_size, const char* file_name, const char* suffix,
        const common::TfsUnlinkType action, const char* ns_addr)
    {
      return TfsClientImplV2::Instance()->unlink(file_size, file_name, suffix, action, ns_addr);
    }

    uint64_t TfsClientV2::get_server_id()
    {
      return TfsClientImplV2::Instance()->get_server_id();
    }

    int32_t TfsClientV2::get_cluster_id(const char* ns_addr)
    {
      return TfsClientImplV2::Instance()->get_cluster_id(ns_addr);
    }

    void TfsClientV2::set_wait_timeout(const int64_t timeout_ms)
    {
      return TfsClientImplV2::Instance()->set_wait_timeout(timeout_ms);
    }

    int64_t TfsClientV2::get_wait_timeout() const
    {
      return TfsClientImplV2::Instance()->get_wait_timeout();
    }
  }
}
