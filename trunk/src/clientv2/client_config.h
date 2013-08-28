/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software_; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *      - initial release
 *   linqing <linqing.zyd@taobao.com>
 *      - modify 2013-07-16
 *
 */
#ifndef TFS_CLIENTV2_CONFIG_H_
#define TFS_CLIENTV2_CONFIG_H_

#include <string>
#include <stdint.h>

namespace tfs
{
  namespace clientv2
  {
    enum UseCacheFlag
    {
      USE_CACHE_FLAG_NO = 0x00,
      USE_CACHE_FLAG_LOCAL = 0x01,
      USE_CACHE_FLAG_REMOTE = 0x02
    };

    struct StatItem
    {
      static std::string client_access_stat_;
      static std::string read_success_;
      static std::string read_fail_;
      static std::string stat_success_;
      static std::string stat_fail_;
      static std::string write_success_;
      static std::string write_fail_;
      static std::string unlink_success_;
      static std::string unlink_fail_;
      static std::string local_read_;

      static std::string client_cache_stat_;
      static std::string local_cache_hit_;
      static std::string local_cache_miss_;
      static std::string local_remove_count_;
#ifdef WITH_TAIR_CACHE
      static std::string remote_cache_hit_;
      static std::string remote_cache_miss_;
      static std::string remote_remove_count_;
#endif
    };

    struct ClientConfig
    {
      static int32_t use_cache_;
      static int64_t cache_items_;
      static int64_t cache_time_;
      static std::string remote_cache_master_addr_;
      static std::string remote_cache_slave_addr_;
      static std::string remote_cache_group_name_;
      static int32_t remote_cache_area_;
      static int64_t segment_size_;
      static int64_t batch_count_;
      static int64_t batch_size_;
      static int64_t stat_interval_;
      static int64_t gc_interval_;
      static int64_t expired_time_;
      static int64_t batch_timeout_;
      static int64_t wait_timeout_;
      static int32_t client_retry_count_;
      static int32_t meta_retry_count_;
      static bool client_retry_flag_;
      static uint32_t update_kmt_interval_count_;
      static uint32_t update_kmt_fail_count_;
      static uint32_t update_dst_interval_count_;
      static uint32_t update_dst_fail_count_;
    };
  }
}

#endif
