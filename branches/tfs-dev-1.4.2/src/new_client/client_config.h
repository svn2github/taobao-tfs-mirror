/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software_; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_CLIENT_CONFIG_H_
#define TFS_CLIENT_CONFIG_H_

#include <string>
#include <stdint.h>

namespace tfs
{
  namespace client
  {

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

      static std::string client_cache_stat_;
      static std::string cache_hit_;
      static std::string cache_miss_;
      static std::string remove_count_;
    };

    struct ClientConfig
    {
      static int64_t cache_items_;
      static int64_t cache_time_;
      static int64_t segment_size_;
      static int64_t batch_count_;
      static int64_t batch_size_;
      static int64_t stat_interval_;
      static int64_t gc_interval_;
      static int64_t expired_time_;
      static int64_t batch_timeout_;
      static int64_t wait_timeout_;
      static int64_t client_retry_count_;
    };
  }
}

#endif
