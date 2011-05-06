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
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_CLIENT_TFSSESSION_H_
#define TFS_CLIENT_TFSSESSION_H_

#include <Mutex.h>
#include "lru.h"
#include "common/define.h"

namespace tfs
{

  namespace client
  {
    struct BlockCache
    {
      time_t last_time_;
      common::VUINT64 ds_;
    };
    enum UseCacheFlag
    {
      USE_CACHE_FLAG_YES = 0x00,
      USE_CACHE_FLAG_NO
    };
    class TfsSession
    {
      typedef lru<uint32_t, BlockCache> BLOCK_CACHE_MAP;
      typedef BLOCK_CACHE_MAP::iterator BLOCK_CACHE_MAP_ITER;
    public:
			TfsSession(const std::string& nsip, const int32_t cache_time, const int32_t cache_items);
      virtual ~TfsSession();

      int initialize();
      void destroy();
      inline const std::string& get_ns_ip_port_str() const
      {
        return ns_ip_port_str_;
      }
      inline const uint64_t get_ns_ip_port() const
      {
        return ns_ip_port_;
      }
      int
          create_block_info(uint32_t& block_id, common::VUINT64 &rds, const int32_t flag, common::VUINT64& fail_servers);
      int get_block_info(uint32_t& block_id, common::VUINT64 &rds);
      int get_unlink_block_info(uint32_t& block_id, common::VUINT64 &rds);

      inline void set_use_cache(UseCacheFlag use_cache_flag = USE_CACHE_FLAG_NO)
      {
        use_cache_ = use_cache_flag;
      }

      inline int32_t get_cluster_id() const
      {
        return cluster_id_;
      }
    private:
      tbutil::Mutex mutex_;
      uint64_t ns_ip_port_;
      std::string ns_ip_port_str_;
      const int32_t block_cache_time_;
      const int32_t block_cache_items_;
      int32_t cluster_id_;
      UseCacheFlag use_cache_;
      BLOCK_CACHE_MAP block_cache_map_;

    private:
      TfsSession();
      DISALLOW_COPY_AND_ASSIGN( TfsSession);
      int get_block_info_ex(uint32_t& block_id, common::VUINT64 &rds, const int32_t mode, common::VUINT64& fail_ds);
      int get_cluster_id_from_ns();
    };
  }
}
#endif
