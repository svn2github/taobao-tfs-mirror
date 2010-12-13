/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_session.h 5 2010-09-29 07:44:56Z duanfei@taobao.com $
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
#include "local_key.h"

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
      int get_block_info(uint32_t& block_id, common::VUINT64 &rds, const int32_t flag);
      int batch_get_block_info(std::vector<SegmentData*>& seg_list, const int32_t flag);

      inline int32_t get_cluster_id() const
      {
        return cluster_id_;
      }

      inline const std::string& get_ns_addr_str() const
      {
        return ns_addr_str_;
      }

      inline const uint64_t get_ns_addr() const
      {
        return ns_addr_;
      }

      inline const int32_t get_cache_time() const
      {
        return block_cache_time_;
      }

      inline const int32_t get_cache_items() const
      {
        return block_cache_items_;
      }

      inline void set_use_cache(UseCacheFlag flag = USE_CACHE_FLAG_YES)
      {
        use_cache_ = flag;
      }

    private:
      TfsSession();
      DISALLOW_COPY_AND_ASSIGN( TfsSession);
      int get_block_info_ex(uint32_t& block_id, common::VUINT64 &rds, const int32_t flag);
      int batch_get_block_info_ex(std::vector<SegmentData*>& seg_list, const int32_t flag);
      int get_cluster_id_from_ns();

    private:
      tbutil::Mutex mutex_;
      uint64_t ns_addr_;
      std::string ns_addr_str_;
      const int32_t block_cache_time_;
      const int32_t block_cache_items_;
      int32_t cluster_id_;
      UseCacheFlag use_cache_;
      BLOCK_CACHE_MAP block_cache_map_;
    };
  }
}
#endif
