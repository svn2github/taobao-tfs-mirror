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
 *   qushan<qushan@taobao.com> 
 *      - modify 2009-03-27
 *   duanfei <duanfei@taobao.com> 
 *      - modify 2010-04-23
 *
 */
#ifndef TFS_NAMESERVER_LAYOUT_MANAGER_H_
#define TFS_NAMESERVER_LAYOUT_MANAGER_H_

#include <errno.h>

#include "common/lock.h"
#include "ns_define.h"
#include "block_chunk.h"
#include "server_collect.h"
#include "meta_scanner.h"

namespace tfs
{
  namespace nameserver
  {
    struct AliveDataServer
    {
    public:
      bool operator()(const std::pair<uint64_t, ServerCollect*>& node)
      {
        return node.second->is_alive();
      }
    };

    struct GetAliveDataServerList
    {
    public:
      GetAliveDataServerList(common::VUINT64& dsList) :
        m_dsList(dsList)
      {

      }
      bool operator()(const std::pair<uint64_t, ServerCollect*>& node)
      {
        if (node.second->is_alive())
        {
          m_dsList.push_back(node.first);
          return true;
        }
        return false;
      }
    private:
      common::VUINT64& m_dsList;
    };

    class LayoutManager
    {
    public:
      LayoutManager();
      explicit LayoutManager(int32_t chunk_num);
      ~LayoutManager();

      int init(int32_t chunk_num);

      int check_ds(const time_t ds_dead_time, common::VUINT64& dead_ds_list, common::VUINT64& writable_ds_list);

      int join_ds(const common::DataServerStatInfo & ds_stat_info, bool& isnew);

      void update_global_info(const common::DataServerStatInfo & ds_stat_info, bool isnew);

      BlockChunkPtr get_block_chunk(const uint32_t block_id) const;

      BlockCollect* get_block_collect(const uint32_t block_id) const;

      BlockCollect* create_block_collect(const uint32_t block_id = 0);

      ServerCollect* get_ds_collect(const uint64_t ds_id) const;

      ServerCollect* get_ds_collect(const uint64_t server_id, bool& renew);

      bool remove_ds_collect(const uint64_t server_id);

      bool remove_block_collect(const uint32_t block_id);

      bool build_ds_block_relation(const uint32_t block_id, const uint64_t server_id, bool master);

      bool release_ds_relation(const uint32_t block_id, const uint64_t server_id);

      bool release_ds_relation(const uint64_t server_id);

      bool release_block_write_relation(const uint32_t block_id);

      bool server_writable(const ServerCollect* server_collect) const;

      bool block_writable(const BlockCollect* block_collect);

      uint32_t calc_max_block_id();

      uint32_t get_avail_block_id();

      inline uint32_t get_max_block_id() const
      {
        return max_block_id_;
      }

      inline int64_t get_elect_seq() const
      {
        return currnet_elect_seq_;
      }

      inline void set_elect_seq(const int64_t seq) const
      {
        currnet_elect_seq_ = seq;
      }

      // traverse all blocks in block chunk array do scanner.run
      int foreach(const MetaScanner & scanner) const;

      inline const common::SERVER_MAP* get_ds_map() const
      {
        return &ds_map_;
      }

      inline const common::VUINT32& get_writable_block_list() const
      {
        return writable_block_list_;
      }

      inline const NsGlobalInfo* get_ns_global_info() const
      {
        return &global_info_;
      }

      inline int32_t get_ds_size() const
      {
        return ds_map_.size();
      }

      int32_t get_alive_ds_size();

      int64_t cacl_all_block_bytes() const;

      inline int64_t cacl_all_block_count() const
      {
        int64_t ret = 0;
        for (int32_t i = 0; i < block_chunk_num_; ++i)
        {
          ret += block_map_array_[i]->get_block_map().size();
        }
        return ret;
      }

      inline int32_t get_block_chunk_num() const
      {
        return block_chunk_num_;
      }

      inline const BlockChunkPtr* get_block_chunk_array() const
      {
        return block_map_array_;
      }

      bool insert(const BlockCollect* block_collect, bool overwrite);

      void sort();
      inline common::RWLock& get_server_mutex()
      {
        return server_mutex_;
      }
      inline common::RWLock& get_writable_mutex()
      {
        return writable_mutex_;
      }
    private:
      bool add_writable_block(const uint32_t block_id);
      bool remove_writable_block(const uint32_t block_id);
      inline bool is_writable(const uint32_t block_id) const;
      inline bool is_writable(const BlockCollect* block_collect) const;
      void clear_ds();

    private:
      DISALLOW_COPY_AND_ASSIGN( LayoutManager);
      common::SERVER_MAP ds_map_;
      common::VUINT32 writable_block_list_;
      NsGlobalInfo global_info_;
      BlockChunkPtr* block_map_array_;
      uint32_t max_block_id_;
      mutable int64_t currnet_elect_seq_;
      mutable int32_t alive_ds_size_;
      int32_t block_chunk_num_;
      common::RWLock global_mutex_;
      common::RWLock server_mutex_;
      common::RWLock writable_mutex_;
    };
  }
}

#endif
