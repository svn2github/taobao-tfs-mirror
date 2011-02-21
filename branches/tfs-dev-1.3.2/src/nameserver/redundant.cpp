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
#include "redundant.h"
#include "nameserver.h"

using namespace tfs::common;
using namespace tfs::message;
using namespace std;

namespace tfs
{
  namespace nameserver
  {

    RedundantLauncher::RedundantLauncher(MetaManager& m) :
      meta_mgr_(m)
    {
    }

    RedundantLauncher::~RedundantLauncher()
    {
    }

    int RedundantLauncher::check_redundant_block(const BlockCollect* block_collect)
    {
      if (block_collect == NULL)
      {
        return false;
      }

      const VUINT64 *server = block_collect->get_ds();
      uint32_t count = server->size() - SYSPARAM_NAMESERVER.max_replication_;
      uint32_t block_id = block_collect->get_block_info()->block_id_;
      if (count > 0)
      {
        VUINT64 result_ds_list;
        cacl_max_capacity_ds(*server, count, result_ds_list);

        uint32_t result_ds_list_size = result_ds_list.size();
        for (uint32_t i = 0; i < result_ds_list_size && i < count; i++)
        {
          meta_mgr_.get_block_ds_mgr().release_ds_relation(block_id, result_ds_list[i]);
          NameServer::rm_block_from_ds(result_ds_list[i], block_id);
        }
				TBSYS_LOG(INFO, "redundant block(%u) delete(%u)", block_id, result_ds_list.size());
        return result_ds_list.size() > 0 ? true : false;
      }
      return false;
    }

    int RedundantLauncher::check_group_block(const BlockCollect* block_collect)
    {
      if (SYSPARAM_NAMESERVER.group_mask_ == 0 || block_collect == NULL)
      {
        return 0;
      }

      const VUINT64 *server = block_collect->get_ds();
      VUINT64 result_ds_list;
      cacl_max_capacity_ds(*server, 0, result_ds_list);
      if (result_ds_list.size() == 0)
      {
        return 0;
      }
      uint32_t block_id = block_collect->get_block_info()->block_id_;
      int32_t ds_size = server->size() - SYSPARAM_NAMESERVER.min_replication_ + 1;
      int32_t result_ds_list_size = static_cast<int32_t> (result_ds_list.size());
      for (int32_t i = 0; i < result_ds_list_size && i < ds_size; ++i)
      {
        meta_mgr_.get_block_ds_mgr().release_ds_relation(block_id, result_ds_list[i]);
        NameServer::rm_block_from_ds(result_ds_list[i], block_id);
      }
      return result_ds_list.size();
    }

    /**
     * RedundantLauncher::cacl_max_capacity_ds
     * find the server who has max capacity with that block replica
     *
     * Parameters:
     * @param [in] ds    DataServerStatInfo list has the block replica
     * @param [in] count need count
     * @param [out] result_ds_list  find result
     *
     * @return: blockcollect obj pointer of this block
     */
    int RedundantLauncher::cacl_max_capacity_ds(const VUINT64& ds, int32_t count, VUINT64& result_ds_list)
    {
      vector<DataServerStatInfo*> middle_result;
      vector<DataServerStatInfo*> result;
      uint32_t i = 0;
      uint32_t ds_list_size = ds.size();
      ServerCollect* server_collect = NULL;
      for (i = 0; i < ds_list_size; ++i)
      {
        server_collect = meta_mgr_.get_block_ds_mgr().get_ds_collect(ds.at(i));
        if (server_collect == NULL)
          continue;
        middle_result.push_back(server_collect->get_ds());
      }

      std::sort(middle_result.begin(), middle_result.end(), CompareBlockCount());

      INT_MAP groups;
      DataServerStatInfo* ds_stat_info = NULL;
      uint32_t middle_result_size = middle_result.size();
      uint32_t lanip = 0;
      for (i = 0; i < middle_result_size; ++i)
      {
        ds_stat_info = middle_result[i];
        if (SYSPARAM_NAMESERVER.group_mask_ == 0)
        {
          result.push_back(ds_stat_info);
          continue;
        }
        lanip = Func::get_lan(ds_stat_info->id_, SYSPARAM_NAMESERVER.group_mask_);
        if (groups.find(lanip) != groups.end())
        {
          result_ds_list.push_back(ds_stat_info->id_);
        }
        else
        {
          result.push_back(ds_stat_info);
          groups.insert(INT_MAP::value_type(lanip, 1));
        }
      }
      count -= result_ds_list.size();
      if (count <= 0)
        return count;

      int32_t result_size = static_cast<int32_t>(result.size());
      for (int32_t j = result_size - 1; (j >= 0) && (count > 0); j--)
      {
        result_ds_list.push_back(result[j]->id_);
        count--;
      }
      return count;
    }

    bool RedundantLauncher::check(const BlockCollect* block_collect)
    {
      return check_redundant_block(block_collect);
    }

    int RedundantLauncher::build_plan(const VUINT32&)
    {
      return 0;
    }
  }
}

