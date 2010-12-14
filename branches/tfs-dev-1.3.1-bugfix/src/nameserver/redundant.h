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
#ifndef TFS_NAMESERVER_REDUNDANT_H_
#define TFS_NAMESERVER_REDUNDANT_H_

#include "scanner_manager.h"
#include "meta_manager.h"

namespace tfs
{
  namespace nameserver
  {

    class RedundantLauncher: public Launcher
    {
      struct CompareBlockCount
      {
        bool operator()(const common::DataServerStatInfo *x, const common::DataServerStatInfo *y) const
        {
          return x->block_count_ < y->block_count_;
        }
      };
    public:
      RedundantLauncher(MetaManager& m);
      virtual ~RedundantLauncher();

    public:
      virtual bool check(const BlockCollect* block_collect);
      virtual int build_plan(const common::VUINT32& lose_block_list);
      int check_redundant_block(const BlockCollect* block_collect);
      int check_group_block(const BlockCollect* block_collect);
      int cacl_max_capacity_ds(const common::VUINT64& ds, int32_t count, common::VUINT64& rds);

    private:
      DISALLOW_COPY_AND_ASSIGN( RedundantLauncher);
      MetaManager& meta_mgr_;
    };
  }
}
#endif 
