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

#include "writable_block.h"

using namespace std;
using namespace tfs::common;

namespace tfs
{
  namespace dataserver
  {

    WritableBlock::WritableBlock(const uint64_t block_id):
      GCObject(Func::get_monotonic_time()),
      block_id_(block_id),
      server_size_(0),
      use_(false)
    {
    }

    WritableBlock::~WritableBlock()
    {
    }

    int WritableBlock::set_servers(const common::ArrayHelper<uint64_t> servers)
    {
      int ret = TFS_SUCCESS;
      if (servers.get_array_index() <= MAX_REPLICATION_NUM)
      {
        server_size_ = servers.get_array_index();
        for (int index = 0; index < server_size_; index++)
        {
          servers_[index] = *(servers.at(index));
        }
      }
      else
      {
        ret = EXIT_PARAMETER_ERROR;
      }
      return ret;
    }

    int WritableBlock::get_servers(common::ArrayHelper<uint64_t>& servers)
    {
      int ret = TFS_SUCCESS;
      if (servers.get_array_size() >= server_size_)
      {
        servers.clear();
        for (int index = 0; index < server_size_; index++)
        {
          servers.push_back(servers_[index]);
        }
      }
      else
      {
        ret = EXIT_PARAMETER_ERROR;
      }
      return ret;
    }

    void WritableBlock::get_servers(common::VUINT64& servers)
    {
      servers.clear();
      for (int index = 0; index < server_size_; index++)
      {
        servers.push_back(servers_[index]);
      }
    }

  }
}

