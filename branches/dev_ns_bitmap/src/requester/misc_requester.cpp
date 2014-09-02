/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *  linqing <linqing.zyd@taobao.com>
 *      - initial release
 *
 */

#include "common/func.h"
#include "common/client_manager.h"
#include "common/base_packet.h"
#include "common/status_message.h"
#include "message/message_factory.h"
#include "ds_requester.h"
#include "ns_requester.h"
#include "misc_requester.h"

using namespace tfs::common;
using namespace tfs::message;
using namespace std;

namespace tfs
{
  namespace requester
  {
    int MiscRequester::read_block_index(const uint64_t ns_id,
        const uint64_t block_id, const uint64_t attach_block_id,
        IndexDataV2& index_data)
    {
      VUINT64 replicas;
      int32_t index = 0;
      int ret = NsRequester::get_block_replicas(ns_id, block_id, replicas);
      if (TFS_SUCCESS == ret)
      {
        ret = replicas.size() > 0 ? TFS_SUCCESS : EXIT_NO_DATASERVER;
        if (TFS_SUCCESS == ret)
        {
          index = random() % replicas.size();
        }

        if (TFS_SUCCESS == ret)
        {
          ret = DsRequester::read_block_index(replicas[index],
            block_id, attach_block_id, index_data);
        }
      }
      return ret;
    }

  }
}

