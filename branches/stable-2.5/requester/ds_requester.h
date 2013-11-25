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
#ifndef TFS_REQUESTER_DSREQUESTER_H_
#define TFS_REQUESTER_DSREQUESTER_H_

#include "common/internal.h"

namespace tfs
{
  namespace requester
  {
    // all requests are sent to dataserver
    class DsRequester
    {
      public:
        // get block's index header and all file info list
        static int read_block_index(const uint64_t ds_id,
            const uint64_t block_id, const uint64_t attach_block_id,
            common::IndexDataV2& index_data);
    };
  }
}

#endif
