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
#ifndef TFS_REQUESTER_MISCREQUESTER_H_
#define TFS_REQUESTER_MISCQUESTER_H_

#include "common/internal.h"

namespace tfs
{
  namespace requester
  {
    // requests that envolve more than one servers
    class MiscRequester
    {
      public:
        // get block's index header and all file info list
        // get block replica info from ns first
        // and then random choose a ds to read index
        static int read_block_index(const uint64_t ns_id,
            const uint64_t block_id, const uint64_t attach_block_id,
            common::IndexDataV2& index_data);
    };
  }
}

#endif
