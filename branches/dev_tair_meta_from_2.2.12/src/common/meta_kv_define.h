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
 *   daoan <daoan@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_COMMON_META_KV_DEFINE_H_
#define TFS_COMMON_META_KV_DEFINE_H_

namespace tfs
{
  namespace common
  {
    struct TfsFileInfo
    {
      TfsFileInfo():block_id(0), file_id(0) { }
      int64_t block_id;
      int64_t file_id;
    };

  }
}

#endif
