/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: meta_server_service.h 49 2011-08-08 09:58:57Z nayan@taobao.com $
 *
 * Authors:
 *   daoan <daoan@taobao.com>
 *      - initial release
 *
 */


#include "kvengine_helper.h"

namespace tfs
{
  using namespace common;
  namespace kvmetaserver
  {
    KvKey::KvKey()
      :key_(NULL), key_size_(0), key_type_(0)
    {
    }
    const char KvKey::DELIMITER = 7;

  }
}

