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

#ifndef TFS_TESTS_TESTKVMETASERVER_META_INFO_HELPER_H_
#define TFS_TESTS_TESTKVMETASERVER_META_INFO_HELPER_H_

#include "common/parameter.h"
#include "common/base_service.h"
#include "common/status_message.h"
#include "common/kv_meta_define.h"
#include "message/message_factory.h"
#include "kvengine_helper.h"
#include "kv_meta_server/meta_info_helper.h"

namespace tfs
{
  namespace kvmetaserver
  {
    class TestMetaInfoHelper :public MetaInfoHelper
    {
    public:
      TestMetaInfoHelper()
      {
      }
      void set_kv_engine(KvEngineHelper *r)
      {
        kv_engine_helper_ = r;
      }

    };
  }
}

#endif
