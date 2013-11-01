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

#ifndef TFS_KVMETASERVER_LIFE_CYCLE_HELPER_H_
#define TFS_KVMETASERVER_LIFE_CYCLE_HELPER_H_
#include <string>

#include "common/parameter.h"
#include "common/base_service.h"
#include "common/status_message.h"
#include "common/kvengine_helper.h"
#include "message/message_factory.h"

namespace tfs
{
  namespace kvmetaserver
  {
    class LifeCycleHelper
    {
      public:
        LifeCycleHelper();
        virtual ~LifeCycleHelper();

        int init(tfs::common::KvEngineHelper*);

        int set_file_lifecycle(const int32_t file_type, const std::string& file_name,
            const int32_t invalid_time_s, const std::string& app_key);

        int get_file_lifecycle(const int32_t file_type, const std::string& file_name,
            int32_t* invalid_time_s);

        int rm_life_cycle(const int32_t file_type, const std::string& file_name);

      protected:

        common::KvEngineHelper* kv_engine_helper_;
        int32_t meta_info_name_area_;
      private:
        DISALLOW_COPY_AND_ASSIGN(LifeCycleHelper);
        int new_lifecycle(
            const common::KvKey& name_exptime_key,
            const common::KvMemValue& name_exptime_value,
            const common::KvKey& exptime_appkey_key,
            const common::KvMemValue& exptime_appkey_value);
    };
  }
}

#endif
