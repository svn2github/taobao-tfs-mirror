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
#ifndef TFS_CHECKSERVER_DEFAULTSERVERHELPER_H
#define TFS_CHECKSERVER_DEFAULTSERVERHELPER_H

#include "tbnet.h"
#include <Handle.h>
#include "common/internal.h"
#include "common/new_client.h"
#include "common/client_manager.h"
#include "message/message_factory.h"
#include "common/config_item.h"

#include "base_server_helper.h"

namespace tfs
{
  namespace checkserver
  {
    class DefaultServerHelper: public BaseServerHelper
    {
      public:
        DefaultServerHelper();
        ~DefaultServerHelper();
        int get_all_ds(const uint64_t ns_id, common::VUINT64& servers);
        int get_block_replicas(const uint64_t ns_id, const uint64_t block_id, common::VUINT64& servers);
        int fetch_check_blocks(const uint64_t ds_id, const common::TimeRange& range,
            const int32_t group_count, const int32_t group_seq, common::VUINT64& blocks);
        int dispatch_check_blocks(const uint64_t ds_id, const int64_t seqno, const int32_t interval, const common::VUINT64& blocks);
        int get_group_info(const uint64_t ns_id, int32_t& group_count, int32_t& group_seq);
      private:
        int get_param_from_ns(const uint64_t ns_id, const int32_t param_index, std::string& param_value);
    };
  }
}
#endif

