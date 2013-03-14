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
#ifndef TFS_CLIENTV2_TFSCLIENTAPI_H_
#define TFS_CLIENTV2_TFSCLIENTAPI_H_

#include <stdio.h>
#include <pthread.h>

#include "common/define.h"

namespace tfs
{
  namespace clientv2
  {
    class TfsClientV2
    {
    public:
      static TfsClientV2* Instance()
      {
        static TfsClientV2 tfs_client_;
        return &tfs_client_;
      }

      int initialize(const char* ns_addr = NULL);
      int destroy();
      int stat_file(common::TfsFileStat* file_stat, const char* file_name, const char* suffix = NULL,
          const common::TfsStatType stat_type = common::NORMAL_STAT, const char* ns_addr = NULL);
      int64_t save_file(char* ret_tfs_name, const int32_t ret_tfs_name_len,
          const char* local_file, const int32_t mode, const char* suffix = NULL,
          const char* ns_addr = NULL);
      int fetch_file(const char* local_file, const char* file_name, const char* suffix = NULL,
          const char* ns_addr = NULL);
      int unlink(int64_t& file_size, const char* file_name, const char* suffix = NULL,
          const common::TfsUnlinkType action = common::DELETE,
          const common::OptionFlag option_flag = common::TFS_FILE_DEFAULT_OPTION,
          const char* ns_addr = NULL);
      int64_t get_server_id();  // get default ns addr, which passed in initialize
      int32_t get_cluster_id(); // get default cluster id, which passed in initialize

    private:
      TfsClientV2();
      DISALLOW_COPY_AND_ASSIGN(TfsClientV2);
      ~TfsClientV2();
    };
  }
}

#endif  // TFS_CLIENTV2_TFSCLIENTAPI_H_
