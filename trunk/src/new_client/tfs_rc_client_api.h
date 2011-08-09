/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_client_api.h 19 2010-10-18 05:48:09Z nayan@taobao.com $
 *
 * Authors:
 *      - initial release
 *
 */
#ifndef TFS_CLIENT_RC_CLIENTAPI_H_
#define TFS_CLIENT_RC_CLIENTAPI_H_

#include <string>
#include "common/define.h"

namespace tfs
{
  namespace client
  {
    typedef int TfsRetType;
    class RcClientImpl;
    class RcClient
    {
      public:
        enum RC_MODE
        {
          CREATE = 1,
          READ = 2,
          STAT = 3,
        };
      public:
        RcClient();
        ~RcClient();

        TfsRetType initialize(const char* str_rc_ip, const char* app_key, const char* str_app_ip,
            const int32_t cache_times = 0,
            const int32_t cache_items = 0,
            const char* dev_name = NULL);
        TfsRetType initialize(const uint64_t rc_ip, const char* app_key, const uint64_t app_ip,
            const int32_t cache_times = 0,
            const int32_t cache_items = 0,
            const char* dev_name = NULL);

        void set_wait_timeout(const int64_t timeout_ms);
        void set_log_level(const char* level);
        void set_log_file(const char* log_file);

        int open(const char* file_name, const char* suffix, const RC_MODE mode,
            const bool large = false, const char* local_key = NULL);
        TfsRetType close(const int fd, char* tfs_name_buff = NULL, const int32_t buff_len = 0);

        int64_t read(const int fd, void* buf, const int64_t count);
        int64_t readv2(const int fd, void* buf, const int64_t count, common::TfsFileStat* tfs_stat_buf);
        int64_t pread(const int fd, void* buf, const int64_t count, const int64_t offset);

        int64_t write(const int fd, const void* buf, const int64_t count);
        //not support pwrite for now
        //int64_t pwrite(const int fd, const void* buf, const int64_t count, const int64_t offset);

        int64_t lseek(const int fd, const int64_t offset, const int whence);
        TfsRetType fstat(const int fd, common::TfsFileStat* buf);

        TfsRetType unlink(const char* file_name, const char* suffix = NULL,
            const common::TfsUnlinkType action = common::DELETE);
        int64_t save_file(const char* local_file, char* tfs_name_buff, const int32_t buff_len,
            const bool is_large_file = false);
        int64_t save_file(const char* source_data, const int32_t data_len,
            char* tfs_name_buff, const int32_t buff_len);

        TfsRetType logout();
      private:
        RcClient(const RcClient&);
        RcClientImpl* impl_;

    };
  }
}

#endif
