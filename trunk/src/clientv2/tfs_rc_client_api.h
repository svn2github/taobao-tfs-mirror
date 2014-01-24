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
#include "common/meta_server_define.h"
#include "common/kv_meta_define.h"

namespace tfs
{
  namespace clientv2
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
          //STAT = 3,
          WRITE = 4, //raw tfs will not use this. only CREATE.
                     //this for name meta tfs.
                     //use create create a tfs_file, use write for writing or appendding
          READ_FORCE = 8
        };
      public:
        RcClient();
        ~RcClient();

        TfsRetType initialize(const char* str_rc_ip, const char* app_key, const char* str_app_ip = NULL,
            const int32_t cache_times = -1,
            const int32_t cache_items = -1,
            const char* dev_name = NULL);
        TfsRetType initialize(const uint64_t rc_ip, const char* app_key, const uint64_t app_ip = 0,
            const int32_t cache_times = -1,
            const int32_t cache_items = -1,
            const char* dev_name = NULL);

        int64_t get_app_id() const;

        // for imgsrc tmp use
        void set_remote_cache_info(const char * remote_cache_info);

        void set_wait_timeout(const int64_t timeout_ms);
        void set_log_level(const char* level);
        void set_log_file(const char* log_file);


        TfsRetType logout();

        // for raw tfs
        int open(const char* file_name, const char* suffix, const RC_MODE mode);
        TfsRetType close(const int fd, char* tfs_name_buff = NULL, const int32_t buff_len = 0);

        int64_t read(const int fd, void* buf, const int64_t count);
        int64_t readv2(const int fd, void* buf, const int64_t count, common::TfsFileStat* tfs_stat_buf);

        int64_t write(const int fd, const void* buf, const int64_t count);

        int64_t lseek(const int fd, const int64_t offset, const int whence);
        TfsRetType fstat(const int fd, common::TfsFileStat* buf, const common::TfsStatType fmode);

        TfsRetType unlink(const char* file_name, const char* suffix = NULL,
            const common::TfsUnlinkType action = common::DELETE);
        int64_t save_file(const char* local_file, char* tfs_name_buff, const int32_t buff_len,
            const char* suffix = NULL);
        int64_t save_buf(const char* source_data, const int32_t data_len,
            char* tfs_name_buff, const int32_t buff_len, const char* suffix = NULL);
        int fetch_file(const char* local_file,
                       const char* file_name, const char* suffix = NULL);
        // for kv meta
        void set_kv_rs_addr(const char *rs_addr); // tmp use

        TfsRetType put_bucket(const char *bucket_name,
            const common::UserInfo &user_info);
        TfsRetType get_bucket(const char *bucket_name, const char *prefix,
            const char *start_key, const char delimiter, const int32_t limit,
            std::vector<common::ObjectMetaInfo> *v_object_meta_info,
            std::vector<std::string> *v_object_name, std::set<std::string> *s_common_prefix,
            int8_t *is_truncated, const common::UserInfo &user_info);
        TfsRetType del_bucket(const char *bucket_name,
            const common::UserInfo &user_info);
        TfsRetType head_bucket(const char *bucket_name,
            common::BucketMetaInfo *bucket_meta_info,
            const common::UserInfo &user_info);

        TfsRetType put_object(const char *bucket_name, const char *object_name,
            const char* local_file, const common::UserInfo &user_info);
        int64_t pwrite_object(const char *bucket_name, const char *object_name,
            const void *buf, const int64_t offset, const int64_t length,
            const common::UserInfo &user_info);
        int64_t pread_object(const char *bucket_name, const char *object_name,
            void *buf, const int64_t offset, const int64_t length,
            common::ObjectMetaInfo *object_meta_info,
            common::CustomizeInfo *customize_info,
            const common::UserInfo &user_info);
        TfsRetType get_object(const char *bucket_name, const char *object_name,
            const char* local_file, const common::UserInfo &user_info);
        TfsRetType del_object(const char *bucket_name, const char *object_name,
            const common::UserInfo &user_info);
        TfsRetType head_object(const char *bucket_name, const char *object_name,
            common::ObjectInfo *object_info, const common::UserInfo &user_info);

        TfsRetType set_life_cycle(const int32_t file_type, const char *file_name,
                                  const int32_t invalid_time_s, const char *app_key);
        TfsRetType get_life_cycle(const int32_t file_type, const char *file_name,
                                        int32_t *invalid_time_s);
        TfsRetType rm_life_cycle(const int32_t file_type, const char *file_name);


      private:
        RcClient(const RcClient&);
        RcClientImpl* impl_;
    };
  }
}

#endif
