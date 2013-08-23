/*
 * (C) 2007-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id
 *
 * Authors:
 *      mingyan(mingyan.zc@taobao.com)
 *      - initial release
 *
 */
#ifndef TFS_CLIENT_KV_META_CLIENT_API_H_
#define TFS_CLIENT_KV_META_CLIENT_API_H_

#include <vector>
#include <set>
#include "common/kv_meta_define.h"

namespace tfs
{
  namespace client
  {
    typedef int TfsRetType;
    class KvMetaClientImpl;
    class TfsClusterManager;
    class KvMetaClient
    {
      public:
        KvMetaClient();
        ~KvMetaClient();

        int initialize(const char *rs_addr);
        int initialize(const uint64_t rs_addr);
        void set_tfs_cluster_manager(TfsClusterManager *tfs_cluster_manager);

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

        TfsRetType put_object(const char *bucket_name,
            const char *object_name, const char* local_file,
            const common::UserInfo &user_info);
        int64_t pwrite_object(const char *bucket_name,
            const char *object_name, const void *buf,
            const int64_t offset, const int64_t length,
            const common::UserInfo &user_info);
        int64_t pread_object(const char *bucket_name,
            const char *object_name, void *buf, const int64_t offset,
            const int64_t length, common::ObjectMetaInfo *object_meta_info,
            common::CustomizeInfo *customize_info, const common::UserInfo &user_info);
        TfsRetType get_object(const char *bucket_name,
            const char *object_name, const char* local_file,
            const common::UserInfo &user_info);
        TfsRetType del_object(const char *bucket_name,
            const char *object_name, const common::UserInfo &user_info);
        TfsRetType head_object(const char *bucket_name, const char *object_name,
            common::ObjectInfo *object_info, const common::UserInfo &user_info);

        TfsRetType set_life_cycle(const int32_t file_type, const char *file_name,
                                  const int32_t invalid_time_s, const char *app_key);
        TfsRetType get_life_cycle(const int32_t file_type, const char *file_name,
                                  int32_t *invalid_time_s);
        TfsRetType rm_life_cycle(const int32_t file_type, const char *file_name);

      private:
        DISALLOW_COPY_AND_ASSIGN(KvMetaClient);
        KvMetaClientImpl* impl_;
    };
  }
}

#endif
