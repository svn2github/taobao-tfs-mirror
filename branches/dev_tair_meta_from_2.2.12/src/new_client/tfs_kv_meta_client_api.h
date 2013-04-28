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

        TfsRetType put_bucket_tag(const char *bucket_name, const common::MAP_STRING &bucket_tag_map);
        TfsRetType get_bucket_tag(const char *bucket_name, common::MAP_STRING *bucket_tag_map);
        TfsRetType del_bucket_tag(const char *bucket_name);

        TfsRetType list_multipart_object(const char *bucket_name, const char *prefix,
            const char *start_key, const char *start_id, const char delimiter,
            const int32_t limit, common::ListMultipartObjectResult *list_multipart_object_result,
            const common::UserInfo &user_info);

        TfsRetType put_object(const char *bucket_name,
            const char *object_name, const char* local_file,
            const common::UserInfo &user_info,
            const common::CustomizeInfo &customize_info);
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

        TfsRetType init_multipart(const char *bucket_name,
            const char *object_name, std::string* const upload_id);
        TfsRetType upload_multipart(const char *bucket_name,
            const char *object_name, const char* local_file, const char* upload_id,
            const int32_t part_num, const common::UserInfo &user_info);
        int64_t pwrite_upload_multipart(const char *bucket_name,
            const char *object_name, const void *buffer, int64_t offset,
            int64_t length, const char* upload_id,
            const int32_t part_num, const common::UserInfo &user_info);
        TfsRetType complete_multipart(const char *bucket_name,
                const char *object_name, const char* upload_id,
                        const std::vector<int32_t>& v_part_num);
        TfsRetType list_multipart(const char *bucket_name,
                const char *object_name, const char* upload_id,
                        std::vector<int32_t>* const p_v_part_num);
        TfsRetType abort_multipart(const char *bucket_name,
            const char *object_name, const char* upload_id);
      private:
        DISALLOW_COPY_AND_ASSIGN(KvMetaClient);
        KvMetaClientImpl* impl_;
    };
  }
}

#endif
