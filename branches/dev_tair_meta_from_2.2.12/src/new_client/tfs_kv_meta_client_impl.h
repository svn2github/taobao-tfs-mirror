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
#ifndef TFS_CLIENT_KV_META_CLIENT_IMPL_H_
#define TFS_CLIENT_KV_META_CLIENT_IMPL_H_

#include <stdio.h>
#include <tbsys.h>
#include "common/kv_meta_define.h"
#include "common/kv_rts_define.h"
#include "tfs_meta_manager.h"
#include "tfs_kv_meta_client_api.h"
#include "tfs_cluster_manager.h"

namespace tfs
{
  namespace client
  {
    class KvMetaClientImpl
    {
      public:
        KvMetaClientImpl();
        ~KvMetaClientImpl();

        int initialize(const char *rs_addr);
        int initialize(const uint64_t rs_addr);
        bool need_update_table(const int ret_status);
        int update_table_from_rootserver();
        uint64_t get_meta_server_id();
        void set_tfs_cluster_manager(TfsClusterManager *tfs_cluster_manager)
        {
          tfs_cluster_manager_ = tfs_cluster_manager;
        }

        TfsRetType put_bucket(const char *bucket_name, const common::UserInfo &user_info);
        TfsRetType get_bucket(const char *bucket_name, const char *prefix,
            const char *start_key, const char delimiter, const int32_t limit,
            std::vector<common::ObjectMetaInfo> *v_object_meta_info,
            std::vector<std::string> *v_object_name, std::set<std::string> *s_common_prefix,
            int8_t *is_trucated, const common::UserInfo &user_info);
        TfsRetType del_bucket(const char *bucket_name, const common::UserInfo &user_info);
        TfsRetType head_bucket(const char *bucket_name,
            common::BucketMetaInfo *bucket_meta_info, const common::UserInfo &user_info);

        TfsRetType put_bucket_tag(const char *bucket_name, const common::MAP_STRING &bucket_tag_map);
        TfsRetType get_bucket_tag(const char *bucket_name, common::MAP_STRING *bucket_tag_map);
        TfsRetType del_bucket_tag(const char *bucket_name);

        TfsRetType list_multipart_object(const char *bucket_name, const char *prefix,
            const char *start_key, const char *star_id, const char delimiter, const int32_t limit,
            common::ListMultipartObjectResult *list_multipart_object_result, const common::UserInfo &user_info);

        int64_t pwrite_object(const char *bucket_name, const char *object_name,
            const void *buffer, int64_t offset, int64_t length,
            const common::UserInfo &user_info, const common::CustomizeInfo *customize_info);
        int64_t pread_object(const char *bucket_name, const char *object_name,
            void *buffer, const int64_t offset, int64_t length,
            common::ObjectMetaInfo *object_meta_info, common::CustomizeInfo *customize_info,
            const common::UserInfo &user_info);
        TfsRetType put_object(const char *bucket_name, const char *object_name,
            const char* local_file, const common::UserInfo &user_info,
            const common::CustomizeInfo &customize_info);
        TfsRetType get_object(const char *bucket_name, const char *object_name,
            const char* local_file, common::ObjectMetaInfo *object_meta_info,
            common::CustomizeInfo *customize_info, const common::UserInfo &user_info);
        TfsRetType del_object(const char *bucket_name, const char *object_name,
            const common::UserInfo &user_info);
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
                const char *object_name, const char* upload_id, const std::vector<int32_t>& v_part_num);
        TfsRetType list_multipart(const char *bucket_name, const char *object_name,
                const char* upload_id, std::vector<int32_t>* const p_v_part_num);
        TfsRetType abort_multipart(const char *bucket_name,
              const char *object_name, const char* upload_id);

        void update_fail_info(const int ret);
        int do_put_bucket(const char *bucket_name,
            const common::BucketMetaInfo& bucket_meta_info, const common::UserInfo &user_info);
        int do_get_bucket(const char *bucket_name, const char *prefix,
            const char *start_key, const char delimiter, const int32_t limit,
            std::vector<common::ObjectMetaInfo> *v_object_meta_info,
            std::vector<std::string> *v_object_name, std::set<std::string> *s_common_prefix,
            int8_t *is_trucated, const common::UserInfo &user_info);

        int do_list_multipart_object(const char *bucket_name, const char *prefix,
            const char *start_key, const char *star_id, const char delimiter, const int32_t limit,
            common::ListMultipartObjectResult *list_multipart_object_result, const common::UserInfo &user_info);

        int do_del_bucket(const char *bucket_name, const common::UserInfo &user_info);
        int do_head_bucket(const char *bucket_name, common::BucketMetaInfo *bucket_meta_info,
            const common::UserInfo &user_info);

        int do_put_bucket_tag(const char *bucket_name, const common::MAP_STRING &bucket_tag_map);
        int do_get_bucket_tag(const char *bucket_name, common::MAP_STRING *bucket_tag_map);
        int do_del_bucket_tag(const char *bucket_name);

        int do_put_object(const char *bucket_name, const char *object_name,
            const common::ObjectInfo &object_info, const common::UserInfo &user_info);
        int do_get_object(const char *bucket_name, const char *object_name,
            const int64_t offset, const int64_t length, common::ObjectInfo *object_info,
            bool *still_have, const common::UserInfo &user_info);
        int do_del_object(const char *bucket_name, const char *object_name,
            common::ObjectInfo *object_info,
            bool *still_have, const common::UserInfo &user_info);
        int do_head_object(const char *bucket_name, const char *object_name,
            common::ObjectInfo *object_info, const common::UserInfo &user_info);
        int do_init_multipart(const char *bucket_name, const char *object_name,
            std::string* const upload_id);
        int do_upload_multipart(const char *bucket_name,
            const char *object_name, const common::ObjectInfo &object_info, const common::UserInfo &user_info,
            const char* upload_id, const int32_t part_num);
        int do_complete_multipart(const char *bucket_name, const char *object_name,
                const char* upload_id, const std::vector<int32_t>& v_part_num);
        int do_list_multipart(const char *bucket_name, const char *object_name,
                const char* upload_id, std::vector<int32_t>* const p_v_part_num);
        int do_abort_multipart(const char *bucket_name, const char *object_name,
            const char* upload_id, common::ObjectInfo *object_info, bool* still_have);
        static bool is_valid_string(const std::string &str);
        static bool is_valid_bucket_tag(const common::MAP_STRING &bucket_tag_map);
        static bool is_valid_bucket_name(const char *bucket_name);
        static bool is_valid_object_name(const char *object_name);
        static bool is_valid_customize_info(const common::CustomizeInfo &customize_info);

      private:
        int64_t write_data(const char *ns_addr,
            const void *buffer, int64_t offset, int64_t length,
            std::vector<common::FragMeta> *v_frag_meta);
        int64_t read_data(
            const std::vector<common::TfsFileInfo> &v_tfs_info,
            void *buffer, int64_t offset, int64_t length, bool still_have);
        int unlink_file(const std::vector<common::TfsFileInfo> &v_tfs_info);

      private:
        DISALLOW_COPY_AND_ASSIGN(KvMetaClientImpl);
        tbsys::CRWLock meta_table_mutex_;
        uint64_t rs_id_;
        uint32_t access_count_;
        uint32_t fail_count_;
        common::KvMetaTable meta_table_;
        common::BasePacketFactory* packet_factory_;
        common::BasePacketStreamer* packet_streamer_;
        TfsMetaManager tfs_meta_manager_;
        TfsClusterManager *tfs_cluster_manager_;
    };
  }
}

#endif
