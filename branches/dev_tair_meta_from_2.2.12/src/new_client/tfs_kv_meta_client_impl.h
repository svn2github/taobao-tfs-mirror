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

namespace tfs
{
  namespace client
  {
    class KvMetaClientImpl
    {
      public:
        KvMetaClientImpl();
        ~KvMetaClientImpl();

        int initialize(const char *rs_addr, const char *ns_addr);
        int initialize(const int64_t rs_addr, const char *ns_addr);
        bool need_update_table(const int ret_status);
        int update_table_from_rootserver();
        uint64_t get_meta_server_id();

        TfsRetType put_bucket(const char *bucket_name, const common::UserInfo &user_info);
        TfsRetType get_bucket(const char *bucket_name, const char *prefix,
                              const char *start_key, const char delimiter, const int32_t limit,
                              std::vector<common::ObjectMetaInfo> *v_object_meta_info,
                              std::vector<std::string> *v_object_name, std::set<std::string> *s_common_prefix,
                              int8_t *is_trucated, const common::UserInfo &user_info);
        TfsRetType del_bucket(const char *bucket_name, const common::UserInfo &user_info);
        TfsRetType head_bucket(const char *bucket_name, common::BucketMetaInfo *bucket_meta_info, const common::UserInfo &user_info);

        int64_t pwrite_object(const char *bucket_name, const char *object_name,
            const void *buffer, int64_t offset, int64_t length, const common::UserInfo &user_info);

        int64_t pread_object(const char *bucket_name, const char *object_name,
            void *buffer, int64_t offset, int64_t length, const common::UserInfo &user_info);

        int64_t put_object_to_buf(const char *bucket_name, const char *object_name,
            const void *buffer, int64_t offset, int64_t length, const common::UserInfo &user_info);
        int64_t get_object_to_buf(const char *bucket_name, const char *object_name,
            void *buffer, const int64_t offset, int64_t length,
            common::ObjectMetaInfo *object_meta_info, common::CustomizeInfo *customize_info, const common::UserInfo &user_info);

        TfsRetType put_object(const char *bucket_name, const char *object_name,
            const char* local_file, const int64_t req_offset, const int64_t req_length, const common::UserInfo &user_info);
        TfsRetType get_object(const char *bucket_name, const char *object_name,
            const char* local_file, const int64_t req_offset, const int64_t req_length,
            common::ObjectMetaInfo *object_meta_info,
            common::CustomizeInfo *customize_info, const common::UserInfo &user_info);

        TfsRetType del_object(const char *bucket_name, const char *object_name, const common::UserInfo &user_info);
        TfsRetType head_object(const char *bucket_name, const char *object_name, common::ObjectInfo *object_info, const common::UserInfo &user_info);

        int do_put_bucket(const char *bucket_name, const common::BucketMetaInfo& bucket_meta_info, const common::UserInfo &user_info);
        int do_get_bucket(const char *bucket_name, const char *prefix,
                          const char *start_key, const char delimiter, const int32_t limit,
                          std::vector<common::ObjectMetaInfo> *v_object_meta_info,
                          std::vector<std::string> *v_object_name, std::set<std::string> *s_common_prefix,
                          int8_t *is_trucated, const common::UserInfo &user_info);

        int do_del_bucket(const char *bucket_name, const common::UserInfo &user_info);
        int do_head_bucket(const char *bucket_name, common::BucketMetaInfo *bucket_meta_info, const common::UserInfo &user_info);


        int do_put_object(const char *bucket_name, const char *object_name,
                          const common::ObjectInfo &object_info, const common::UserInfo &user_info);
        int do_get_object(const char *bucket_name, const char *object_name,
                          const int64_t offset, const int64_t length, common::ObjectInfo *object_info,
                          bool *still_have, const common::UserInfo &user_info);
        int do_del_object(const char *bucket_name, const char *object_name,
                          common::ObjectInfo *object_info,
                          bool *still_have, const common::UserInfo &user_info);
        int do_head_object(const char *bucket_name, const char *object_name, common::ObjectInfo *object_info, const common::UserInfo &user_info);

        static bool is_valid_bucket_name(const char *bucket_name);
        static bool is_valid_object_name(const char *object_name);

      private:
        int64_t write_data(const char *ns_addr,
            const void *buffer, int64_t offset, int64_t length,
            std::vector<common::FragMeta> *v_frag_meta);
        int64_t read_data(const char* ns_addr,
            const std::vector<common::FragMeta> &v_frag_meta,
            void *buffer, int64_t offset, int64_t length);
        int unlink_file(const std::vector<common::FragMeta> &v_frag_meta, const char *ns_addr, int32_t cluster_id);
        int del_file(const std::vector< std::pair <common::FragMeta, int32_t> > &v_frag_pair, const char* ns_addr);

      private:
        DISALLOW_COPY_AND_ASSIGN(KvMetaClientImpl);
        tbsys::CRWLock meta_table_mutex_;
        uint64_t rs_id_;
        uint32_t access_count_;
        common::KvMetaTable meta_table_;
        std::string ns_addr_;
        common::BasePacketFactory* packet_factory_;
        common::BasePacketStreamer* packet_streamer_;
        TfsMetaManager tfs_meta_manager_;
    };
  }
}

#endif
