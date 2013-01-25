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

        int initialize(const char *kms_addr, const char *ns_addr);
        int initialize(const int64_t kms_addr, const char *ns_addr);

        TfsRetType put_bucket(const char *bucket_name);
        TfsRetType get_bucket(const char *bucket_name, const char *prefix,
                              const char *start_key, const char delimiter, const int32_t limit,
                              std::vector<common::ObjectMetaInfo> *v_object_meta_info,
                              std::vector<std::string> *v_object_name, std::set<std::string> *s_common_prefix,
                              int8_t *is_trucated);
        TfsRetType del_bucket(const char *bucket_name);
        TfsRetType head_bucket(const char *bucket_name, common::BucketMetaInfo *bucket_meta_info);

        int64_t pwrite_object(const char *bucket_name, const char *object_name,
            const void *buffer, int64_t offset, int64_t length);

        int64_t put_object_to_buf(const char *bucket_name, const char *object_name,
            const void *buffer, int64_t offset, int64_t length);
        int64_t get_object_to_buf(const char *bucket_name, const char *object_name,
            void *buffer, const int64_t offset, int64_t length,
            common::ObjectMetaInfo *object_meta_info, common::CustomizeInfo *customize_info);

        TfsRetType put_object(const char *bucket_name, const char *object_name,
            const char* local_file, const int64_t req_offset, const int64_t req_length);
        TfsRetType get_object(const char *bucket_name, const char *object_name,
            const char* local_file, const int64_t req_offset, const int64_t req_length,
            common::ObjectMetaInfo *object_meta_info,
            common::CustomizeInfo *customize_info);

        TfsRetType del_object(const char *bucket_name, const char *object_name);
        TfsRetType head_object(const char *bucket_name, const char *object_name, common::ObjectInfo *object_info);

        int do_put_bucket(const char *bucket_name);
        int do_get_bucket(const char *bucket_name, const char *prefix,
                          const char *start_key, const char delimiter, const int32_t limit,
                          std::vector<common::ObjectMetaInfo> *v_object_meta_info,
                          std::vector<std::string> *v_object_name, std::set<std::string> *s_common_prefix,
                          int8_t *is_trucated);

        int do_del_bucket(const char *bucket_name);
        int do_head_bucket(const char *bucket_name, common::BucketMetaInfo *bucket_meta_info);


        int do_put_object(const char *bucket_name, const char *object_name,
                          const common::ObjectInfo &object_info);
        int do_get_object(const char *bucket_name, const char *object_name,
                          const int64_t offset, const int64_t length, common::ObjectInfo *object_info,
                          bool *still_have);
        int do_del_object(const char *bucket_name, const char *object_name);
        int do_head_object(const char *bucket_name, const char *object_name, common::ObjectInfo *object_info);

        static bool is_valid_bucket_name(const char *bucket_name);
        static bool is_valid_object_name(const char *object_name);

      private:
        int64_t write_data(const char *ns_addr,
            const void *buffer, int64_t offset, int64_t length,
            std::vector<common::FragMeta> *v_frag_meta);
        int64_t read_data(const char* ns_addr,
            const std::vector<common::FragMeta> &v_frag_meta,
            void *buffer, int64_t offset, int64_t length);

      private:
        DISALLOW_COPY_AND_ASSIGN(KvMetaClientImpl);
        uint64_t kms_id_;
        std::string ns_addr_;
        common::BasePacketFactory* packet_factory_;
        common::BasePacketStreamer* packet_streamer_;
        TfsMetaManager tfs_meta_manager_;
    };
  }
}

#endif
