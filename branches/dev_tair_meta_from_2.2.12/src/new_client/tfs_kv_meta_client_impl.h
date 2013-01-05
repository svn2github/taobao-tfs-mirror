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
#include "common/define.h"
#include "common/meta_kv_define.h"
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

        int initialize(const char *kms_addr);
        int initialize(const int64_t kms_addr);

        TfsRetType put_bucket(const char *bucket_name);
        TfsRetType get_bucket(const char *bucket_name);
        TfsRetType del_bucket(const char *bucket_name);

        TfsRetType put_object(const char *bucket_name, const char *object_name,
            const char* local_file);
        TfsRetType get_object(const char *bucket_name, const char *object_name,
            const char* local_file);
        TfsRetType del_object(const char *bucket_name, const char *object_name);

        int do_put_bucket(const char *bucket_name);
        int do_get_bucket(const char *bucket_name);
        int do_del_bucket(const char *bucket_name);

        int do_put_object(const char *bucket_name,
            const char *object_name, const common::TfsFileInfo& tfs_file_info);
        int do_get_object(const char *bucket_name,
            const char *object_name, common::TfsFileInfo& tfs_file_info);
        int do_del_object(const char *bucket_name,
            const char *object_name);

        bool is_valid_file_path(const char* file_path);

      private:
        DISALLOW_COPY_AND_ASSIGN(KvMetaClientImpl);
        uint64_t kms_id_;
        common::BasePacketFactory* packet_factory_;
        common::BasePacketStreamer* packet_streamer_;
        TfsMetaManager tfs_meta_manager_;
    };
  }
}

#endif
