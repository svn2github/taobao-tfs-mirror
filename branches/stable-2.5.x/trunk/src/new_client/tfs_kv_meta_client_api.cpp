/*
 * (C) 2007-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_meta_client_api.cpp 943 2011-10-20 01:40:25Z chuyu $
 *
 * Authors:
 *    mingyan<mingyan.zc@taobao.com>
 *      - initial release
 *
 */

#include "common/define.h"
#include "tfs_kv_meta_client_api.h"
#include "tfs_kv_meta_client_impl.h"

namespace tfs
{
  namespace client
  {
    using namespace std;

    KvMetaClient::KvMetaClient():impl_(NULL)
    {
      impl_ = new KvMetaClientImpl();
    }

    KvMetaClient::~KvMetaClient()
    {
      delete impl_;
      impl_ = NULL;
    }

    int KvMetaClient::initialize(const char *rs_addr)
    {
      return impl_->initialize(rs_addr);
    }

    int KvMetaClient::initialize(const uint64_t rs_addr)
    {
      return impl_->initialize(rs_addr);
    }

    void KvMetaClient::set_tfs_cluster_manager(TfsClusterManager *tfs_cluster_manager)
    {
      impl_->set_tfs_cluster_manager(tfs_cluster_manager);
    }

    TfsRetType KvMetaClient::put_bucket(const char *bucket_name,
        const common::UserInfo &user_info)
    {
      return impl_->put_bucket(bucket_name, user_info);
    }

    TfsRetType KvMetaClient::get_bucket(const char *bucket_name, const char *prefix,
        const char *start_key, const char delimiter, const int32_t limit,
        vector<common::ObjectMetaInfo> *v_object_meta_info,
        vector<string> *v_object_name, set<string> *s_common_prefix,
        int8_t *is_truncated, const common::UserInfo &user_info)
    {
      return impl_->get_bucket(bucket_name, prefix, start_key, delimiter, limit,
          v_object_meta_info, v_object_name, s_common_prefix, is_truncated, user_info);
    }

    TfsRetType KvMetaClient::del_bucket(const char *bucket_name,
        const common::UserInfo &user_info)
    {
      return impl_->del_bucket(bucket_name, user_info);
    }

    TfsRetType KvMetaClient::head_bucket(const char *bucket_name,
        common::BucketMetaInfo *bucket_meta_info, const common::UserInfo &user_info)
    {
      return impl_->head_bucket(bucket_name, bucket_meta_info, user_info);
    }


    TfsRetType KvMetaClient::put_object(const char *bucket_name,
        const char *object_name, const char* local_file,
        const common::UserInfo &user_info)
    {
      return impl_->put_object(bucket_name, object_name, local_file, user_info);
    }

    int64_t KvMetaClient::pwrite_object(const char *bucket_name,
        const char *object_name, const void *buf, const int64_t offset,
        const int64_t length, const common::UserInfo &user_info)
    {
      return impl_->pwrite_object(bucket_name, object_name, buf, offset,
          length, user_info);
    }

    int64_t KvMetaClient::pread_object(const char *bucket_name,
        const char *object_name, void *buf, const int64_t offset,
        const int64_t length, common::ObjectMetaInfo *object_meta_info,
        common::CustomizeInfo *customize_info,
        const common::UserInfo &user_info)
    {
      return impl_->pread_object(bucket_name, object_name, buf, offset,
          length, object_meta_info, customize_info, user_info);
    }

    TfsRetType KvMetaClient::get_object(const char *bucket_name,
        const char *object_name, const char* local_file,
        const common::UserInfo &user_info)
    {
      return impl_->get_object(bucket_name, object_name, local_file,
          NULL, NULL, user_info);
    }

    TfsRetType KvMetaClient::del_object(const char *bucket_name,
        const char *object_name, const common::UserInfo &user_info)
    {
      return impl_->del_object(bucket_name, object_name, user_info);
    }

    TfsRetType KvMetaClient::head_object(const char *bucket_name,
        const char *object_name, common::ObjectInfo *object_info,
        const common::UserInfo &user_info)
    {
      return impl_->head_object(bucket_name, object_name, object_info, user_info);
    }

  }
}

