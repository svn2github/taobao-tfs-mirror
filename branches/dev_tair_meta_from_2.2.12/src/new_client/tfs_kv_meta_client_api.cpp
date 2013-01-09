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

    int KvMetaClient::initialize(const char *kms_addr, const char *ns_addr)
    {
      return impl_->initialize(kms_addr, ns_addr);
    }

    int KvMetaClient::initialize(const int64_t kms_addr, const char *ns_addr)
    {
      return impl_->initialize(kms_addr, ns_addr);
    }

    TfsRetType KvMetaClient::put_bucket(const char *bucket_name)
    {
      return impl_->put_bucket(bucket_name);
    }

    TfsRetType KvMetaClient::get_bucket(const char *bucket_name, const char *prefix,
                                        const char* start_key, const int32_t limit,
                                        vector<string>& v_object_name)
    {
      return impl_->get_bucket(bucket_name, prefix, start_key, limit, v_object_name);
    }

    TfsRetType KvMetaClient::del_bucket(const char *bucket_name)
    {
      return impl_->del_bucket(bucket_name);
    }

    TfsRetType KvMetaClient::put_object(const char *bucket_name, const char *object_name,
        const char* local_file)
    {
      return impl_->put_object(bucket_name, object_name, local_file);
    }

    TfsRetType KvMetaClient::get_object(const char *bucket_name, const char *object_name,
        const char* local_file)
    {
      return impl_->get_object(bucket_name, object_name, local_file);
    }

    TfsRetType KvMetaClient::del_object(const char *bucket_name, const char *object_name)
    {
      return impl_->del_object(bucket_name, object_name);
    }

  }
}

