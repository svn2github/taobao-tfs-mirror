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
    class KvMetaClient
    {
      public:
        KvMetaClient();
        ~KvMetaClient();

        int initialize(const char *kms_addr, const char *ns_addr);
        int initialize(const int64_t kms_addr, const char *ns_addr);

        TfsRetType put_bucket(const char *bucket_name);
        TfsRetType get_bucket(const char *bucket_name, const char *prefix,
                              const char *start_key, const char delimiter, const int32_t limit,
                              std::vector<common::ObjectMetaInfo> *v_object_meta_info,
                              std::vector<std::string> *v_object_name, std::set<std::string> *s_common_prefix,
                              int8_t *is_truncated);
        TfsRetType del_bucket(const char *bucket_name);

        TfsRetType put_object(const char *bucket_name, const char *object_name,
            const char* local_file);
        TfsRetType get_object(const char *bucket_name, const char *object_name,
            const char* local_file);
        TfsRetType del_object(const char *bucket_name, const char *object_name);

      private:
        DISALLOW_COPY_AND_ASSIGN(KvMetaClient);
        KvMetaClientImpl* impl_;
    };
  }
}

#endif
