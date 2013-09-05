/*
 * (C) 2007-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_meta_helper.cpp 918 2011-10-14 03:16:21Z daoan@taobao.com $
 *
 * Authors:
 *      mingyan(mingyan.zc@taobao.com)
 *      - initial release
 *
 */
#ifndef TFS_CLIENT_KVMETA_HELPER_H_
#define TFS_CLIENT_KVMETA_HELPER_H_

#include <vector>
#include <set>
#include "common/kv_meta_define.h"
#include "common/kv_rts_define.h"

namespace tfs
{
  namespace client
  {
    class KvMetaHelper
    {
    public:
      static int do_put_bucket(const uint64_t server_id, const char *bucket_name, const common::BucketMetaInfo& bucket_meta_info,
                               const common::UserInfo &user_info);
      static int get_table(const uint64_t server_id, common::KvMetaTable& table);
      // TODO: parameter
      static int do_get_bucket(const uint64_t server_id, const char *bucket_name,
                               const char *prefix, const char *start_key, char delimiter, const int32_t limit,
                               std::vector<common::ObjectMetaInfo> *v_object_meta_info,
                               std::vector<std::string> *v_object_name, std::set<std::string> *s_common_prefix,
                               int8_t *is_trucated, const common::UserInfo &user_info);
      static int do_del_bucket(const uint64_t server_id, const char *bucket_name, const common::UserInfo &user_info);
      static int do_head_bucket(const uint64_t server_id, const char *bucket_name, common::BucketMetaInfo *bucket_meta_info,
                                const common::UserInfo &user_info);

      static int do_put_object(const uint64_t server_id,
                               const char *bucket_name, const char *object_name,
                               const common::ObjectInfo &object_info, const common::UserInfo &user_info);
      static int do_get_object(const uint64_t server_id,
                               const char *bucket_name, const char *object_name,
                               const int64_t offset, const int64_t length,
                               common::ObjectInfo *object_info, bool *still_have,
                               const common::UserInfo &user_info);
      static int do_del_object(const uint64_t server_id,
                               const char *bucket_name,
                               const char *object_name,
                               common::ObjectInfo *object_info, bool *still_have,
                               const common::UserInfo &user_info);
      static int do_head_object(const uint64_t server_id,
                               const char *bucket_name,
                               const char *object_name,
                               common::ObjectInfo *object_info,
                               const common::UserInfo &user_info);

      static int do_set_life_cycle(const uint64_t server_id,
                                   const int32_t file_type, const char *file_name,
                                   const int32_t invalid_time_s, const char *app_key);

      static int do_get_life_cycle(const uint64_t server_id,
                                   const int32_t file_type, const char *file_name,
                                   int32_t *invalid_time_s);

      static int do_rm_life_cycle(const uint64_t server_id,
                                  const int32_t file_type, const char *file_name);
    };
  }
}

#endif
