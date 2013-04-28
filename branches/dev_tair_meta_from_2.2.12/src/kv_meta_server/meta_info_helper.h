/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: meta_server_service.h 49 2011-08-08 09:58:57Z nayan@taobao.com $
 *
 * Authors:
 *   daoan <daoan@taobao.com>
 *      - initial release
 *
 */

#ifndef TFS_KVMETASERVER_META_INFO_HELPER_H_
#define TFS_KVMETASERVER_META_INFO_HELPER_H_

#include "common/parameter.h"
#include "common/base_service.h"
#include "common/status_message.h"
#include "common/kv_meta_define.h"
#include "message/message_factory.h"
#include "kvengine_helper.h"

namespace tfs
{
  namespace kvmetaserver
  {
    class MetaInfoHelper
    {
      public:
        MetaInfoHelper();
        virtual ~MetaInfoHelper();
        int init();
        int put_meta(const std::string& bucket_name, const std::string& file_name,
            /*const int64_t offset,*/ const common::TfsFileInfo& tfs_file_info
            /* const taglist , versioning*/
            );
        int get_meta(const std::string& bucket_name, const std::string& file_name,
            /*const int64_t offset,*/ /*const get_tag_list*/ common::TfsFileInfo* tfs_file_info
            /*taglist* */);
        //int scan_metas();
        //int delete_metas();


      /*----------------------------object part-----------------------------*/

        int head_object(const std::string &bucket_name,
                        const std::string &file_name,
                        common::ObjectInfo *object_info_zero);

        int put_object(const std::string& bucket_name,
                       const std::string& file_name,
                       common::ObjectInfo &object_info,
                       const common::UserInfo &user_info);

        int get_object(const std::string& bucket_name,
                       const std::string& file_name,
                       const int64_t offset, const int64_t length,
                       common::ObjectInfo *object_info, bool *still_have);

        int del_object(const std::string& bucket_name,
                       const std::string& file_name,
                       common::ObjectInfo *object_info, bool* still_have);

        /*----------------------------bucket part-----------------------------*/

        int head_bucket(const std::string& bucket_name, common::BucketMetaInfo *bucket_meta_info, int64_t *version);
        static int get_common_prefix(const char *key, const std::string &prefix, const char delimiter,
            bool *prefix_flag, bool *common_flag, int *common_end_pos);

        int put_bucket(const std::string& bucket_name, common::BucketMetaInfo& bucket_meta_info,
                       const common::UserInfo &user_info);
        int get_bucket(const std::string& bucket_name, const std::string& prefix,
            const std::string& start_key, const char delimiter, int32_t *limit,
            std::vector<common::ObjectMetaInfo>* v_object_meta_info, common::VSTRING* v_object_name,
            std::set<std::string>* s_common_prefix, int8_t* is_truncated);
        int del_bucket(const std::string& bucket_name);
        int list_multipart_object(const std::string &bucket_name, const std::string &prefix, const std::string &start_key, const std::string &start_id, const char delimiter, const int32_t &limit, common::ListMultipartObjectResult *list_multipart_object_result);

        int put_bucket_tag(const std::string &bucket_name, const common::MAP_STRING &bucket_tag_map);
        int get_bucket_tag(const std::string &bucket_name, common::MAP_STRING *bucket_tag_map);
        int del_bucket_tag(const std::string &bucket_name);

        int put_bucket_acl(const std::string &bucket_name, const common::MAP_STRING_INT &bucket_acl_map);
        int get_bucket_acl(const std::string &bucket_name, common::MAP_STRING_INT *bucket_acl_map);

        /*----------------------------multi part-----------------------------*/
        int joint_multi_objectname(const std::string &file_name,
                const std::string &upload_id, const int32_t part_num, std::string &new_objectname);
        int is_equal_v_part_num(const std::vector<int32_t>& v_part_num_kv, const std::vector<int32_t>& v_part_num);
        int get_v_partnum_kv(const std::string& bucket_name, const std::string& file_name,
            const std::string &upload_id, std::vector<int32_t>* const p_v_part_num);
        int init_multipart(const std::string& bucket_name,
                         const std::string& file_name, std::string* upload_id);
        int upload_multipart(const std::string &bucket_name,
             const std::string &file_name, const std::string &upload_id, const int32_t part_num,
             common::ObjectInfo &object_info, const common::UserInfo &user_info);
        int complete_multipart(const std::string &bucket_name,
            const std::string &file_name, const std::string &upload_id, const std::vector<int32_t>& v_part_num);
        int list_multipart(const std::string& bucket_name, const std::string& file_name,
                           const std::string& upload_id, std::vector<int32_t>* const p_v_part_num);
        int abort_multipart(const std::string &bucket_name, const std::string &file_name,
            const std::string &upload_id, common::ObjectInfo *object_info, bool* still_have);
        int del_part_num_key(const std::string &bucket_name,
                const std::string &file_name, const std::string &upload_id);
      public:
        int put_bucket_ex(const std::string &bucket_name, const common::BucketMetaInfo &bucket_meta_info,
            const int64_t lock_version = 0);
        int put_object_ex(const std::string &bucket_name, const std::string &file_name,
            const int64_t offset, const common::ObjectInfo &object_info, const int64_t lock_version = 0);

        int get_object_part(const std::string &bucket_name,
                           const std::string &file_name,
                           const int64_t offset,
                           common::ObjectInfo *object_info,
                           int64_t *version);
        int deserialize_key(const char *key, const int32_t key_size, std::string *bucket_name, std::string *object_name,
            int64_t *offset, int64_t *version);
        int serialize_key(const std::string &bucket_name,
                        const std::string &file_name, const int64_t offset,
                        KvKey *key, char *key_buff, const int32_t buff_size, int32_t key_type);
        int serialize_key_ex(const std::string &file_name, const int64_t offset,
                        KvKey *key, char *key_buff, const int32_t buff_size, int32_t key_type);

        int serialize_upload_key(const std::string &file_name, const std::string &upload_id,
            const int64_t offset, const int64_t part_num,
            KvKey *key, char *key_buff, const int32_t buff_size, int32_t key_type);

        int deserialize_upload_key(const char *key, const int32_t key_size, std::string *bucket_name,
            std::string *upload_id, std::string *object_name, int64_t *version,
            int64_t *part_num, int64_t *offset);

      protected:
        int group_objects(const std::string &object_name, const std::string &v, const std::string &prefix,
            const char delimiter, std::vector<common::ObjectMetaInfo> *v_object_meta_info,
            std::vector<std::string> *v_object_name, std::set<std::string> *s_common_prefix);

        int group_upload_objects(const std::string &object_name, const std::string &upload_id,
            const std::string &v, const std::string &prefix, const char delimiter,
            std::vector<common::ObjectUploadInfo> *v_object_upload_info, std::set<std::string> *s_common_prefix);

        int put_object_part(const std::string &bucket_name, const std::string &file_name,
            const common::ObjectInfo &object_info);

        int put_object_zero(const std::string &bucket_name, const std::string &file_name,
            common::ObjectInfo *object_info_zero, int64_t *offset,
            const int64_t length, int64_t version, bool is_append);

        int get_range(const KvKey &pkey, const std::string &start_key,
            int32_t offset, const int32_t limit, std::vector<KvValue*> *kv_value_keys,
            std::vector<KvValue*> *kv_value_values, int32_t *result_size);

        int list_objects(const KvKey &pkey, const std::string &prefix,
            const std::string &start_key, const char delimiter, int32_t *limit,
            std::vector<common::ObjectMetaInfo> *v_object_meta_info, common::VSTRING *v_object_name,
            std::set<std::string> *s_common_prefix, int8_t *is_truncated);

        int list_upload_objects(const KvKey &pkey, const std::string &prefix,
            const std::string &start_key, const std::string &start_id, const char delimiter,
            const int32_t &limit, common::ListMultipartObjectResult *list_multipart_object_result);

      protected:
        KvEngineHelper* kv_engine_helper_;
      private:
        DISALLOW_COPY_AND_ASSIGN(MetaInfoHelper);
    };
  }
}

#endif
