/*
* (C) 2007-2011 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
*
* Version: $Id
*
* Authors:
*   xueya.yy <xueya.yy@taobao.com>
*      - initial release
*
*/

#ifndef TFS_KV_META_MESSAGE_H
#define TFS_KV_META_MESSAGE_H

#include "common/base_packet.h"
#include "common/kv_meta_define.h"

namespace tfs
{
  namespace message
  {
    class ReqKvMetaPutObjectMessage : public common::BasePacket
    {
      public:
        ReqKvMetaPutObjectMessage();
        virtual ~ReqKvMetaPutObjectMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        const std::string& get_bucket_name() const
        {
          return bucket_name_;
        }

        const std::string& get_file_name() const
        {
          return file_name_;
        }

        const common::ObjectInfo& get_object_info() const
        {
          return object_info_;
        }

        const common::UserInfo& get_user_info() const
        {
          return user_info_;
        }

        void set_bucket_name(const std::string& bucket_name)
        {
          bucket_name_ = bucket_name;
        }

        void set_file_name(const std::string& file_name)
        {
          file_name_ = file_name;
        }

        void set_object_info(const common::ObjectInfo& object_info)
        {
          object_info_ = object_info;
        }

        void set_user_info(const common::UserInfo& user_info)
        {
          user_info_ = user_info;
        }

      private:
        std::string bucket_name_;
        std::string file_name_;
        common::ObjectInfo object_info_;
        common::UserInfo user_info_;
    };

    class ReqKvMetaGetObjectMessage : public common::BasePacket
    {
      public:
        ReqKvMetaGetObjectMessage();
        virtual ~ReqKvMetaGetObjectMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        const std::string& get_bucket_name() const
        {
          return bucket_name_;
        }

        const std::string& get_file_name() const
        {
          return file_name_;
        }

        int64_t get_offset() const
        {
          return offset_;
        }

        int64_t get_length() const
        {
          return length_;
        }

        const common::UserInfo& get_user_info() const
        {
          return user_info_;
        }

        void set_bucket_name(const std::string& bucket_name)
        {
          bucket_name_ = bucket_name;
        }

        void set_file_name(const std::string& file_name)
        {
          file_name_ = file_name;
        }

        void set_offset(const int64_t offset)
        {
          offset_ = offset;
        }

        void set_length(const int64_t length)
        {
          length_ = length;
        }

        void set_user_info(const common::UserInfo& user_info)
        {
          user_info_ = user_info;
        }
      private:
        std::string bucket_name_;
        std::string file_name_;
        int64_t offset_;
        int64_t length_;
        common::UserInfo user_info_;
    };

    class RspKvMetaGetObjectMessage : public common::BasePacket
    {
      public:
        RspKvMetaGetObjectMessage();
        virtual ~RspKvMetaGetObjectMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        const bool get_still_have() const
        {
          return still_have_;
        }

        void set_still_have(const bool still_have)
        {
          still_have_ = still_have;
        }

        const common::ObjectInfo& get_object_info() const
        {
          return object_info_;
        }

        void set_object_info(const common::ObjectInfo& object_info)
        {
          object_info_ = object_info;
        }

      private:
        bool still_have_;
        common::ObjectInfo object_info_;
    };

    class ReqKvMetaDelObjectMessage : public common::BasePacket
    {
      public:
        ReqKvMetaDelObjectMessage();
        virtual ~ReqKvMetaDelObjectMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        const std::string& get_bucket_name() const
        {
          return bucket_name_;
        }

        const std::string& get_file_name() const
        {
          return file_name_;
        }

        const common::UserInfo& get_user_info() const
        {
          return user_info_;
        }

        void set_bucket_name(const std::string& bucket_name)
        {
          bucket_name_ = bucket_name;
        }

        void set_file_name(const std::string& file_name)
        {
          file_name_ = file_name;
        }

        void set_user_info(const common::UserInfo& user_info)
        {
          user_info_ = user_info;
        }


      private:
        std::string bucket_name_;
        std::string file_name_;
        common::UserInfo user_info_;
    };

    class RspKvMetaDelObjectMessage : public common::BasePacket
    {
      public:
        RspKvMetaDelObjectMessage();
        virtual ~RspKvMetaDelObjectMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        const bool get_still_have() const
        {
          return still_have_;
        }

        void set_still_have(const bool still_have)
        {
          still_have_ = still_have;
        }

        const common::ObjectInfo& get_object_info() const
        {
          return object_info_;
        }

        void set_object_info(const common::ObjectInfo& object_info)
        {
          object_info_ = object_info;
        }

      private:
        bool still_have_;
        common::ObjectInfo object_info_;
    };

    class ReqKvMetaHeadObjectMessage : public common::BasePacket
    {
      public:
        ReqKvMetaHeadObjectMessage();
        virtual ~ReqKvMetaHeadObjectMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        const std::string& get_bucket_name() const
        {
          return bucket_name_;
        }

        const std::string& get_file_name() const
        {
          return file_name_;
        }

        const common::UserInfo& get_user_info() const
        {
          return user_info_;
        }

        void set_bucket_name(const std::string& bucket_name)
        {
          bucket_name_ = bucket_name;
        }

        void set_file_name(const std::string& file_name)
        {
          file_name_ = file_name;
        }

        void set_user_info(const common::UserInfo& user_info)
        {
          user_info_ = user_info;
        }
      private:
        std::string bucket_name_;
        std::string file_name_;
        common::UserInfo user_info_;
    };

    class RspKvMetaHeadObjectMessage : public common::BasePacket
    {
      public:
        RspKvMetaHeadObjectMessage();
        virtual ~RspKvMetaHeadObjectMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        const std::string& get_bucket_name() const
        {
          return bucket_name_;
        }

        const std::string& get_file_name() const
        {
          return file_name_;
        }

        void set_bucket_name(const std::string& bucket_name)
        {
          bucket_name_ = bucket_name;
        }

        void set_file_name(const std::string& file_name)
        {
          file_name_ = file_name;
        }

        const common::ObjectInfo* get_object_info() const
        {
          return &object_info_;
        }

        common::ObjectInfo* get_mutable_object_info()
        {
          return &object_info_;
        }

      private:
        std::string bucket_name_;
        std::string file_name_;
        common::ObjectInfo object_info_;
    };

    class ReqKvMetaPutBucketMessage : public common::BasePacket
    {
      public:
        ReqKvMetaPutBucketMessage();
        virtual ~ReqKvMetaPutBucketMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_bucket_name(const std::string& bucket_name)
        {
          bucket_name_ = bucket_name;
        }

        const std::string& get_bucket_name() const
        {
          return bucket_name_;
        }

        void set_bucket_meta_info(const common::BucketMetaInfo& bucket_meta_info)
        {
          bucket_meta_info_ = bucket_meta_info;
        }

        const common::BucketMetaInfo* get_bucket_meta_info() const
        {
          return &bucket_meta_info_;
        }

        common::BucketMetaInfo* get_mutable_bucket_meta_info()
        {
          return &bucket_meta_info_;
        }

        const common::UserInfo& get_user_info() const
        {
          return user_info_;
        }

        void set_user_info(const common::UserInfo& user_info)
        {
          user_info_ = user_info;
        }

      private:
        std::string bucket_name_;
        common::BucketMetaInfo bucket_meta_info_;
        common::UserInfo user_info_;
    };

    class ReqKvMetaGetBucketMessage : public common::BasePacket
    {
      public:
        ReqKvMetaGetBucketMessage();
        virtual ~ReqKvMetaGetBucketMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_bucket_name(const std::string& bucket_name)
        {
          bucket_name_ = bucket_name;
        }

        const std::string& get_bucket_name() const
        {
          return bucket_name_;
        }

        void set_prefix(const std::string& prefix)
        {
          prefix_ = prefix;
        }

        const std::string& get_prefix() const
        {
          return prefix_;
        }

        void set_start_key(const std::string& start_key)
        {
          start_key_ = start_key;
        }

        const std::string& get_start_key() const
        {
          return start_key_;
        }

        void set_delimiter(char delimiter)
        {
          delimiter_ = delimiter;
        }

        char get_delimiter() const
        {
          return delimiter_;
        }

        void set_limit(const int32_t limit)
        {
          limit_ = limit;
        }

        const int32_t get_limit() const
        {
          return limit_;
        }

        const common::UserInfo& get_user_info() const
        {
          return user_info_;
        }

        void set_user_info(const common::UserInfo& user_info)
        {
          user_info_ = user_info;
        }
      private:
        std::string bucket_name_;
        std::string prefix_;
        std::string start_key_;
        int32_t limit_;
        char delimiter_; //just a character to group keys
        common::UserInfo user_info_;
    };

    class RspKvMetaGetBucketMessage : public common::BasePacket
    {
      public:
        RspKvMetaGetBucketMessage();
        virtual ~RspKvMetaGetBucketMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_bucket_name(const std::string& bucket_name)
        {
          bucket_name_ = bucket_name;
        }

        void set_prefix(const std::string& prefix)
        {
          prefix_ = prefix;
        }

        void set_start_key(const std::string& start_key)
        {
          start_key_ = start_key;
        }

        void set_delimiter(const char delimiter)
        {
          delimiter_ = delimiter;
        }

        void set_limit(const int32_t limit)
        {
          limit_ = limit;
        }

        const std::set<std::string>* get_s_common_prefix() const
        {
          return &s_common_prefix_;
        }

        const common::VSTRING* get_v_object_name() const
        {
          return &v_object_name_;
        }

        const std::vector<common::ObjectMetaInfo>* get_v_object_meta_info() const
        {
          return &v_object_meta_info_;
        }

        const int8_t* get_truncated() const
        {
          return &is_truncated;
        }

        std::set<std::string>* get_mutable_s_common_prefix()
        {
          return &s_common_prefix_;
        }

        common::VSTRING* get_mutable_v_object_name()
        {
          return &v_object_name_;
        }

        std::vector<common::ObjectMetaInfo>* get_mutable_v_object_meta_info()
        {
          return &v_object_meta_info_;
        }

        int8_t* get_mutable_truncated()
        {
          return &is_truncated;
        }


      private:
        std::string bucket_name_;
        std::string prefix_;
        std::string start_key_;
        std::set<std::string> s_common_prefix_; //the same common_prefix count as single
        common::VSTRING v_object_name_;
        std::vector<common::ObjectMetaInfo> v_object_meta_info_;
        int32_t limit_;
        int8_t is_truncated;
        char delimiter_;
    };

    class ReqKvMetaDelBucketMessage : public common::BasePacket
    {
      public:
        ReqKvMetaDelBucketMessage();
        virtual ~ReqKvMetaDelBucketMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_bucket_name(const std::string& bucket_name)
        {
          bucket_name_ = bucket_name;
        }

        const std::string& get_bucket_name() const
        {
          return bucket_name_;
        }

        const common::UserInfo& get_user_info() const
        {
          return user_info_;
        }

        void set_user_info(const common::UserInfo& user_info)
        {
          user_info_ = user_info;
        }

      private:
        std::string bucket_name_;
        common::UserInfo user_info_;
    };

    class ReqKvMetaHeadBucketMessage : public common::BasePacket
    {
      public:
        ReqKvMetaHeadBucketMessage();
        virtual ~ReqKvMetaHeadBucketMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_bucket_name(const std::string& bucket_name)
        {
          bucket_name_ = bucket_name;
        }

        const std::string& get_bucket_name() const
        {
          return bucket_name_;
        }

        const common::UserInfo& get_user_info() const
        {
          return user_info_;
        }

        void set_user_info(const common::UserInfo& user_info)
        {
          user_info_ = user_info;
        }

      private:
        std::string bucket_name_;
        common::UserInfo user_info_;
    };

    class RspKvMetaHeadBucketMessage : public common::BasePacket
    {
      public:
        RspKvMetaHeadBucketMessage();
        virtual ~RspKvMetaHeadBucketMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_bucket_name(const std::string& bucket_name)
        {
          bucket_name_ = bucket_name;
        }

        const std::string& get_bucket_name() const
        {
          return bucket_name_;
        }

        common::BucketMetaInfo* get_mutable_bucket_meta_info()
        {
          return &bucket_meta_info_;
        }

        const common::BucketMetaInfo* get_bucket_meta_info() const
        {
          return &bucket_meta_info_;
        }

      private:
        std::string bucket_name_;
        common::BucketMetaInfo bucket_meta_info_;
    };

    class ReqKvMetaListMultipartObjectMessage : public common::BasePacket
    {
      public:
        ReqKvMetaListMultipartObjectMessage();
        virtual ~ReqKvMetaListMultipartObjectMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_bucket_name(const std::string& bucket_name)
        {
          bucket_name_ = bucket_name;
        }

        const std::string& get_bucket_name() const
        {
          return bucket_name_;
        }

        void set_prefix(const std::string& prefix)
        {
          prefix_ = prefix;
        }

        const std::string& get_prefix() const
        {
          return prefix_;
        }

        void set_start_key(const std::string& start_key)
        {
          start_key_ = start_key;
        }

        const std::string& get_start_key() const
        {
          return start_key_;
        }

        void set_start_id(const std::string &start_id)
        {
          start_id_ = start_id;
        }

        const std::string& get_start_id() const
        {
          return start_id_;
        }

        void set_delimiter(char delimiter)
        {
          delimiter_ = delimiter;
        }

        char get_delimiter() const
        {
          return delimiter_;
        }

        void set_limit(const int32_t limit)
        {
          limit_ = limit;
        }

        const int32_t get_limit() const
        {
          return limit_;
        }

        const common::UserInfo& get_user_info() const
        {
          return user_info_;
        }

        void set_user_info(const common::UserInfo& user_info)
        {
          user_info_ = user_info;
        }
      private:
        std::string bucket_name_;
        std::string prefix_;
        std::string start_key_;
        std::string start_id_;
        int32_t limit_;
        char delimiter_; //just a character to group keys
        common::UserInfo user_info_;
    };

    class RspKvMetaListMultipartObjectMessage : public common::BasePacket
    {
      public:
        RspKvMetaListMultipartObjectMessage();
        virtual ~RspKvMetaListMultipartObjectMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_bucket_name(const std::string& bucket_name)
        {
          bucket_name_ = bucket_name;
        }

        void set_prefix(const std::string& prefix)
        {
          prefix_ = prefix;
        }

        void set_start_key(const std::string& start_key)
        {
          start_key_ = start_key;
        }

        void set_start_id(const std::string &start_id)
        {
          start_id_ = start_id;
        }

        void set_delimiter(const char delimiter)
        {
          delimiter_ = delimiter;
        }

        void set_limit(const int32_t limit)
        {
          limit_ = limit;
        }

        void set_s_common_prefix(const std::set<std::string> &s_common_prefix)
        {
          s_common_prefix_ = s_common_prefix;
        }

        void set_v_object_upload_info(const std::vector<common::ObjectUploadInfo> &v_object_upload_info)
        {
          v_object_upload_info_ = v_object_upload_info;
        }

        void set_truncated(const bool is_truncated)
        {
          is_truncated_ = is_truncated;
        }

        void set_next_start_key(const std::string next_start_key)
        {
          next_start_key_ = next_start_key;
        }

        void set_next_start_id(const std::string next_start_id)
        {
          next_start_id_ = next_start_id;
        }

        const std::set<std::string>* get_s_common_prefix() const
        {
          return &s_common_prefix_;
        }

        const std::vector<common::ObjectUploadInfo>* get_v_object_upload_info() const
        {
          return &v_object_upload_info_;
        }

        const bool* get_truncated() const
        {
          return &is_truncated_;
        }

        const int32_t* get_limit() const
        {
          return &limit_;
        }

        std::set<std::string>* get_mutable_s_common_prefix()
        {
          return &s_common_prefix_;
        }

        std::vector<common::ObjectUploadInfo>* get_mutable_v_object_upload_info()
        {
          return &v_object_upload_info_;
        }


      private:
        std::string bucket_name_;
        std::string prefix_;
        std::string start_key_;
        std::string start_id_;
        std::string next_start_key_;
        std::string next_start_id_;

        std::set<std::string> s_common_prefix_; //the same common_prefix count as single
        std::vector<common::ObjectUploadInfo> v_object_upload_info_;
        int32_t limit_;
        bool is_truncated_;
        char delimiter_;
    };

    //about msg of tag
    class ReqKvMetaPutBucketTagMessage : public common::BasePacket
    {
      public:
        ReqKvMetaPutBucketTagMessage();
        virtual ~ReqKvMetaPutBucketTagMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_bucket_name(const std::string& bucket_name)
        {
          bucket_name_ = bucket_name;
        }

        const std::string& get_bucket_name() const
        {
          return bucket_name_;
        }

        void set_bucket_tag_map(const common::MAP_STRING &bucket_tag_map)
        {
          bucket_tag_map_ = bucket_tag_map;
        }

        const common::MAP_STRING* get_bucket_tag_map() const
        {
          return &bucket_tag_map_;
        }

        common::MAP_STRING* get_mutable_bucket_tag_map()
        {
          return &bucket_tag_map_;
        }

      private:
        std::string bucket_name_;
        common::MAP_STRING bucket_tag_map_;
    };

    class ReqKvMetaGetBucketTagMessage : public common::BasePacket
    {
      public:
        ReqKvMetaGetBucketTagMessage();
        virtual ~ReqKvMetaGetBucketTagMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_bucket_name(const std::string& bucket_name)
        {
          bucket_name_ = bucket_name;
        }

        const std::string& get_bucket_name() const
        {
          return bucket_name_;
        }

      private:
        std::string bucket_name_;
    };

    class RspKvMetaGetBucketTagMessage : public common::BasePacket
    {
      public:
        RspKvMetaGetBucketTagMessage();
        virtual ~RspKvMetaGetBucketTagMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_bucket_name(const std::string& bucket_name)
        {
          bucket_name_ = bucket_name;
        }

        const std::string& get_bucket_name() const
        {
          return bucket_name_;
        }

        void set_bucket_tag_map(const common::MAP_STRING& bucket_tag_map)
        {
          bucket_tag_map_ = bucket_tag_map;
        }

        const common::MAP_STRING* get_bucket_tag_map() const
        {
          return &bucket_tag_map_;
        }

        common::MAP_STRING* get_mutable_bucket_tag_map()
        {
          return &bucket_tag_map_;
        }

      private:
        std::string bucket_name_;
        common::MAP_STRING bucket_tag_map_;
    };

    class ReqKvMetaDelBucketTagMessage : public common::BasePacket
    {
      public:
        ReqKvMetaDelBucketTagMessage();
        virtual ~ReqKvMetaDelBucketTagMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_bucket_name(const std::string& bucket_name)
        {
          bucket_name_ = bucket_name;
        }

        const std::string& get_bucket_name() const
        {
          return bucket_name_;
        }

      private:
        std::string bucket_name_;
    };

    //about bucket acl
    class ReqKvMetaPutBucketAclMessage : public common::BasePacket
    {
      public:
        ReqKvMetaPutBucketAclMessage();
        virtual ~ReqKvMetaPutBucketAclMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_bucket_name(const std::string& bucket_name)
        {
          bucket_name_ = bucket_name;
        }

        const std::string& get_bucket_name() const
        {
          return bucket_name_;
        }

        void set_bucket_acl_map(const common::MAP_STRING_INT &bucket_acl_map)
        {
          bucket_acl_map_ = bucket_acl_map;
        }

        const common::MAP_STRING_INT* get_bucket_acl_map() const
        {
          return &bucket_acl_map_;
        }

        common::MAP_STRING_INT* get_mutable_bucket_acl_map()
        {
          return &bucket_acl_map_;
        }

      private:
        std::string bucket_name_;
        common::MAP_STRING_INT bucket_acl_map_;
    };

    class ReqKvMetaGetBucketAclMessage : public common::BasePacket
    {
      public:
        ReqKvMetaGetBucketAclMessage();
        virtual ~ReqKvMetaGetBucketAclMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_bucket_name(const std::string& bucket_name)
        {
          bucket_name_ = bucket_name;
        }

        const std::string& get_bucket_name() const
        {
          return bucket_name_;
        }

      private:
        std::string bucket_name_;
    };

    class RspKvMetaGetBucketAclMessage : public common::BasePacket
    {
      public:
        RspKvMetaGetBucketAclMessage();
        virtual ~RspKvMetaGetBucketAclMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_bucket_name(const std::string& bucket_name)
        {
          bucket_name_ = bucket_name;
        }

        const std::string& get_bucket_name() const
        {
          return bucket_name_;
        }

        void set_bucket_acl_map(const common::MAP_STRING_INT& bucket_acl_map)
        {
          bucket_acl_map_ = bucket_acl_map;
        }

        const common::MAP_STRING_INT* get_bucket_acl_map() const
        {
          return &bucket_acl_map_;
        }

        common::MAP_STRING_INT* get_mutable_bucket_acl_map()
        {
          return &bucket_acl_map_;
        }

      private:
        std::string bucket_name_;
        common::MAP_STRING_INT bucket_acl_map_;
    };

    // init multipart
    class ReqKvMetaInitMulitpartMessage : public common::BasePacket
    {
      public:
        ReqKvMetaInitMulitpartMessage();
        virtual ~ReqKvMetaInitMulitpartMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        const std::string& get_bucket_name() const
        {
          return bucket_name_;
        }

        const std::string& get_file_name() const
        {
          return file_name_;
        }

        void set_bucket_name(const std::string& bucket_name)
        {
          bucket_name_ = bucket_name;
        }

        void set_file_name(const std::string& file_name)
        {
          file_name_ = file_name;
        }

      private:
        std::string bucket_name_;
        std::string file_name_;
    };

    class RspKvMetaInitMulitpartMessage : public common::BasePacket
    {
      public:
        RspKvMetaInitMulitpartMessage();
        virtual ~RspKvMetaInitMulitpartMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        const std::string& get_upload_id() const
        {
          return upload_id_;
        }

        void set_upload_id(const std::string& upload_id)
        {
          upload_id_ = upload_id;
        }

      private:
        std::string upload_id_;
    };

    //upload multipart
    class ReqKvMetaUploadMulitpartMessage : public common::BasePacket
    {
      public:
        ReqKvMetaUploadMulitpartMessage();
        virtual ~ReqKvMetaUploadMulitpartMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        const std::string& get_bucket_name() const
        {
          return bucket_name_;
        }

        const std::string& get_file_name() const
        {
          return file_name_;
        }

        const std::string& get_upload_id() const
        {
          return upload_id_;
        }

        int32_t get_part_num() const
        {
          return part_num_;
        }

        const common::ObjectInfo& get_object_info() const
        {
          return object_info_;
        }

        const common::UserInfo& get_user_info() const
        {
          return user_info_;
        }

        void set_bucket_name(const std::string& bucket_name)
        {
          bucket_name_ = bucket_name;
        }

        void set_file_name(const std::string& file_name)
        {
          file_name_ = file_name;
        }

        void set_upload_id(const std::string& upload_id)
        {
          upload_id_ = upload_id;
        }

        void set_part_num(const int32_t part_num)
        {
          part_num_ = part_num;
        }

        void set_object_info(const common::ObjectInfo& object_info)
        {
          object_info_ = object_info;
        }

        void set_user_info(const common::UserInfo& user_info)
        {
          user_info_ = user_info;
        }

      private:
        std::string bucket_name_;
        std::string file_name_;
        std::string upload_id_;
        int32_t part_num_;
        common::ObjectInfo object_info_;
        common::UserInfo user_info_;
    };

    //complete multipart
    class ReqKvMetaCompleteMulitpartMessage : public common::BasePacket
    {
      public:
        ReqKvMetaCompleteMulitpartMessage();
        virtual ~ReqKvMetaCompleteMulitpartMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        const std::string& get_bucket_name() const
        {
          return bucket_name_;
        }

        const std::string& get_file_name() const
        {
          return file_name_;
        }

        const std::string& get_upload_id() const
        {
          return upload_id_;
        }

        std::vector<int32_t>& get_v_part_num()
        {
          return v_part_num_;
        }

        void set_bucket_name(const std::string& bucket_name)
        {
          bucket_name_ = bucket_name;
        }

        void set_file_name(const std::string& file_name)
        {
          file_name_ = file_name;
        }

        void set_upload_id(const std::string& upload_id)
        {
          upload_id_ = upload_id;
        }

        void set_v_part_num(const std::vector<int32_t>& v_part_num)
        {
          v_part_num_ = v_part_num;
        }
      protected:
        std::string bucket_name_;
        std::string file_name_;
        std::string upload_id_;
        std::vector<int32_t> v_part_num_;
    };

    //list multipart
    class ReqKvMetaListMulitpartMessage : public common::BasePacket
    {
      public:
        ReqKvMetaListMulitpartMessage();
        virtual ~ReqKvMetaListMulitpartMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        const std::string& get_bucket_name() const
        {
          return bucket_name_;
        }

        const std::string& get_file_name() const
        {
          return file_name_;
        }

        const std::string& get_upload_id() const
        {
          return upload_id_;
        }


        void set_bucket_name(const std::string& bucket_name)
        {
          bucket_name_ = bucket_name;
        }

        void set_file_name(const std::string& file_name)
        {
          file_name_ = file_name;
        }

        void set_upload_id(const std::string& upload_id)
        {
          upload_id_ = upload_id;
        }

      protected:
        std::string bucket_name_;
        std::string file_name_;
        std::string upload_id_;
    };

    class RspKvMetaListMulitpartMessage : public common::BasePacket
    {
      public:
        RspKvMetaListMulitpartMessage();
        virtual ~RspKvMetaListMulitpartMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        std::vector<int32_t>& get_v_part_num()
        {
          return v_part_num_;
        }

        void set_v_part_num(const std::vector<int32_t>& v_part_num)
        {
          v_part_num_ = v_part_num;
        }
      protected:
        std::vector<int32_t> v_part_num_;
    };

    //abort multipart
    class ReqKvMetaAbortMulitpartMessage : public common::BasePacket
    {
      public:
        ReqKvMetaAbortMulitpartMessage();
        virtual ~ReqKvMetaAbortMulitpartMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        const std::string& get_bucket_name() const
        {
          return bucket_name_;
        }

        const std::string& get_file_name() const
        {
          return file_name_;
        }

        const std::string& get_upload_id() const
        {
          return upload_id_;
        }


        void set_bucket_name(const std::string& bucket_name)
        {
          bucket_name_ = bucket_name;
        }

        void set_file_name(const std::string& file_name)
        {
          file_name_ = file_name;
        }

        void set_upload_id(const std::string& upload_id)
        {
          upload_id_ = upload_id;
        }

      protected:
        std::string bucket_name_;
        std::string file_name_;
        std::string upload_id_;
    };

    class RspKvMetaAbortMulitpartMessage : public common::BasePacket
    {
      public:
        RspKvMetaAbortMulitpartMessage();
        virtual ~RspKvMetaAbortMulitpartMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        const bool get_still_have() const
        {
          return still_have_;
        }

        void set_still_have(const bool still_have)
        {
          still_have_ = still_have;
        }

        const common::ObjectInfo& get_object_info() const
        {
          return object_info_;
        }

        void set_object_info(const common::ObjectInfo& object_info)
        {
          object_info_ = object_info;
        }

      private:
        bool still_have_;
        common::ObjectInfo object_info_;
    };
  }
}

#endif
