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

        int32_t get_mutable_limit() const
        {
          return limit_;
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

    class ReqKvMetaSetLifeCycleMessage : public common::BasePacket
    {
      public:
        ReqKvMetaSetLifeCycleMessage();
        virtual ~ReqKvMetaSetLifeCycleMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        const int32_t get_file_type() const
        {
          return file_type_;
        }

        const int32_t get_invalid_time_s() const
        {
          return invalid_time_s_;
        }

        const std::string& get_app_key() const
        {
          return app_key_;
        }

        const std::string& get_file_name() const
        {
          return file_name_;
        }

        void set_file_type(const int32_t file_type)
        {
          file_type_ = file_type;
        }

        void set_invalid_time_s(const int32_t invalid_time_s)
        {
          invalid_time_s_ = invalid_time_s;
        }

        void set_app_key(const std::string& app_key)
        {
          app_key_ = app_key;
        }

        void set_file_name(const std::string& file_name)
        {
          file_name_ = file_name;
        }

      private:
        int32_t file_type_;
        std::string file_name_;
        int32_t invalid_time_s_;
        std::string app_key_;
    };

    class ReqKvMetaGetLifeCycleMessage : public common::BasePacket
    {
      public:
        ReqKvMetaGetLifeCycleMessage();
        virtual ~ReqKvMetaGetLifeCycleMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        const int32_t get_file_type() const
        {
          return file_type_;
        }

        const std::string& get_file_name() const
        {
          return file_name_;
        }

        void set_file_type(const int32_t file_type)
        {
          file_type_ = file_type;
        }

        void set_file_name(const std::string& file_name)
        {
          file_name_ = file_name;
        }

      private:
        int32_t file_type_;
        std::string file_name_;
    };

    class RspKvMetaGetLifeCycleMessage : public common::BasePacket
    {
      public:
        RspKvMetaGetLifeCycleMessage();
        virtual ~RspKvMetaGetLifeCycleMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        const int32_t get_invalid_time_s() const
        {
          return invalid_time_s_;
        }

        void set_invalid_time_s(const int32_t invalid_time_s)
        {
          invalid_time_s_ = invalid_time_s;
        }

      private:
        int32_t invalid_time_s_;
    };

    class ReqKvMetaRmLifeCycleMessage : public common::BasePacket
    {
      public:
        ReqKvMetaRmLifeCycleMessage();
        virtual ~ReqKvMetaRmLifeCycleMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        const int32_t get_file_type() const
        {
          return file_type_;
        }

        const std::string& get_file_name() const
        {
          return file_name_;
        }

        void set_file_type(const int32_t file_type)
        {
          file_type_ = file_type;
        }

        void set_file_name(const std::string& file_name)
        {
          file_name_ = file_name;
        }

      private:
        int32_t file_type_;
        std::string file_name_;
    };

  }
}

#endif
