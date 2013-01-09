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

      private:
        std::string bucket_name_;
        std::string file_name_;
        common::ObjectInfo object_info_;
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

    class RspKvMetaGetObjectMessage : public common::BasePacket
    {
      public:
        RspKvMetaGetObjectMessage();
        virtual ~RspKvMetaGetObjectMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        const common::ObjectInfo& get_object_info() const
        {
          return object_info_;
        }

        void set_object_info(const common::ObjectInfo& object_info)
        {
          object_info_ = object_info;
        }

      private:
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

      private:
        std::string bucket_name_;
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

        void set_limit(const int32_t limit)
        {
          limit_ = limit;
        }

        const int32_t get_limit() const
        {
          return limit_;
        }


      private:
        std::string bucket_name_;
        std::string prefix_;
        std::string start_key_;
        //std::string delimiter_;
        int32_t limit_;
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

        const std::string& get_bucket_name() const
        {
          return bucket_name_;
        }

        common::VSTRING& get_v_object_name()
        {
          return v_object_name_;
        }

      private:
        std::string bucket_name_;
        common::VSTRING v_object_name_;
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

      private:
        std::string bucket_name_;
    };

  }
}

#endif
