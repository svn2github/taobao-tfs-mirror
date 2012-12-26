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
#include "common/meta_kv_define.h"

namespace tfs
{
  namespace message
  {
    class KvReqPutMetaMessage : public common::BasePacket
    {
      public:
        KvReqPutMetaMessage();
        virtual ~KvReqPutMetaMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        std::string& get_bucket_name()
        {
          return bucket_name_;
        }

        std::string& get_file_name()
        {
          return file_name_;
        }

        common::TfsFileInfo& get_file_info()
        {
          return tfs_file_info_;
        }

        void set_bucket_name(const std::string& bucket_name)
        {
          bucket_name_ = bucket_name;
        }

        void set_file_name(const std::string& file_name)
        {
          file_name_ = file_name;
        }

        void set_file_info(const common::TfsFileInfo& tfs_file_info)
        {
          tfs_file_info_ = tfs_file_info;
        }

      private:
        std::string bucket_name_;
        std::string file_name_;
        common::TfsFileInfo tfs_file_info_;
    };

    class KvReqGetMetaMessage : public common::BasePacket
    {
      public:
        KvReqGetMetaMessage();
        virtual ~KvReqGetMetaMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        std::string& get_bucket_name()
        {
          return bucket_name_;
        }

        std::string& get_file_name()
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

    class KvRspGetMetaMessage : public common::BasePacket
    {
      public:
        KvRspGetMetaMessage();
        virtual ~KvRspGetMetaMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        common::TfsFileInfo* get_file_info()
        {
          return &tfs_file_info_;
        }
      private:
        mutable common::TfsFileInfo tfs_file_info_;
    };

    class KvReqPutBucketMessage : public common::BasePacket
    {
      public:
        KvReqPutBucketMessage();
        virtual ~KvReqPutBucketMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_bucket_name(std::string& bucket_name)
        {
          bucket_name_ = bucket_name;
        }

        void set_create_time(int64_t create_time)
        {
          create_time_ = create_time;
        }

        std::string& get_bucket_name()
        {
          return bucket_name_;
        }

        int64_t get_create_time() const
        {
          return create_time_;
        }

      private:
        std::string bucket_name_;
        int64_t create_time_;
    };

    /*class KvReqGetBucketMessage : public common::BasePacket
    {
      public:
        KvReqGetBucketMessage();
        virtual ~KvReqGetBucketMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_bucket_name(std::string& bucket_name)
        {
          bucket_name_ = bucket_name;
        }

        std::string& get_bucket_name()
        {
          return bucket_name_;
        }

      private:
        std::string bucket_name_;
    };

    class KvRspGetBucketMessage : public common::BasePacket
    {
      public:
        KvRspGetBucketMessage();
        virtual ~KvRspGetBucketMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        std::vector<std::string>& get_v_object_name()
        {
          return v_object_name_;
        }
      private:
        mutable std::vector<std::string> v_object_name_;
    };

    class KvReqDeleteBucketMessage : public common::BasePacket
    {
      public:
        KvReqDeleteBucketMessage();
        virtual ~KvReqDeleteBucketMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_bucket_name(std::string& bucket_name)
        {
          bucket_name_ = bucket_name;
        }

      private:
        std::string bucket_name_;
    };*/
  }
}

#endif
