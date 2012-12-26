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

        const common::TfsFileInfo& get_file_info() const
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

        const common::TfsFileInfo& get_file_info() const
        {
          return tfs_file_info_;
        }
        void set_tfs_file_info(const common::TfsFileInfo& tfs_file_info)
        {
          tfs_file_info_ = tfs_file_info;
        }
      private:
        common::TfsFileInfo tfs_file_info_;
    };

  }
}

#endif
