/*
* (C) 2007-2010 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
*
* Authors:
*   linqing <linqing.zyd@taobao.com>
*      - initial release
*
*/

#ifndef TFS_MESSAGE_WRITEFILEMESSAGEV2_H_
#define TFS_MESSAGE_WRITEFILEMESSAGEV2_H_

#include <common/base_packet.h>
#include <common/status_message.h>

namespace tfs
{
  namespace message
  {
    class WriteFileMessageV2: public common::BasePacket
    {
      public:
        WriteFileMessageV2();
        virtual ~WriteFileMessageV2();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_block_id(const uint64_t block_id)
        {
          file_seg_.block_id_ = block_id;
        }

        uint64_t get_block_id() const
        {
          return file_seg_.block_id_;
        }

        void set_file_id(const uint64_t file_id)
        {
          file_seg_.file_id_ = file_id;
        }

        uint64_t get_file_id() const
        {
          return file_seg_.file_id_;
        }

        void set_offset(const int32_t offset)
        {
          file_seg_.offset_ = offset;
        }

        int32_t get_offset() const
        {
          return file_seg_.offset_;
        }

        void set_length(const int32_t length)
        {
          file_seg_.length_ = length;
        }

        int32_t get_length() const
        {
          return file_seg_.length_;
        }

        void set_file_seg(const common::FileSegment& file_seg)
        {
          file_seg_ = file_seg;
        }

        const common::FileSegment& get_file_seg() const
        {
          return file_seg_;
        }

        void set_ds(const common::VUINT64& ds)
        {
          ds_ = ds;
        }

        const common::VUINT64& get_ds() const
        {
          return ds_;
        }

        void set_lease_id(const uint64_t lease_id)
        {
          lease_id_ = lease_id;
        }

        uint64_t get_lease_id() const
        {
          return lease_id_;
        }

        void set_data(const char* data)
        {
          data_ = data;
        }

        const char* get_data() const
        {
          return data_;
        }

        void set_attach_block_id(const uint64_t attach_block_id)
        {
          attach_block_id_ = attach_block_id;
        }

        uint64_t get_attach_block_id() const
        {
          return attach_block_id_;
        }

        void set_master_id(const uint64_t master_id)
        {
          master_id_ = master_id;
        }

        uint64_t get_master_id() const
        {
          return master_id_;
        }

        void set_version(const int32_t version)
        {
          version_ = version;
        }

        int32_t get_version() const
        {
          return version_;
        }

        void set_flag(const int32_t flag)
        {
          flag_ |= flag;
        }

        int32_t get_flag() const
        {
          return flag_;
        }

        void set_family_info(const common::FamilyInfoExt& family_info)
        {
          family_info_ = family_info;
        }

        common::FamilyInfoExt& get_family_info()
        {
          return family_info_;
        }

      private:
        common::FileSegment file_seg_;
        common::VUINT64 ds_;
        common::FamilyInfoExt family_info_;
        uint64_t attach_block_id_;
        uint64_t lease_id_;
        uint64_t master_id_;
        int32_t version_;
        int32_t flag_;
        const char* data_;
    };

    class WriteFileRespMessageV2: public common::BasePacket
    {
      public:
        WriteFileRespMessageV2();
        virtual ~WriteFileRespMessageV2();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_file_id(const uint64_t file_id)
        {
          file_id_ = file_id;
        }

        uint64_t get_file_id() const
        {
          return file_id_;
        }

        void set_lease_id(const uint64_t lease_id)
        {
          lease_id_ = lease_id;
        }

        uint64_t get_lease_id() const
        {
          return lease_id_;
        }

      private:
        uint64_t file_id_;
        uint64_t lease_id_;
    };

    class SlaveDsRespMessage: public common::BasePacket
    {
      public:
        SlaveDsRespMessage();
        virtual ~SlaveDsRespMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_server_id(const uint64_t server_id)
        {
          server_id_ = server_id;
        }

        uint64_t get_server_id() const
        {
          return server_id_;
        }

        void set_status(const int32_t status)
        {
          status_ = status;
        }

        int32_t get_status() const
        {
          return status_;
        }

        void set_block_info(const common::BlockInfoV2& block_info)
        {
          block_info_ = block_info;
        }

        common::BlockInfoV2& get_block_info()
        {
          return block_info_;
        }

      private:
        common::BlockInfoV2 block_info_;
        uint64_t server_id_;
        int32_t status_;
    };

  }
}
#endif
