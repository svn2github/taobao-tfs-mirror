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

#ifndef TFS_MESSAGE_READFILEMESSAGEV2_H_
#define TFS_MESSAGE_READFILEMESSAGEV2_H_

#include <tbsys.h>
#include <common/base_packet.h>
#include <common/status_message.h>

namespace tfs
{
  namespace message
  {
    class StatFileMessageV2: public common::BasePacket
    {
      public:
        StatFileMessageV2();
        virtual ~StatFileMessageV2();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_block_id(const uint64_t block_id)
        {
          block_id_ = block_id;
        }

        uint64_t get_block_id() const
        {
          return block_id_;
        }

        void set_attach_block_id(const uint64_t attach_block_id)
        {
          attach_block_id_ = attach_block_id;
        }

        uint64_t get_attach_block_id() const
        {
          return attach_block_id_;
        }

        void set_file_id(const uint64_t file_id)
        {
          file_id_ = file_id;
        }

        uint64_t get_file_id() const
        {
          return file_id_;
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
        uint64_t block_id_;
        uint64_t attach_block_id_;
        uint64_t file_id_;
        int32_t flag_;
        common::FamilyInfoExt family_info_;
    };

    class StatFileRespMessageV2: public common::BasePacket
    {
      public:
        StatFileRespMessageV2();
        virtual ~StatFileRespMessageV2();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_file_info(const common::FileInfoV2& file_info)
        {
          file_info_ = file_info;
        }

        const common::FileInfoV2& get_file_info() const
        {
          return file_info_;
        }

      private:
        common::FileInfoV2 file_info_;
    };

    class ReadFileMessageV2: public common::BasePacket
    {
      public:
        ReadFileMessageV2();
        virtual ~ReadFileMessageV2();
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

        void set_attach_block_id(const uint64_t attach_block_id)
        {
          attach_block_id_ = attach_block_id;
        }

        uint64_t get_attach_block_id() const
        {
          return attach_block_id_;
        }

      protected:
        common::FileSegment file_seg_;
        common::FamilyInfoExt family_info_;
        uint64_t attach_block_id_;
        int32_t flag_;
    };

    class ReadFileRespMessageV2: public common::BasePacket
    {
      public:
        ReadFileRespMessageV2();
        virtual ~ReadFileRespMessageV2();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        char* alloc_data(const int32_t len);
        char* get_data() const
        {
          return data_;
        }

        int32_t get_length() const
        {
          return length_;
        }

        void set_length(const int32_t len)
        {
          if (length_ <= 0 && alloc_ && data_)
          {
            ::free(data_);
            data_ = NULL;
            alloc_ = false;
          }
          length_ = len;
        }

        void set_file_info(const common::FileInfoV2& file_info)
        {
          file_info_ = file_info;
        }

        const common::FileInfoV2& get_file_info() const
        {
          return file_info_;
        }

      protected:
        char* data_;
        int32_t length_;
        bool alloc_;
        common::FileInfoV2 file_info_;
    };

  }
}
#endif
