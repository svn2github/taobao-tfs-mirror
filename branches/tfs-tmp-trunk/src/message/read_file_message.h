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

#ifndef TFS_MESSAGE_READFILEMESSAGE_H_
#define TFS_MESSAGE_READFILEMESSAGE_H_

#include <tbsys.h>
#include <common/base_packet.h>
#include <common/status_message.h>

namespace tfs
{
  namespace message
  {
    class StatFileMessage: public common::BasePacket
    {
      public:
        StatFileMessage();
        virtual ~StatFileMessage();
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
          flag_ = flag;
        }

        int32_t get_flag() const
        {
          return flag_;
        }

        void set_force()
        {
          flag_ |= common::MF_READ_FORCE;
        }

        bool get_force()
        {
          return (flag_ & common::MF_READ_FORCE);
        }

        void set_family_info(const common::FamilyInfoExt& family_info)
        {
          family_info_ = family_info;
        }

        const common::FamilyInfoExt& get_family_info() const
        {
          return family_info_;
        }

      private:
        uint64_t block_id_;
        uint64_t file_id_;
        int32_t flag_;
        common::FamilyInfoExt family_info_;
    };

    class StatFileRespMessage: public common::StatusMessage
    {
      public:
        StatFileRespMessage();
        virtual ~StatFileRespMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_file_info(const common::FileInfo& file_info)
        {
          file_info_ = file_info;
        }

        const common::FileInfo& get_file_info() const
        {
          return file_info_;
        }

      private:
        common::FileInfo file_info_;
    };

    class ReadFileMessage: public common::BasePacket
    {
      public:
        ReadFileMessage();
        virtual ~ReadFileMessage();
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
          flag_ = flag;
        }

        int32_t get_flag() const
        {
          return flag_;
        }

        void set_force()
        {
          flag_ |= common::MF_READ_FORCE;
        }

        bool get_force()
        {
          return (flag_ & common::MF_READ_FORCE);
        }

        void set_family_info(const common::FamilyInfoExt& family_info)
        {
          family_info_ = family_info;
        }

        const common::FamilyInfoExt& get_family_info() const
        {
          return family_info_;
        }

      protected:
        common::FileSegment file_seg_;
        int32_t flag_;
        common::FamilyInfoExt family_info_;
    };

    class ReadFileRespMessage: public common::StatusMessage
    {
      public:
        ReadFileRespMessage();
        virtual ~ReadFileRespMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        /** reply is different with post */
        void set_data(const char* data, const int32_t length)
        {
          if ((data != NULL) && (length > 0))
          {
            length_ = 0;
            tbsys::gDeleteA(data_);
          }
          data_ = new (std::nothrow) char[length];
          assert(NULL != data_);
          length_ = length;
          memcpy(data_, data, length);
        }

        char* get_data() const
        {
          return data_;
        }

        void set_length(const int32_t length)
        {
          length_ = length;
        }

        int32_t get_length() const
        {
          return length_;
        }

      protected:
        char* data_;
        int32_t length_;
    };

    class ReadFileV2Message: public ReadFileMessage
    {
      public:
        ReadFileV2Message();
        virtual ~ReadFileV2Message();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
    };

    class ReadFileV2RespMessage: public ReadFileRespMessage
    {
      public:
        ReadFileV2RespMessage();
        virtual ~ReadFileV2RespMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_file_info(const common::FileInfo& file_info)
        {
          file_info_ = file_info;
        }

        const common::FileInfo& get_file_info() const
        {
          return file_info_;
        }

      private:
        common::FileInfo file_info_;
    };

  }
}
#endif
