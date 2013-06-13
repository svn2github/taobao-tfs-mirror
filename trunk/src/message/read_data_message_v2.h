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

#ifndef TFS_MESSAGE_READDATAMESSAGEV2_H_
#define TFS_MESSAGE_READDATAMESSAGEV2_H_

#include "common/base_packet.h"
namespace tfs
{
  namespace message
  {
    class ReadRawdataMessageV2: public common::BasePacket
    {
      public:
        ReadRawdataMessageV2();
        virtual ~ReadRawdataMessageV2();
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

      protected:
        common::FileSegment file_seg_;
    };

    class ReadRawdataRespMessageV2: public common::BasePacket
    {
      public:
        ReadRawdataRespMessageV2();
        virtual ~ReadRawdataRespMessageV2();
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

      protected:
        char* data_;
        int32_t length_;
        bool alloc_;
    };


  }
}
#endif
