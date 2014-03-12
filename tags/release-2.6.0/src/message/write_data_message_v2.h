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

#ifndef TFS_MESSAGE_WRITEDATAMESSAGEV2_H_
#define TFS_MESSAGE_WRITEDATAMESSAGEV2_H_

#include <common/base_packet.h>
#include <common/status_message.h>

namespace tfs
{
  namespace message
  {
    class WriteRawdataMessageV2: public common::BasePacket
    {
      public:
        WriteRawdataMessageV2();
        virtual ~WriteRawdataMessageV2();
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

        void set_data(const char* data)
        {
          data_ = data;
        }

        const char* get_data() const
        {
          return data_;
        }

        void set_tmp_flag(const bool tmp_flag)
        {
          tmp_flag_ = tmp_flag;
        }

        bool get_tmp_flag()
        {
          return tmp_flag_;
        }

        void set_new_flag(const bool new_flag)
        {
          new_flag_ = new_flag;
        }

        bool get_new_flag()
        {
          return new_flag_;
        }

      private:
        common::FileSegment file_seg_;
        const char* data_;
        int8_t tmp_flag_;
        int8_t new_flag_;
    };

  }
}
#endif
