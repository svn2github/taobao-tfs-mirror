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

#ifndef TFS_MESSAGE_READINDEXMESSAGEV2_H_
#define TFS_MESSAGE_READINDEXMESSAGEV2_H_

#include <common/base_packet.h>
#include <common/status_message.h>

namespace tfs
{
  namespace message
  {
    class ReadIndexMessageV2: public common::BasePacket
    {
      public:
        ReadIndexMessageV2();
        ~ReadIndexMessageV2();
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

      private:
        uint64_t block_id_;
        uint64_t attach_block_id_;
    };

    class ReadIndexRespMessageV2: public common::BasePacket
    {
      public:
        ReadIndexRespMessageV2();
        ~ReadIndexRespMessageV2();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        common::IndexDataV2& get_index_data()
        {
          return index_data_;
        }

        void set_index_data(const common::IndexDataV2& index_data)
        {
          index_data_= index_data;
        }

      private:
        common::IndexDataV2 index_data_;
    };

  }
}
#endif
