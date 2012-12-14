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

#ifndef TFS_MESSAGE_BLOCKOPERATIONMESSAGE_H_
#define TFS_MESSAGE_BLOCKOPERATIONMESSAGE_H_

#include <common/base_packet.h>
#include <common/status_message.h>

namespace tfs
{
  namespace message
  {
    class CreateBlockMessage: public common::BasePacket
    {
      public:
        CreateBlockMessage();
        virtual ~CreateBlockMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_block_ids(const common::VUINT64& block_ids)
        {
          block_ids_ = block_ids;
        }

        void add_block(const uint64_t block_id)
        {
          block_ids_.push_back(block_id);
        }

        const common::VUINT64& get_block_ids() const
        {
          return block_ids_;
        }

      private:
        common::VUINT64 block_ids_;
    };

    class DeleteBlockMessage: public common::BasePacket
    {
      public:
        DeleteBlockMessage();
        virtual ~DeleteBlockMessage();
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

      private:
        uint64_t block_id_;
    };

  }
}
#endif
