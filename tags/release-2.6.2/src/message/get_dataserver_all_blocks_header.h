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

#ifndef TFS_MESSAGE_GETALLBLOCKHEADERMESSAGE_H_
#define TFS_MESSAGE_GETALLBLOCKHEADERMESSAGE_H_

#include <common/base_packet.h>

namespace tfs
{
  namespace message
  {
    class GetAllBlocksHeaderMessage: public common::BasePacket
    {
      public:
        GetAllBlocksHeaderMessage();
        virtual ~GetAllBlocksHeaderMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_value(const int32_t value)
        {
          value_ = value;
        }

        int32_t get_value() const
        {
          return value_;
        }
      private:
        int32_t value_;// reserve
    };

    class GetAllBlocksHeaderRespMessage: public common::BasePacket
    {
      public:
        GetAllBlocksHeaderRespMessage();
        virtual ~GetAllBlocksHeaderRespMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_all_blocks_header(const std::vector<common::IndexHeaderV2>& blocks_header)
        {
          all_blocks_header_ = blocks_header;
        }

        std::vector<common::IndexHeaderV2>& get_all_blocks_header()
        {
          return all_blocks_header_;
        }

      private:
        std::vector<common::IndexHeaderV2> all_blocks_header_;
    };

  }
}
#endif
