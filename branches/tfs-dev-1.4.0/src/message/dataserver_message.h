/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_MESSAGE_DATASERVERMESSAGE_H_
#define TFS_MESSAGE_DATASERVERMESSAGE_H_
#include "common/base_packet.h"
namespace tfs
{
  namespace message
  {
    class SetDataserverMessage: public common::BasePacket 
    {
      public:
        SetDataserverMessage();
        virtual ~SetDataserverMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        void set_ds(common::DataServerStatInfo* ds);
        inline void set_has_block(const common::HasBlockFlag has_block)
        {
          has_block_ = has_block;
        }
        void add_block(common::BlockInfo* block_info);
        inline common::HasBlockFlag get_has_block() const
        {
          return has_block_;
        }
        inline const common::DataServerStatInfo& get_ds() const
        {
          return ds_;
        }
        inline common::BLOCK_INFO_LIST& get_blocks()
        {
          return blocks_;
        }
      protected:
        common::DataServerStatInfo ds_;
        common::BLOCK_INFO_LIST blocks_;
        common::HasBlockFlag has_block_;
    };

    class SuspectDataserverMessage: public common::BasePacket 
    {
      public:
        SuspectDataserverMessage();
        virtual ~SuspectDataserverMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline void set_server_id(const uint64_t server_id)
        {
          server_id_ = server_id;
        }
        inline uint64_t get_server_id() const
        {
          return server_id_;
        }
      protected:
        uint64_t server_id_;
    };
  }
}
#endif
