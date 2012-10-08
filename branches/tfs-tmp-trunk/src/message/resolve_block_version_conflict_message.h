/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: reslove_block_version_conflict_message.h 439 2012-08-21 08:35:08Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_MESSAGE_RESOLVE_BLOCK_VERSION_CONFLICT_MESSAGE_H_
#define TFS_MESSAGE_RESOLVE_BLOCK_VERSION_CONFLICT_MESSAGE_H_

#include "common/base_packet.h"

namespace tfs
{
  namespace message
  {
    class ResolveBlockVersionConflictMessage: public common::BasePacket
    {
      public:
        ResolveBlockVersionConflictMessage();
        virtual ~ResolveBlockVersionConflictMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline std::vector<std::pair<uint64_t, common::BlockInfo> >& get_members() { return members_;}
        inline void set_block(const uint32_t block) { block_ = block;}
        inline uint32_t get_block() const { return block_;}
      private:
        uint32_t block_;
        std::vector<std::pair<uint64_t, common::BlockInfo> > members_;
    };

    class ResolveBlockVersionConflictResponseMessage: public common::BasePacket
    {
      public:
        ResolveBlockVersionConflictResponseMessage();
        virtual ~ResolveBlockVersionConflictResponseMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline int32_t get_status() const { return status_;}
        inline void set_status(const int32_t status) { status_ = status;}
      private:
        int32_t  status_;
    };
  }/** message **/
}/** tfs **/
#endif
