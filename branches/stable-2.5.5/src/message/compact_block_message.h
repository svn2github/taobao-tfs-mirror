/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: compact_block_message.h 706 2011-08-12 08:24:41Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_MESSAGE_COMPACTBLOCKMESSAGE_H_
#define TFS_MESSAGE_COMPACTBLOCKMESSAGE_H_
#include "base_task_message.h"
#include "common/internal.h"
namespace tfs
{
  namespace message
  {
    class NsRequestCompactBlockMessage: public BaseTaskMessage
    {
      public:
        NsRequestCompactBlockMessage();
        virtual ~NsRequestCompactBlockMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline void set_block_id(const uint32_t block_id) { block_id_ = block_id;}
        inline uint32_t get_block_id() const { return block_id_;}
        inline std::vector<uint64_t>& get_servers() { return servers_;}
        inline void set_servers(const std::vector<uint64_t>& servers) { servers_ = servers; }
      protected:
        uint32_t block_id_;
        std::vector<uint64_t> servers_;
    };

    class DsCommitCompactBlockCompleteToNsMessage: public BaseTaskMessage
    {
      public:
        DsCommitCompactBlockCompleteToNsMessage();
        virtual ~DsCommitCompactBlockCompleteToNsMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        int deserialize(const char* data, const int64_t data_len, int64_t& pos);
        virtual int64_t length() const;
        void dump(void) const;
        inline void set_block_info(const common::BlockInfoV2& block_info) { block_info_ = block_info;}
        inline common::BlockInfoV2& get_block_info() { return block_info_;}
        inline const std::vector<std::pair<uint64_t, int8_t> >& get_result() const { return result_;}
        inline void set_result(const std::vector<std::pair<uint64_t, int8_t> >& result) { result_ = result;}
      protected:
        common::BlockInfoV2 block_info_;
        std::vector<std::pair<uint64_t, int8_t> > result_;
    };
  }
}
#endif
