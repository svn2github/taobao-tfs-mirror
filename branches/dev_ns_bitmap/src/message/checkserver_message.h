/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: checkserver_message.h 706 2012-04-12 14:24:41Z linqing.zyd@taobao.com $
 *
 * Authors:
 *   linqing <linqing.zyd@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_MESSAGE_CHECKSERVERMESSAGE_H_
#define TFS_MESSAGE_CHECKSERVERMESSAGE_H_

#include "common/base_packet.h"
#include "common/internal.h"
#include <vector>

namespace tfs
{
  namespace message
  {
    class CheckBlockRequestMessage: public common::BasePacket
    {
      public:
        CheckBlockRequestMessage():
          group_count_(1), group_seq_(0)
        {
          _packetHeader._pcode = common::REQ_CHECK_BLOCK_MESSAGE;
        }
        virtual ~CheckBlockRequestMessage()
        {
        }

        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_time_range(const common::TimeRange& range)
        {
          range_ = range;
        }

        common::TimeRange& get_time_range()
        {
          return range_;
        }

        void set_group_count(const int32_t group_count)
        {
          group_count_ = group_count;
        }

        int32_t get_group_count() const
        {
          return group_count_;
        }

        void set_group_seq(const int32_t group_seq)
        {
          group_seq_ = group_seq;
        }

        int32_t get_group_seq() const
        {
          return group_seq_;
        }

      private:
        common::TimeRange range_;
        int32_t group_count_;
        int32_t group_seq_;
    };

    class CheckBlockResponseMessage: public common::BasePacket
    {
      public:
        CheckBlockResponseMessage()
        {
          _packetHeader._pcode = common::RSP_CHECK_BLOCK_MESSAGE;
        }

        virtual ~CheckBlockResponseMessage()
        {
        }

        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        common::VUINT64& get_blocks()
        {
          return blocks_;
        }

      private:
        common::VUINT64 blocks_;
    };

    class ReportCheckBlockMessage: public common::BasePacket
    {
      public:
        ReportCheckBlockMessage()
        {
          _packetHeader._pcode = common::REPORT_CHECK_BLOCK_MESSAGE;
        }

        virtual ~ReportCheckBlockMessage()
        {
        }

        common::CheckParam& get_param()
        {
          return param_;
        }

        void set_param(const common::CheckParam& param)
        {
          param_ = param;
        }

        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

      private:
        common::CheckParam param_;
    };

    class ReportCheckBlockResponseMessage: public common::BasePacket
    {
      public:
        ReportCheckBlockResponseMessage(): seqno_(0), server_id_(common::INVALID_SERVER_ID)
        {
          _packetHeader._pcode = common::REPORT_CHECK_BLOCK_RESPONSE_MESSAGE;
        }

        virtual ~ReportCheckBlockResponseMessage()
        {
        }

        void set_seqno(const int64_t seqno)
        {
          seqno_ = seqno;
        }

        int64_t get_seqno() const
        {
          return seqno_;
        }

        void set_server_id(const uint64_t server_id)
        {
          server_id_ = server_id;
        }

        uint64_t get_server_id() const
        {
          return server_id_;
        }

        std::vector<common::CheckResult>& get_result()
        {
          return result_;
        }

        void set_result(const std::vector<common::CheckResult>& result)
        {
          result_ = result;
        }

        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

      private:
        int64_t seqno_;
        uint64_t server_id_;
        std::vector<common::CheckResult> result_;
    };


 }/** end namespace message **/
}/** end namespace tfs **/
#endif
