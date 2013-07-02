/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: dataserver_message.h 706 2011-08-12 08:24:41Z duanfei@taobao.com $
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
        inline void set_dataserver_information(const common::DataServerStatInfo& info) { information_ = info;}
        inline const common::DataServerStatInfo& get_dataserver_information() const { return information_;}
      protected:
        common::DataServerStatInfo information_;
    };

    class CallDsReportBlockRequestMessage: public common::BasePacket
    {
      public:
        CallDsReportBlockRequestMessage();
        virtual ~CallDsReportBlockRequestMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline uint64_t get_server() const
        {
          return server_;
        }
        inline void set_server(const uint64_t server)
        {
          server_ = server;
        }
        inline int32_t get_flag() const
        {
          return flag_;
        }
        inline void set_flag(const int32_t flag)
        {
          flag_ = flag;
        }
      private:
        uint64_t server_;
        int32_t flag_;
    };

    class ReportBlocksToNsRequestMessage: public common::BasePacket
    {
      public:
        ReportBlocksToNsRequestMessage();
        virtual ~ReportBlocksToNsRequestMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline void set_server(const uint64_t server) { server_ = server;}
        inline uint64_t get_server(void) const { return server_;}
        inline common::BlockInfoV2* alloc_blocks_ext(const int32_t count)
        {
          block_count_ = count;
          tbsys::gDeleteA(blocks_ext_);
          blocks_ext_ = new (std::nothrow)common::BlockInfoV2[count];
          assert(blocks_ext_);
          return blocks_ext_;
        }
        inline common::BlockInfoV2* get_blocks_ext() { return blocks_ext_;}
        inline int32_t get_block_count() const { return block_count_;}
      protected:
        common::BlockInfoV2* blocks_ext_;
        uint64_t server_;
        int32_t  block_count_;
    };

    class ReportBlocksToNsResponseMessage: public common::BasePacket
    {
      public:
        ReportBlocksToNsResponseMessage();
        virtual ~ReportBlocksToNsResponseMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline std::vector<uint64_t>& get_blocks()
        {
          return expire_blocks_;
        }
        inline void set_server(const uint64_t server)
        {
          server_ = server;
        }
        inline uint64_t get_server(void) const
        {
          return server_;
        }
        inline void set_status(const int8_t status)
        {
          status_ = status;
        }
        inline int8_t get_status(void) const
        {
          return status_;
        }
      protected:
        std::vector<uint64_t> expire_blocks_;
        uint64_t server_;
        int8_t status_;
    };
  }/** end namespace message **/
}/** end namespace tfs **/
#endif
