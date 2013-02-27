/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: $
 *
 * Authors:
 *   qixiao.zs <qixiao.zs@alibaba-inc.com>
 *      - initial release
 *
 */
#ifndef TFS_MESSAGE_KV_RTS_MESSAGE_H_
#define TFS_MESSAGE_KV_RTS_MESSAGE_H_

#include "common/kv_rts_define.h"
#include "common/base_packet.h"

namespace tfs
{
  namespace message
  {
    class KvRtsMsHeartMessage: public common::BasePacket
    {
      public:
        KvRtsMsHeartMessage();
        virtual ~KvRtsMsHeartMessage();

        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline common::KvMetaServerBaseInformation& get_ms(void) { return base_info_;}
        inline void set_ms(const common::KvMetaServerBaseInformation& base_info){ memcpy(&base_info_, &base_info, sizeof(common::KvMetaServerBaseInformation));}

      private:
        common::KvMetaServerBaseInformation base_info_;
    };

    class KvRtsMsHeartResponseMessage: public common::BasePacket
    {
      public:
        KvRtsMsHeartResponseMessage();
        virtual ~KvRtsMsHeartResponseMessage();

        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline int32_t get_time(void) { return heart_interval_;}
        inline void set_time(int32_t& heart_interval){ heart_interval_ = heart_interval;}

      private:
        int32_t heart_interval_;

    };

    class GetTableFromKvRtsMessage: public common::BasePacket
    {
      public:
        GetTableFromKvRtsMessage();
        virtual ~GetTableFromKvRtsMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
      private:
        int8_t  reserve_;
    };

    class GetTableFromKvRtsResponseMessage: public common::BasePacket
    {
      public:
        GetTableFromKvRtsResponseMessage();
        virtual ~GetTableFromKvRtsResponseMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        common::KvMetaTable& get_mutable_table() { return kv_meta_table_;}

      private:
        common::KvMetaTable kv_meta_table_;
    };



  }
}
#endif
