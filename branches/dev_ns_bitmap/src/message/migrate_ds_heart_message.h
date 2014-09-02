/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: migrate_ds_heart_message.h 439 2013-09-02 08:35:08Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_MESSAGE_MIGRATE_DS_HEARTMESSAGE_H_
#define TFS_MESSAGE_MIGRATE_DS_HEARTMESSAGE_H_

#include "common/rts_define.h"
#include "common/base_packet.h"

namespace tfs
{
  namespace message
  {
    class MigrateDsHeartMessage: public common::BasePacket
    {
      public:
        MigrateDsHeartMessage();
        virtual ~MigrateDsHeartMessage();

        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline void set_dataserver_information(const common::DataServerStatInfo& info) { information_ = info;}
        inline const common::DataServerStatInfo& get_dataserver_information() const { return information_;}
      private:
        common::DataServerStatInfo information_;
    };

    class MigrateDsHeartResponseMessage: public common::BasePacket
    {
      public:
        MigrateDsHeartResponseMessage();
        virtual ~MigrateDsHeartResponseMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline int32_t get_ret_value() const { return ret_value_;}
        inline void set_ret_value(const int32_t ret) { ret_value_ = ret;}
      private:
        int32_t ret_value_;
    };
  }/** end namespace message **/
}/** end namesapce tfs **/
#endif
