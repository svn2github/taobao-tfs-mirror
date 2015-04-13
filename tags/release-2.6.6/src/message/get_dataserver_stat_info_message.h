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
#ifndef TFS_MESSAGE_GET_DS_STAT_INFOMESSAGE_H_
#define TFS_MESSAGE_GET_DS_STAT_INFOMESSAGE_H_

#include "common/rts_define.h"
#include "common/base_packet.h"

namespace tfs
{
  namespace message
  {
    class GetDsStatInfoMessage: public common::BasePacket
    {
      public:
        GetDsStatInfoMessage();
        virtual ~GetDsStatInfoMessage();

        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline void set_dataserver_information(const common::DataServerStatInfo& info) { information_ = info;}
        inline const common::DataServerStatInfo& get_dataserver_information() const { return information_;}
      private:
        common::DataServerStatInfo information_;
    };
  }/** end namespace message **/
}/** end namesapce tfs **/
#endif
