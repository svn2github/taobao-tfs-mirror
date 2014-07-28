/*
 * (C) 2007-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: sync_file_entry_message.h 346 2013-08-29 10:18:07Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_MESSAGE_SYNC_FILE_ENTRY_MESSAGE_H_
#define TFS_MESSAGE_SYNC_FILE_ENTRY_MESSAGE_H_
#include "common/base_packet.h"
namespace tfs
{
  namespace message
  {
    class SyncFileEntryMessage: public common::BasePacket
    {
      public:
        SyncFileEntryMessage();
        virtual ~SyncFileEntryMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline common::SyncFileEntry* get_entry()  { return entry_;}
        inline int32_t get_count() const { return count_;}
        inline void set_count(const int32_t count) { count_ = count;}
      private:
        common::SyncFileEntry entry_[common::MAX_SYNC_FILE_ENTRY_COUNT];
        int32_t count_;
    };

    class SyncFileEntryResponseMessage: public common::BasePacket
    {
      public:
        SyncFileEntryResponseMessage();
        virtual ~SyncFileEntryResponseMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
      private:
        int32_t result_;
    };
  } /** end namespace message **/
}/** end namesapce tfs **/
#endif
