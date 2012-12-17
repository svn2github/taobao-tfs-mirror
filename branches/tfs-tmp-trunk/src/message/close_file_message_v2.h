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

#ifndef TFS_MESSAGE_COMMITFILEMESSAGE_H_
#define TFS_MESSAGE_COMMITFILEMESSAGE_H_

#include <common/base_packet.h>
#include <common/status_message.h>

namespace tfs
{
  namespace message
  {
    class CloseFileMessageV2: public common::BasePacket
    {
      public:
        CloseFileMessageV2();
        virtual ~CloseFileMessageV2();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_block_id(const uint64_t block_id)
        {
          block_id_ = block_id;
        }

        uint64_t get_block_id() const
        {
          return block_id_;
        }

        void set_file_id(const uint64_t file_id)
        {
          file_id_ = file_id;
        }

        uint64_t get_file_id() const
        {
          return file_id_;
        }

        void set_lease_id(const uint64_t lease_id)
        {
          lease_id_ = lease_id;
        }

        uint64_t get_lease_id() const
        {
          return lease_id_;
        }

        void set_ds(const common::VUINT64& ds)
        {
          ds_ = ds;
        }

        const common::VUINT64& get_ds() const
        {
          return ds_;
        }

        void set_crc(const uint32_t crc)
        {
          crc_ = crc;
        }

        uint32_t get_crc() const
        {
          return crc_;
        }

        void set_flag(const int32_t flag)
        {
          flag_ = flag;
        }

        int32_t get_flag() const
        {
          return flag_;
        }

        void set_master()
        {
          flag_ |= common::MF_IS_MASTER;
        }

        void set_slave()
        {
          flag_ &= (~common::MF_IS_MASTER);
        }

        bool is_master()
        {
          return flag_ & common::MF_IS_MASTER;
        }

      private:
        uint64_t block_id_;
        uint64_t file_id_;
        uint64_t lease_id_;
        common::VUINT64 ds_;
        uint32_t crc_;
        int32_t flag_;
    };

    class CommitBlockUpdateMessage: public common::BasePacket
    {
      public:
        CommitBlockUpdateMessage();
        virtual ~CommitBlockUpdateMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_block_info(const common::BlockInfoV2& block_info)
        {
          block_info_ = block_info;
        }

        common::BlockInfoV2& get_block_info()
        {
          return block_info_;
        }

        void set_server_id(const uint64_t server_id)
        {
          server_id_ = server_id;
        }

        uint64_t get_server_id() const
        {
          return server_id_;
        }

        void set_oper(const common::BlockOper oper)
        {
          oper_ = oper;
        }

        common::BlockOper get_oper() const
        {
          return oper_;
        }

      private:
        common::BlockInfoV2 block_info_;
        uint64_t server_id_;
        common::BlockOper oper_;
    };

  }
}
#endif
