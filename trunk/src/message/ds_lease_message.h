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

#ifndef TFS_MESSAGE_DS_LEASE_MESSAGE_H_
#define TFS_MESSAGE_DS_LEASE_MESSAGE_H_

#include "common/internal.h"
#include "common/base_packet.h"
#include "common/error_msg.h"

namespace tfs
{
  namespace message
  {
    class DsApplyLeaseMessage : public common::BasePacket
    {
      public:
        DsApplyLeaseMessage();
        virtual ~DsApplyLeaseMessage();
        virtual int serialize(common::Stream& output)  const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        common::DataServerStatInfo& get_ds_stat()
        {
          return ds_stat_;
        }

        void set_ds_stat(common::DataServerStatInfo& ds_stat)
        {
          ds_stat_ = ds_stat;
        }

      protected:
        common::DataServerStatInfo ds_stat_;
    };

    class DsApplyLeaseResponseMessage : public common::BasePacket
    {
      public:
        DsApplyLeaseResponseMessage();
        virtual ~DsApplyLeaseResponseMessage();
        virtual int serialize(common::Stream& output)  const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_lease_meta(const common::LeaseMeta& lease_meta)
        {
          lease_meta_ = lease_meta;
        }

        common::LeaseMeta& get_lease_meta()
        {
          return lease_meta_;
        }

      protected:
        common::LeaseMeta lease_meta_;
    };

    class DsRenewLeaseMessage : public DsApplyLeaseMessage
    {
      public:
        DsRenewLeaseMessage();
        virtual ~DsRenewLeaseMessage();
        virtual int serialize(common::Stream& output)  const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        common::BlockInfoV2* get_block_infos()
        {
          return block_infos_;
        }

        int32_t get_size() const
        {
          return size_;
        }

        void set_size(const int32_t size)
        {
          size_ = size;
        }

      protected:
        common::BlockInfoV2 block_infos_[common::MAX_WRITABLE_BLOCK_COUNT];
        int32_t size_;
    };

    class DsRenewLeaseResponseMessage : public DsApplyLeaseResponseMessage
    {
      public:
        DsRenewLeaseResponseMessage();
        virtual ~DsRenewLeaseResponseMessage();
        virtual int serialize(common::Stream& output)  const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        common::BlockLease* get_block_lease()
        {
          return block_lease_;
        }

        int32_t get_size() const
        {
          return size_;
        }

        void set_size(const int32_t size)
        {
          size_ = size;
        }

      protected:
        common::BlockLease block_lease_[common::MAX_WRITABLE_BLOCK_COUNT];
        int32_t size_;
    };

    // reponse with StatusMessage
    class DsGiveupLeaseMessage : public DsRenewLeaseMessage
    {
      public:
        DsGiveupLeaseMessage();
        virtual ~DsGiveupLeaseMessage();
        virtual int serialize(common::Stream& output)  const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
    };

    class DsApplyBlockMessage: public common::BasePacket
    {
      public:
        DsApplyBlockMessage();
        virtual ~DsApplyBlockMessage();
        virtual int serialize(common::Stream& output)  const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_server_id(const uint64_t server_id)
        {
          server_id_ = server_id;
        }

        uint64_t get_server_id() const
        {
          return server_id_;
        }

        void set_count(const int32_t count)
        {
          count_ = count;
        }

        int32_t get_count() const
        {
          return count_;
        }

      protected:
        uint64_t server_id_;
        int32_t count_;
    };

    class DsApplyBlockResponseMessage: public common::BasePacket
    {
      public:
        DsApplyBlockResponseMessage();
        virtual ~DsApplyBlockResponseMessage();
        virtual int serialize(common::Stream& output)  const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        common::BlockLease* get_block_lease()
        {
          return block_lease_;
        }

        int32_t get_size() const
        {
          return size_;
        }

        void set_size(const int32_t size)
        {
          size_ = size;
        }

      protected:
        common::BlockLease block_lease_[common::MAX_WRITABLE_BLOCK_COUNT];
        int32_t size_;
   };

    class DsApplyBlockForUpdateMessage: public common::BasePacket
    {
      public:
        DsApplyBlockForUpdateMessage();
        virtual ~DsApplyBlockForUpdateMessage();
        virtual int serialize(common::Stream& output)  const;
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

        void set_server_id(const uint64_t server_id)
        {
          server_id_ = server_id;
        }

        uint64_t get_server_id() const
        {
          return server_id_;
        }

      protected:
        uint64_t block_id_;
        uint64_t server_id_;
    };

    class DsApplyBlockForUpdateResponseMessage: public common::BasePacket
    {
      public:
        DsApplyBlockForUpdateResponseMessage();
        virtual ~DsApplyBlockForUpdateResponseMessage();
        virtual int serialize(common::Stream& output)  const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        common::BlockLease& get_block_lease()
        {
          return block_lease_;
        }

      protected:
        common::BlockLease block_lease_;
    };

    class DsGiveupBlockMessage: public common::BasePacket
    {
      public:
        DsGiveupBlockMessage();
        virtual ~DsGiveupBlockMessage();
        virtual int serialize(common::Stream& output)  const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        common::BlockInfoV2* get_block_infos()
        {
          return block_infos_;
        }

        int32_t get_size() const
        {
          return size_;
        }

        void set_size(const int32_t size)
        {
          size_ = size;
        }

        void set_server_id(const uint64_t server_id)
        {
          server_id_ = server_id;
        }

        uint64_t get_server_id() const
        {
          return server_id_;
        }

      protected:
        uint64_t server_id_;
        common::BlockInfoV2 block_infos_[common::MAX_WRITABLE_BLOCK_COUNT];
        int32_t size_;
    };

    class DsGiveupBlockResponseMessage: public DsApplyBlockResponseMessage
    {
      public:
        DsGiveupBlockResponseMessage();
        virtual ~DsGiveupBlockResponseMessage();
        virtual int serialize(common::Stream& output)  const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
    };

  }/** end namespace message **/
}/** end namespace tfs **/
#endif
