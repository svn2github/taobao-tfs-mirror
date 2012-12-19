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

#ifndef TFS_MESSAGE_OPENFILEMESSAGE_H_
#define TFS_MESSAGE_OPENFILEMESSAGE_H_

#include <common/base_packet.h>
#include <common/status_message.h>

namespace tfs
{
  namespace message
  {
    class GetBlockInfoMessageV2: public common::BasePacket
    {
      public:
        GetBlockInfoMessageV2();
        virtual ~GetBlockInfoMessageV2();
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

        void set_mode(const int32_t mode)
        {
          mode_ = mode;
        }

        int32_t get_mode() const
        {
          return mode_;
        }

      private:
        uint64_t block_id_;
        int32_t mode_;
    };

    class GetBlockInfoRespMessageV2: public common::BasePacket
    {
      public:
        GetBlockInfoRespMessageV2();
        virtual ~GetBlockInfoRespMessageV2();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_block_meta(const common::BlockMeta& block_meta)
        {
          block_meta_ = block_meta;
        }

        common::BlockMeta& get_block_meta()
        {
          return block_meta_;
        }

      private:
        common::BlockMeta block_meta_;
    };

    class BatchGetBlockInfoMessageV2: public common::BasePacket
    {
      public:
        BatchGetBlockInfoMessageV2();
        virtual ~BatchGetBlockInfoMessageV2();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void add_block(const uint64_t block_id)
        {
          block_ids_.push_back(block_id);
        }

        void set_block_ids(const common::VUINT64& block_ids)
        {
          block_ids_ = block_ids;
        }

        const common::VUINT64& get_block_ids() const
        {
          return block_ids_;
        }

        void set_mode(const int32_t mode)
        {
          mode_ = mode;
        }

        int32_t get_mode() const
        {
          return mode_;
        }

      private:
        common::VUINT64 block_ids_;
        int32_t mode_;
    };

    class BatchGetBlockInfoRespMessageV2: public common::BasePacket
    {
      public:
        BatchGetBlockInfoRespMessageV2();
        virtual ~BatchGetBlockInfoRespMessageV2();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void add_block_meta(const common::BlockMeta& meta)
        {
          block_metas_.push_back(meta);
        }

        std::vector<common::BlockMeta>& get_block_metas()
        {
          return block_metas_;
        }

      private:
        std::vector<common::BlockMeta> block_metas_;
    };

    class NewBlockMessageV2: public common::BasePacket
    {
      public:
        NewBlockMessageV2();
        virtual ~NewBlockMessageV2();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_block_ids(const common::VUINT64& block_ids)
        {
          block_ids_ = block_ids;
        }

        void add_block(const uint64_t block_id)
        {
          block_ids_.push_back(block_id);
        }

        common::VUINT64& get_block_ids()
        {
          return block_ids_;
        }

      private:
        common::VUINT64 block_ids_;
    };

    class RemoveBlockMessageV2: public common::BasePacket
    {
      public:
        RemoveBlockMessageV2();
        virtual ~RemoveBlockMessageV2();
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

      private:
        uint64_t block_id_;
    };

    class UpdateBlockInfoMessageV2: public common::BasePacket
    {
      public:
        UpdateBlockInfoMessageV2();
        virtual ~UpdateBlockInfoMessageV2();
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

        inline void set_unlink_flag(const common::UnlinkFlag flag)
        {
          unlink_flag_ = flag;
        }

        inline common::UnlinkFlag get_unlink_flag() const
        {
          return unlink_flag_;
        }

      private:
        common::BlockInfoV2 block_info_;
        uint64_t server_id_;
        common::UnlinkFlag unlink_flag_;
    };

    class RepairBlockMessageV2: public common::BasePacket
    {
      public:
        RepairBlockMessageV2();
        virtual ~RepairBlockMessageV2();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_repair_type(common::RepairType type)
        {
          type_ = type;
        }

        common::RepairType get_repair_type() const
        {
          return type_;
        }

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

        void set_family_id(const uint64_t family_id)
        {
          family_id_ = family_id;
        }

        uint64_t get_family_id() const
        {
          return family_id_;
        }

      private:
        common::RepairType type_;
        uint64_t block_id_;
        uint64_t server_id_;
        uint64_t family_id_;
    };
  }
}
#endif
