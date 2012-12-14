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
    class OpenFileMessage: public common::BasePacket
    {
      public:
        OpenFileMessage();
        virtual ~OpenFileMessage();
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

    class OpenFileRespMessage: public common::BasePacket
    {
      public:
        OpenFileRespMessage();
        virtual ~OpenFileRespMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_block_meta(const common::BlockMeta& block_meta)
        {
          block_meta_ = block_meta;
        }

        const common::BlockMeta& get_block_meta() const
        {
          return block_meta_;
        }

      private:
        common::BlockMeta block_meta_;
    };

    class BatchOpenFileMessage: public common::BasePacket
    {
      public:
        BatchOpenFileMessage();
        virtual ~BatchOpenFileMessage();
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

    class BatchOpenFileRespMessage: public common::BasePacket
    {
      public:
        BatchOpenFileRespMessage();
        virtual ~BatchOpenFileRespMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void add_block_meta(const uint64_t block_id, const common::BlockMeta& meta)
        {
          block_metas_.insert(std::make_pair(block_id, meta));
        }

        void set_block_metas(const std::map<uint64_t, common::BlockMeta>& block_metas)
        {
          block_metas_ = block_metas;
        }

        const std::map<uint64_t, common::BlockMeta>& get_block_metas() const
        {
          return block_metas_;
        }

      private:
        std::map<uint64_t, common::BlockMeta> block_metas_;
    };

  }
}
#endif
