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

#ifndef TFS_MESSAGE_ECMETAMESSAGE_H_
#define TFS_MESSAGE_ECMETAMESSAGE_H_

#include <common/base_packet.h>
#include <common/status_message.h>

namespace tfs
{
  namespace message
  {
    class QueryEcMetaMessage: public common::BasePacket
    {
      public:
        QueryEcMetaMessage();
        ~QueryEcMetaMessage();
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

        void set_lock_time(const int32_t lock_time)
        {
          lock_time_ = lock_time;
        }

        int32_t get_lock_time() const
        {
          return lock_time_;
        }

      private:
        uint64_t block_id_;
        int32_t lock_time_;
    };

    class QueryEcMetaRespMessage: public common::BasePacket
    {
      public:
        QueryEcMetaRespMessage();
        ~QueryEcMetaRespMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        common::ECMeta& get_ec_meta()
        {
          return ec_meta_;
        }

        void set_ec_meta(const common::ECMeta& ec_meta)
        {
          ec_meta_ = ec_meta;
        }

      private:
        common::ECMeta ec_meta_;
    };

    class CommitEcMetaMessage: public common::BasePacket
    {
      public:
        CommitEcMetaMessage();
        ~CommitEcMetaMessage();
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

        common::ECMeta& get_ec_meta()
        {
          return ec_meta_;
        }

        void set_ec_meta(const common::ECMeta& ec_meta)
        {
          ec_meta_ = ec_meta;
        }

        void set_switch_flag(const int8_t switch_flag)
        {
          switch_flag_ = switch_flag;
        }

        int8_t get_switch_flag() const
        {
          return switch_flag_;
        }

        void set_unlock_flag(const int8_t unlock_flag)
        {
          unlock_flag_ = unlock_flag;
        }

        int8_t get_unlock_flag() const
        {
          return unlock_flag_;
        }

      private:
        common::ECMeta ec_meta_;
        uint64_t block_id_;
        int8_t switch_flag_;
        int8_t unlock_flag_;
    };

  }
}
#endif
