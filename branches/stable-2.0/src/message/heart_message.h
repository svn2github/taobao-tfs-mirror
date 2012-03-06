/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: heart_message.h 439 2011-06-08 08:35:08Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_MESSAGE_HEARTMESSAGE_H_
#define TFS_MESSAGE_HEARTMESSAGE_H_
#include <vector>
#include "common/base_packet.h"

namespace tfs
{
  namespace message
  {
    class RespHeartMessage: public common::BasePacket 
    {
      public:
        RespHeartMessage();
        virtual ~RespHeartMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        inline const common::VUINT32* get_expire_blocks() const
        {
          return &expire_blocks_;
        }
        inline const common::VUINT32* get_new_blocks() const
        {
          return &new_blocks_;
        }
        inline void add_expire_id(const uint32_t block_id)
        {
          expire_blocks_.push_back(block_id);
        }
        inline void set_expire_blocks(const common::VUINT32 & blocks)
        {
          expire_blocks_ = blocks;
        }
        inline void set_status(const int32_t status)
        {
          status_ = status;
        }
        inline void set_sync_mirror_status(const int32_t mirror_status)
        {
          sync_mirror_status_ = mirror_status;
        }
        inline int32_t get_status() const
        {
          return status_;
        }
        inline int32_t get_sync_mirror_status() const
        {
          return sync_mirror_status_;
        }
      protected:
        int32_t status_;
        int32_t sync_mirror_status_;
        common::VUINT32 expire_blocks_;
        common::VUINT32 new_blocks_;
    };

#pragma pack(4)
    struct NSIdentityNetPacket
    {
      int serialize(char*data, const int64_t data_len, int64_t& pos) const;
      int deserialize(const char*data, const int64_t data_len, int64_t& pos);
      int64_t length() const;
      uint64_t ip_port_;
      uint8_t role_;
      uint8_t status_;
      uint8_t flags_;
      uint8_t force_;
    };
#pragma pack()

    enum HeartGetDataserverListFlags
    {
      HEART_GET_DATASERVER_LIST_FLAGS_NO = 0x00,
      HEART_GET_DATASERVER_LIST_FLAGS_YES
    };

    enum HeartForceModifyOthersideRoleAndStatus
    {
      HEART_FORCE_MODIFY_OTHERSIDE_ROLE_FLAGS_NO = 0x00,
      HEART_FORCE_MODIFY_OTHERSIDE_ROLE_FLAGS_YES
    };

    class MasterAndSlaveHeartMessage: public common::BasePacket 
    {
      public:
        MasterAndSlaveHeartMessage();
        virtual ~MasterAndSlaveHeartMessage();

        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline void set_ip_port(const uint64_t server_ip_port)
        {
          ns_identity_.ip_port_ = server_ip_port;
        }
        inline uint64_t get_ip_port() const
        {
          return ns_identity_.ip_port_;
        }
        inline void set_role(const uint8_t role)
        {
          ns_identity_.role_ = role;
        }
        inline uint8_t get_role() const
        {
          return ns_identity_.role_;
        }
        inline void set_status(const uint8_t status)
        {
          ns_identity_.status_ = status;
        }
        inline uint8_t get_status() const
        {
          return ns_identity_.status_;
        }
        inline void set_flags(const uint8_t flags = HEART_GET_DATASERVER_LIST_FLAGS_YES)
        {
          ns_identity_.flags_ = flags;
        }
        inline uint8_t get_flags() const
        {
          return ns_identity_.flags_;
        }
        inline void set_force_flags(const uint8_t flags = HEART_FORCE_MODIFY_OTHERSIDE_ROLE_FLAGS_YES)
        {
          ns_identity_.force_ = flags;
        }
        inline uint8_t get_force_flags() const
        {
          return ns_identity_.force_;
        }
      private:
        NSIdentityNetPacket ns_identity_;
    };

    class MasterAndSlaveHeartResponseMessage: public common::BasePacket 
    {
      public:
        MasterAndSlaveHeartResponseMessage();
        virtual ~MasterAndSlaveHeartResponseMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline void set_ip_port(const uint64_t server_ip_port)
        {
          ns_identity_.ip_port_ = server_ip_port;
        }
        inline uint64_t get_ip_port() const
        {
          return ns_identity_.ip_port_;
        }
        inline void set_role(const uint8_t role)
        {
          ns_identity_.role_ = role;
        }
        inline uint8_t get_role() const
        {
          return ns_identity_.role_;
        }
        inline void set_status(const uint8_t status)
        {
          ns_identity_.status_ = status;
        }
        inline uint8_t get_status() const
        {
          return ns_identity_.status_;
        }
        inline void set_flags(const uint8_t flags = HEART_GET_DATASERVER_LIST_FLAGS_YES)
        {
          ns_identity_.flags_ = flags;
        }
        inline uint8_t get_flags() const
        {
          return ns_identity_.flags_;
        }
        inline const common::VUINT64* get_ds_list() const
        {
          return &ds_list_;
        }
        inline void set_ds_list(const common::VUINT64& ds_list)
        {
          ds_list_ = ds_list;
        }
      private:
        NSIdentityNetPacket ns_identity_;
        common::VUINT64 ds_list_;
    };

    class HeartBeatAndNSHeartMessage: public common::BasePacket 
    {
      public:
        HeartBeatAndNSHeartMessage();
        virtual ~HeartBeatAndNSHeartMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline void set_ns_switch_flag_and_status(const uint32_t switch_flag, const uint32_t status)
        {
          flags_ = (status & 0x0000FFFF);
          flags_ |= (switch_flag & 0x0000FFFF) << 16;
        }
        inline int32_t get_ns_switch_flag() const
        {
          return ((flags_ >> 16) & 0x0000FFFF);
        }
        inline int32_t get_ns_status() const
        {
          return (flags_ & 0x0000FFFF);
        }
      private:
        int32_t flags_;
    };

    class OwnerCheckMessage: public common::BasePacket 
    {
      public:
        OwnerCheckMessage();
        virtual ~OwnerCheckMessage();
        virtual int serialize(common::Stream& ) const { return common::TFS_SUCCESS;}
        virtual int deserialize(common::Stream& ){ return common::TFS_SUCCESS;}
        virtual int64_t length() const{return 0;}
        inline void set_start_time(const int64_t start = time(NULL))
        {
          start_time_ = start;
        }
        inline int64_t get_start_time() const
        {
          return start_time_;
        }
      private:
        int64_t start_time_;
    };
  }
}
#endif
