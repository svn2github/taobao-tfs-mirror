/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: family_info_message.h 746 2012-09-03 14:27:59Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_MESSAGE_FAMILY_INFO_MESSAGE_H_
#define TFS_MESSAGE_FAMILY_INFO_MESSAGE_H_
#include "common/base_packet.h"
#include "common/error_msg.h"
namespace tfs
{
  namespace message
  {
    class GetFamilyInfoMessage:  public common::BasePacket
    {
      public:
        explicit GetFamilyInfoMessage(const int32_t mode = common::T_READ);
        virtual ~GetFamilyInfoMessage();
        virtual int serialize(common::Stream& output)  const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline void set_family_id(const int64_t family_id) { family_id_ = family_id;}
        inline int64_t get_family_id() const { return family_id_;}
        inline void set_mode(const int32_t mode) { mode_ = mode;}
        inline int32_t get_mode() const { return mode_;}
      private:
        int64_t family_id_;
        int32_t mode_;
    };

    class GetFamilyInfoResponseMessage:  public common::BasePacket
    {
      public:
        GetFamilyInfoResponseMessage();
        virtual ~GetFamilyInfoResponseMessage();
        virtual int serialize(common::Stream& output)  const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline void set_family_id(const int64_t family_id) { family_id_ = family_id;}
        inline int64_t get_family_id() const { return family_id_;}
        inline void set_family_aid_info(const int32_t family_aid_info) { family_aid_info_ = family_aid_info;}
        inline int32_t get_family_aid_info() { return family_aid_info_;}
        inline std::pair<uint64_t,uint64_t>*  get_members() { return members_;}
      private:
        int64_t family_id_;
        int32_t family_aid_info_;
        std::pair<uint64_t, uint64_t> members_[common::MAX_MARSHALLING_NUM];
    };
  }/** end namespace message **/
}/** end namespace tfs **/
#endif
