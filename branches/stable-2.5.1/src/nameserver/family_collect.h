/*
 * (C) 2007-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */

#ifndef TFS_NAMESERVER_FAMILY_COLLECT_H_
#define TFS_NAMESERVER_FAMILY_COLLECT_H_

#include <stdint.h>
#include <time.h>
#include <vector>
#include "ns_define.h"
#include "common/internal.h"
#include "common/array_helper.h"
#include "common/base_object.h"

#ifdef TFS_GTEST
#include <gtest/gtest.h>
#endif

namespace tfs
{
  namespace nameserver
  {
    class LayoutManager;
    class FamilyCollect : public common::BaseObject<LayoutManager>
    {
      #ifdef TFS_GTEST
      friend class FamilyCollectTest;
      FRIEND_TEST(FamilyCollectTest, add);
      FRIEND_TEST(FamilyCollectTest, update);
      FRIEND_TEST(FamilyCollectTest, exist);
      FRIEND_TEST(FamilyCollectTest, get_members);
      int get_version(int32_t& version, const uint64_t block) const;
      #endif
    public:
      explicit FamilyCollect(const int64_t family_id);
      FamilyCollect(const int64_t family_id, const int32_t family_aid_info,const time_t now);
      virtual ~FamilyCollect();
      int add(const uint64_t block, const int32_t version);
      int add(const common::ArrayHelper<std::pair<uint64_t, int32_t> >& member);
      int update(const uint64_t block, const int32_t version);
      bool exist(const uint64_t block) const;
      bool exist(int32_t& current_version, const uint64_t block, const int32_t version) const;
      void get_members(common::ArrayHelper<std::pair<uint64_t, int32_t> >& members) const;
      int scan(common::SSMScanParameter& param) const;
      void dump(int32_t level, const char* file = __FILE__,
          const int32_t line = __LINE__, const char* function = __FUNCTION__, const pthread_t thid = pthread_self()) const;
      inline void set_family_id(const int64_t family_id){ family_id_ = family_id;}
      inline void set_data_member_num(const int16_t data_member_num){SET_DATA_MEMBER_NUM(family_aid_info_, data_member_num);}
      inline void set_check_member_num(const int16_t check_member_num){ SET_CHECK_MEMBER_NUM(family_aid_info_, check_member_num);}
      inline void set_code_type(const int8_t code_type) { SET_MARSHALLING_TYPE(family_aid_info_, code_type);}
      inline void set_master_index(const int8_t index) { SET_MASTER_INDEX(family_aid_info_, index);}
      inline int64_t get_family_id() const { return family_id_;}
      inline int get_data_member_num() const { return GET_DATA_MEMBER_NUM(family_aid_info_);}
      inline int get_check_member_num() const { return GET_CHECK_MEMBER_NUM(family_aid_info_);}
      inline int get_code_type() const { return GET_MARSHALLING_TYPE(family_aid_info_);}
      inline int get_master_index() const { return GET_MASTER_INDEX(family_aid_info_);}
      inline int32_t get_family_aid_info() const { return family_aid_info_;}
      inline bool in_reinstate_or_dissolve_queue() const { return FAMILY_IN_REINSTATE_OR_DISSOLVE_QUEUE_YES == in_reinstate_or_dissolve_queue_;}
      inline void set_in_reinstate_or_dissolve_queue(const int8_t falg = FAMILY_IN_REINSTATE_OR_DISSOLVE_QUEUE_YES) { in_reinstate_or_dissolve_queue_ = falg;}
      bool check_need_reinstate(const time_t now) const;
      bool check_need_dissolve(const time_t now) const;
      bool check_need_compact(const time_t now) const;

    private:
      DISALLOW_COPY_AND_ASSIGN(FamilyCollect);
      int64_t family_id_;//family id
      int32_t family_aid_info_;//family 辅助信息(高8位: 数据成员个数，中8位: 校验块成员个数: 中8位: master所在下标,低8位: 编码方法,eg. rs, rs2)
      int8_t  in_reinstate_or_dissolve_queue_;
      int8_t  reserve[3];
      std::pair<uint64_t, int32_t>* members_;//当前family的成员列表 block,version
    };
  }/** end namespace nameserver **/
}/** end namespace tfs **/

#endif /* FAMILY_COLLECT_H_*/
