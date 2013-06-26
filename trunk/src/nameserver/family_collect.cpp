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
#include "layout_manager.h"
#include "family_collect.h"

using namespace tfs::common;
namespace tfs
{
  namespace nameserver
  {
    FamilyCollect::FamilyCollect(const int64_t family_id):
      GCObject(0),
      family_id_(family_id),
      family_aid_info_(0),
      members_(NULL)
    {
      //for query
    }

    FamilyCollect::FamilyCollect(const int64_t family_id, const int32_t family_aid_info, const time_t now):
      GCObject(now),
      family_id_(family_id),
      family_aid_info_(family_aid_info),
      members_(NULL)
    {
      const int32_t MEMBER_NUM = get_data_member_num() + get_check_member_num();
      members_ = new (std::nothrow)std::pair<uint64_t, int32_t>[MEMBER_NUM];
      memset(members_, 0, (sizeof(std::pair<uint64_t, int32_t>) * MEMBER_NUM));
      assert(members_);
    }

    FamilyCollect::~FamilyCollect()
    {
      tbsys::gDeleteA(members_);
    }

    int FamilyCollect::add(const uint64_t block, const int32_t version)
    {
      bool complete = false;
      int32_t ret = INVALID_BLOCK_ID != block && version > 0 ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        const int32_t MEMBER_NUM = get_data_member_num() + get_check_member_num();
        for (int32_t i = 0; i < MEMBER_NUM && !complete; ++i)
        {
          complete = INVALID_BLOCK_ID == members_[i].first;
          if (complete)
          {
            members_[i].first = block;
            members_[i].second= version;
          }
        }
      }
      return TFS_SUCCESS == ret ? complete ? TFS_SUCCESS: EXIT_OUT_OF_RANGE : ret;
    }

    int FamilyCollect::add(const common::ArrayHelper<std::pair<uint64_t, int32_t> >& members)
    {
      const int32_t MEMBER_NUM = get_data_member_num() + get_check_member_num();
      int32_t ret = (members.get_array_index() > MEMBER_NUM || members.get_array_index() <= 0)
             ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        for (int64_t index = 0; index < members.get_array_index(); ++index)
        {
          members_[index] = *members.at(index);
        }
      }
      return ret;
    }

    int FamilyCollect::update(const uint64_t block, const int32_t version)
    {
      bool complete = false;
      int32_t ret = INVALID_BLOCK_ID != block && version > 0 ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        const int32_t MEMBER_NUM = get_data_member_num() + get_check_member_num();
        for (int32_t i = 0; i < MEMBER_NUM && !complete; ++i)
        {
          complete = block == members_[i].first;
          if (complete)
            members_[i].second = version;
        }
      }
      return TFS_SUCCESS == ret ? complete ? TFS_SUCCESS : EXIT_NO_BLOCK : ret;
    }

    bool FamilyCollect::exist(const uint64_t block) const
    {
      bool ret = INVALID_BLOCK_ID != block;
      if (ret)
      {
        const int32_t MEMBER_NUM = get_data_member_num() + get_check_member_num();
        for (int32_t i = 0; i < MEMBER_NUM && !ret; ++i)
        {
          ret = members_[i].first == block;
        }
      }
      return ret;
    }

    bool FamilyCollect::exist(int32_t& current_version, const uint64_t block, const int32_t version) const
    {
      bool ret = INVALID_BLOCK_ID != block;
      if (ret)
      {
        const int32_t MEMBER_NUM = get_data_member_num() + get_check_member_num();
        for (int32_t i = 0; i < MEMBER_NUM && !ret; ++i)
        {
          ret = members_[i].first == block && version >= members_[i].second;
          if (ret)
            current_version = members_[i].second;
        }
      }
      return ret;
    }

    bool FamilyCollect::clear(LayoutManager& manager, const time_t now)
    {
      UNUSED(manager);
      UNUSED(now);
      return true;
    }

    void FamilyCollect::get_members(ArrayHelper<std::pair<uint64_t, int32_t> >& members) const
    {
      const int32_t MEMBER_NUM = get_data_member_num() + get_check_member_num();
      for (int32_t i = 0; i < MEMBER_NUM; ++i)
      {
        members.push_back((members_[i]));
      }
    }

    int FamilyCollect::scan(SSMScanParameter& param, LayoutManager& manager) const
    {
      param.data_.writeInt64(family_id_);
      param.data_.writeInt32(family_aid_info_);
      const int32_t MEMBER_NUM = get_data_member_num() + get_check_member_num();
      for (int32_t i = 0; i < MEMBER_NUM ; ++i)
      {
        if (INVALID_BLOCK_ID != members_[i].first)
        {
          param.data_.writeInt64(members_[i].first);//block_id
          param.data_.writeInt32(members_[i].second);//version
          uint64_t server = manager.get_block_manager().get_server(members_[i].first);
          param.data_.writeInt64(server);
        }
      }
      return TFS_SUCCESS;
    }

    void FamilyCollect::dump(int32_t level, const char* file, const int32_t line, const char* function, const pthread_t thid) const
    {
      if (level >= TBSYS_LOGGER._level)
      {
        std::ostringstream str;
        str <<"family_id: " << family_id_ <<",data_member_num: " << get_data_member_num() << ",check_member_num: "<<
          get_check_member_num() << ",code_type:" << get_code_type() << ",master_index: " << get_master_index();
        int32_t i = 0;
        str << ", data_members: ";
        for (i = 0; i < get_data_member_num(); ++i)
        {
          str <<members_[i].first <<":" << members_[i].second<< ",";
        }
        str << ", check_members: ";
        const int32_t MEMBER_NUM = get_data_member_num() + get_check_member_num();
        for (; i < MEMBER_NUM; ++i)
        {
          str <<members_[i].first <<":" << members_[i].second<< ",";
        }
        TBSYS_LOGGER.logMessage(level, file, line, function, thid,"%s", str.str().c_str());
      }
    }

    #ifdef TFS_GTEST
    int FamilyCollect::get_version(int32_t& version, const uint64_t block) const
    {
      version = -1;
      bool complete = false;
      int32_t ret = INVALID_BLOCK_ID != block ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        const int32_t MEMBER_NUM = get_data_member_num() + get_check_member_num();
        for (int32_t i = 0; i < MEMBER_NUM && !complete; ++i)
        {
          complete = (members_[i].first == block);
          if (complete)
            version = members_[i].second;
        }
      }
      return TFS_SUCCESS == ret ? complete ? TFS_SUCCESS : EXIT_NO_BLOCK : ret;
    }
    #endif

    bool FamilyCollect::check_need_reinstate(const time_t now) const
    {
      return (last_update_time_ + SYSPARAM_NAMESERVER.reinstate_task_expired_time_) <= now;
    }

    bool FamilyCollect::check_need_dissolve(const time_t now) const
    {
      return (last_update_time_ + SYSPARAM_NAMESERVER.dissolve_task_expired_time_) <= now;
    }

    bool FamilyCollect::check_need_compact(const time_t now) const
    {
      return (last_update_time_ + SYSPARAM_NAMESERVER.compact_task_expired_time_) <= now;
    }
  }/** end namespace nameserver **/
}/** end namespace tfs **/
