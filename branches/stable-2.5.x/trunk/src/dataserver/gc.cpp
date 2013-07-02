/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: gc.cpp 2014 2011-01-06 07:41:45Z duanfei $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */
#include <execinfo.h>
#include <tbsys.h>
#include "gc.h"
#include "common/array_helper.h"
#include "common/error_msg.h"
#include "common/config_item.h"

using namespace tfs::common;
namespace tfs
{
  namespace dataserver
  {
    GCObjectManager::GCObjectManager()
    {

    }

    GCObjectManager::~GCObjectManager()
    {
      std::set<GCObject*>::iterator iter = wait_free_list_.begin();
      for (; iter != wait_free_list_.end(); ++iter)
      {
        (*iter)->callback();
        (*iter)->free();
      }
      wait_free_list_.clear();
    }

    int GCObjectManager::add(GCObject* object, const time_t now)
    {
      if (NULL != object)
      {
        tbutil::Mutex::Lock lock(mutex_);
        std::pair<std::set<GCObject*>::iterator, bool> res = wait_free_list_.insert(object);
        if (!res.second)
          TBSYS_LOG(INFO, "%p %p is exist", object, (*res.first));
        else
          object->update_last_time(now);
      }
      return TFS_SUCCESS;
    }

    int GCObjectManager::gc(const time_t now)
    {
      const int32_t MAX_GC_COUNT = 1024;
      GCObject* arrays[MAX_GC_COUNT];
      common::ArrayHelper<GCObject*> helper(MAX_GC_COUNT, arrays);
      mutex_.lock();
      std::set<GCObject*>::iterator iter = wait_free_list_.begin();
      while (iter != wait_free_list_.end() && helper.get_array_index() < MAX_GC_COUNT)
      {
        if ((*iter)->can_be_free(now))
        {
          helper.push_back((*iter));
          wait_free_list_.erase(iter++);
        }
        else
        {
          ++iter;
        }
      }
      mutex_.unlock();
      for (int64_t index = 0; index < helper.get_array_index(); ++index)
      {
        GCObject* obj = *helper.at(index);
        assert(NULL != obj);
        obj->callback();
        obj->free();
      }
      return helper.get_array_index();
    }

    int64_t GCObjectManager::size() const
    {
      return wait_free_list_.size();
    }
  }/** end namespace nameserver **/
}/** end namespace tfs **/
