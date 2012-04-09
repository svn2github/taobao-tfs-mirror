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
#include "tbsys.h"
#include "gc.h"
#include "common/error_msg.h"
#include "common/config_item.h"
#include "global_factory.h"
#include "layout_manager.h"

using namespace tfs::common;
namespace tfs
{
  namespace nameserver
  {
    GCObjectManager::GCObjectManager(LayoutManager& manager):
      manager_(manager)
    {

    }

    GCObjectManager::~GCObjectManager()
    {
      std::set<GCObject*>::iterator iter = object_list_.begin();
      for (; iter != object_list_.end(); ++iter)
      {
        (*iter)->free();
      }
      object_list_.clear();
    }

    int GCObjectManager::add(GCObject* object)
    {
      if (NULL != object)
      {
        tbutil::Mutex::Lock lock(mutex_);
        TBSYS_LOG(DEBUG, "gc object list size: %zd", object_list_.size());
        std::pair<std::set<GCObject*>::iterator, bool> res = object_list_.insert(object);
        if (!res.second)
        {
          TBSYS_LOG(INFO, "%p is exist", object);
        }
      }
      return TFS_SUCCESS;
    }

    int GCObjectManager::gc(const time_t now)
    {
      GCObject* obj = NULL;
      const int32_t MAX_GC_COUNT = 1024;
      GCObject* objects[MAX_GC_COUNT];
      ArrayHelper<GCObject*> helper(MAX_GC_COUNT, objects);
      mutex_.lock();
      std::set<GCObject*>::iterator iter = object_list_.begin();
      while (iter != object_list_.end() && helper.get_array_index() < MAX_GC_COUNT)
      {
        obj = (*iter);
        assert(NULL != obj);
        if (obj->can_be_free(now))
        {
          helper.push_back(obj);
          object_list_.erase(iter++);
        }
        else
        {
          ++iter;
        }
      }
      mutex_.unlock();

      for (int32_t i = 0; i < helper.get_array_index(); ++i)
      {
        obj = *helper.at(i);
        assert(NULL != obj);
        obj->callback(manager_);
        obj->free();
      }
      return helper.get_array_index();
    }
  }/** end namespace nameserver **/
}/** end namespace tfs **/
