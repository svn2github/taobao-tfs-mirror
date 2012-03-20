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

using namespace tfs::common;
namespace tfs
{
namespace nameserver
{
  GCObjectManager GCObjectManager::instance_;

  GCObjectManager::GCObjectManager():
    manager_(NULL),
    destroy_(false)
  {

  }

  GCObjectManager::~GCObjectManager()
  {

  }

  int GCObjectManager::add(GCObject* object)
  {
    bool bret = object == NULL ? false : destroy_ ? false : true;
    if (bret)
    {
      tbutil::Mutex::Lock lock(mutex_);
      TBSYS_LOG(INFO, "gc object list size: %zd", object_list_.size());
      std::pair<std::set<GCObject*>::iterator, bool> res = object_list_.insert(object);
      bret = res.second;
      if (!bret)
      {
        TBSYS_LOG(ERROR, "%p is exist", object);
      }
    }
    return bret ? TFS_SUCCESS : TFS_ERROR;
  }


  int GCObjectManager::add(const std::vector<GCObject*>& objects)
  {
    int32_t iret = TFS_SUCCESS;
    if (!objects.empty())
    {
      tbutil::Mutex::Lock lock(mutex_);
      TBSYS_LOG(INFO, "gc object list size: %zd", object_list_.size());
      std::pair<std::set<GCObject*>::iterator, bool> res;
      std::vector<GCObject*>::const_iterator iter = objects.begin();
      for (; iter != objects.end(); ++iter)
      {
        res = object_list_.insert((*iter));
        if (!res.second)
        {
          TBSYS_LOG(ERROR, "%p is exist", (*iter));
        }
      }
    }
    return iret;
  }

  void GCObjectManager::run()
  {
    int64_t now = Func::get_monotonic_time();
    GCObject* obj = NULL;
    int32_t total = 0;
    int32_t gc_count = 0;
    std::vector<GCObject*> tmp;
    {
      tbutil::Mutex::Lock lock(mutex_);
      total = object_list_.size();
      std::set<GCObject*>::iterator iter = object_list_.begin();
      while (iter != object_list_.end() && !destroy_)
      {
        if ((*iter)->can_be_clear(now))
        {
          tmp.push_back((*iter));
          ++iter;
        }
        else if ((*iter)->is_dead(now))
        {
          ++gc_count;
          obj = (*iter);
          obj->free();
          object_list_.erase(iter++);
        }
        else
        {
          ++iter;
        }
      }
    }

    TBSYS_LOG(INFO, "GC object total: %d, gc: %d", total, gc_count);

    std::vector<GCObject*>::iterator iter = tmp.begin();
    for (; iter != tmp.begin(); ++iter)
    {
      (*iter)->callback(manager_);
    }
  }

  int GCObjectManager::initialize()
  {
    ExpireTimerTaskPtr task = new ExpireTimerTask(*this);
    int iret = GFactory::get_timer()->scheduleRepeated(task, tbutil::Time::seconds(SYSPARAM_NAMESERVER.object_clear_max_time_));
    return iret < 0 ? TFS_ERROR : TFS_SUCCESS;
  }

  int GCObjectManager::wait_for_shut_down()
  {
    tbutil::Mutex::Lock lock(mutex_);
    std::set<GCObject*>::iterator iter = object_list_.begin();
    for (; iter != object_list_.end(); ++iter)
    {
      (*iter)->free();
    }
    object_list_.clear();
    return TFS_SUCCESS;
  }

  void GCObjectManager::destroy()
  {
    destroy_ = true;
  }

  void GCObjectManager::ExpireTimerTask::runTimerTask()
  {
    manager_.run();
  }
}
}
