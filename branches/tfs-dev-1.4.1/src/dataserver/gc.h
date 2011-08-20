/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_DATASERVER_GC_H
#define TFS_DATASERVER_GC_H

#include <Timer.h>
#include <Mutex.h>
#include "dataserver_define.h"

#define TFS_DS_GTEST

namespace tfs
{
namespace dataserver
{
  class GCObjectManager
  {
  public:
    GCObjectManager();
    virtual ~GCObjectManager();
    int add(GCObject* object);
    void run();
    int initialize(tbutil::TimerPtr timer);
    int wait_for_shut_down();
    void destroy();
    static GCObjectManager& instance()
    {
      return instance_;
    }
  #if defined(TFS_DS_GTEST) || defined(TFS_DS_INTEGRATION)
  public:
  #else
  private:
  #endif
    DISALLOW_COPY_AND_ASSIGN(GCObjectManager);
    std::list<GCObject*> object_list_;
    tbutil::Mutex mutex_;
    static GCObjectManager instance_;
    bool destroy_;

  #if defined(TFS_DS_GTEST) || defined(TFS_DS_INTEGRATION)
  public:
  #else
  private:
  #endif
    class ExpireTimerTask: public tbutil::TimerTask
    {
    public:
      explicit ExpireTimerTask(GCObjectManager& manager)
        :manager_(manager)
      {

      }

      virtual ~ExpireTimerTask() {}
      virtual void runTimerTask();
    private:
      GCObjectManager& manager_;
    };
    typedef tbutil::Handle<ExpireTimerTask> ExpireTimerTaskPtr;
  };
}
}

#endif
