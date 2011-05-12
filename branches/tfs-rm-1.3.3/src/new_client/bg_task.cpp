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
 *      - initial release
 *
 */

#include "bg_task.h"
#include "client_config.h"

using namespace tfs::client;
using namespace tfs::common;
using namespace std;

tbutil::TimerPtr BgTask::timer_ = 0;
StatManager<string, string, StatEntry> BgTask::stat_mgr_;
GcManager BgTask::gc_mgr_;

int BgTask::initialize(int64_t gc_schedule_interval)
{
  int ret = TFS_SUCCESS;
  if (timer_ == 0)
  {
    timer_ = new tbutil::Timer();
  }
  ret = stat_mgr_.initialize(timer_);
  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "initialize stat manager fail. ret: %d", ret);
  }
  else
  {
    int64_t current = tbsys::CTimeUtil::getTime();
    StatEntry<string, string>::StatEntryPtr access_ptr = 
      new StatEntry<string, string>(StatItem::client_access_stat_, current, true);
    access_ptr->add_sub_key(StatItem::read_success_);
    access_ptr->add_sub_key(StatItem::read_fail_);
    access_ptr->add_sub_key(StatItem::stat_success_);
    access_ptr->add_sub_key(StatItem::stat_fail_);
    access_ptr->add_sub_key(StatItem::write_success_);
    access_ptr->add_sub_key(StatItem::write_fail_);
    access_ptr->add_sub_key(StatItem::unlink_success_);
    access_ptr->add_sub_key(StatItem::unlink_fail_);

    StatEntry<string, string>::StatEntryPtr cache_ptr = 
      new StatEntry<string, string>(StatItem::client_cache_stat_, current, true);
    cache_ptr->add_sub_key(StatItem::cache_hit_);
    cache_ptr->add_sub_key(StatItem::cache_miss_);
    cache_ptr->add_sub_key(StatItem::remove_count_);

    int64_t stat_interval = 60 * 1000 * 1000; //60s
    stat_mgr_.add_entry(access_ptr, stat_interval); 
    stat_mgr_.add_entry(cache_ptr, stat_interval); 
  }

  if (TFS_SUCCESS == ret)
  {
    gc_mgr_.initialize(timer_, gc_schedule_interval);
  }
  return ret;
}

int BgTask::wait_for_shut_down()
{
  stat_mgr_.wait_for_shut_down();
  gc_mgr_.wait_for_shut_down();
  if (timer_ != 0)
  {
    timer_ = 0;
  }
  return TFS_SUCCESS;
}

int BgTask::destroy()
{
  stat_mgr_.destroy();
  gc_mgr_.destroy();
  if (timer_ != 0)
  {
    timer_->destroy();
  }

  return TFS_SUCCESS;
}
