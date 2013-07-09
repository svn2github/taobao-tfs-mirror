/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: meta_server_service.h 49 2011-08-08 09:58:57Z nayan@taobao.com $
 *
 * Authors:
 *   daoan <daoan@taobao.com>
 *      - initial release
 *
 */


#include "life_cycle_helper.h"

#include <malloc.h>
#include "common/tairengine_helper.h"
using namespace std;
namespace tfs
{
  using namespace common;
  namespace kvmetaserver
  {
    LifeCycleHelper::LifeCycleHelper()
    {
      kv_engine_helper_ = NULL;
      meta_info_name_area_ = 0;
    }

    LifeCycleHelper::~LifeCycleHelper()
    {
      kv_engine_helper_ = NULL;
    }

    int LifeCycleHelper::init(common::KvEngineHelper* kv_engine_helper)
    {
      int ret = TFS_SUCCESS;
      if (NULL == kv_engine_helper)
      {
        ret = TFS_ERROR;
      }
      else
      {
        kv_engine_helper_ = kv_engine_helper;
      }
      //TODO change later
      meta_info_name_area_ = 911;
      return ret;
    }

    int LifeCycleHelper::set_file_lifecycle(const int file_type, const string& file_name,
        const int32_t invalid_time_s, const string& app_key)
    {
      UNUSED(file_type);
      UNUSED(file_name);
      UNUSED(invalid_time_s);
      UNUSED(app_key);
      return 0;
    }
    int LifeCycleHelper::new_lifecycle(const int file_type, const string& file_name,
        const int32_t invalid_time_s, const string& app_key)
    {
      UNUSED(file_type);
      UNUSED(file_name);
      UNUSED(invalid_time_s);
      UNUSED(app_key);
      return 0;

    }
    int LifeCycleHelper::rm_life_cycle(const int file_type, const string& file_name)
    {
      UNUSED(file_type);
      UNUSED(file_name);
      return 0;
    }
  }// end for kvmetaserver
}// end for tfs

