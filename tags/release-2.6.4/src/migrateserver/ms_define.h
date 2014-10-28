/*
 * (C) 2007-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: ms_define.h 983 2013-08-29 09:59:33Z duanfei $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */
#ifndef MIGRATESERVER_DEFINE_H_
#define MIGRATESERVER_DEFINE_H_

#include <Mutex.h>
#include <tbsys.h>
#include "common/internal.h"
#include "common/func.h"
#include "common/lock.h"
#include "common/parameter.h"
#include "common/new_client.h"
#include "common/array_helper.h"

namespace tfs
{
  namespace migrateserver
  {
    struct MsRuntimeGlobalInformation
    {
      bool is_destroyed() const { return is_destroy_;}
      void destroy() { is_destroy_ = true;}
      void dump(const int32_t level, const char* file, const int32_t line,
            const char* function, const pthread_t thid, const char* format, ...);
      bool is_destroy_;
      MsRuntimeGlobalInformation();
      static MsRuntimeGlobalInformation& instance();
    };
    extern int ms_async_callback(common::NewClient* client);
 }/** migrateserver **/
}/** tfs **/

#endif
