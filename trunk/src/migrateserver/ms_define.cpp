/*
 * (C) 2007-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: ss_define.h 344 2013-08-29 11:17:38Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 */
#include "common/define.h"
#include "common/atomic.h"
#include "ms_define.h"
#include "common/error_msg.h"
#include "common/parameter.h"

using namespace tfs::common;

namespace tfs
{
  namespace migrateserver
  {
    MsRuntimeGlobalInformation::MsRuntimeGlobalInformation():
      is_destroy_(false)
    {

    }
    void MsRuntimeGlobalInformation::dump(const int32_t level, const char* file, const int32_t line,
            const char* function, const pthread_t thid, const char* format, ...)
    {
        char msgstr[256] = {'\0'};/** include '\0'*/
        va_list ap;
        va_start(ap, format);
        vsnprintf(msgstr, 256, NULL == format ? "" : format, ap);
        va_end(ap);
        TBSYS_LOGGER.logMessage(level, file, line, function, thid, "%s", msgstr);
    }

    MsRuntimeGlobalInformation& MsRuntimeGlobalInformation::instance()
    {
      static MsRuntimeGlobalInformation instance;
      return instance;
    }
  }/** migrateserver **/
}/** tfs **/
