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
 *   zongdai <zongdai@taobao.com>
 *      - initial release
 *
 */
#include <tbsys.h>
#include "common/config_item.h"
#include "common/define.h"
#include "common/error_msg.h"
#include "rc_param.h"

namespace tfs
{
  namespace rcserver 
  {
    using namespace std;
    using namespace tbsys;
    using namespace common;
    RcParam RcParam::instance_;

    RcParam::RcParam()
    {
    }

    int RcParam::load(const string& tfsfile)
    {
      const char* top_work_dir = TBSYS_CONFIG.getString(CONF_SN_PUBLIC, CONF_WORK_DIR);
      if (top_work_dir == NULL)
      {
        TBSYS_LOG(ERROR, "RcParam::load work directory config not found");
        return EXIT_CONFIG_ERROR;
      }

      char default_work_dir[MAX_PATH_LENGTH], default_log_file[MAX_PATH_LENGTH], default_pid_file[MAX_PATH_LENGTH];
      snprintf(default_work_dir, MAX_PATH_LENGTH - 1, "%s/rcserver", top_work_dir);
      snprintf(default_log_file, MAX_PATH_LENGTH - 1, "%s/logs/rcserver.log", top_work_dir);
      snprintf(default_pid_file, MAX_PATH_LENGTH - 1, "%s/logs/rcserver.pid", top_work_dir);
      rcserver_.work_dir_ = TBSYS_CONFIG.getString(CONF_SN_RCSERVER, CONF_WORK_DIR, default_work_dir);
      rcserver_.log_file_ = TBSYS_CONFIG.getString(CONF_SN_RCSERVER, CONF_LOG_FILE, default_log_file);
      rcserver_.pid_file_ = TBSYS_CONFIG.getString(CONF_SN_RCSERVER, CONF_LOCK_FILE, default_pid_file);

      return TFS_SUCCESS;
    }
  }
}
