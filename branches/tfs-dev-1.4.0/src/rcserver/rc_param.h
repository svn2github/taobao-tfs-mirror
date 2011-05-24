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
#ifndef TFS_RCSERVER_RCPARAM_H_
#define TFS_RCSERVER_RCPARAM_H_

namespace tfs
{
  namespace rcserver 
  {
    class RcParam
    {
    public:
      struct RcServer
      {
        std::string work_dir_;
        std::string log_file_;
        std::string pid_file_;
        std::string db_info_;
        std::string db_user_;
        std::string db_pwd_;
      };
    
    public:
      const RcServer& rcserver() const
      {
        return rcserver_;
      }
      static RcParam& instance()
      {
        return instance_;
      }

      int load(const std::string& tfsfile);

    private:
      static const int32_t PARAM_BUF_LEN = 64;

    private:
      RcServer rcserver_;
      static RcParam instance_;

      RcParam();
    };

#define PARAM_RCSERVER RcParam::instance().rcserver()
  }
}
#endif //TFS_RCSERVER_RCPARAM_H_
