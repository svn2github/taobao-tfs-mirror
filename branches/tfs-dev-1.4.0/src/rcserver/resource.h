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
#ifndef TFS_RCSERVER_RESOURCE_H_
#define TFS_RCSERVER_RESOURCE_H_

#include "common/define.h"
#include "mysql_conn.h"

namespace tfs
{
  namespace rcserver
  {
    class MysqlConn;
    class IResource
    {
      public:
        IResource(MysqlConn* mysql_conn) : mysql_conn_(mysql_conn)
        {
        }
        virtual ~IResource()
        {
        }

      public:
        virtual int load() = 0;

      private:
        IResource();
        DISALLOW_COPY_AND_ASSIGN(IResource);

      protected:
        MysqlConn* mysql_conn_;
    };
  }
}
#endif //TFS_RCSERVER_RESOURCE_H_
