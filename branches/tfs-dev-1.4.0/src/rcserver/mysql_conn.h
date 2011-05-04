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
#ifndef TFS_RCSERVER_MYSQLCONN_H_
#define TFS_RCSERVER_MYSQLCONN_H_

#include <mysql/mysql.h>
#include <mysql/errmsg.h>
#include "common/define.h"
#include "resource.h"

namespace tfs
{
  namespace rcserver
  {
    static const int64_t MYSQL_IDENT_LEN = 32;
    struct MysqlIdent
    {
      char host[MYSQL_IDENT_LEN];
      char user[MYSQL_IDENT_LEN];
      char passwd[MYSQL_IDENT_LEN];
      MYSQL mysql;
    };

    class MysqlConn
    {
      public:
        MysqlConn() : is_init_(false)
        {
        }

        ~MysqlConn()
        {
          // destory();
        }

      public:
        int connect(const MysqlIdent& mysql_ident);
        // load info from store 
        int query(const std::string& query_sql);
        // modify session info or app stat info
        int modify(const std::string& modify_sql);

      private:
        DISALLOW_COPY_AND_ASSIGN(MysqlConn);

        MYSQL mysql_;
        bool is_init_;
    };
  }
}
#endif //TFS_RCSERVER_MYSQLCONN_H_
