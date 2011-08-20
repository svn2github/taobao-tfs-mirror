/*
* (C) 2007-2010 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
*
* Version: $Id
*
* Authors:
*   daoan <daoan@taobao.com>
*      - initial release
*
*/
#ifndef TFS_NAMEMETASERVER_MYSQL_STORE_HANDLER_H_
#define TFS_NAMEMETASERVER_MYSQL_STORE_HANDLER_H_
#include <mysql/mysql.h>
#include <tbsys.h>
#include <Mutex.h>
#include "meta_store_handler.h"
namespace tfs
{
  namespace namemetaserver
  {
    class MysqlStoreHandler :public MetaStoreHandler 
    {
      public:
        MysqlStoreHandler();
        virtual ~MysqlStoreHandler();
        virtual int connect();
        virtual int close();

        virtual int ls_meta_info(std::vector<MetaInfo>& out_v_meta_info,
            const int64_t app_id, const int64_t uid,
            const int64_t pid = 0, const char* name = NULL, const int32_t name_len = 0);

        virtual int create_dir(const int64_t app_id, const int64_t uid,
            const int64_t ppid, const char* pname, const int32_t pname_len,
            const int64_t pid, const int64_t id, const char* name, const int32_t name_len,
            int64_t& mysql_proc_ret);

        virtual int rm_dir(const int64_t app_id, const int64_t uid, const int64_t ppid,
            const char* pname, const int32_t pname_len, const int64_t pid, const int64_t id,
            const char* name, const int32_t name_len, int64_t& mysql_proc_ret);

        virtual int mv_dir(const int64_t app_id, const int64_t uid, 
            const int64_t s_ppid, const int64_t s_pid, const char* s_pname, const int32_t s_pname_len,
            const int64_t d_ppid, const int64_t d_pid, const char* d_pname, const int32_t d_pname_len,
            const char* s_name, const int32_t s_name_len,
            const char* d_name, const int32_t d_name_len, int64_t& mysql_proc_ret);

        virtual int create_file(const int64_t app_id, const int64_t uid, 
            const int64_t ppid, const int64_t pid, const char* pname, const int32_t pname_len,
            const char* name, const int32_t name_len, int64_t& mysql_proc_ret);

        virtual int rm_file(const int64_t app_id, const int64_t uid, 
            const int64_t ppid, const int64_t pid, const char* pname, const int32_t pname_len,
            const char* name, const int32_t name_len, int64_t& mysql_proc_ret);

        virtual int pwrite_file(const int64_t app_id, const int64_t uid, 
            const int64_t pid, const char* name, const int32_t name_len,
            const int64_t size, const int16_t ver_no, const char* meta_info, const int32_t meta_len,
            int64_t& mysql_proc_ret);

        virtual int mv_file(const int64_t app_id, const int64_t uid, 
            const int64_t s_ppid, const int64_t s_pid, const char* s_pname, const int32_t s_pname_len,
            const int64_t d_ppid, const int64_t d_pid, const char* d_pname, const int32_t d_pname_len,
            const char* s_name, const int32_t s_name_len,
            const char* d_name, const int32_t d_name_len, int64_t& mysql_proc_ret);

        virtual int get_nex_val(int64_t& next_val);

      private:
        enum 
        {
          ROW_LIMIT = 500,
          META_NAME_LEN = 512,
          SLIDE_INFO_LEN = 65535,
        };
        MYSQL_STMT *stmt_;
        MYSQL_BIND ps_params_[4];  /* input parameter buffers */
        int64_t app_id_;
        int64_t uid_;
        int64_t pid_;
        char pname_[META_NAME_LEN];
        unsigned long pname_len_;

      private:
        tbutil::Mutex mutex_;

    };

  }
}
#endif
