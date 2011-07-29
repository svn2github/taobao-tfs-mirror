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
*   chuyu <chuyu@taobao.com>
*      - initial release
*
*/
#include "meta_store_manager.h"

using namespace tfs::common;
namespace tfs
{
  namespace namemetaserver
  {
    MetaStoreManager::MetaStoreManager()
    {
      database_helper_ = new MysqlDatabaseHelper();
      database_helper_->set_conn_param(ConnStr::mysql_conn_str_.c_str(), ConnStr::mysql_user_.c_str(), ConnStr::mysql_password_.c_str());
      //meta_cache_handler_ = new MetaCacheHandler();

      database_helper_->connect();
    }

    MetaStoreManager::~MetaStoreManager()
    {
      tbsys::gDelete(database_helper_);
      //tbsys::gDelete(meta_cache_handler_);
    }


    int MetaStoreManager::select(const int64_t app_id, const int64_t uid, const int64_t pid, const char* name, const int32_t name_len, std::vector<MetaInfo>& out_v_meta_info)
    {
      int ret = TFS_ERROR;
      out_v_meta_info.clear();
      std::vector<MetaInfo> tmp_meta_info;
      int64_t comp_pid = 0;
      ret = database_helper_->ls_meta_info(tmp_meta_info, app_id, uid, pid, name, name_len);
      if (TFS_SUCCESS == ret)
      {
        std::vector<MetaInfo>::const_iterator it = tmp_meta_info.begin();
        for( ;it != tmp_meta_info.end(); it++)
        {
          comp_pid = it->pid_ & ~(1L<<63);
          if (comp_pid == pid && 0 == memcmp(it->name_.data(), name, name_len))
          {
            out_v_meta_info.push_back(*it);
          }
          else
          {
            break;
          }
        }

      }
      return ret;
    }

    int MetaStoreManager::insert(const int64_t app_id, const int64_t uid,
        const int64_t ppid, const char* pname, const int32_t pname_len,
        const int64_t pid, const char* name, const int32_t name_len, const FileType type, MetaInfo* meta_info)
    {
      int ret = TFS_ERROR;
      int status = TFS_ERROR;
      int64_t proc_ret = 0;

      if (type == NORMAL_FILE)
      {
        status = database_helper_->create_file(app_id, uid, ppid, pid, pname, pname_len, name, name_len, proc_ret);
        if (TFS_SUCCESS != status)
        {
          TBSYS_LOG(DEBUG, "database helper create file, status: %d", status);
        }
      }
      else if (type == DIRECTORY)
      {
        int64_t id = 0;
        status = database_helper_->get_nex_val(id);
        if (TFS_SUCCESS == status && id != 0)
        {
          status = database_helper_->create_dir(app_id, uid, ppid, pname, pname_len, pid, id, name, name_len, proc_ret);
          if (TFS_SUCCESS != status)
          {
            TBSYS_LOG(DEBUG, "database helper create dir, status: %d", status);
          }
        }
      }
      else if (type == PWRITE_FILE)
      {
        if (NULL == meta_info)
        {
          TBSYS_LOG(ERROR, "meta_info should not be NULL");
        }
        else
        {
          int64_t frag_len = meta_info->frag_info_.get_length();
          if (frag_len > 65535)
          {
            TBSYS_LOG(ERROR, "meta info is too long(%d > %d)", frag_len, 65535);
            ret = TFS_ERROR;
          }
          else
          {
            int64_t pos = 0;
            char* frag_info = (char*) malloc(frag_len);
            if (NULL == frag_info)
            {
              TBSYS_LOG(ERROR, "mem not enough");
              ret = TFS_ERROR;
            }
            else
            {
              status = meta_info->frag_info_.serialize(frag_info, frag_len, pos);
              if (TFS_SUCCESS != status)
              {
                TBSYS_LOG(ERROR, "get meta info failed, status: %d ", status);
              }
              else
              {
                status = database_helper_->pwrite_file(app_id, uid, pid, name, name_len, meta_info->size_, meta_info->ver_no_, frag_info, frag_len, proc_ret);
                if (TFS_SUCCESS != status)
                {
                  TBSYS_LOG(DEBUG, "database helper pwrite file, status: %d", status);
                }
              }
              free (frag_info);
            }
          }
        }
      }

      if ((static_cast<int32_t>(proc_ret)) > 0)
      {
        ret = TFS_SUCCESS;
      }
      return ret;
    }

    int MetaStoreManager::update(const int64_t app_id, const int64_t uid,
        const int64_t s_ppid, const int64_t s_pid, const char* s_pname, const int32_t s_pname_len,
        const int64_t d_ppid, const int64_t d_pid, const char* d_pname, const int32_t d_pname_len,
        const char* s_name, const int32_t s_name_len,
        const char* d_name, const int32_t d_name_len,
        const FileType type)
    {
      int ret = TFS_ERROR;
      int status = TFS_ERROR;
      int64_t proc_ret = 0;

      if (type & NORMAL_FILE)
      {
        status = database_helper_->mv_file(app_id, uid, s_ppid, s_pid, s_pname, s_pname_len,
        d_ppid, d_pid, d_pname, d_pname_len, s_name, s_name_len, d_name, d_name_len, proc_ret);
        if (TFS_SUCCESS != status)
        {
          TBSYS_LOG(DEBUG, "database helper mv file, status: %d", status);
        }
      }
      else if (type & DIRECTORY)
      {
        status = database_helper_->mv_dir(app_id, uid, s_ppid, s_pid, s_pname, s_pname_len,
        d_ppid, d_pid, d_pname, d_pname_len, s_name, s_name_len, d_name, d_name_len, proc_ret);
        if (TFS_SUCCESS != status)
        {
          TBSYS_LOG(DEBUG, "database helper mv dir, status: %d", status);
        }
      }

      if ((static_cast<int32_t>(proc_ret)) > 0)
      {
        ret = TFS_SUCCESS;
      }
      return ret;
    }

    int MetaStoreManager::remove(const int64_t app_id, const int64_t uid, const int64_t ppid,
            const char* pname, const int64_t pname_len, const int64_t pid, const int64_t id,
            const char* name, const int64_t name_len,
            const FileType type)
    {
      int ret = TFS_ERROR;
      int status = TFS_ERROR;
      int64_t proc_ret = 0;

      if (type & NORMAL_FILE)
      {
        status = database_helper_->rm_file(app_id, uid, ppid, pid, pname, pname_len, name, name_len, proc_ret);
        if (TFS_SUCCESS != status)
        {
          TBSYS_LOG(DEBUG, "database helper rm file, status: %d", status);
        }
      }
      else if (type & DIRECTORY)
      {
        if (id == 0)
        {
          TBSYS_LOG(DEBUG, "wrong type, target is file.");
        }
        else
        {
          status = database_helper_->rm_dir(app_id, uid, ppid, pname, pname_len, pid, id, name, name_len, proc_ret);
          if (TFS_SUCCESS != status)
          {
            TBSYS_LOG(DEBUG, "database helper rm dir, status: %d", status);
          }
        }
      }

      if ((static_cast<int32_t>(proc_ret)) > 0)
      {
        ret = TFS_SUCCESS;
      }
      return ret;
    }
  }
}
