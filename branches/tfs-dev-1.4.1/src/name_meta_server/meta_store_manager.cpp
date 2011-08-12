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
*   chuyu <chuyu@taobao.com>
*      - initial release
*
*/
#include "meta_store_manager.h"
#include "database_helper.h"
#include "common/parameter.h"

using namespace tfs::common;
namespace tfs
{
  namespace namemetaserver
  {
    MetaStoreManager::MetaStoreManager()
    {
      database_pool_ = new DataBasePool();
    }
    int MetaStoreManager::init(const int32_t pool_size)
    {
      char* conn_str[DataBasePool::MAX_POOL_SIZE];
      char* user_name[DataBasePool::MAX_POOL_SIZE];
      char* passwd[DataBasePool::MAX_POOL_SIZE];
      int32_t hash_flag[DataBasePool::MAX_POOL_SIZE];

      int32_t my_pool_size = pool_size;
      int ret = TFS_SUCCESS;

      if (pool_size > DataBasePool::MAX_POOL_SIZE)
      {
        TBSYS_LOG(INFO, "pool size is too lage set it to %d",
            DataBasePool::MAX_POOL_SIZE);
        my_pool_size = DataBasePool::MAX_POOL_SIZE;
      }
      for (int i = 0; i < pool_size; i++)
      {
        int data_base_index = i % SYSPARAM_NAMEMETASERVER.db_infos_.size();
        const NameMeatServerParameter::DbInfo& dbinfo = SYSPARAM_NAMEMETASERVER.db_infos_[data_base_index];

        conn_str[i] = (char*)malloc(100);
        snprintf(conn_str[i], 100, "%s", dbinfo.conn_str_.c_str());
        user_name[i] = (char*)malloc(100);
        snprintf(user_name[i], 100, "%s", dbinfo.user_.c_str());
        passwd[i] = (char*)malloc(100);
        snprintf(passwd[i], 100, "%s", dbinfo.passwd_.c_str());
        hash_flag[i] = dbinfo.hash_value_;
      }
      bool pool_ret = database_pool_->init_pool(pool_size,
          conn_str, user_name, passwd, hash_flag);

      if(!pool_ret)
      {
        TBSYS_LOG(ERROR, "database pool init error");
        ret = TFS_ERROR;
      }
      for (int i = 0; i < pool_size; i++)
      {
        free(conn_str[i]);
        free(user_name[i]);
        free(passwd[i]);
      }
      return ret;
    }

    MetaStoreManager::~MetaStoreManager()
    {
      tbsys::gDelete(database_pool_);
    }


    int MetaStoreManager::select(const int64_t app_id, const int64_t uid, const int64_t pid, const char* name, const int32_t name_len, const bool is_file, std::vector<MetaInfo>& out_v_meta_info)
    {
      int ret = TFS_ERROR;
      out_v_meta_info.clear();
      bool still_have = false;
      std::vector<MetaInfo> tmp_meta_info;
      ret = ls(app_id, uid, pid, name, name_len, is_file, tmp_meta_info, still_have);
      if (TFS_SUCCESS == ret)
      {
        std::vector<MetaInfo>::const_iterator it = tmp_meta_info.begin();
        for( ;it != tmp_meta_info.end(); it++)
        {
          if (0 == memcmp(it->file_info_.name_.data(), name, name_len))
          {
            out_v_meta_info.push_back(*it);
          }
        }

      }
      return ret;
    }

    int MetaStoreManager::ls(const int64_t app_id, const int64_t uid, const int64_t pid, const char* name, const int32_t name_len, const bool is_file, std::vector<MetaInfo>& out_v_meta_info, bool& still_have)
    {
      int ret = TFS_ERROR;
      still_have = false;
      out_v_meta_info.clear();
      int64_t real_pid = 0;
      if (is_file)
      {
        real_pid = pid | (1L<<63);
      }
      else
      {
        real_pid = pid & ~(1L<<63);
      }

      DatabaseHelper* database_helper = NULL;
      database_helper = database_pool_->get(database_pool_->get_hash_flag(app_id, uid));
      if (NULL != database_helper)
      {
        ret = database_helper->ls_meta_info(out_v_meta_info, app_id, uid, real_pid, name, name_len);
        database_pool_->release(database_helper);
      }

      if (static_cast<int32_t>(out_v_meta_info.size()) >= ROW_LIMIT)
      {
        still_have = true;
      }
      return ret;
    }

    int MetaStoreManager::insert(const int64_t app_id, const int64_t uid,
        const int64_t ppid, const char* pname, const int32_t pname_len,
        const int64_t pid, const char* name, const int32_t name_len, const FileType type, MetaInfo* meta_info)
    {
      int ret = TFS_ERROR;
      int status = TFS_ERROR;
      int64_t proc_ret = EXIT_UNKNOWN_SQL_ERROR;
      int64_t id = 0;
      DatabaseHelper* database_helper = NULL;
      if (type == DIRECTORY)
      {
        database_helper = database_pool_->get(0);
        if (NULL != database_helper)
        {
          database_helper->get_nex_val(id);
          database_pool_->release(database_helper);
          database_helper = NULL;
        }
      }
      database_helper = database_pool_->get(database_pool_->get_hash_flag(app_id, uid));
      if (NULL != database_helper)
      {
        if (type == NORMAL_FILE)
        {
          status = database_helper->create_file(app_id, uid, ppid, pid, pname, pname_len, name, name_len, proc_ret);
          if (TFS_SUCCESS != status)
          {
            TBSYS_LOG(DEBUG, "database helper create file, status: %d", status);
          }
        }
        else if (type == DIRECTORY)
        {
          if (id != 0)
          {
            status = database_helper->create_dir(app_id, uid, ppid, pname, pname_len, pid, id, name, name_len, proc_ret);
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
            if (frag_len > MAX_FRAG_INFO_SIZE)
            {
              TBSYS_LOG(ERROR, "meta info is too long(%d > %d)", frag_len, MAX_FRAG_INFO_SIZE);
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
                  status = database_helper->pwrite_file(app_id, uid, pid, name, name_len,
                  meta_info->file_info_.size_, meta_info->file_info_.ver_no_, frag_info, frag_len, proc_ret);
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

        ret = get_return_status(status, proc_ret);
        database_pool_->release(database_helper);
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
      int64_t proc_ret = EXIT_UNKNOWN_SQL_ERROR;

      DatabaseHelper* database_helper = NULL;
      database_helper = database_pool_->get(database_pool_->get_hash_flag(app_id, uid));
      if (NULL != database_helper)
      {
        if (type & NORMAL_FILE)
        {
          status = database_helper->mv_file(app_id, uid, s_ppid, s_pid, s_pname, s_pname_len,
              d_ppid, d_pid, d_pname, d_pname_len, s_name, s_name_len, d_name, d_name_len, proc_ret);
          if (TFS_SUCCESS != status)
          {
            TBSYS_LOG(DEBUG, "database helper mv file, status: %d", status);
          }
        }
        else if (type & DIRECTORY)
        {
          status = database_helper->mv_dir(app_id, uid, s_ppid, s_pid, s_pname, s_pname_len,
              d_ppid, d_pid, d_pname, d_pname_len, s_name, s_name_len, d_name, d_name_len, proc_ret);
          if (TFS_SUCCESS != status)
          {
            TBSYS_LOG(DEBUG, "database helper mv dir, status: %d", status);
          }
        }

        ret = get_return_status(status, proc_ret);
        database_pool_->release(database_helper);
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
      int64_t proc_ret = EXIT_UNKNOWN_SQL_ERROR;

      DatabaseHelper* database_helper = NULL;
      database_helper = database_pool_->get(database_pool_->get_hash_flag(app_id, uid));
      if (NULL != database_helper)
      {
        if (type & NORMAL_FILE)
        {
          status = database_helper->rm_file(app_id, uid, ppid, pid, pname, pname_len, name, name_len, proc_ret);
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
            status = database_helper->rm_dir(app_id, uid, ppid, pname, pname_len, pid, id, name, name_len, proc_ret);
            if (TFS_SUCCESS != status)
            {
              TBSYS_LOG(DEBUG, "database helper rm dir, status: %d", status);
            }
          }
        }

        ret = get_return_status(status, proc_ret);
        database_pool_->release(database_helper);
      }

      return ret;
    }

    // proc_ret depend database's logic
    // database level and manager level should be be consensus about error code definition,
    // otherwise, manager level SHOULD merge difference
    int MetaStoreManager::get_return_status(const int status, const int proc_ret)
    {
      int ret = TFS_SUCCESS;
      // maybe network fail
      if (status != TFS_SUCCESS)
      {
        // // TODO. maybe do sth.
        // ret = status;
      }
      if (proc_ret < 0)
      {
        ret = proc_ret;
      }

      return ret;
    }
  }
}
