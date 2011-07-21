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
#include "database_pool.h"
#include "mysql_database_helper.h"
#include "common/internal.h"
namespace tfs
{
  using namespace common;
  namespace namemetaserver
  {
    DataBasePool::DataBasePool(const int32_t pool_size)
    {
      my_init();
      pool_size_ = pool_size;
      if (pool_size_ > MAX_POOL_SIZE)
      {
        pool_size_ = MAX_POOL_SIZE;
      } else if (pool_size_ < 1)
      {
        pool_size_ = 1;
      }
    }
    DataBasePool::~DataBasePool()
    {
      for (int i = 0; i < MAX_POOL_SIZE; i++)
      {
        if (NULL != base_info_[i].database_helper_)
        {
          if (base_info_[i].busy_flag_)
          {
            TBSYS_LOG(ERROR, "release not match with get");
          }
          delete base_info_[i].database_helper_;
          base_info_[i].database_helper_ = NULL;
        }
      }
      mysql_thread_end();
    }
    bool DataBasePool::init_pool(const char** conn_str, const char** user_name,
        const char** passwd, const int32_t* hash_flag)
    {
      bool ret = true;
      for (int i = 0; i < pool_size_; i++)
      {
        if (NULL != base_info_[i].database_helper_)
        {
          delete base_info_[i].database_helper_;
        }
        base_info_[i].database_helper_ = new MysqlDatabaseHelper();
        if (TFS_SUCCESS != base_info_[i].database_helper_->set_conn_param(conn_str[i], user_name[i], passwd[i]))
        {
          ret = false;
          break;
        }
        base_info_[i].busy_flag_ = false;
        base_info_[i].hash_flag_ = hash_flag[i];
      }
      if (!ret)
      {
        for (int i = 0; i < pool_size_; i++)
        {
          base_info_[i].busy_flag_ = true;
        }
      }
      return ret;
    }
    DatabaseHelper* DataBasePool::get(const int32_t hash_flag)
    {
      DatabaseHelper* ret = NULL;
      tbutil::Mutex::Lock lock(mutex_);
      for (int i = 0; i < pool_size_; i++)
      {
        if (!base_info_[i].busy_flag_ && base_info_[i].hash_flag_ == hash_flag)
        {
          ret = base_info_[i].database_helper_;
          base_info_[i].busy_flag_ = true;
          break;
        }
      }
      return ret;
    }
    void DataBasePool::release(DatabaseHelper* database_helper)
    {
      tbutil::Mutex::Lock lock(mutex_);
      for (int i = 0; i < pool_size_; i++)
      {
        if (base_info_[i].database_helper_ == database_helper)
        {
          if (base_info_[i].busy_flag_)
          {
            base_info_[i].busy_flag_ = false;
          }
          else
          {
            TBSYS_LOG(ERROR, "some error in your code");
          }
          break;
        }
      }
    }

  }
}
