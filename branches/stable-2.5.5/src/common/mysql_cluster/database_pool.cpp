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
*   daoan <daoan@taobao.com>
*      - initial release
*
*/
#include "database_pool.h"

#include "common/internal.h"
#include "common/parameter.h"
#include "mysql_database_helper.h"
namespace tfs
{
  namespace common
  {
    DataBasePool::DataBasePool():pool_size_(0),choose_index_(0)
    {
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
      //mysql_thread_end();
    }
    bool DataBasePool::init_pool(const int32_t pool_size,
        const char* conn_str,  const char* user_name,
        const char* passwd)
    {
      bool ret = true;
      for (int i = 0; i < pool_size_; i++)
      {
        if (NULL != base_info_[i].database_helper_)
        {
          delete base_info_[i].database_helper_;
        }
      }
      choose_index_ = 0;
      pool_size_ = pool_size;
      if (pool_size_ > MAX_POOL_SIZE)
      {
        pool_size_ = MAX_POOL_SIZE;
      } else if (pool_size_ < 1)
      {
        pool_size_ = 1;
      }
      TBSYS_LOG(DEBUG, "connn_str %s user_name %s passwd %s ",
            conn_str, user_name, passwd);
      for (int i = 0; i < pool_size_; i++)
      {
        base_info_[i].database_helper_ = new MysqlDatabaseHelper();
        if (TFS_SUCCESS != base_info_[i].database_helper_->set_conn_param(conn_str,
              user_name, passwd))
        {
          ret = false;
          break;
        }
        base_info_[i].busy_flag_ = false;
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

    bool DataBasePool::destroy_pool(void)
    {
      mysql_thread_end();
      return true;
    }
    MysqlDatabaseHelper* DataBasePool::get()
    {
      MysqlDatabaseHelper* ret = NULL;
      if (0 == pool_size_)
      {
        TBSYS_LOG(ERROR, "pool size is 0");
      }
      else
      {
        while(NULL == ret)
        {
          {
            tbutil::Mutex::Lock lock(mutex_);
            for (int i = 0; i < pool_size_; i++)
            {
              choose_index_++;
              if (choose_index_ >= pool_size_)
              {
                choose_index_ = 0;
              }
              if (!base_info_[choose_index_].busy_flag_)
              {
                ret = base_info_[choose_index_].database_helper_;
                base_info_[choose_index_].busy_flag_ = true;
                break;
              }
            }
          }
          if (NULL == ret)
          {
            TBSYS_LOG(WARN, "Data Base Pool is not enought");
            usleep(500);
          }
        }
      }
      return ret;
    }
    void DataBasePool::release(MysqlDatabaseHelper* database_helper)
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
