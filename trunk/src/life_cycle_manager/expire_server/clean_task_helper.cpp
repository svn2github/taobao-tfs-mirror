/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id:
 *
 * Authors:
 *   qixiao <qixiao.zs@alibaba-inc.com>
 *      - initial release
 *
 */


#include "clean_task_helper.h"
#include <malloc.h>
#include "common/func.h"
#include "common/tairengine_helper.h"
#include "common/atomic.h"

using namespace std;
namespace tfs
{
  using namespace common;
  namespace expireserver
  {
    const int32_t KEY_BUFF_SIZE = 512 + 8 + 8;
    const int32_t VALUE_BUFF_SIZE = 512;
    const int32_t SCAN_LIMIT = 1000;
    const int32_t SEC_ONE_DAY = 24 * 60 * 60;
    const int32_t MAX_INT32 = (1<<31) - 1;
    const char del = '/';

    enum
    {
      CMD_RANGE_ALL = 1,
      CMD_RANGE_VALUE_ONLY,
      CMD_RANGE_KEY_ONLY,
    };

    CleanTaskHelper::CleanTaskHelper()
    {
      kv_engine_helper_ = NULL;
      tair_lifecycle_area_ = 0;
      running_task_count_ = 0;
    }

    CleanTaskHelper::~CleanTaskHelper()
    {
      kv_engine_helper_ = NULL;
    }

    int CleanTaskHelper::init(common::KvEngineHelper* kv_engine_helper)
    {
      int ret = TFS_SUCCESS;

      if (NULL == kv_engine_helper)
      {
        ret = TFS_ERROR;
      }
      else
      {
        kv_engine_helper_ = kv_engine_helper;
      }

      if (TFS_SUCCESS == ret)
      {
        ret = restful_client_.initialize(SYSPARAM_EXPIRESERVER.nginx_root_.c_str(),
                                                   SYSPARAM_EXPIRESERVER.es_appkey_.c_str());
        TBSYS_LOG(INFO, "ret: %d, restful_client initialize SUCCESS", ret);
      }

      tair_lifecycle_area_ = SYSPARAM_EXPIRESERVER.lifecycle_area_;

      running_task_count_ = 0;

      if (TFS_SUCCESS ==  ret)
      {
        ret = restful_client_.initialize(SYSPARAM_EXPIRESERVER.nginx_root_.c_str(),
                                         SYSPARAM_EXPIRESERVER.es_appkey_.c_str(),
                                         SYSPARAM_EXPIRESERVER.log_level_.c_str());
        if (TFS_SUCCESS ==  ret)
        {
          TBSYS_LOG(INFO, "restful_client initialize SUCCESS");
        }
      }

      return ret;
    }

    int32_t CleanTaskHelper::get_running_threads_count()
    {
      return running_task_count_;
    }


    //record stat to kv store
    int CleanTaskHelper::stat_to_kvstore(const uint64_t local_ipport, const int32_t num_es,
                                   const int32_t task_time, const int32_t hash_bucket_num,
                                   const int64_t sum_file_num)
    {
      //op key
      int ret = TFS_SUCCESS;
      char *key_buff = (char*)malloc(KEY_BUFF_SIZE);
      char *value_buff = (char*)malloc(VALUE_BUFF_SIZE);

      if(NULL == key_buff || NULL == value_buff)
      {
        ret = TFS_ERROR;
      }
      KvKey key;

      if (TFS_SUCCESS == ret)
      {
        ret = ExpireDefine::serialize_es_stat_key(local_ipport, num_es,
                                task_time, hash_bucket_num,
                                sum_file_num, &key,
                                key_buff, KEY_BUFF_SIZE);
      }

      TBSYS_LOG(DEBUG, "serialize_es_stat_key %d", ret);
      //op value
      int64_t pos = 0;
      int64_t lock_version = 0;
      KvMemValue kv_value;
      value_buff[0] = 'v';
      pos = 1;
      if (TFS_SUCCESS == ret)
      {
        kv_value.set_data(value_buff, pos);
        ret = kv_engine_helper_->put_key(tair_lifecycle_area_, key, kv_value, lock_version);
      }
      TBSYS_LOG(DEBUG, "put es_stat_key %d", ret);
      if (NULL != value_buff)
      {
        free(value_buff);
        value_buff = NULL;
      }

      if (NULL != key_buff)
      {
        free(key_buff);
        key_buff = NULL;
      }

      return ret;
    }


    int CleanTaskHelper::do_clean_task(const uint64_t local_ipport, const int32_t total_es, const int32_t num_es,
                                    const int32_t note_interval, const int32_t task_time)
    {
      UNUSED(local_ipport);
      int32_t ret;
      int32_t days_secs;
      int32_t hours_secs;
      int32_t relative_days_secs;
      int32_t relative_hours_secs;
      int32_t zero_secs;
      int32_t hash_mod;
      int32_t start_bucket_num;
      int32_t end_bucket_num;
      std::string file_name;

      struct timeval start;
      struct timeval end;

      struct timeval scan_start;
      struct timeval scan_end;
      int32_t g_scan_time = 0;

      gettimeofday(&start, NULL);

      int32_t expire_file_count = 0;

      atomic_inc(&running_task_count_);

      ret = ExpireDefine::transfer_time(task_time, &days_secs, &hours_secs);

      if (TFS_SUCCESS == ret)
      {
        /* 0 -- 10242 */
        start_bucket_num = num_es * ExpireDefine::HASH_BUCKET_NUM / total_es;
        end_bucket_num = (num_es + 1) * ExpireDefine::HASH_BUCKET_NUM / total_es - 1;
        TBSYS_LOG(DEBUG, "start_bucket_num is %d and end_bucket_num is %d", start_bucket_num, end_bucket_num);
      }

      if (ret == TFS_SUCCESS)
      {
          //op key
          char *start_key_buff = NULL;
          if (TFS_SUCCESS == ret)
          {
            start_key_buff = (char*) malloc(KEY_BUFF_SIZE);
          }
          if (NULL == start_key_buff)
          {
            ret = TFS_ERROR;
          }
          char *end_key_buff = NULL;
          if (ret == TFS_SUCCESS)
          {
            end_key_buff = (char*) malloc(KEY_BUFF_SIZE);
          }
          if (NULL == end_key_buff)
          {
            ret = TFS_ERROR;
          }
          KvKey start_key;
          KvKey end_key;
          int64_t sum_file_num = 0;
          int32_t day_num;
          int64_t first, second;
          first = Func::get_monotonic_time();
          zero_secs = 0;

          for (hash_mod = start_bucket_num; hash_mod <= end_bucket_num; ++hash_mod)
          {
            for (day_num = SYSPARAM_EXPIRESERVER.re_clean_days_; day_num >= 0; --day_num)
            {
              TBSYS_LOG(DEBUG, "now day is %d hashnum is %d", day_num, hash_mod);
              relative_days_secs = days_secs - day_num * SEC_ONE_DAY;
              if (day_num != 0)
              {
                relative_hours_secs = SEC_ONE_DAY;
              }
              else
              {
                relative_hours_secs = hours_secs;
              }

              if (TFS_SUCCESS == ret)
              {
                ret = ExpireDefine::serialize_exptime_app_key(relative_days_secs,
                          zero_secs, hash_mod, 0, file_name,
                          &start_key, start_key_buff, KEY_BUFF_SIZE);
                TBSYS_LOG(DEBUG, "serialize_exptime_app_key %d", ret);
              }
              if (TFS_SUCCESS == ret)
              {
                ret = ExpireDefine::serialize_exptime_app_key(relative_days_secs,
                          relative_hours_secs, hash_mod, MAX_INT32, file_name,
                          &end_key, end_key_buff, KEY_BUFF_SIZE);
                TBSYS_LOG(DEBUG, "serialize_exptime_app_key %d", ret);
              }
              if (TFS_SUCCESS == ret)
              {
                int32_t i;
                int32_t first = 0;
                bool go_on = true;
                short scan_type = CMD_RANGE_ALL;//scan all
                int32_t t_days_secs;
                int32_t t_hours_secs;
                int32_t t_hash_num;
                int32_t t_file_type;
                std::string t_file_name;
                std::string t_appkey;
                vector<KvValue*> kv_value_keys;
                vector<KvValue*> kv_value_values;

                while (go_on)
                {
                  int32_t result_size = 0;
                  gettimeofday(&scan_start, NULL);
                  ret = kv_engine_helper_->scan_keys(tair_lifecycle_area_, start_key, end_key, SCAN_LIMIT, first,
                                                     &kv_value_keys, &kv_value_values,
                                                     &result_size, scan_type);

                  gettimeofday(&scan_end, NULL);

                  g_scan_time += 1000000 * (scan_end.tv_sec - scan_start.tv_sec) + scan_end.tv_usec - scan_start.tv_usec;

                  if (EXIT_KV_RETURN_DATA_NOT_EXIST == ret)
                  {//no data
                    TBSYS_LOG(DEBUG, "data not exist, result_size: %d", result_size);
                    ret = TFS_SUCCESS;
                  }

                  expire_file_count += result_size;

                  for (i = 0; i < result_size; ++i)
                  {
                    ret = ExpireDefine::deserialize_exptime_app_key(kv_value_keys[i]->get_data(),
                                                 kv_value_keys[i]->get_size(),
                                                 &t_days_secs, &t_hours_secs, &t_hash_num,
                                                 &t_file_type, &t_file_name);

                    t_appkey.assign(kv_value_values[i]->get_data(),kv_value_values[i]->get_size());

                    TBSYS_LOG(DEBUG, "this appkey is %s", t_appkey.c_str());
                    sum_file_num++;
                    second = Func::get_monotonic_time();

                    if ((int32_t)(second - first) >= note_interval)
                    {
                       first = second;
                       //stat_to_kvstore(local_ipport, num_es, task_time, hash_mod, sum_file_num);
                       TBSYS_LOG(INFO, "task num:%d this tasktime:%d start_bucket_num:%d end_bucket_num:%d now hash_num:%d now deal file: %s", num_es, task_time, start_bucket_num, end_bucket_num, hash_mod, t_file_name.c_str());
                    }

                    uint32_t file_len = t_file_name.length();
                    if (file_len > 0 && (t_file_name[0] == 'T' || t_file_name[0] == 'L'))
                    {
                      ret = restful_client_.unlink(t_file_name.c_str(), NULL, tfs::restful::DELETE, t_appkey.c_str());
                    }
                    else if (file_len > 0)
                    {
                      //file_name like appid/uid/file/aa/bb
                      string::size_type pos = 0;
                      string::size_type uid_pos = string::npos;
                      string::size_type metaname_pos = string::npos;

                      int i = 0;
                      TBSYS_LOG(INFO, "file_name: %s", t_file_name.c_str());
                      for (i = 0; i < 3; ++i)
                      {
                        pos = t_file_name.find_first_of(del, pos);
                        if (pos != string::npos)
                        {
                          if (i == 0)
                          {
                            uid_pos = pos;
                          }
                          else if (i == 2)
                          {
                            metaname_pos = pos;
                            break;
                          }
                          pos++;
                        }
                        else
                        {
                          TBSYS_LOG(ERROR, "file_name: %s is illegal", t_file_name.c_str());
                          break;
                        }
                      }

                      if (uid_pos != string::npos && metaname_pos != string::npos)
                      {
                        int64_t appid = atoll(t_file_name.c_str());
                        int64_t uid = atoll(t_file_name.c_str() + uid_pos + 1);
                        //string meta_name = t_file_name.substr(metaname_pos, file_len - metaname_pos);
                        TBSYS_LOG(INFO, "appid: %"PRI64_PREFIX"d, uid: %"PRI64_PREFIX"d, meta_name: %s",
                                  appid, uid, t_file_name.c_str() + metaname_pos);
                        if (appid < 0 || uid < 0)
                        {
                            TBSYS_LOG(ERROR, "error appid: %"PRI64_PREFIX"d, uid: %"PRI64_PREFIX"d, meta_name: %s",
                                      appid, uid, t_file_name.c_str() + metaname_pos);
                            continue;
                        }
                        ret = restful_client_.rm_file(uid, t_file_name.c_str() + metaname_pos, t_appkey.c_str(), appid);
                      }
                    }//else if() custom
                  }//for

                  TBSYS_LOG(DEBUG, "this time result_size is: %d", result_size);

                  if (result_size == SCAN_LIMIT)
                  {
                    first = 1;
                    ret = ExpireDefine::serialize_exptime_app_key(days_secs, hours_secs, hash_mod, t_file_type,
                        t_file_name, &start_key, start_key_buff, KEY_BUFF_SIZE);
                  }
                  else
                  {
                    go_on = false;
                  }

                  for(i = 0; i < result_size; ++i)//free kv
                  {
                    kv_value_keys[i]->free();
                    kv_value_values[i]->free();
                  }
                  kv_value_keys.clear();
                  kv_value_values.clear();
                }//end while
              }//end success
            }//end for day_num
          }//end for hash mod

          if (NULL != start_key_buff)
          {
            free(start_key_buff);
            start_key_buff = NULL;
          }
          if (NULL != end_key_buff)
          {
            free(end_key_buff);
            end_key_buff = NULL;
          }
      }

      gettimeofday(&end, NULL);
      int32_t timeuse = 1000000 * (end.tv_sec - start.tv_sec) + end.tv_usec - start.tv_usec;

      TBSYS_LOG(INFO, "time: %d task use %d us, scan use %d us, unlink %d file", task_time, timeuse,
          g_scan_time, expire_file_count);
      atomic_dec(&running_task_count_);
      return ret;
    }//end for clean task


  }// end for expireserver
}// end for tfs

