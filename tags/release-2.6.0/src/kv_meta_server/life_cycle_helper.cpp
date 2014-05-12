/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: meta_server_service.h 49 2011-08-08 09:58:57Z nayan@taobao.com $
 *
 * Authors:
 *   daoan <daoan@taobao.com>
 *      - initial release
 *
 */


#include "life_cycle_helper.h"

#include <malloc.h>
#include "common/tairengine_helper.h"
#include "common/expire_define.h"
using namespace std;
namespace tfs
{
  using namespace common;
  namespace kvmetaserver
  {
    LifeCycleHelper::LifeCycleHelper()
    {
      kv_engine_helper_ = NULL;
      kv_lifecycle_area_ = 0;
    }

    LifeCycleHelper::~LifeCycleHelper()
    {
      kv_engine_helper_ = NULL;
    }

    int LifeCycleHelper::init(common::KvEngineHelper* kv_engine_helper)
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
      //TODO change later
      kv_lifecycle_area_ = SYSPARAM_KVMETA.lifecycle_area_;
      return ret;
    }

    int LifeCycleHelper::set_file_lifecycle(const int32_t file_type, const string& file_name,
        const int32_t invalid_time_s, const string& app_key)
    {
      int ret = TFS_SUCCESS;
      char *name_exptime_key_buff = NULL;
      char *exptime_appkey_key_buff = NULL;
      KvKey name_exptime_key;
      KvKey exptime_appkey_key;
      KvMemValue name_exptime_value;
      KvMemValue exptime_appkey_value;
      char exptime_buff[4];
      uint32_t hash_mod = 0;

      name_exptime_key_buff= (char*) malloc(ExpireDefine::EXPTIME_KEY_BUFF);
      if (NULL == name_exptime_key_buff)
      {
        ret = TFS_ERROR;
      }

      if (TFS_SUCCESS == ret)
      {
        ret = ExpireDefine::serialize_name_expiretime_key(
            file_type, file_name, &name_exptime_key,
            name_exptime_key_buff, ExpireDefine::EXPTIME_KEY_BUFF);
      }

      if (TFS_SUCCESS == ret)
      {
        exptime_appkey_key_buff = (char*) malloc(ExpireDefine::EXPTIME_KEY_BUFF);
        if (NULL == exptime_appkey_key_buff)
        {
          ret = TFS_ERROR;
        }
      }
      if (TFS_SUCCESS == ret)
      {
        int days_sec = 0;
        int hours_sec = 0;
        ExpireDefine::transfer_time(invalid_time_s, &days_sec, &hours_sec);

        hash_mod = tbsys::CStringUtil::murMurHash(file_name.c_str(), file_name.length());
        hash_mod = hash_mod % ExpireDefine::HASH_BUCKET_NUM;
        TBSYS_LOG(DEBUG, "hash_mod is : %u", hash_mod);
        ret = ExpireDefine::serialize_exptime_app_key(days_sec, hours_sec,
            static_cast<int32_t>(hash_mod), file_type, file_name, &exptime_appkey_key,
            exptime_appkey_key_buff, ExpireDefine::EXPTIME_KEY_BUFF);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = ExpireDefine::serialize_name_expiretime_value(
            invalid_time_s, &name_exptime_value, exptime_buff, 4);
      }

      if (TFS_SUCCESS == ret)
      {
        exptime_appkey_value.set_data((char*)app_key.c_str(), app_key.length());
      }

      if (TFS_SUCCESS == ret)
      {
        ret = new_lifecycle(name_exptime_key, name_exptime_value,
            exptime_appkey_key, exptime_appkey_value);
      }

      if (EXIT_LIFE_CYCLE_INFO_EXIST == ret)
      {
        KvValue *kv_value = NULL;
        ret = kv_engine_helper_->get_key(kv_lifecycle_area_, name_exptime_key, &kv_value, 0);
        if (TFS_SUCCESS == ret)
        {
          int32_t old_invlaid_time = 0;
          ret = ExpireDefine::deserialize_name_expiretime_value(
              kv_value->get_data(), kv_value->get_size(), &old_invlaid_time);
          if (TFS_SUCCESS == ret)
          {
            //delet kv(key = invalidtime + filename)
            KvKey exptime_appkey_key_old;
            int days_sec = 0;
            int hours_sec = 0;
            char* name_exptime_key_old_buff= (char*) malloc(ExpireDefine::EXPTIME_KEY_BUFF);
            if (NULL == name_exptime_key_buff)
            {
              ret = TFS_ERROR;
            }
            if (TFS_SUCCESS == ret)
            {
              ExpireDefine::transfer_time(old_invlaid_time, &days_sec, &hours_sec);

              ret = ExpireDefine::serialize_exptime_app_key(days_sec, hours_sec,
                  static_cast<int32_t>(hash_mod), file_type, file_name, &exptime_appkey_key_old,
                  name_exptime_key_old_buff, ExpireDefine::EXPTIME_KEY_BUFF);
            }
            if (TFS_SUCCESS == ret)
            {
              ret = kv_engine_helper_->delete_key(kv_lifecycle_area_, exptime_appkey_key_old);
              TBSYS_LOG(DEBUG, "del exptime_appkey_key_old, ret %d", ret);
              /* no care ret */
              if (EXIT_KV_RETURN_DATA_NOT_EXIST == ret)
              {
                ret = TFS_SUCCESS;
              }
            }
            if (NULL != name_exptime_key_old_buff)
            {
              free(name_exptime_key_old_buff);
            }

          }
          if (TFS_SUCCESS == ret)
          {
            //delete name:invalidtime
            ret = kv_engine_helper_->delete_key(kv_lifecycle_area_, name_exptime_key);
            TBSYS_LOG(DEBUG, "del name_exptime_key ,ret %d", ret);
          }
        }
        if (NULL != kv_value)
        {
          kv_value->free();
        }

        if (TFS_SUCCESS == ret)
        {
          ret = new_lifecycle(name_exptime_key, name_exptime_value,
              exptime_appkey_key, exptime_appkey_value);
        }
      }//end EXIT_LIFE_CYCLE_INFO_EXIST
      if (NULL != exptime_appkey_key_buff)
      {
        free(exptime_appkey_key_buff);
        exptime_appkey_key_buff = NULL;
      }
      if (NULL != name_exptime_key_buff)
      {
        free(name_exptime_key_buff);
        name_exptime_key_buff = NULL;
      }
      return ret;
    }

    int LifeCycleHelper::get_file_lifecycle(const int32_t file_type, const std::string& file_name,
            int32_t* invalid_time_s)
    {
      int ret =  (NULL != invalid_time_s) ? TFS_SUCCESS : TFS_ERROR;
      char *name_exptime_key_buff = NULL;
      KvKey name_exptime_key;
      name_exptime_key_buff= (char*) malloc(ExpireDefine::EXPTIME_KEY_BUFF);
      KvValue *kv_value = NULL;

      if (NULL == name_exptime_key_buff)
      {
        ret = TFS_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        ret = ExpireDefine::serialize_name_expiretime_key(
            file_type, file_name, &name_exptime_key,
            name_exptime_key_buff, ExpireDefine::EXPTIME_KEY_BUFF);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = kv_engine_helper_->get_key(kv_lifecycle_area_, name_exptime_key, &kv_value, 0);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = ExpireDefine::deserialize_name_expiretime_value(
              kv_value->get_data(), kv_value->get_size(), invalid_time_s);
      }
      if (NULL != kv_value)
      {
        free(kv_value);
      }
      if (NULL != name_exptime_key_buff)
      {
        free(name_exptime_key_buff);
        name_exptime_key_buff = NULL;
      }
      return ret;

    }
    int LifeCycleHelper::new_lifecycle(
        const common::KvKey& name_exptime_key, const common::KvMemValue& name_exptime_value,
            const common::KvKey& exptime_appkey_key, const common::KvMemValue& exptime_appkey_value)
    {
      int ret = kv_engine_helper_->put_key(kv_lifecycle_area_, name_exptime_key,
          name_exptime_value, KvDefine::MAX_VERSION);
      if (TFS_SUCCESS == ret)
      {
        ret = kv_engine_helper_->put_key(kv_lifecycle_area_, exptime_appkey_key,
            exptime_appkey_value, 0/*version*/);
      }
      else if (EXIT_KV_RETURN_VERSION_ERROR == ret)
      {
        ret = EXIT_LIFE_CYCLE_INFO_EXIST;
        TBSYS_LOG(DEBUG, "EXIT_LIFE_CYCLE_INFO_EXIST %d",ret);
      }
      return ret;
    }
    int LifeCycleHelper::rm_life_cycle(const int file_type, const string& file_name)
    {
      /*
      ret = get_kv(key = filename);
      if (TFS_SUCCESS == ret)
      {
        ret = delete kv(key = invalid_time + filename);
        if (TFS_SUCCESS == ret) //success or not exist
        {
          ret =  delete kv(key = filename);
          if(TFS_SUCCESS == ret) //success or not exist
          {
            put_invalid_info()
          }
        }
      }
      else if (EXIT_KV_RETURN_DATA_NOT_EXIST == ret)
      {
        ret = TFS_SUCCESS;
      }
      */
      int ret = TFS_SUCCESS;

      char *name_exptime_key_buff = NULL;
      char *exptime_appkey_key_buff = NULL;
      KvKey name_exptime_key;
      KvKey exptime_appkey_key;
      KvMemValue name_exptime_value;
      KvMemValue  exptime_appkey_value;
      uint32_t hash_mod = 0;

      name_exptime_key_buff= (char*) malloc(ExpireDefine::EXPTIME_KEY_BUFF);
      if (NULL == name_exptime_key_buff)
      {
        ret = TFS_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        exptime_appkey_key_buff = (char*) malloc(ExpireDefine::EXPTIME_KEY_BUFF);
        if (NULL == exptime_appkey_key_buff)
        {
          ret = TFS_ERROR;
        }
      }

      if (TFS_SUCCESS == ret)
      {
        ret = ExpireDefine::serialize_name_expiretime_key(
            file_type, file_name, &name_exptime_key,
            name_exptime_key_buff, ExpireDefine::EXPTIME_KEY_BUFF);
      }
      // we got name_expirekey, so we can get invalid time
      KvValue *kv_value = NULL;
      if (TFS_SUCCESS == ret)
      {
        ret = kv_engine_helper_->get_key(kv_lifecycle_area_, name_exptime_key, &kv_value, 0);
      }
      if (TFS_SUCCESS == ret)
      {
        int32_t old_invlaid_time = 0;
        int32_t days_sec = 0;
        int32_t hours_sec = 0;
        ret = ExpireDefine::deserialize_name_expiretime_value(
            kv_value->get_data(), kv_value->get_size(), &old_invlaid_time);
        //we got invalid time, we can make next kvkey
        if (TFS_SUCCESS == ret)
        {
          ExpireDefine::transfer_time(old_invlaid_time, &days_sec, &hours_sec);
          hash_mod = tbsys::CStringUtil::murMurHash(file_name.c_str(), file_name.length());
          hash_mod = hash_mod % ExpireDefine::HASH_BUCKET_NUM;
          ret = ExpireDefine::serialize_exptime_app_key(days_sec, hours_sec,
              static_cast<int32_t>(hash_mod), file_type, file_name, &exptime_appkey_key,
              exptime_appkey_key_buff, ExpireDefine::EXPTIME_KEY_BUFF);
        }
        //will delete expire_time appkey firtst,
        if (TFS_SUCCESS == ret)
        {
          ret = kv_engine_helper_->delete_key(kv_lifecycle_area_, exptime_appkey_key);
          TBSYS_LOG(DEBUG, "del exptime_appkey_key ,ret %d", ret);
          if (EXIT_KV_RETURN_DATA_NOT_EXIST == ret)
          {
            ret = TFS_SUCCESS;
          }
        }
        if (TFS_SUCCESS == ret)
        {
          ret = kv_engine_helper_->delete_key(kv_lifecycle_area_, name_exptime_key);
          TBSYS_LOG(DEBUG, "del name_exptime_key ,ret %d", ret);
        }
        //TODO put_invalid_info

      }
      else if (EXIT_KV_RETURN_DATA_NOT_EXIST == ret)
      {
        ret = TFS_SUCCESS;
      }
      if (NULL != kv_value)
      {
        kv_value->free();
        kv_value = NULL;
      }

      if (NULL != exptime_appkey_key_buff)
      {
        free(exptime_appkey_key_buff);
        exptime_appkey_key_buff = NULL;
      }
      if (NULL != name_exptime_key_buff)
      {
        free(name_exptime_key_buff);
        name_exptime_key_buff = NULL;
      }
      return ret;
    }
  }// end for kvmetaserver
}// end for tfs

