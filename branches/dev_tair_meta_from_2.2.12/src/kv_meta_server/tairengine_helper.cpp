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

#include "tairengine_helper.h"

#include "Memory.hpp"
#include "common/parameter.h"
#include "common/error_msg.h"
#include "common/serialization.h"
using namespace std;
using namespace tair;
using namespace common;
namespace tfs
{

  using namespace common;
  namespace kvmetaserver
  {
   /*-------------------TairValue--------------*/
    TairValue::TairValue():tair_value_(NULL)
    {
    }
    TairValue::~TairValue()
    {
      if (NULL != tair_value_)
      {
        delete tair_value_;
        tair_value_ = NULL;
      }
    }
    tair::data_entry* TairValue::get_mutable_tair_value()
    {
      return tair_value_;
    }
    void TairValue::set_tair_value(tair::data_entry* tair_value)
    {
      if (NULL != tair_value_)
      {
        delete tair_value_;
      }
      tair_value_ = tair_value;
    }
    void TairValue::free()
    {
      delete this;
    }
    int32_t TairValue::get_size() const
    {
      int32_t size = 0;
      if (NULL != tair_value_)
      {
        size = tair_value_->get_size();
      }
      return size;
    }
    const char* TairValue::get_data() const
    {
      const char* data = NULL;
      if (NULL != tair_value_)
      {
        data = tair_value_->get_data();
      }
      return data;
    }
    /*---------------END------------------------*/

    const int TairEngineHelper::TAIR_RETRY_COUNT = 1;
    //const int TairEngineHelper::SCAN_KEYS_MAX_NUM = 500;
    //different key type will use different namespace in tair
    TairEngineHelper::TairEngineHelper()
      :tair_client_(NULL), object_area_(-1)
    {

    }

    TairEngineHelper::~TairEngineHelper()
    {
      tbsys::gDelete(tair_client_);
    }

    int TairEngineHelper::init()
    {
      int ret = TFS_SUCCESS;
      if (NULL != tair_client_)
      {
        TBSYS_LOG(ERROR, "tair_client_ have been inited");
        ret = TFS_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        tair_client_ = new tair::tair_client_api();
        assert(NULL != tair_client_);
        string master_addr = SYSPARAM_KVMETA.tair_master_;
        string slave_addr = SYSPARAM_KVMETA.tair_slave_;
        string group_name = SYSPARAM_KVMETA.tair_group_;
        object_area_ = SYSPARAM_KVMETA.tair_object_area_;

        if (!tair_client_->startup(master_addr.c_str(), slave_addr.c_str(), group_name.c_str()))
        {
          TBSYS_LOG(ERROR, "starup tair client fail. "
              "master addr: %s, slave addr: %s, group name:  %s, area: %d",
              master_addr.c_str(), slave_addr.c_str(), group_name.c_str(), object_area_);
          ret = TFS_ERROR;
        }
      }
      return ret;
    }
    //qixiao new add
    int TairEngineHelper::put_key(const KvKey& key, const KvMemValue &value, const int64_t version)
    {
      int ret = TFS_SUCCESS;
      switch (key.key_type_)
      {
        case KvKey::KEY_TYPE_OBJECT:
          {
            tair::data_entry t_pkey;
            tair::data_entry t_skey;
            tair::data_entry tvalue;

            int64_t pos = 0;
            ret = NULL != key.key_ && key.key_size_ - pos >= 0 ? TFS_SUCCESS : TFS_ERROR;
            if (TFS_SUCCESS == ret)
            {
              ret = split_key_for_tair(key, &t_pkey, &t_skey);
            }
            tvalue.set_data(value.get_data(), value.get_size());

            ret = prefix_put_to_tair(object_area_,
                t_pkey, t_skey, tvalue, version);

          }
          break;
        case KvKey::KEY_TYPE_BUCKET:
          {
            data_entry pkey(key.key_, key.key_size_);
            data_entry pvalue(value.get_data(), value.get_size());
            ret = tair_client_->put(object_area_, pkey, pvalue, 0, 0);
          }
          break;
        default:
          TBSYS_LOG(ERROR, "not support");
          ret = TFS_ERROR;
      }
      return ret;
    }

    int TairEngineHelper::get_key(const KvKey& key, KvValue **pp_value, int64_t *version)
    {
      int ret = TFS_SUCCESS;
      if (NULL == pp_value || NULL != *pp_value)
      {
        TBSYS_LOG(ERROR, "shuold never got this, bug!");
        ret = TFS_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        switch (key.key_type_)
        {
          case KvKey::KEY_TYPE_OBJECT:
            {
              tair::data_entry t_pkey;
              tair::data_entry t_skey;
              tair::data_entry *tvalue = NULL;

              string pkey_buff;
              int64_t pos = 0;
              ret = NULL != key.key_ && key.key_size_ - pos >= 0 ? TFS_SUCCESS : TFS_ERROR;
              if (TFS_SUCCESS == ret)
              {
                ret = split_key_for_tair(key, &t_pkey, &t_skey);
              }
              if(TFS_SUCCESS == ret)
              {
                ret = prefix_get_from_tair(object_area_, t_pkey, t_skey, tvalue/*p_tair_value->get_tair_value()*/);
              }

              if (TFS_SUCCESS == ret && NULL != version)
              {
                *version = tvalue->get_version();
              }
              TairValue *p_tair_value = new TairValue();
              if (NULL == p_tair_value)
              {
                ret = TFS_ERROR;
              }
              p_tair_value->set_tair_value(tvalue);
              *pp_value = p_tair_value;
            }
            break;
          case KvKey::KEY_TYPE_BUCKET:
            {
              data_entry pkey(key.key_, key.key_size_);
              data_entry* pvalue = NULL;
              ret = tair_client_->get(object_area_, pkey, pvalue);

              if (TAIR_RETURN_SUCCESS != ret)
              {
                ret = EXIT_BUCKET_NOT_EXIST;
              }

              if (TFS_SUCCESS == ret && NULL != version)
              {
                *version = pvalue->get_version();
              }

              TairValue *p_tair_value = new TairValue();
              if (NULL == p_tair_value)
              {
                ret = TFS_ERROR;
              }

              if (TFS_SUCCESS == ret)
              {
                p_tair_value->set_tair_value(pvalue);
                *pp_value = p_tair_value;
              }
            }
            break;
          default:
            TBSYS_LOG(ERROR, "not support");
            ret = TFS_ERROR;
        }
      }
      return ret;
    }

    int TairEngineHelper::scan_keys(const KvKey& start_key, const KvKey& end_key, const int32_t limit,
        const int32_t offset, vector<KvValue*> *keys, vector<KvValue*> *values, int32_t* result_size, short scan_type)
    {
      int ret = TFS_SUCCESS;

      switch (start_key.key_type_)
      {
        case KvKey::KEY_TYPE_OBJECT:
          {
            tair::data_entry t_pkey;
            tair::data_entry t_start_skey;
            tair::data_entry t_end_skey;
            vector<tair::data_entry *> tvalues;

            if (NULL != start_key.key_)
            {
              ret = split_key_for_tair(start_key, &t_pkey, &t_start_skey);
            }
            if (TFS_SUCCESS == ret && NULL != end_key.key_)
            {
              ret = split_key_for_tair(end_key, &t_pkey, &t_end_skey);
            }

            if (TFS_SUCCESS == ret)
            {
              tvalues.clear();
              ret = prefix_scan_from_tair(object_area_, t_pkey,
                  NULL == start_key.key_ ? "" : t_start_skey, NULL == end_key.key_ ? "" : t_end_skey,
                  offset, limit, tvalues, scan_type);
            }

            if (TFS_SUCCESS == ret || ret == EXIT_KV_RETURN_DATA_NOT_EXIST)
            {
              if (scan_type == CMD_RANGE_ALL)
              {
                for(size_t i = 0; i < tvalues.size(); i += 2)
                {
                  TairValue* k_tmp = new TairValue();
                  k_tmp->set_tair_value(tvalues[i]);
                  keys->push_back(k_tmp);
                  //TBSYS_LOG(INFO, "keys: %s", string(tvalues[i]->get_data(), tvalues[i]->get_size()).c_str());
                  TairValue* v_tmp = new TairValue();
                  v_tmp->set_tair_value(tvalues[i+1]);
                  values->push_back(v_tmp);
                }
                *result_size = static_cast<int32_t>(tvalues.size())/2;
              }
              else if(scan_type == CMD_RANGE_VALUE_ONLY)
              {
                for(size_t i = 0; i < tvalues.size(); ++i)
                {
                  TairValue* p_tmp = new TairValue();
                  p_tmp->set_tair_value(tvalues[i]);
                  values->push_back(p_tmp);
                }
                *result_size = static_cast<int32_t>(tvalues.size());
              }
              ret = TFS_SUCCESS;
            }
          }
          break;
        default:
          TBSYS_LOG(ERROR, "not support");
          break;
      }
      return ret;
    }
    //qixiao new end

    int TairEngineHelper::delete_key(const KvKey& key)
    {
      int ret = TFS_SUCCESS;
      switch (key.key_type_)
      {
        case KvKey::KEY_TYPE_OBJECT:
          {
            tair::data_entry pkey;
            tair::data_entry skey;
            ret = split_key_for_tair(key, &pkey, &skey);
            if (TFS_SUCCESS == ret)
            {
              ret = prefix_remove_from_tair(object_area_, pkey, skey);
            }
          }
          break;
        case KvKey::KEY_TYPE_BUCKET:
          {
            data_entry pkey(key.key_);
            ret = tair_client_->remove(object_area_, pkey);
          }
          break;
        default:
          TBSYS_LOG(ERROR, "not support");
          break;
      }
      return ret;
    }

    int TairEngineHelper::delete_keys(const std::vector<KvKey>& vec_keys)
    {
      int ret = TFS_SUCCESS;

      switch (vec_keys.front().key_type_)
      {
        case KvKey::KEY_TYPE_OBJECT:
          {
            tair::data_entry pkey;
            tair::data_entry skey;
            tair::tair_dataentry_set skey_set;
            tair::key_code_map_t key_code_map;

            std::vector<KvKey>::const_iterator iter = vec_keys.begin();
            for(; iter != vec_keys.end(); ++iter)
            {
              ret = split_key_for_tair(*iter, &pkey, &skey);
              skey_set.insert(&skey);

            }
            if (TFS_SUCCESS == ret)
            {
              ret = prefix_removes_from_tair(object_area_, pkey, skey_set, key_code_map);
            }
          }
          break;
        default:
          TBSYS_LOG(ERROR, "not support");
          break;
      }
      return ret;
    }

    int TairEngineHelper::split_key_for_tair(const KvKey& key, tair::data_entry* prefix_key, tair::
        data_entry* second_key)
    {
      int ret = TFS_SUCCESS;
      if (NULL == prefix_key || NULL == second_key
          || NULL == key.key_ || 0 == key.key_size_ || KvKey::KEY_TYPE_OBJECT != key.key_type_)
      {
        TBSYS_LOG(ERROR, "parameters error");
        ret = TFS_ERROR;
      }

      const char* pos = NULL;
      int64_t prefix_key_size = -1;
      int64_t second_key_size = -1;
      if (TFS_SUCCESS == ret)
      {
        const char* p = key.key_;
        do
        {
          if (KvKey::DELIMITER == *p)
          {
            pos = p;
            break;
          }
          p++;
        } while(p - key.key_ < key.key_size_);

        prefix_key_size = pos - key.key_ + 1;
        second_key_size = key.key_size_ - prefix_key_size;
        if (NULL == pos || prefix_key_size <= 0 || second_key_size < 0)
        {
          TBSYS_LOG(ERROR, "invalid key is %s", key.key_);
          ret = TFS_ERROR;
        }
      }

      if (TFS_SUCCESS == ret)
      {
        prefix_key->set_data(key.key_, prefix_key_size);
        second_key->set_data(pos+1, second_key_size);
      }

      return ret;
    }

    int TairEngineHelper::prefix_put_to_tair(const int area, const tair::data_entry &pkey,
        const tair::data_entry &skey, const tair::data_entry &value, const int version)

    {
      int retry_count = TAIR_RETRY_COUNT;
      int ret = TFS_SUCCESS;
      int tair_ret = 0;
      do
      {
        tair_ret = tair_client_->prefix_put(area, pkey, skey, value, 0 /*never expire*/, version);
      } while (TAIR_RETURN_TIMEOUT == tair_ret && --retry_count > 0);

      if (TAIR_RETURN_SUCCESS != tair_ret)
      {
        TBSYS_LOG(INFO, "tair ret: %d", tair_ret);
        ret = EXIT_KV_RETURN_ERROR;
        if (TAIR_RETURN_VERSION_ERROR == tair_ret)
        {
          TBSYS_LOG(WARN, "put to tair version error.");
          ret = EXIT_TAIR_VERSION_ERROR;
        }
        //TODO change tair errno to TFS errno
      }

      return ret;
    }

    int TairEngineHelper::prefix_get_from_tair(const int area, const tair::data_entry &pkey,
        const tair::data_entry &skey, tair::data_entry* &value)

    {
      int retry_count = TAIR_RETRY_COUNT;
      int ret = TFS_SUCCESS;
      int tair_ret = 0;
      do
      {
        tair_ret = tair_client_->prefix_get(area, pkey, skey, value);
      } while (TAIR_RETURN_TIMEOUT == tair_ret && --retry_count > 0);

      if(TAIR_RETURN_DATA_NOT_EXIST == tair_ret)
      {
        ret = EXIT_KV_RETURN_DATA_NOT_EXIST;
      }
      if (TAIR_RETURN_SUCCESS != tair_ret && TAIR_RETURN_DATA_NOT_EXIST != tair_ret)
      {
        //TODO change tair errno to TFS errno
        ret = EXIT_KV_RETURN_ERROR;
      }
      if (TAIR_RETURN_SUCCESS == tair_ret)
      {
        ret = TFS_SUCCESS;
      }

      return ret;
    }

    int TairEngineHelper::prefix_remove_from_tair(const int area, const tair::data_entry &pkey,
        const tair::data_entry &skey)
    {
      int retry_count = TAIR_RETRY_COUNT;
      int ret = TFS_SUCCESS;
      int tair_ret = 0;
      do
      {
        tair_ret = tair_client_->prefix_remove(area, pkey, skey);
      } while (TAIR_RETURN_TIMEOUT == tair_ret && --retry_count > 0);

      if (TAIR_RETURN_SUCCESS != tair_ret)
      {
        //TODO change tair errno to TFS errno
        ret = TFS_ERROR;
      }

      return ret;
    }

    int TairEngineHelper::prefix_removes_from_tair(const int area, const tair::data_entry &pkey,
        const tair::tair_dataentry_set &skey_set, tair::key_code_map_t &key_code_map)
    {
      int retry_count = TAIR_RETRY_COUNT;
      int ret = TFS_SUCCESS;
      int tair_ret = 0;
      do
      {
        tair_ret = tair_client_->prefix_removes(area, pkey, skey_set,key_code_map);
      } while (TAIR_RETURN_TIMEOUT == tair_ret && --retry_count > 0);

      if (TAIR_RETURN_SUCCESS != tair_ret)
      {
        //TODO change tair errno to TFS errno
        ret = TFS_ERROR;
      }
      return ret;
    }

    int TairEngineHelper::prefix_scan_from_tair(int area, const tair::data_entry &pkey,
        const tair::data_entry &start_key, const tair::data_entry &end_key,
        int offset, int limit, std::vector<tair::data_entry *> &values, short type)
    {
      int retry_count = TAIR_RETRY_COUNT;
      int ret = TFS_SUCCESS;
      int tair_ret = 0;
      do
      {
        tair_ret = tair_client_->get_range(area, pkey, start_key, end_key, offset, limit, values, type);
      } while (TAIR_RETURN_TIMEOUT == tair_ret && --retry_count > 0);

      if (TAIR_RETURN_DATA_NOT_EXIST == tair_ret)
      {
        ret = EXIT_KV_RETURN_DATA_NOT_EXIST;
      }

      if (TAIR_RETURN_SUCCESS != tair_ret && TAIR_RETURN_DATA_NOT_EXIST != tair_ret)
      {
        //TODO change tair errno to TFS errno
        ret = EXIT_KV_RETURN_ERROR;
      }
      return ret;
    }
  }
}
