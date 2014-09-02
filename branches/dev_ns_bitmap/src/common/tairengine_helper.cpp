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
#include "parameter.h"
#include "error_msg.h"
#include "serialization.h"
using namespace std;
using namespace tair;
using namespace common;
namespace tfs
{

  namespace common
  {
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

    const int TairEngineHelper::TAIR_RETRY_COUNT = 1;

    TairEngineHelper::TairEngineHelper(const string &master_addr, const string &slave_addr, const string &group_name)
      :tair_client_(NULL), master_addr_(master_addr),
      slave_addr_(slave_addr), group_name_(group_name), object_area_(-1)
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

        if (!tair_client_->startup(master_addr_.c_str(), slave_addr_.c_str(), group_name_.c_str()))
        {
          TBSYS_LOG(ERROR, "starup tair client fail. "
              "master addr: %s, slave addr: %s, group name:  %s",
              master_addr_.c_str(), slave_addr_.c_str(), group_name_.c_str());
          ret = TFS_ERROR;
        }
      }
      return ret;
    }

    int TairEngineHelper::put_key(const int32_t name_area, const KvKey& key, const KvMemValue &value, const int64_t version)
    {
      int ret = TFS_SUCCESS;
      switch (key.key_type_)
      {
        case KvKey::KEY_TYPE_OBJECT:
        case KvKey::KEY_TYPE_EXPTIME_APPKEY:
        case KvKey::KEY_TYPE_ES_STAT:
          {
            ret = prefix_put_key(name_area, key, value, version);
          }
          break;
        case KvKey::KEY_TYPE_BUCKET:
        case KvKey::KEY_TYPE_NAME_EXPTIME:
          {
            ret = none_range_put_key(name_area, key, value, version);
          }
          break;
        default:
          TBSYS_LOG(ERROR, "not support");
          ret = TFS_ERROR;
      }
      return ret;
    }
    int TairEngineHelper::prefix_put_key(const int area, const KvKey& key, const KvMemValue &value, const int64_t version)
    {
      int ret = TFS_SUCCESS;
      tair::data_entry t_pkey;
      tair::data_entry t_skey;
      tair::data_entry tvalue;

      int64_t pos = 0;
      ret = NULL != key.key_ && key.key_size_ - pos >= 0 ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = split_key_for_tair(key, &t_pkey, &t_skey);
      }
      if (TFS_SUCCESS == ret)
      {
        tvalue.set_data(value.get_data(), value.get_size());

        ret = prefix_put_to_tair(area,
            t_pkey, t_skey, tvalue, version);

        if (TAIR_RETURN_SUCCESS != ret)
        {
          TBSYS_LOG(INFO, "tair ret: %d", ret);
          if (TAIR_RETURN_VERSION_ERROR == ret)
          {
            TBSYS_LOG(WARN, "put to tair version error.");
            ret = EXIT_KV_RETURN_VERSION_ERROR;
          }
          else
          {
            ret = EXIT_KV_RETURN_ERROR;
          }
        }
      }
      return ret;
    }

    int TairEngineHelper::none_range_put_key(const int area, const KvKey& key, const KvMemValue &value, const int64_t version)
    {
      int ret = TFS_SUCCESS;
      int tair_ret;
      data_entry pkey(key.key_, key.key_size_);
      data_entry pvalue(value.get_data(), value.get_size());
      tair_ret = tair_client_->put(area, pkey, pvalue, 0, version);
      if (TAIR_RETURN_SUCCESS != tair_ret)
      {
        TBSYS_LOG(INFO, "tair ret: %d", tair_ret);
        ret = EXIT_KV_RETURN_ERROR;
        if (TAIR_RETURN_VERSION_ERROR == tair_ret)
        {
          TBSYS_LOG(WARN, "put to tair version error.");
          ret = EXIT_KV_RETURN_VERSION_ERROR;
        }
      }
      return ret;
    }

    int TairEngineHelper::get_key(const int32_t name_area, const KvKey& key, KvValue **pp_value, int64_t *version)
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
          case KvKey::KEY_TYPE_EXPTIME_APPKEY:
            {
              ret = prefix_get_key(name_area, key, pp_value, version);
            }
            break;
          case KvKey::KEY_TYPE_BUCKET:
          case KvKey::KEY_TYPE_NAME_EXPTIME:
          case KvKey::KEY_TYPE_ES_STAT:
            {
              ret = none_range_get_key(name_area, key, pp_value, version);
            }
            break;
          default:
            TBSYS_LOG(ERROR, "not support");
            ret = TFS_ERROR;
        }
      }
      return ret;
    }
    int TairEngineHelper::prefix_get_key(const int area, const KvKey& key, KvValue **pp_value, int64_t *version)
    {
      int ret = TFS_SUCCESS;
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
        ret = prefix_get_from_tair(area, t_pkey, t_skey, tvalue/*p_tair_value->get_tair_value()*/);
      }

      if (TFS_SUCCESS == ret && NULL != version)
      {
        *version = tvalue->get_version();
      }
      if (TFS_SUCCESS == ret)
      {
        TairValue *p_tair_value = new (std::nothrow) TairValue();
        assert(NULL != p_tair_value);

        p_tair_value->set_tair_value(tvalue);
        *pp_value = p_tair_value;
      }
      else
      {
        delete tvalue;
        tvalue = NULL;
      }
      return ret;
    }
    int TairEngineHelper::none_range_get_key(const int area, const KvKey& key, KvValue **pp_value, int64_t *version)
    {
      int ret = TFS_SUCCESS;
      data_entry pkey(key.key_, key.key_size_);
      data_entry* pvalue = NULL;
      int tair_ret = 0;
      tair_ret = tair_client_->get(area, pkey, pvalue);

      if(TAIR_RETURN_DATA_NOT_EXIST == tair_ret || TAIR_RETURN_DATA_EXPIRED == tair_ret)
      {
        ret = EXIT_KV_RETURN_DATA_NOT_EXIST;
      }
      else if (TAIR_RETURN_SUCCESS != tair_ret)
      {
        ret = EXIT_KV_RETURN_ERROR;
      }
      else if (TAIR_RETURN_SUCCESS == tair_ret)
      {
        ret = TFS_SUCCESS;
      }

      if (TFS_SUCCESS == ret && NULL != version)
      {
        *version = pvalue->get_version();
      }

      if (TFS_SUCCESS == ret)
      {
        TairValue *p_tair_value = new (std::nothrow) TairValue();
        assert(NULL != p_tair_value);

        p_tair_value->set_tair_value(pvalue);
        *pp_value = p_tair_value;
      }
      else
      {
        delete pvalue;
        pvalue = NULL;
      }
      return ret;
    }

    int TairEngineHelper::scan_keys(const int32_t name_area, const KvKey& start_key, const KvKey& end_key, const int32_t limit,
        const int32_t offset, vector<KvValue*> *keys, vector<KvValue*> *values, int32_t* result_size, short scan_type)
    {
      int ret = TFS_SUCCESS;

      switch (start_key.key_type_)
      {
        case KvKey::KEY_TYPE_OBJECT:
        case KvKey::KEY_TYPE_EXPTIME_APPKEY:
          {
            ret = scan_prefix_keys(name_area, start_key, end_key, limit, offset,
                keys, values, result_size, scan_type);
          }
          break;
        default:
          TBSYS_LOG(ERROR, "not support");
          break;
      }
      return ret;
    }

    int TairEngineHelper::scan_prefix_keys(const int area, const KvKey& start_key, const KvKey& end_key,
        const int32_t limit, const int32_t offset, vector<KvValue*> *keys,
        vector<KvValue*> *values, int32_t* result_size, short scan_type)
    {
      int ret = TFS_SUCCESS;
      tair::data_entry t_start_pkey;
      tair::data_entry t_end_pkey;
      tair::data_entry t_start_skey;
      tair::data_entry t_end_skey;
      vector<tair::data_entry *> tvalues;

      if (NULL != start_key.key_)
      {
        ret = split_key_for_tair(start_key, &t_start_pkey, &t_start_skey);
      }
      if (TFS_SUCCESS == ret && NULL != end_key.key_)
      {
        ret = split_key_for_tair(end_key, &t_end_pkey, &t_end_skey);
      }

      if (NULL != start_key.key_ && NULL != end_key.key_ && TFS_SUCCESS == ret)
      {
        if ( t_start_pkey.get_size() != t_end_pkey.get_size()
            || memcmp(t_start_pkey.get_data(), t_end_pkey.get_data(), t_start_pkey.get_size()))
        {
          TBSYS_LOG(WARN, "start pkey is not the same with end pkey");
          ret = EXIT_KV_SCAN_ERROR;
        }
      }

      if (TFS_SUCCESS == ret)
      {
        tvalues.clear();
        ret = prefix_scan_from_tair(area, t_start_pkey,
            NULL == start_key.key_ ? "" : t_start_skey, NULL == end_key.key_ ? "" : t_end_skey,
            offset, limit, tvalues, scan_type);
      }

      if (TFS_SUCCESS == ret || EXIT_KV_RETURN_DATA_NOT_EXIST == ret || EXIT_KV_RETURN_HAS_MORE_DATA == ret)
      {
        if (scan_type == CMD_RANGE_ALL)
        {
          for(size_t i = 0; i < tvalues.size(); i += 2)
          {
            KvMemValue* k_tmp = new (std::nothrow) KvMemValue();
            assert(NULL != k_tmp);
            char* buff = k_tmp->malloc_data(t_start_pkey.get_size() + tvalues[i]->get_size());

            int64_t pos = 0;
            memcpy(buff + pos, t_start_pkey.get_data(), t_start_pkey.get_size());
            pos += t_start_pkey.get_size();
            memcpy(buff + pos, tvalues[i]->get_data(), tvalues[i]->get_size());
            pos += tvalues[i]->get_size();
            keys->push_back(k_tmp);
            delete tvalues[i];
            tvalues[i] = NULL;
            //TBSYS_LOG(INFO, "keys: %s", string(tvalues[i]->get_data(), tvalues[i]->get_size()).c_str());
            TairValue* v_tmp = new (std::nothrow) TairValue();
            assert(NULL != v_tmp);
            v_tmp->set_tair_value(tvalues[i+1]);
            values->push_back(v_tmp);
          }
          *result_size = static_cast<int32_t>(tvalues.size())/2;
        }
        else if(scan_type == CMD_RANGE_VALUE_ONLY)
        {
          for(size_t i = 0; i < tvalues.size(); ++i)
          {
            TairValue* p_tmp = new (std::nothrow) TairValue();
            assert(NULL != p_tmp);
            p_tmp->set_tair_value(tvalues[i]);
            values->push_back(p_tmp);
          }
          *result_size = static_cast<int32_t>(tvalues.size());
        }

        if (EXIT_KV_RETURN_HAS_MORE_DATA != ret)
        {
          ret = TFS_SUCCESS;
        }
      }
      else
      {
        for(size_t i = 0; i < tvalues.size(); ++i)
        {
          if (NULL != tvalues[i])
          {
            delete tvalues[i];
            tvalues[i] = NULL;
          }
        }
      }
      return ret;
    }

    int TairEngineHelper::delete_key(const int32_t name_area, const KvKey& key)
    {
      int ret = TFS_SUCCESS;
      switch (key.key_type_)
      {
        case KvKey::KEY_TYPE_OBJECT:
        case KvKey::KEY_TYPE_EXPTIME_APPKEY:
          {
            ret = delete_prefix_key(name_area, key);
          }
          break;
        case KvKey::KEY_TYPE_BUCKET:
        case KvKey::KEY_TYPE_NAME_EXPTIME:
        case KvKey::KEY_TYPE_ES_STAT:
          {
            ret = delete_none_range_key(name_area, key);
          }
          break;
        default:
          TBSYS_LOG(ERROR, "not support");
          break;
      }
      return ret;
    }

    int TairEngineHelper::delete_prefix_key(const int area, const KvKey& key)
    {
      int ret = TFS_SUCCESS;
      tair::data_entry pkey;
      tair::data_entry skey;
      ret = split_key_for_tair(key, &pkey, &skey);
      if (TFS_SUCCESS == ret)
      {
        ret = prefix_remove_from_tair(area, pkey, skey);
      }
      return ret;
    }

    int TairEngineHelper::delete_none_range_key(const int area, const KvKey& key)
    {
      int ret = TFS_SUCCESS;
      int tair_ret = 0;
      data_entry pkey(key.key_, key.key_size_);
      tair_ret = tair_client_->remove(area, pkey);
      if (TAIR_RETURN_SUCCESS != tair_ret)
      {
        ret = EXIT_KV_RETURN_ERROR;
        if (TAIR_RETURN_DATA_NOT_EXIST == tair_ret || TAIR_RETURN_DATA_EXPIRED == tair_ret)
        {
          TBSYS_LOG(WARN, "del data noesxit.");
          ret = EXIT_KV_RETURN_DATA_NOT_EXIST;
        }
        TBSYS_LOG(ERROR, "tair_ret removes is %d", tair_ret);
      }
      return ret;
    }


    int TairEngineHelper::delete_keys(const int32_t name_area, const std::vector<KvKey>& vec_keys)
    {
      int ret = TFS_SUCCESS;
      switch (vec_keys.front().key_type_)
      {
        case KvKey::KEY_TYPE_OBJECT:
        case KvKey::KEY_TYPE_EXPTIME_APPKEY:
          {
            ret = delete_prefix_keys(name_area, vec_keys);
          }
          break;
        default:
          TBSYS_LOG(ERROR, "not support");
          break;
      }
      return ret;
    }
    int TairEngineHelper::delete_prefix_keys(const int area, const std::vector<KvKey>& vec_keys)
    {
      int ret = TFS_SUCCESS;
      tair::data_entry pkey;
      std::vector<tair::data_entry* > v_skey;
      tair::tair_dataentry_set skey_set;
      tair::key_code_map_t key_code_map;

      std::vector<KvKey>::const_iterator iter = vec_keys.begin();
      for(; iter != vec_keys.end(); ++iter)
      {
        data_entry* p_skey = new data_entry();
        ret = split_key_for_tair(*iter, &pkey, p_skey);
        v_skey.push_back(p_skey);
        skey_set.insert(p_skey);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = prefix_removes_from_tair(area, pkey, skey_set, key_code_map);
        TBSYS_LOG(DEBUG, "removes %lu keys, ret: %d", skey_set.size(), ret);
      }
      std::vector<tair::data_entry* >::const_iterator iter_skey = v_skey.begin();
      for(; iter_skey != v_skey.end(); ++iter_skey)
      {
        delete (*iter_skey);
      }
      return ret;
    }

    int TairEngineHelper::split_key_for_tair(const KvKey& key, tair::data_entry* prefix_key, tair::
        data_entry* second_key)
    {
      int ret = TFS_SUCCESS;
      if (NULL == prefix_key || NULL == second_key
          || NULL == key.key_ || 0 == key.key_size_)
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
        TBSYS_LOG(DEBUG, "PKEY POS is %"PRI64_PREFIX"d", prefix_key_size);
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
          ret = EXIT_KV_RETURN_VERSION_ERROR;
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

      if (TAIR_RETURN_DATA_NOT_EXIST == tair_ret || TAIR_RETURN_DATA_EXPIRED == tair_ret)
      {
        ret = EXIT_KV_RETURN_DATA_NOT_EXIST;
      }
      else if (TAIR_RETURN_SUCCESS != tair_ret)
      {
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
        ret = EXIT_KV_RETURN_ERROR;
        if (TAIR_RETURN_DATA_NOT_EXIST == tair_ret || TAIR_RETURN_DATA_EXPIRED == tair_ret)
        {
          TBSYS_LOG(WARN, "del data noesxit.");
          ret = EXIT_KV_RETURN_DATA_NOT_EXIST;
        }
        TBSYS_LOG(ERROR, "tair_ret removes is %d", tair_ret);
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
        ret = EXIT_KV_RETURN_ERROR;
        if (TAIR_RETURN_DATA_NOT_EXIST == tair_ret || TAIR_RETURN_DATA_EXPIRED == tair_ret)
        {
          TBSYS_LOG(WARN, "del data noesxit.");
          ret = EXIT_KV_RETURN_DATA_NOT_EXIST;
        }
        TBSYS_LOG(ERROR, "tair_ret removes is %d", tair_ret);
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

      if (TAIR_RETURN_DATA_NOT_EXIST == tair_ret || TAIR_RETURN_DATA_EXPIRED == tair_ret)
      {
        ret = EXIT_KV_RETURN_DATA_NOT_EXIST;
      }
      else if (TAIR_RETURN_SUCCESS != tair_ret)
      {
        if (TAIR_HAS_MORE_DATA == tair_ret)
        {
          ret = EXIT_KV_RETURN_HAS_MORE_DATA;
        }
        else
        {
          TBSYS_LOG(ERROR, "tair_ret getrange is %d and area is %d and type is %d", tair_ret, area, type);
          ret = EXIT_KV_RETURN_ERROR;
        }
      }
      else if (TAIR_RETURN_SUCCESS == tair_ret)
      {
        ret = TFS_SUCCESS;
      }
      return ret;
    }
  }
}
