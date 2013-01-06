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

using namespace std;
using namespace tair;
namespace tfs
{

  using namespace common;
  namespace kvmetaserver
  {
    const int TairEngineHelper::TAIR_RETRY_COUNT = 1;
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

    int TairEngineHelper::put_key(const KvKey& key, const string& value, const int64_t version)
    {
      int ret = TFS_SUCCESS;
      switch (key.key_type_)
      {
        case KvKey::KEY_TYPE_OBJECT:
        {
          tair::data_entry pkey;
          tair::data_entry skey;
          tair::data_entry tvalue;
          ret = split_key_for_tair(key, &pkey, &skey);
          if (TFS_SUCCESS == ret)
          {
            tvalue.set_data(value.c_str(), value.length());
            ret = prefix_put_to_tair(object_area_,
                pkey, skey, tvalue, version);
          }

         }
        break;
        case KvKey::KEY_TYPE_BUCKET:
          {
            data_entry pkey(key.key_);
            data_entry pvalue(value.c_str());
            ret = tair_client_->put(object_area_, pkey, pvalue, 0, 0);
          }
          break;
        default:
          TBSYS_LOG(ERROR, "not support");
          ret = TFS_ERROR;
      }
      return ret;
    }

    int TairEngineHelper::get_key(const KvKey& key, std::string* value, int64_t* version)
    {
      int ret = TFS_SUCCESS;
      if (NULL == value || NULL == version)
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
              tair::data_entry pkey;
              tair::data_entry skey;
              tair::data_entry* tvalue = NULL;
              ret = split_key_for_tair(key, &pkey, &skey);
              if (TFS_SUCCESS == ret)
              {
                ret = prefix_get_from_tair(object_area_,
                    pkey, skey, tvalue);

              }
              if (TFS_SUCCESS == ret)
              {
                value->assign(tvalue->get_data(), tvalue->get_size());
                *version = tvalue->get_version();
              }
              tbsys::gDelete(tvalue);
            }
            break;
          case KvKey::KEY_TYPE_BUCKET:
            {
              data_entry pkey(key.key_);
              data_entry* pvalue = NULL;
              ret = tair_client_->get(object_area_, pkey, pvalue);
              if (TFS_SUCCESS == ret)
              {
                value->assign(pvalue->get_data(), pvalue->get_size());
              }
              tbsys::gDelete(pvalue);
            }
            break;
          default:
            TBSYS_LOG(ERROR, "not support");
            ret = TFS_ERROR;
        }
      }
      return ret;
    }

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
          vector<data_entry*> res;
          int limit = LIMIT_NUM;
          ret = tair_client_->get_range(object_area_, pkey, "", "", 0, limit, res);
          if (TAIR_RETURN_DATA_NOT_EXIST == ret)
          {
            ret = TFS_SUCCESS;
          }
          else
          {
            ret = EXIT_DELETE_DIR_WITH_FILE_ERROR;
            for (size_t i = 0; i < res.size(); i++)
            {
              delete res[i];
            }
            res.clear();
          }
          if (TFS_SUCCESS == ret)
          {
            ret = tair_client_->remove(object_area_, pkey);
          }
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

    int TairEngineHelper::scan_keys(const KvKey& start_key, const KvKey& end_key,
        const int32_t limit, std::vector<KvKey>* vec_keys, std::vector<std::string>* vec_realkey,
        std::vector<std::string>* vec_values, int32_t* result_size)
    {
      int ret = TFS_SUCCESS;

      switch (start_key.key_type_)
      {
        case KvKey::KEY_TYPE_OBJECT:
        {
          tair::data_entry pkey;
          tair::data_entry start_skey;
          tair::data_entry end_skey;
          vector<tair::data_entry *> tvalues;
          short type = 1;
          ret = split_key_for_tair(start_key, &pkey, &start_skey);
          if (TFS_SUCCESS == ret)
          {
            ret = split_key_for_tair(end_key, &pkey, &end_skey);
          }
          if (TFS_SUCCESS == ret)
          {
             ret = prefix_scan_from_tair(object_area_, pkey, start_skey, end_skey,
                   0/*offset*/, limit, tvalues, type);
          }
          if (TFS_SUCCESS == ret)
          {
            KvKey tmp_key;
            vector<tair::data_entry *>::iterator iter = tvalues.begin();
            for(; iter != tvalues.end(); ++iter)
            {
              vec_realkey->push_back((*iter)->get_printable_key().c_str());
              tmp_key.key_ = vec_realkey->back().c_str();
              tmp_key.key_size_ = vec_realkey->back().size();
              tmp_key.key_type_ = KvKey::KEY_TYPE_OBJECT;
              vec_keys->push_back(tmp_key);
              ++iter;
              vec_values->push_back((*iter)->get_printable_key().c_str());
              (*result_size)++;
            }
            *result_size = *result_size / 2;
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

        prefix_key_size = pos - key.key_;
        second_key_size = key.key_size_ - 1 - prefix_key_size;
        if (NULL == pos || 0 >= prefix_key_size || 0 >= second_key_size)
        {
          TBSYS_LOG(ERROR, "invalid key is %s", key.key_);
          ret = TFS_ERROR;
        }
      }

      if (TFS_SUCCESS == ret)
      {
        prefix_key->set_data(key.key_, prefix_key_size);
        second_key->set_data(pos + 1, second_key_size);
      }

      return ret;
    }

    int TairEngineHelper::list_skeys(const KvKey& key, const string& prefix,
        const string& start_key, const int32_t limit, common::VSTRING& v_object_name)
    {
      v_object_name.clear();
      int ret = TFS_SUCCESS;
      data_entry pkey(key.key_);
      string skey(start_key);
      string ekey;
      vector<data_entry*> res;
      bool loop, first_loop;
      loop = first_loop = true;
      int32_t limit_size = limit;

      while (loop)
      {
        ret = tair_client_->get_range(object_area_, pkey, data_entry(skey.data(), static_cast<int>(skey.size())),
              data_entry(ekey.data(), static_cast<int>(ekey.size())), first_loop ? 0 : 1, limit_size, res, CMD_RANGE_KEY_ONLY);

        if (ret < 0)
        {
          TBSYS_LOG(ERROR, "get range fail, ret: %d", ret);
          break;
        }

        size_t res_size = res.size();
        for (size_t i = 0; i < res_size; i++)
        {
          char* s = res[i]->get_data();
          char* pos = strstr(s, prefix.c_str());
          if (NULL != pos && pos == s)
          {
            string object(s);
            v_object_name.push_back(object);
          }
        }

        if (static_cast<int>(res_size) == limit_size)
        {
          skey.assign(res[res_size-1]->get_data(), res[res_size-1]->get_size());
          fprintf(stderr, "new start key: %s", skey.c_str());
          first_loop = false;
        }
        else
        {
          loop = false;
        }

        for (size_t i = 0; i < res_size; i++)
        {
          delete res[i];
        }

        res.clear();
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
        if (TAIR_RETURN_VERSION_ERROR == tair_ret)
        {
          TBSYS_LOG(WARN, "put to tair version error.");
        }
        //TODO change tair errno to TFS errno
        ret = TFS_ERROR;
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

      if (TAIR_RETURN_SUCCESS != tair_ret)
      {
        //TODO change tair errno to TFS errno
        ret = TFS_ERROR;
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

      if (TAIR_RETURN_SUCCESS != tair_ret)
      {
        //TODO change tair errno to TFS errno
        ret = TFS_ERROR;
      }
      return ret;
    }
  }
}
