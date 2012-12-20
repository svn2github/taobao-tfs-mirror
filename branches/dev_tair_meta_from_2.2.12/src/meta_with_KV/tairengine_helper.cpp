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

using namespace std;
namespace tfs
{

  using namespace common;
  namespace metawithkv
  {
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
      UNUSED(key);
      UNUSED(value);
      UNUSED(version);
      return TFS_SUCCESS;
    }

    int TairEngineHelper::get_key(const KvKey& key, std::string* value, int64_t* version)
    {
      UNUSED(key);
      UNUSED(value);
      UNUSED(version);
      return TFS_SUCCESS;
    }

    int TairEngineHelper::delete_key(const KvKey& key)
    {
      UNUSED(key);
      return TFS_SUCCESS;
    }

    int TairEngineHelper::delete_keys(const std::vector<KvKey>& vec_keys)
    {
      UNUSED(vec_keys);
      return TFS_SUCCESS;
    }

    int TairEngineHelper::scan_keys(const KvKey& start_key, const KvKey& end_key,
        const int32_t limit, std::vector<KvKey>* vec_keys,
        std::vector<std::string>* vec_values, int32_t* result_size)
    {
      UNUSED(start_key);
      UNUSED(end_key);
      UNUSED(limit);
      UNUSED(vec_keys);
      UNUSED(vec_values);
      UNUSED(result_size);
      return TFS_SUCCESS;
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

      int64_t prefix_key_size = pos - key.key_;
      int64_t second_key_size = key.key_size_ - 1 - prefix_key_size;
      if (NULL == pos || 0 >= prefix_key_size || 0 >= second_key_size)
      {
        TBSYS_LOG(ERROR, "invalid key is %s", key.key_);
        ret = TFS_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        prefix_key->set_data(key.key_, prefix_key_size);
        second_key->set_data(pos + 1, second_key_size);
      }

      return ret;
    }

  }
}
