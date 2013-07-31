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

#include "test_kvengine.h"
#include "Memory.hpp"
#include "kv_meta_define.h"
#include "common/parameter.h"
#include "common/error_msg.h"

using namespace std;
namespace tfs
{
  namespace common
  {
    TestEngineHelper::TestEngineHelper()
    {

    }

    TestEngineHelper::~TestEngineHelper()
    {
    }

    int TestEngineHelper::init()
    {
      int ret = TFS_SUCCESS;
      return ret;
    }

    int TestEngineHelper::put_key(const int32_t name_area, const KvKey& key, const KvMemValue& value, const int64_t version)
    {
      int ret = TFS_SUCCESS;
      //TBSYS_LOG(DEBUG, "put name_area, %d key size %d key_type %d", name_area, key.key_size_, key.key_type_);
      //for (int i = 0 ; i < key.key_size_; i++)
      //{
      //  TBSYS_LOG(DEBUG, "key char %d: %d", i, key.key_[i]);
      //}
      string inner_key;
      //inner_key += name_area;
      UNUSED(name_area);
      string tmp_key(key.key_, key.key_size_);
      inner_key += tmp_key;
      CONTAINER::iterator iter = map_store_.find(inner_key);
      int64_t inner_version = 0;
      if (iter != map_store_.end())
      {
        inner_version = iter->second.version_;
        if (version != inner_version && 0 != version)
        {
          ret = EXIT_KV_RETURN_VERSION_ERROR;
        }
      }
      if (TFS_SUCCESS == ret)
      {
        inner_version++;
        InnerValue iv;
        iv.version_ = inner_version;
        iv.value_.assign(value.get_data(), value.get_size());
        map_store_[inner_key] = iv;
      }
      return ret;
    }

    int TestEngineHelper::get_key(const int32_t name_area, const KvKey& key, KvValue** value, int64_t* version)
    {
      int ret = TFS_SUCCESS;
      string inner_key;
      //inner_key += name_area;
      UNUSED(name_area);
      string tmp_key(key.key_, key.key_size_);
      inner_key += tmp_key;
      CONTAINER::iterator iter = map_store_.find(inner_key);
      if (iter == map_store_.end())
      {
        ret = EXIT_KV_RETURN_DATA_NOT_EXIST;
      }
      else
      {
        KvMemValue *v = new KvMemValue();
        v->set_data((char*)iter->second.value_.c_str(), iter->second.value_.length());
        *value = v;
        if (NULL != version)
        {
          *version = iter->second.version_;
        }
      }
      return ret;
    }

    int TestEngineHelper::delete_key(const int32_t name_area, const KvKey& key)
    {
      int ret = TFS_SUCCESS;
      //TBSYS_LOG(DEBUG, "delete name_area, %d key size %d key_type %d", name_area, key.key_size_, key.key_type_);
      //for (int i = 0 ; i < key.key_size_; i++)
      //{
      //  TBSYS_LOG(DEBUG, "key char %d: %d", i, key.key_[i]);
      //}
      string inner_key;
      //inner_key += name_area;
      UNUSED(name_area);
      string tmp_key(key.key_, key.key_size_);
      inner_key += tmp_key;
      map_store_.erase(inner_key);
      return ret;
    }

    int TestEngineHelper::delete_keys(const int32_t name_area, const std::vector<KvKey>& vec_keys)
    {
      int ret = TFS_SUCCESS;

      std::vector<KvKey>::const_iterator iter = vec_keys.begin();
      for(; iter != vec_keys.end(); ++iter)
      {
        delete_key(name_area, *iter);
      }
      return ret;
    }
    int TestEngineHelper::scan_keys(const int32_t name_area, const KvKey& start_key, const KvKey& end_key,
        const int32_t limit, const int32_t offset, std::vector<KvValue*> *vec_realkey,
        std::vector<KvValue*> *vec_values, int32_t* result_size, short scan_type)
    {

      string temp_start_key1 = NULL != start_key.key_ ? string(start_key.key_, start_key.key_size_) : "";
      string temp_end_key1 = NULL != end_key.key_ ? string(end_key.key_, end_key.key_size_) : "";
      string temp_start_key;
      string temp_end_key;
      //temp_start_key += name_area;
      temp_start_key += temp_start_key1;
      //temp_end_key += name_area;
      UNUSED(name_area);
      temp_end_key += temp_end_key1;


      CONTAINER::iterator iter = map_store_.lower_bound(temp_start_key);
      int count = 0;
      int temp_offset = 0;
      for (; iter != map_store_.end() && count < limit; iter++)
      {

        if (iter->first <=  temp_end_key || "" == temp_end_key)
        {
          if (temp_offset < offset)
          {
            temp_offset++;
          }
          else
          {

            KvMemValue *key;
            if (1 == scan_type)
            {
              key = new KvMemValue();
              key->set_data((char*)(iter->first).c_str(), (iter->first).length());
              vec_realkey->push_back(key);
            }

            KvMemValue *value = new KvMemValue();
            value->set_data((char*)(iter->second.value_).c_str(), (iter->second.value_).length());

            vec_values->push_back(value);
            count++;
          }
        }
      }

      *result_size = count;
      return TFS_SUCCESS;
    }

  }
}
