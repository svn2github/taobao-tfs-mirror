/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tair_helper.cpp 2140 2012-07-18 10:28:04Z duanfei $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */
#include <Memory.hpp>
#include "common/error_msg.h"
#include "tair_helper.h"
using namespace tfs::common;
using namespace tair;
namespace tfs
{
  namespace nameserver
  {
    const char* GLOBAL_BLOCK_ID_SKEY = "global_block_id";

    TairHelper::TairHelper(const std::string& key_prefix, const std::string& config_id, const int32_t area):
      key_prefix_(key_prefix),
      config_id_(config_id),
      area_(area)
    {

    }

    TairHelper::~TairHelper()
    {
      destroy();
    }

    int TairHelper::create_family_id(int64_t& family_id)
    {
      family_id = -1;
      char key[128] = {'\0'};
      snprintf(key, 128, "%skey_family_seq", key_prefix_.c_str());
      int32_t ret = incr_(key, 1, family_id);
      if (TAIR_RETURN_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "call tair incr error, ret: %d, key: %s", ret , key);
      }
      ret = (TAIR_RETURN_SUCCESS == ret) ? TFS_SUCCESS : EXIT_OP_TAIR_ERROR;
      return ret;
    }

    int TairHelper::create_family(FamilyInfo& family_info)
    {
      int64_t pos = 0;
      char pkey[64] = {'\0'};
      char skey[128] = {'\0'};
      char value[1024] = {'\0'};
      snprintf(pkey, 64, "%s%06d", key_prefix_.c_str(), get_bucket(family_info.family_id_));
      snprintf(skey, 64, "%020"PRI64_PREFIX"d", family_info.family_id_);
      int32_t ret = TFS_SUCCESS != family_info.serialize(value, 1024, pos) ? EXIT_SERIALIZE_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        tbutil::Mutex::Lock lock(mutex_);
        ret = put_(pkey, skey, value, family_info.length());
        if (TAIR_RETURN_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "create family : %"PRI64_PREFIX"d error: call tair put error, ret: %d, pkey: %s, skey: %s", family_info.family_id_, ret, pkey, skey);
        }
        // always try to delete after ns put tair timout, avoid tair put successfully itself finally
        if (TAIR_RETURN_TIMEOUT == ret)
        {
          del_(pkey, skey);
        }
        ret = (TAIR_RETURN_SUCCESS == ret) ? TFS_SUCCESS : EXIT_OP_TAIR_ERROR;
      }
      return ret;
    }

    int TairHelper::query_family(FamilyInfo& family_info)
    {
      int64_t pos = 0;
      char pkey[64] = {'\0'};
      char skey[128] = {'\0'};
      char value[1024] = {'\0'};
      snprintf(pkey, 64, "%s%06d", key_prefix_.c_str(), get_bucket(family_info.family_id_));
      snprintf(skey, 64, "%020"PRI64_PREFIX"d", family_info.family_id_);
      tbutil::Mutex::Lock lock(mutex_);
      int32_t ret = get_(pkey, skey, value, 1024);
      if (TAIR_RETURN_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "query family : %"PRI64_PREFIX"d error: call tair get error, ret: %d, pkey: %s, skey: %s", family_info.family_id_, ret, pkey, skey);
        ret = EXIT_OP_TAIR_ERROR;
      }
      else
      {
        ret = family_info.deserialize(value, 1024, pos);
      }
      return ret;
    }

    int TairHelper::del_family(const int64_t family_id, const bool del, const bool log, const uint64_t own_ipport)
    {
      char pkey[128] = {'\0'};
      char skey[128] = {'\0'};
      char suffix[64] = {'\0'};
      if (del)
        snprintf(suffix, 64, "_del_%"PRI64_PREFIX"u",own_ipport);
      snprintf(pkey, 128, "%s%06d%s", key_prefix_.c_str(), del ? DELETE_FAMILY_CHUNK_DEFAULT_VALUE : get_bucket(family_id), del ? suffix : "");
      snprintf(skey, 128, "%020"PRI64_PREFIX"d", family_id);
      data_entry tair_pkey(pkey, false);
      data_entry tair_skey(skey, false);
      int32_t ret = del_(pkey, skey);
      ret = TAIR_RETURN_DATA_NOT_EXIST == ret ? TAIR_RETURN_SUCCESS : ret;
      if (TAIR_RETURN_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "del family : %"PRI64_PREFIX"d error: call tair put error, ret: %d, pkey: %s, skey: %s", family_id, ret, pkey, skey);
        ret = EXIT_OP_TAIR_ERROR;
      }
      else
      {
        if (log)
          ret = insert_del_family_log_(family_id, own_ipport);
      }
      return ret;
    }

    int TairHelper::update_global_block_id(const uint64_t block_id)
    {
      char pkey[64] = {'\0'};
      char skey[64] = {'\0'};
      char value[64] = {'\0'};
      snprintf(pkey, 64, "%s", key_prefix_.c_str());
      snprintf(skey, 64, "%s", GLOBAL_BLOCK_ID_SKEY);
      snprintf(value, 64, "%lu", block_id);

      int ret = put_(pkey, skey, value, strlen(value));
      if (TAIR_RETURN_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "update global block id : %"PRI64_PREFIX"u error. "
            "call tair put error, ret: %d, pkey: %s, skey: %s", block_id, ret, pkey, skey);
      }
      return ret;
    }

    int TairHelper::query_global_block_id(uint64_t& block_id)
    {
      block_id = 0;
      char pkey[64] = {'\0'};
      char skey[64] = {'\0'};
      char value[64] = {'\0'};
      snprintf(pkey, 64, "%s", key_prefix_.c_str());
      snprintf(skey, 64, "%s", GLOBAL_BLOCK_ID_SKEY);

      int32_t ret = get_(pkey, skey, value, 64);
      if (TAIR_RETURN_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "query global block id error. "
            "call tair get error, ret: %d, pkey: %s, skey: %s", ret, pkey, skey);
      }
      else
      {
        block_id = strtoul(value, NULL, 10);
      }
      return ret;
    }

    int TairHelper::insert_del_family_log_(const int64_t family_id, const uint64_t own_ipport)
    {
      char pkey[128] = {'\0'};
      char skey[128] = {'\0'};
      char value[1024] = {'\0'};
      snprintf(pkey, 128, "%s%06d_del_%"PRI64_PREFIX"u", key_prefix_.c_str(), DELETE_FAMILY_CHUNK_DEFAULT_VALUE, own_ipport);
      snprintf(skey, 128, "%020"PRI64_PREFIX"d", family_id);
      FamilyInfo family_info;
      family_info.family_id_ = family_id;
      int64_t pos = 0;
      int32_t ret = TFS_SUCCESS != family_info.serialize(value, 1024, pos) ? EXIT_SERIALIZE_ERROR : TFS_SUCCESS;
      ret = put_(pkey, skey, value, family_info.length());
      if (TAIR_RETURN_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "del family log : %"PRI64_PREFIX"d error: call tair put error, ret: %d, pkey: %s, skey: %s", family_info.family_id_, ret, pkey, skey);
        ret = EXIT_OP_TAIR_ERROR;
      }
      return ret;
    }

    int TairHelper::initialize()
    {
      tbutil::Mutex::Lock lock(mutex_);
      return tair_client_.startup(config_id_.c_str()) ? TFS_SUCCESS : EXIT_INITIALIZE_TAIR_ERROR;
    }

    int TairHelper::destroy()
    {
      tair_client_.close();
      return TFS_SUCCESS;
    }

    int TairHelper::scan(std::vector<FamilyInfo>& family_infos, const int64_t start_family_id, const int32_t chunk, const bool del, const uint64_t peer_ipport, const int32_t limit)
    {
      char pkey[128] = {'\0'};
      char start_key[128] = {'\0'};
      char end_key[128] = {'\0'};
      char suffix[64] = {'\0'};
      if (del)
        snprintf(suffix, 64,"_del_%"PRI64_PREFIX"u", peer_ipport);
      snprintf(pkey, 128, "%s%06d%s", key_prefix_.c_str(), chunk, del ? suffix : "");
      snprintf(start_key, 128, "%020"PRI64_PREFIX"d", start_family_id);
      data_entry tair_pkey(pkey, false);
      data_entry tair_start_key(start_key, true);
      data_entry tair_end_key(end_key, false);
      int32_t ret = TAIR_RETURN_SUCCESS, retry = 0;

      family_infos.clear();
      std::vector<data_entry*> values;

      do
      {
        tbutil::Mutex::Lock lock(mutex_);
        ret = tair_client_.get_range(area_, tair_pkey, tair_start_key, tair_end_key,
            0, limit, values, CMD_RANGE_VALUE_ONLY);
      }
      while (TAIR_RETURN_DATA_NOT_EXIST != ret
            && TAIR_HAS_MORE_DATA != ret
            && TAIR_RETURN_SUCCESS != ret
            && retry++ < 3);
      std::vector<data_entry*>::iterator iter = values.begin();
      for (; iter != values.end(); ++iter)
      {
        int64_t pos = 0;
        family_infos.push_back(FamilyInfo());
        std::vector<FamilyInfo>::iterator it = family_infos.end() - 1;
        int32_t rt = (*it).deserialize((*iter)->get_data(), (*iter)->get_size(), pos);
        assert(TFS_SUCCESS == rt);
        tbsys::gDelete((*iter));
      }
      TBSYS_LOG(DEBUG, "scan family information %d, start_family_id: %"PRI64_PREFIX"d, chunk: %d, pkey: %s, start_key: %s, end_key: %s , infos.size(): %zd",
        ret, start_family_id, chunk, pkey, start_key, end_key, family_infos.size());
      return ret;
    };

    int TairHelper::put_(const char* pkey, const char* skey, const char* value, const int32_t value_len)
    {
      int32_t ret = (NULL != pkey && NULL != skey && NULL != value && value_len > 0) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        int32_t index = 0;
        data_entry tair_pkey(pkey, false);
        data_entry tair_skey(skey, false);
        data_entry tair_value(value, value_len, false);
        do
        {
          ret = tair_client_.prefix_put(area_, tair_pkey, tair_skey, tair_value, 0, 0);
        }
        while (TAIR_RETURN_SUCCESS != ret && index++ < 3);
      }
      return ret;
    }

    int TairHelper::del_(const char* pkey, const char* skey)
    {
      int32_t ret = (NULL != pkey && NULL != skey) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        int32_t index = 0;
        data_entry tair_pkey(pkey, false);
        data_entry tair_skey(skey, false);
        do
        {
          ret = tair_client_.prefix_remove(area_, tair_pkey, tair_skey);
          if (TAIR_RETURN_DATA_NOT_EXIST == ret)
            ret = TAIR_RETURN_SUCCESS;
        }
        while (TAIR_RETURN_SUCCESS != ret && index++ < 3);
      }
      return ret;
    }

    int TairHelper::incr_(const char* key, const int32_t step, int64_t& value)
    {
      int32_t ret = (NULL != key && step >= 0) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        int32_t ret_value = 0, index = 0;
        data_entry tair_key(key, false);
        do
        {
          ret = tair_client_.incr(area_, tair_key, step, &ret_value);
          value = ret_value;
        }
        while (TAIR_RETURN_SUCCESS != ret && index++ < 3);
      }
      return ret;
    }

    int TairHelper::get_(const char* pkey, const char* skey, char* value, const int32_t value_len)
    {
      int32_t ret = (NULL != pkey && NULL != skey && NULL != value && value_len > 0) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        int32_t index = 0;
        data_entry tair_pkey(pkey, false);
        data_entry tair_skey(skey, false);
        data_entry* tair_value = NULL;
        do
        {
          ret = tair_client_.prefix_get(area_, tair_pkey, tair_skey, tair_value);
        }
        while (TAIR_RETURN_SUCCESS != ret && TAIR_RETURN_DATA_NOT_EXIST != ret && index++ < 3);

        if (TFS_SUCCESS == ret)
        {
          assert(tair_value->get_size() <= value_len);
          memcpy(value, tair_value->get_data(), tair_value->get_size());
          tbsys::gDelete(tair_value);
        }
      }
      return ret;
    }
  }/** end namespace nameserver **/
}/** end namespace tfs **/
