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
    TairHelper::TairHelper(const std::string& key_prefix, const std::string& master_ipaddr, const std::string& slave_ipaddr,
          const std::string& group_name, const int32_t area):
      key_prefix_(key_prefix),
      master_ipaddr_(master_ipaddr),
      slave_ipaddr_(slave_ipaddr),
      group_name_(group_name),
      max_key_("4294967295"),
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
      int64_t tair_ret = -1;
      int32_t ret_value = 0, retry = 0;
      char key[128] = {'\0'};
      snprintf(key, 128, "%skey_family_seq", key_prefix_.c_str());
      data_entry tair_key(key, false);

      do
      {
        tbutil::Mutex::Lock lock(mutex_);
        tair_ret = tair_client_.incr(area_, tair_key, 1, &ret_value);
        family_id = ret_value;
      }
      while (0 != tair_ret && retry++ < 3);
      if (0 != tair_ret)
      {
        TBSYS_LOG(WARN, "call tair incr error, ret: %"PRI64_PREFIX"d, key: %s", tair_ret, key);
      }
      return (0 == tair_ret) ? TFS_SUCCESS : EXIT_OP_TAIR_ERROR;
    }

    int TairHelper::create_family(FamilyInfo& family_info)
    {
      int64_t tair_ret = -1;
      int64_t pos = 0;

      char pkey[64] = {'\0'};
      char skey[128] = {'\0'};
      char value[1024] = {'\0'};
      snprintf(pkey, 64, "%s", key_prefix_.c_str());
      snprintf(skey, 64, "%010"PRI64_PREFIX"d", family_info.family_id_);
      int32_t ret = TFS_SUCCESS != family_info.serialize(value, 1024, pos) ? EXIT_SERIALIZE_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        int32_t retry = 0;
        data_entry tair_pkey(pkey, false);
        data_entry tair_skey(skey, false);
        data_entry tair_value(value, family_info.length(), false);
        do
        {
          tbutil::Mutex::Lock lock(mutex_);
          tair_ret = tair_client_.prefix_put(area_, tair_pkey, tair_skey,tair_value, 0, 0);
        }
        while (0 != tair_ret && retry++ < 3);
        if (0 != tair_ret)
        {
          TBSYS_LOG(WARN, "create family : %"PRI64_PREFIX"d error: call tair put error, ret: %"PRI64_PREFIX"d, pkey: %s, skey: %s", family_info.family_id_,tair_ret, pkey, skey);
        }
        ret = (0 == tair_ret) ? TFS_SUCCESS : EXIT_OP_TAIR_ERROR;
      }
      return ret;
    }

    int TairHelper::query_family(FamilyInfo& family_info)
    {
      char pkey[64] = {'\0'};
      char skey[128] = {'\0'};
      snprintf(pkey, 64, "%s", key_prefix_.c_str());
      snprintf(skey, 128, "%010"PRI64_PREFIX"d", family_info.family_id_);
      int32_t retry = 0, ret = TFS_SUCCESS;
      data_entry tair_pkey(pkey, false);
      data_entry tair_skey(skey, false);
      data_entry* tair_value = NULL;
      do
      {
        tbutil::Mutex::Lock lock(mutex_);
        ret = tair_client_.prefix_get(area_, tair_pkey, tair_skey, tair_value);
      }
      while (0 != ret && retry++ < 3);
      if (0 != ret )
      {
        TBSYS_LOG(WARN, "query family : %"PRI64_PREFIX"d error: call tair put error, ret: %d, pkey: %s, skey: %s", family_info.family_id_,ret, pkey, skey);
        ret = EXIT_OP_TAIR_ERROR;
      }
      else
      {
        int64_t pos = 0;
        ret = TFS_SUCCESS != family_info.deserialize(tair_value->get_data(), tair_value->get_size(), pos) ? EXIT_DESERIALIZE_ERROR : TFS_SUCCESS;
        tbsys::gDelete(tair_value);
      }
      return ret;
    }

    int TairHelper::del_family(const int64_t family_id)
    {
      int64_t tair_ret = -1;
      int32_t retry = 0;
      char pkey[64] = {'\0'};
      char skey[128] = {'\0'};
      snprintf(pkey, 64, "%s", key_prefix_.c_str());
      snprintf(skey, 128, "%010"PRI64_PREFIX"d", family_id);
      data_entry tair_pkey(pkey, false);
      data_entry tair_skey(skey, false);
      do
      {
        tbutil::Mutex::Lock lock(mutex_);
        tair_ret = tair_client_.prefix_remove(area_, tair_pkey, tair_skey);
      }
      while (0 != tair_ret && retry++ < 3);
      if (0 != tair_ret)
      {
        TBSYS_LOG(WARN, "create family : %"PRI64_PREFIX"d error: call tair put error, ret: %"PRI64_PREFIX"d, pkey: %s, skey: %s", family_id, tair_ret, pkey, skey);
      }
      return (0 == tair_ret) ? TFS_SUCCESS : EXIT_OP_TAIR_ERROR;
    }

    int TairHelper::initialize()
    {
      tbutil::Mutex::Lock lock(mutex_);
      return tair_client_.startup(master_ipaddr_.c_str(), slave_ipaddr_.c_str(), group_name_.c_str()) ? TFS_SUCCESS : EXIT_INITIALIZE_TAIR_ERROR;
    }

    int TairHelper::destroy()
    {
      tair_client_.close();
      return TFS_SUCCESS;
    }

    int TairHelper::scan(std::vector<FamilyInfo>& family_infos, const int64_t start_family_id)
    {
      const int32_t ROW_LIMIT = INT_MAX;
      char pkey[64] = {'\0'};
      char start_key[128] = {'\0'};
      char end_key[128] = {'\0'};
      snprintf(pkey, 64, "%s", key_prefix_.c_str());
      snprintf(start_key, 128, "%010"PRI64_PREFIX"d", start_family_id);
      snprintf(end_key, 128, "%s", max_key_.c_str());
      data_entry tair_pkey(pkey, false);
      data_entry tair_start_key(start_key, true);
      data_entry tair_end_key(end_key, false);
      int32_t key_offset = 0;
      int32_t ret = TFS_SUCCESS, retry = 0;
      do
      {
        std::vector<data_entry*> values;
        do
        {
          tbutil::Mutex::Lock lock(mutex_);
          ret = tair_client_.get_range(area_, tair_pkey, tair_start_key, tair_end_key, key_offset, ROW_LIMIT, values);
        }
        while (TAIR_RETURN_DATA_NOT_EXIST != ret &&
            TAIR_HAS_MORE_DATA != ret && TAIR_RETURN_SUCCESS != ret && retry++ < 3);
        if (TAIR_HAS_MORE_DATA == ret || TAIR_RETURN_SUCCESS == ret)
        {
          int tmp_ret = TFS_SUCCESS;
          uint32_t value_size = values.size();
          for (uint32_t index = 1; index < value_size; index += 2)
          {
            int64_t pos = 0;
            family_infos.push_back(FamilyInfo());
            std::vector<FamilyInfo>::iterator it = family_infos.end() - 1;
            tmp_ret = TFS_SUCCESS != (*it).deserialize((values[index])->get_data(),
                values[index]->get_size(), pos) ? EXIT_DESERIALIZE_ERROR : TFS_SUCCESS;
            if (TFS_SUCCESS != tmp_ret)
            {
              ret = tmp_ret;
              break;
            }
          }

          // rest start key , set key offset to 1 to read from next key
          if (TAIR_HAS_MORE_DATA == ret)
          {
            tair_start_key = data_entry(values[value_size-2]->get_data(),
                values[value_size-2]->get_size());
            key_offset = 1;
          }

          for (uint32_t index = 0; index < values.size(); index++)
          {
            tbsys::gDelete(values[index]);
          }
        }
        else if (TAIR_RETURN_DATA_NOT_EXIST == ret)
        {
          ret = TFS_SUCCESS;
          TBSYS_LOG(INFO, "there is no family information currently");
        }
        else
        {
          TBSYS_LOG(WARN, "scan family information error: %d, pkey: %s start_key: %s, end_key: %s", ret, pkey, start_key, end_key);
        }
      } while (TAIR_HAS_MORE_DATA == ret); // if has more data, continue get range

      return ret;
    };
  }/** end namespace nameserver **/
}/** end namespace tfs **/
