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

#ifndef TFS_KVMETASERVER_TAIR_ENGINE_HELPER_H_
#define TFS_KVMETASERVER_TAIR_ENGINE_HELPER_H_

#include "kv_meta_define.h"
#include "kvengine_helper.h"
#include "tair_client_api.hpp"

namespace tfs
{
  namespace common
  {
    class TairValue : public KvValue
    {
    public:
      TairValue();
      virtual ~TairValue();
      virtual int32_t get_size() const;
      virtual const char* get_data() const;
      virtual void free();
      tair::data_entry* get_mutable_tair_value();
      void set_tair_value(tair::data_entry* tair_value);
    private:
      tair::data_entry* tair_value_;
    };


    class TairEngineHelper : public KvEngineHelper
    {
    public:
      TairEngineHelper(const std::string &master_addr, const std::string &slave_addr, const std::string &group_name);
      virtual ~TairEngineHelper();
      virtual int init();
      virtual int scan_keys(const int32_t name_area, const KvKey& start_key, const KvKey& end_key, const int32_t limit, const int32_t offset,
                           std::vector<KvValue*> *keys, std::vector<KvValue*> *values, int32_t* result_size, short scan_type);
      virtual int get_key(const int32_t name_area, const KvKey& key, KvValue **pp_value, int64_t *version);
      virtual int put_key(const int32_t name_area, const KvKey& key, const KvMemValue &value, const int64_t version);

      virtual int delete_key(const int32_t name_area, const KvKey& key);
      virtual int delete_keys(const int32_t name_area, const std::vector<KvKey>& vec_keys);

    public:
      //we split object key like bucketname\objecetname to prefix_key = bucketname, seconde_key = object_name
      static int split_key_for_tair(const KvKey& key, tair::data_entry* prefix_key, tair::data_entry* second_key);

    private:
      int prefix_put_to_tair(const int area, const tair::data_entry &pkey,
          const tair::data_entry &skey, const tair::data_entry &value, const int version);
      int prefix_get_from_tair(const int area, const tair::data_entry &pkey,
        const tair::data_entry &skey, tair::data_entry* &value);
      int prefix_remove_from_tair(const int area, const tair::data_entry &pkey, const tair::data_entry &skey);
      int prefix_removes_from_tair(const int area, const tair::data_entry &pkey,
                                   const tair::tair_dataentry_set &skey_set, tair::key_code_map_t &key_code_map);
      int prefix_scan_from_tair(const int area, const tair::data_entry &pkey, const tair::data_entry &start_key, const tair::data_entry &end_key,
                                int offset, int limit, std::vector<tair::data_entry *> &values, short type);

      int prefix_put_key(const int area, const KvKey& key, const KvMemValue &value, const int64_t version);
      int none_range_put_key(const int area, const KvKey& key, const KvMemValue &value, const int64_t version);

      int prefix_get_key(const int area, const KvKey& key, KvValue **pp_value, int64_t *version);
      int none_range_get_key(const int area, const KvKey& key, KvValue **pp_value, int64_t *version);

      int scan_prefix_keys(const int area, const KvKey& start_key, const KvKey& end_key,
          const int32_t limit, const int32_t offset,
          std::vector<KvValue*> *keys, std::vector<KvValue*> *values,
          int32_t* result_size, short scan_type);

      int delete_prefix_key(const int area, const KvKey& key);
      int delete_none_range_key(const int area, const KvKey& key);

      int delete_prefix_keys(const int area, const std::vector<KvKey>& vec_keys);

    private:
      DISALLOW_COPY_AND_ASSIGN(TairEngineHelper);
      tair::tair_client_api* tair_client_;
      std::string master_addr_;
      std::string slave_addr_;
      std::string group_name_;
      int32_t object_area_;
      static const int TAIR_RETRY_COUNT;
    };
  }
}
#endif
