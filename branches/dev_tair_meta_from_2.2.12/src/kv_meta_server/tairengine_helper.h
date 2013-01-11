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

#include "common/kv_meta_define.h"
#include "kvengine_helper.h"
#include "tair_client_api.hpp"

namespace tfs
{
  namespace kvmetaserver
  {
    //different key type will use different namespace in tair
    class TairEngineHelper : public KvEngineHelper
    {
    public:
      TairEngineHelper();
      virtual ~TairEngineHelper();
      virtual int init();
      virtual int put_key(const KvKey& key, const std::string& value, const int64_t version);
      virtual int get_key(const KvKey& key, std::string* value, int64_t* version);
      virtual int delete_key(const KvKey& key);
      virtual int delete_keys(const std::vector<KvKey>& vec_keys);
      virtual int scan_keys(const KvKey& start_key, const KvKey& end_key,
          const int32_t offset, const int32_t limit, std::vector<KvKey>* vec_keys,
          std::vector<std::string>* vec_realkey,
          std::vector<std::string>* vec_values, int32_t* result_size);

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
      int prefix_scan_from_tair(int area, const tair::data_entry &pkey, const tair::data_entry &start_key, const tair::data_entry &end_key,
                                int offset, int limit, std::vector<tair::data_entry *> &values, short type);

    private:
      DISALLOW_COPY_AND_ASSIGN(TairEngineHelper);
      tair::tair_client_api* tair_client_;
      int32_t object_area_;
      static const int TAIR_RETRY_COUNT;
    };
  }
}
#endif
