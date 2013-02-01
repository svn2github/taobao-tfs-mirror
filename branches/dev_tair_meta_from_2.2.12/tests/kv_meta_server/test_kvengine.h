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

#ifndef TFS_METAWITHKV_Test_ENGINE_HELPER_H_
#define TFS_METAWITHKV_Test_ENGINE_HELPER_H_

#include "tairengine_helper.h"
#include <string>
#include <map>
using namespace std;
namespace tfs
{
  namespace kvmetaserver
  {
    //different key type will use different namespace in tair

    class TestEngineHelper : public TairEngineHelper
    {
    public:
      struct InnerValue {
        int64_t version_;
        std::string value_;
        InnerValue():version_(0){}
      };
      TestEngineHelper();
      virtual ~TestEngineHelper();
      virtual int init();
      virtual int put_key(const KvKey& key, const KvMemValue& value, const int64_t version);
      virtual int get_key(const KvKey& key, KvValue** value, int64_t* version);
      virtual int delete_key(const KvKey& key);
      int split_key(const KvKey& key, KvKey *prefix_key, KvKey *second_key);
      virtual int scan_keys(const KvKey& start_key, const KvKey& end_key,
          const int32_t limit, const int32_t offset, std::vector<KvValue*> *vec_realkey,
          std::vector<KvValue*> *vec_values, int32_t* result_size, short scan_type);

      int scan_from_map(const KvKey &start_key, const KvKey &end_key,
          const int32_t offset, const int32_t limit, std::vector<KvValue*> *vec_realkey, std::vector<KvValue*> *vec_values, int *result_size);

      virtual int delete_keys(const std::vector<KvKey>& vec_keys);
    private:
      DISALLOW_COPY_AND_ASSIGN(TestEngineHelper);
      typedef std::map<std::string, InnerValue> CONTAINER;
      CONTAINER map_store_;
    };
  }
}
#endif
