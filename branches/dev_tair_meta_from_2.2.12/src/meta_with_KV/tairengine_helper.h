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

#ifndef TFS_METAWITHKV_Tair_ENGINE_HELPER_H_
#define TFS_METAWITHKV_Tair_ENGINE_HELPER_H_

#include <string>
#include <vector>

namespace tfs
{
  namespace metawithkv
  {
    //different key type will use different namespace in tair

    class TairEngineHelper
    {
    public:
      TairEngineHelper();
      virtual ~TairEngineHelper();
      virtual int put_key(const KvKey& key, const char* value, const int64_t version);
      virtual int get_key(const KvKey& key, std::string* value, int64_t* version);
      virtual int delete_key(const KvKey& key);
      virtual int delete_keys(const std::vector<KvKey>& vec_keys);
      virtual int scan_keys(const KvKey& start_key, const KvKey& end_key,
          const int32_t limit, std::vector<KvKey>* vec_keys,
          std::vector<std::string>* vec_values, int32_t* result_size);

    private:
      DISALLOW_COPY_AND_ASSIGN(TairEngineHelper);

    };
  }
}
#endif
