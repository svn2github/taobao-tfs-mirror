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

#ifndef TFS_KVMETASERVER_KV_ENGINE_HELPER_H_
#define TFS_KVMETASERVER_KV_ENGINE_HELPER_H_

#include <string>
#include <vector>

#include "define.h"
#include "internal.h"
#include "kv_meta_define.h"

namespace tfs
{
  namespace common
  {
    //key of object is like this
    //  bucketname\filename\offset\version_id
    //key of bucket is like this
    //  bucketname
    //

    struct KvKey
    {
      KvKey();

      static const char DELIMITER;

      const char* key_; //no malloc no free
      int32_t key_size_;
      int32_t key_type_;
      enum
      {
        KEY_TYPE_USER = 1,
        KEY_TYPE_BUCKET = 2,
        KEY_TYPE_OBJECT = 3,
        KEY_TYPE_NAME_EXPTIME = 4,
        KEY_TYPE_EXPTIME_APPKEY = 5,
        KEY_TYPE_ES_STAT = 6,
      };
    };

    class KvValue
    {
    public:
      KvValue();
      virtual ~KvValue();

      virtual int32_t get_size() const = 0;
      virtual const char* get_data() const = 0;
      virtual void free();
    };

    class KvMemValue :public KvValue
    {
      public:
        KvMemValue();
        virtual ~KvMemValue();
        virtual int32_t get_size() const;
        virtual const char* get_data() const;
        virtual void free();
      public:
        void set_data(char* data, const int32_t size);
        char* malloc_data(const int32_t buffer_size);
      protected:
        char* data_;
        int32_t size_;
        bool is_owner_;
    };

    class KvEngineHelper
    {
    public:
      KvEngineHelper(){};
      virtual ~KvEngineHelper(){};
      virtual int init() = 0;
      virtual int scan_keys(const int32_t name_area, const KvKey& start_key, const KvKey& end_key, const int32_t limit, const int32_t offset,
                            std::vector<KvValue*> *keys, std::vector<KvValue*> *values, int32_t* result_size, short scan_type) = 0;
      virtual int get_key(const int32_t name_area, const KvKey& key, KvValue **pp_value, int64_t *version) = 0;
      virtual int put_key(const int32_t name_area, const KvKey& key, const KvMemValue &value, const int64_t version) = 0;
      virtual int delete_key(const int32_t name_area, const KvKey& key) = 0;
      virtual int delete_keys(const int32_t name_area, const std::vector<KvKey>& vec_keys) = 0;

    private:
      DISALLOW_COPY_AND_ASSIGN(KvEngineHelper);

    };

  }
}
#endif
