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


#include "meta_info_helper.h"
#include "tairengine_helper.h"
using namespace std;
namespace tfs
{
  using namespace common;
  namespace metawithkv
  {
    const int TFS_INFO_BUFF_SIZE = 128;
    MetaInfoHelper::MetaInfoHelper()
    {
      kv_engine_helper_ = new TairEngineHelper();
    }

    MetaInfoHelper::~MetaInfoHelper()
    {
      delete kv_engine_helper_;
      kv_engine_helper_ = NULL;
    }

    int MetaInfoHelper::init()
    {
      int ret = TFS_ERROR;
      if (NULL != kv_engine_helper_)
      {
        ret = kv_engine_helper_->init();
      }
      return ret;
    }

    int MetaInfoHelper::put_meta(const std::string& bucket_name, const std::string& file_name,
         const common::TfsFileInfo& tfs_file_info)
    {
      //TODO for test now
      int ret = TFS_SUCCESS;

      string real_key(bucket_name + KvKey::DELIMITER + file_name);
      real_key += KvKey::DELIMITER;
      real_key += "0" ; //offset
      real_key += KvKey::DELIMITER;
      real_key += "0" ; //version_id;

      KvKey key;
      key.key_ = real_key.c_str();
      key.key_size_ = real_key.length();
      key.key_type_ = KvKey::KEY_TYPE_OBJECT;
      char tfs_info_buff[TFS_INFO_BUFF_SIZE];
      int64_t pos = 0;
      assert(TFS_SUCCESS == tfs_file_info.serialize(tfs_info_buff,
            TFS_INFO_BUFF_SIZE, pos));
      string value(tfs_info_buff, pos);
      ret = kv_engine_helper_->put_key(key, value, 0);
      return ret;
    }

    int MetaInfoHelper::get_meta(const std::string& bucket_name, const std::string& file_name,
        common::TfsFileInfo* tfs_file_info)
    {
      UNUSED(bucket_name);
      UNUSED(file_name);
      UNUSED(tfs_file_info);
      return TFS_SUCCESS;
    }

    int MetaInfoHelper::put_bucket(const std::string& bucket_name, const int64_t create_time)
    {
      //TODO for test now
      int ret = TFS_SUCCESS;

      string real_key(bucket_name);

      KvKey key;
      key.key_ = real_key.c_str();
      key.key_size_ = real_key.length();
      key.key_type_ = KvKey::KEY_TYPE_BUCKET;
      char time_buff[30];
      snprintf(time_buff, 30, "%"PRI64_PREFIX"d", create_time);
      string value(time_buff);
      ret = kv_engine_helper_->put_key(key, value, 0);
      return ret;
    }

    int MetaInfoHelper::get_bucket(const std::string& bucket_name, const std::string& prefix,
        const std::string& start_key, const int32_t limit, VSTRING& v_object_name)
    {
      //TODO for test now
      int ret = TFS_SUCCESS;

      string real_key(bucket_name);

      KvKey pkey;
      pkey.key_ = real_key.c_str();
      pkey.key_size_ = real_key.length();
      pkey.key_type_ = KvKey::KEY_TYPE_BUCKET;
      ret = kv_engine_helper_->list_skeys(pkey, prefix, start_key, limit, v_object_name);
      return ret;
    }

    int MetaInfoHelper::del_bucket(const std::string& bucket_name)
    {
      //TODO for test now
      int ret = TFS_SUCCESS;
      KvKey pkey;
      pkey.key_ = bucket_name.c_str();
      pkey.key_size_ = bucket_name.length();
      pkey.key_type_ = KvKey::KEY_TYPE_BUCKET;
      ret = kv_engine_helper_->delete_key(pkey);
      return ret;
    }


  }// end for metawithkv
}// end for tfs

