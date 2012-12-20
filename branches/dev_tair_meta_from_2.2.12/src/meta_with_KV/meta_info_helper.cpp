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

namespace tfs
{
  using namespace common;
  namespace metawithkv
  {
    MetaInfoHelper::MetaInfoHelper()
    {
      kv_engine_helper_ = new TairEngineHelper();
    }

    MetaInfoHelper::~MetaInfoHelper()
    {
      delete kv_engine_helper_;
      kv_engine_helper_ = NULL;
    }

    int MetaInfoHelper::put_meta(const std::string& bucket_name, const std::string& file_name,
         const common::TfsFileInfo* tfs_file_info)
    {
      UNUSED(bucket_name);
      UNUSED(file_name);
      UNUSED(tfs_file_info);
      return TFS_SUCCESS;
    }

    int MetaInfoHelper::get_meta(const std::string& bucket_name, const std::string& file_name,
        common::TfsFileInfo* tfs_file_info)
    {
      UNUSED(bucket_name);
      UNUSED(file_name);
      UNUSED(tfs_file_info);
      return TFS_SUCCESS;
    }

  }
}

