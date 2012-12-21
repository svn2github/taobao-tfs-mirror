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

#ifndef TFS_METAWITHKV_META_INFO_HELPER_H_
#define TFS_METAWITHKV_META_INFO_HELPER_H_

#include "common/parameter.h"
#include "common/base_service.h"
#include "common/status_message.h"
#include "common/meta_kv_define.h"
#include "message/message_factory.h"
#include "kvengine_helper.h"

namespace tfs
{
  namespace metawithkv
  {
    class MetaInfoHelper
    {
    public:
      MetaInfoHelper();
      virtual ~MetaInfoHelper();
      int put_meta(const std::string& bucket_name, const std::string& file_name,
          /*const int64_t offset,*/ const common::TfsFileInfo& tfs_file_info
          /* const taglist , versioning*/
          );
      int get_meta(const std::string& bucket_name, const std::string& file_name,
          /*const int64_t offset,*/ /*const get_tag_list*/ common::TfsFileInfo* tfs_file_info
          /*taglist* */);
      //int scan_metas();
      //int delete_metas();


    private:
      DISALLOW_COPY_AND_ASSIGN(MetaInfoHelper);
      KvEngineHelper* kv_engine_helper_;
    };
  }
}

#endif
