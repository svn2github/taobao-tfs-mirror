/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   qushan<qushan@taobao.com>
 *      - modify 2009-03-27
 *   duanfei <duanfei@taobao.com>
 *      - modify 2010-04-23
 *
 */
#ifndef TFS_NAMESERVER_FILE_SYSTEM_IMAGE_H_
#define TFS_NAMESERVER_FILE_SYSTEM_IMAGE_H_

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string>
#include <ext/hash_map>
#include <errno.h>

#include "common/interval.h"
#include "common/config.h"
#include "oplog.h"
#include "block_collect.h"
#include "layout_manager.h"

namespace tfs
{
  namespace nameserver
  {
    class MetaManager;
    class FileSystemImage: public MetaScanner
    {
      struct ImageHeader
      {
        int32_t flag_;
        int32_t version_;
        int64_t block_count_;
        int64_t total_bytes_;
        int64_t total_file_count_;
      };

    public:
      FileSystemImage(MetaManager* mm);
      virtual ~FileSystemImage();

      virtual int process(const BlockCollect* blkcol) const;

      int initialize(LayoutManager& block_ds_map, const OpLogRotateHeader& head, common::FileQueue* file_queue);
      int save(const LayoutManager& blockServerMap);

    private:
      static const int64_t IMAGE_FLAG = 0x49534654; //TFSI
      char image_file_path_[common::MAX_PATH_LENGTH];
      char image_file_new_path_[common::MAX_PATH_LENGTH];
      mutable int32_t fd_;
      mutable ImageHeader header_;
      MetaManager* meta_;
    private:
      FileSystemImage();
      DISALLOW_COPY_AND_ASSIGN( FileSystemImage);
      int save_new_image(const LayoutManager & blockServerMap);
    };

  }// end namesapce nameserver
}// end namespace tfs

#endif
