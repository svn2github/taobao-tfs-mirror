/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_CLIENT_GCFILE_H_
#define TFS_CLIENT_GCFILE_H_

#include "common/internal.h"
#include "common/file_op.h"

namespace tfs
{
  namespace client
  {
    const int32_t GC_BATCH_WIRTE_COUNT = 10;
    extern const char* GC_FILE_PATH;
    const mode_t GC_FILE_PATH_MODE = 0777;

    class GcFile
    {
    public:
      explicit GcFile(const bool need_save_seg_infos = true);
      ~GcFile();

      int initialize(const char* name);
      int load_file(const char* name);
      int add_segment(const common::SegmentInfo& seg_info);
      int save();
      int remove();

      int validate(const int64_t total_size = 0)
      {
        return common::TFS_SUCCESS;
      }
      int32_t get_data_size() const;    // get raw data size of segment head and data
      int64_t get_file_size() const;    // get size that segments contain
      int32_t get_segment_size() const; // get segment count
      std::vector<common::SegmentInfo>& get_seg_info()
      {
        return seg_info_;
      }

    private:
      DISALLOW_COPY_AND_ASSIGN(GcFile);
      int save_gc();
      int load();
      int load_head();
      void dump(char* buf, const int32_t size);

    private:
      bool need_save_seg_infos_;
      int file_pos_;
      common::FileOperation* file_op_;
      common::SegmentHead seg_head_;
      std::vector<common::SegmentInfo> seg_info_;
    };
  }
}

#endif
