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
#ifndef TFS_CLIENT_LOCALKEY_H_
#define TFS_CLIENT_LOCALKEY_H_

#include <tbsys.h>
#include <Memory.hpp>
#include "common/file_op.h"
#include "common/internal.h"
#include "gc_file.h"

namespace tfs
{
  namespace client
  {
    enum TfsFileEofFlag
    {
      TFS_FILE_EOF_FLAG_NO = 0x00,
      TFS_FILE_EOF_FLAG_YES
    };
    enum SegmentStatus
    {
      SEG_STATUS_NOT_INIT = 0,      // not initialized
      SEG_STATUS_OPEN_OVER,         // all is completed
      SEG_STATUS_CREATE_OVER,       // create file is completed
      SEG_STATUS_BEFORE_CLOSE_OVER, // all before final close is completed
      SEG_STATUS_ALL_OVER           // all is completed
    };

    struct SegmentData
    {
      bool delete_flag_;        // delete flag
      common::SegmentInfo seg_info_;
      char* buf_;                   // buffer start
      int32_t inner_offset_;        // offset of this segment to operate
      common::FileInfo* file_info_;
      uint64_t file_number_;
      common::VUINT64 ds_;
      int32_t pri_ds_index_;
      int32_t status_;
      TfsFileEofFlag eof_;

      SegmentData() : delete_flag_(true), buf_(NULL), inner_offset_(0), file_info_(NULL),
                      file_number_(0), pri_ds_index_(0),
                      status_(SEG_STATUS_NOT_INIT), eof_(TFS_FILE_EOF_FLAG_NO)
      {
      }

      SegmentData(const SegmentData& seg_data)
      {
        delete_flag_ = false;
        memcpy(&seg_info_, &seg_data.seg_info_, sizeof(seg_info_));
        buf_ = seg_data.buf_;
        inner_offset_ = seg_data.inner_offset_;
        file_info_ = NULL;      // not copy
        file_number_ = seg_data.file_number_;
        ds_ = seg_data.ds_;
        pri_ds_index_ = seg_data.pri_ds_index_;
        status_ = seg_data.status_;
        eof_ = seg_data.eof_;
      }

      ~SegmentData()
      {
        tbsys::gDelete(file_info_);
      }
    };

    extern const char* LOCAL_KEY_PATH;
    const mode_t LOCAL_KEY_PATH_MODE = 0777;

    typedef std::vector<SegmentData*> SEG_DATA_LIST;
    typedef std::vector<SegmentData*>::iterator SEG_DATA_LIST_ITER;
    typedef std::vector<SegmentData*>::const_iterator SEG_DATA_LIST_CONST_ITER;

    typedef std::set<common::SegmentInfo> SEG_SET;
    typedef std::set<common::SegmentInfo>::iterator SEG_SET_ITER;
    typedef std::set<common::SegmentInfo>::const_iterator SEG_SET_CONST_ITER;

    class LocalKey
    {
    public:

      LocalKey();
      ~LocalKey();

      int initialize(const char* local_key, const uint64_t addr);

      int load();
      int load(const char* buf);
      int load_file(const char* name);
      int validate(const int64_t total_size = 0);
      int save();
      int remove();

      int64_t get_segment_for_write(const int64_t offset, const char* buf,
                                const int64_t size, SEG_DATA_LIST& seg_list);
      int64_t get_segment_for_read(const int64_t offset, char* buf,
                               const int64_t size, SEG_DATA_LIST& seg_list);

      int add_segment(const common::SegmentInfo& seg_info);
      int dump_data(char* buf, const int32_t buff_size) const;

      int32_t get_data_size() const;    // get raw data size of segment head and data
      int64_t get_file_size() const;    // get size that segments contain
      int32_t get_segment_size() const; // get segment count
      SEG_SET& get_seg_info();

    private:
      int init_local_key_name(const char* key, const uint64_t addr, char* local_key_name);
      void clear();
      void clear_info();
      int load_head(const char* buf);
      int load_segment(const char* buf);
      static void get_segment(const int64_t offset, const char* buf,
                       const int64_t size, SEG_DATA_LIST& seg_list);
      void check_overlap(const int64_t offset, SEG_SET_ITER& it);

      void gc_segment(SEG_SET_CONST_ITER it);
      void gc_segment(SEG_SET_CONST_ITER first, SEG_SET_CONST_ITER last);

    private:
      common::SegmentHead seg_head_;
      common::FileOperation* file_op_;
      GcFile gc_file_;
      SEG_SET seg_info_;
    };
  }
}

#endif  // TFS_CLIENT_LOCALKEY_H_
