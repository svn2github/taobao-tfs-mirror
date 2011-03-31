#ifndef TFS_CLIENT_LOCALKEY_H_
#define TFS_CLIENT_LOCALKEY_H_

#include <tbsys.h>
#include <Memory.hpp>
#include "common/file_op.h"
#include "common/interval.h"
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
      bool delete_flag_;  // delete flag
      bool whole_file_flag_;
      common::SegmentInfo seg_info_;
      char* buf_;                   // buffer start
      common::FileInfo* file_info_;
      uint64_t file_number_;
      common::VUINT64 ds_;
      int32_t pri_ds_index_;
      int32_t status_;
      TfsFileEofFlag eof_;

      SegmentData() : delete_flag_(true), whole_file_flag_(true), buf_(NULL), file_info_(NULL),
                      file_number_(0), pri_ds_index_(-1),
                      status_(SEG_STATUS_NOT_INIT), eof_(TFS_FILE_EOF_FLAG_NO)
      {
      }

      SegmentData(SegmentData& seg_data)
      {
        delete_flag_ = false;
        whole_file_flag_ = seg_data.whole_file_flag_;
        memcpy(&seg_info_, &seg_data.seg_info_, sizeof(seg_info_));
        buf_ = seg_data.buf_;
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

    typedef std::vector<SegmentData*> SEG_DATA_LIST;
    typedef std::vector<SegmentData*>::iterator SEG_DATA_LIST_ITER;

    typedef std::set<common::SegmentInfo> SEG_SET;
    typedef std::set<common::SegmentInfo>::iterator SEG_SET_ITER;

    class LocalKey
    {
    public:

      LocalKey();
      ~LocalKey();

      int initialize(const char* local_key, const uint64_t addr);

      int load();
      int load(const char* buf);
      int load_file(const char* name);
      int validate(int64_t total_size = 0);
      int save();
      int remove();

      int get_segment_for_write(const int64_t offset, const char* buf,
                                int64_t size, SEG_DATA_LIST& seg_list);
      int get_segment_for_read(const int64_t offset, const char* buf,
                               const int64_t size, SEG_DATA_LIST& seg_list);

      int add_segment(common::SegmentInfo& seg_info);

      int64_t get_file_size();  // get size that segments contain
      int32_t get_data_size();  // get raw data size of segment head and data
      int dump_data(char* buf);

      // for unit test
      int32_t get_segment_size(); // get segment count
      SEG_SET& get_seg_info()
      {
        return seg_info_;
      }

    private:
      void destroy_info();
      int load_head(const char* buf);
      int load_segment(const char* buf);
      void get_segment(const int64_t start, const int64_t end,
                       const char* buf, int64_t& size, SEG_DATA_LIST& seg_list);
      void gc_segment(SEG_SET_ITER it);
      void gc_segment(SEG_SET_ITER first, SEG_SET_ITER last);

    private:
      common::SegmentHead seg_head_;
      common::FileOperation* file_op_;
      GcFile gc_file_;
      SEG_SET seg_info_;
    };
  }
}

#endif  // TFS_CLIENT_LOCALKEY_H_
