#ifndef TFS_CLIENT_LOCALKEY_H_
#define TFS_CLIENT_LOCALKEY_H_

#include <tbsys.h>
#include <Memory.hpp>
#include "common/file_op.h"
#include "common/interval.h"

namespace tfs
{
  namespace client
  {
    enum SegmentStatus
    {
      SEG_STATUS_SUCCESS = 0,
      SEG_STATUS_FAIL
    };

    enum TfsFileEofFlag
    {
      TFS_FILE_EOF_FLAG_NO = 0x00,
      TFS_FILE_EOF_FLAG_YES
    };

#pragma pack(4)
    struct SegmentHead
    {
      int32_t count_;           // segment count
      int64_t size_;            // total size that segments contain
      SegmentHead() : count_(0), size_(0)
      {
      }
    };

    struct SegmentInfo
    {
      uint32_t block_id_;       // block id
      uint64_t file_id_;        // file id
      int64_t offset_;          // offset in current file
      int32_t size_;            // size of segment
      int32_t crc_;             // crc checksum of segment

      SegmentInfo()
      {
        memset(this, 0, sizeof(*this));
      }
      bool operator < (const SegmentInfo& si) const
      {
        return offset_ < si.offset_;
      }
    };
#pragma pack()

    struct SegmentData
    {
      bool delete_flag_;  // delete flag
      bool whole_file_flag_;
      SegmentInfo seg_info_;
      char* buf_;                   // buffer start
      common::FileInfo* file_info_;
      uint64_t file_number_;
      common::VUINT64 ds_;
      int32_t pri_ds_index_;
      int32_t status_;
      TfsFileEofFlag eof_;

      SegmentData() : delete_flag_(true), whole_file_flag_(false), buf_(NULL), file_info_(NULL),
                      file_number_(0), pri_ds_index_(-1),
                      status_(SEG_STATUS_SUCCESS), eof_(TFS_FILE_EOF_FLAG_NO)
      {
      }

      SegmentData(SegmentData& seg_data)
      {
        delete_flag_ = false;
        whole_file_flag_ = seg_data.whole_file_flag_;
        memcpy(&seg_info_, &seg_data.seg_info_, sizeof(seg_info_));
        buf_ = seg_data.buf_;
        file_info_ = seg_data.file_info_;
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

    typedef std::vector<SegmentData*> SEG_DATA_LIST;
    typedef std::vector<SegmentData*>::iterator SEG_DATA_LIST_ITER;

    class LocalKey
    {
    public:
      typedef std::set<SegmentInfo> SEG_SET;
      typedef std::set<SegmentInfo>::iterator SEG_SET_ITER;

      LocalKey();
      //LocalKey(const char* local_key, const uint64_t addr);
      ~LocalKey();

      int initialize(const char* local_key, const uint64_t addr);

      int load();
      int load(const char* buf);
      int save();
      int remove();

      int get_segment_for_write(const int64_t offset, const char* buf,
                                int64_t size, SEG_DATA_LIST& seg_list);
      int get_segment_for_read(const int64_t offset, const char* buf,
                               const int64_t size, SEG_DATA_LIST& seg_list);

      int add_segment(SegmentInfo& seg_info);

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

    private:
      SegmentHead seg_head_;
      common::FileOperation* file_op_;
      SEG_SET seg_info_;
    };
  }
}

#endif  // TFS_CLIENT_LOCALKEY_H_
