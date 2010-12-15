#ifndef TFS_CLIENT_LOCALKEY_H_
#define TFS_CLIENT_LOCALKEY_H_

#include "Mutex.h"

#include "common/file_op.h"

namespace tfs
{
  namespace client
  {
    const static char* g_tmp_path = "/tmp";
    const static int64_t SEGMENT_SIZE = 1 << 10;

    enum SegmentStatus
    {
      SEG_STATUS_UNINIT = 0,
      SEG_STATUS_RUNNING,
      SEG_STATUS_SUCCESS,
      SEG_STATUS_FAIL,
      SEG_STATUS_TIMEOUT
    };

    enum TfsFileEofFlag
    {
      TFS_FILE_EOF_FLAG_NO = 0x00,
      TFS_FILE_EOF_FLAG_YES
    };

    struct SegmentInfo
    {
      uint32_t block_id_;        // block id
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

    struct SegmentData
    {
      SegmentInfo seg_info_;
      char* buf_;                   // buffer start
      common::FileInfo* file_info_;
      int64_t cur_offset_;
      int32_t cur_size_;
      uint64_t file_number_;
      common::VUINT64 ds_;
      int32_t pri_ds_index_;
      int32_t last_elect_ds_id_;
      int32_t status_;
      TfsFileEofFlag eof_;

      SegmentData() : buf_(NULL), file_info_(NULL), cur_offset_(0), cur_size_(0), file_number_(0), pri_ds_index_(-1),
        last_elect_ds_id_(0), status_(0), eof_(TFS_FILE_EOF_FLAG_NO)
      {
      }
    };

    class LocalKey
    {
    public:
      typedef std::set<SegmentInfo> SEG_SET;
      typedef std::set<SegmentInfo>::iterator SEG_SET_ITER;

      LocalKey();
      LocalKey(const char* local_key, const uint64_t addr);
      ~LocalKey();

      int initialize(const char* local_key, const uint64_t addr);
      void destroy_info();

      int load();
      int load(const char* buf);
      int load(const char* buf, const int32_t count);
      int save();
      int get_segment_for_write(const int64_t offset, const char* buf,
                                int64_t size, std::vector<SegmentData*>& seg_list);
      int get_segment_for_read(const int64_t offset, const char* buf,
                               const int64_t size, std::vector<SegmentData*>& seg_list);
      int add_segment(SegmentInfo& seg_info);

    private:
      void insert_seg(const int64_t start, const int64_t end,
                      const char* buf, int64_t& size, std::vector<SegmentData*>& seg_list);
    private:
      common::FileOperation* file_op_;
      SEG_SET seg_info_;
    };
  }
}

#endif  // TFS_CLIENT_LOCALKEY_H_
