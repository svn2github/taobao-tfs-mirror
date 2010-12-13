#ifndef TFS_CLIENT_LOCALKEY_H_
#define TFS_CLIENT_LOCALKEY_H_

#include "Mutex.h"

#include "common/file_op.h"

namespace tfs
{
  namespace client
  {
    const static char* g_tmp_path = "/tmp";

    struct SegmentInfo
    {
      int32_t block_id_;        // block id
      uint64_t file_id_;        // file id
      int32_t offset_;          // offset in current file
      int32_t size_;            // size of segment
      int32_t crc_;             // crc checksum of segment

      SegmentInfo()
      {
        memset(this, 0, sizeof(*this));
      }
    };

    struct SegmentData
    {
      SegmentInfo seg_info_;
      char* buf_;                   // buffer start
      int64_t file_number_;
      common::VUINT64 ds_;
      int32_t status_;

      SegmentData() : buf_(NULL), file_number_(0)
      {
      }
    };

    class LocalKey
    {
    public:
      struct SegmentComp
      {
        bool operator() (const SegmentInfo& lhs, const SegmentInfo& rhs) const
        {
          return lhs.offset_ < rhs.offset_;
        }
      };
      typedef std::set<SegmentInfo, SegmentComp> SEG_SET;
      typedef std::set<SegmentInfo, SegmentComp>::iterator SEG_SET_ITER;

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
                      const char* buf, int64_t& size, std::vector<SegmentData>& seg_list);
    private:
      common::FileOperation* file_op_;
      SEG_SET seg_info_;
    };
  }
}

#endif  // TFS_CLIENT_LOCALKEY_H_
