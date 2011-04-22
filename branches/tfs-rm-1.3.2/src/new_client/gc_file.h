#ifndef TFS_CLIENT_GCFILE_H_
#define TFS_CLIENT_GCFILE_H_

#include "common/interval.h"
#include "common/file_op.h"

namespace tfs
{
  namespace client
  {
    const int32_t GC_BATCH_WIRTE_COUNT = 10;
    extern const char* GC_FILE_PATH;

    class GcFile
    {
    public:
      GcFile();
      ~GcFile();

      int initialize(const char* name);
      int load_file(const char* name);
      int add_segment(const common::SegmentInfo& seg_info);
      int save();
      int remove();

      int validate(int64_t total_size = 0)
      {
        return common::TFS_SUCCESS;
      }
      int32_t get_data_size();    // get raw data size of segment head and data
      int64_t get_file_size();    // get size that segments contain
      int32_t get_segment_size(); // get segment count
      std::vector<common::SegmentInfo>& get_seg_info()
      {
        return seg_info_;
      }

    private:
      int save_gc();
      int load();
      int load_head();
      void dump(char* buf);

    private:
      bool is_load_;
      int file_pos_;
      common::FileOperation* file_op_;
      common::SegmentHead seg_head_;
      std::vector<common::SegmentInfo> seg_info_;
    };
  }
}

#endif
