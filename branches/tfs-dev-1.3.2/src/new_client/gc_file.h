#ifndef TFS_CLIENT_GCFILE_H_
#define TFS_CLIENT_GCFILE_H_

#include "common/interval.h"
#include "common/file_op.h"

namespace tfs
{
  namespace client
  {
    const int32_t GC_BATCH_WIRTE_COUNT = 10;
    static const char* GC_FILE_PATH = "/tmp/TFSlocalkeyDIR/gc/";

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

      std::vector<common::SegmentInfo>& get_seg_info()
      {
        return seg_info_;
      }

    private:
      int load();
      int load_head();
      void dump(char* buf);

    private:
      bool is_load_;
      common::FileOperation* file_op_;
      common::SegmentHead seg_head_;
      std::vector<common::SegmentInfo> seg_info_;
    };
  }
}

#endif
