#ifndef TFS_CLIENT_TFSLARGEFILE_H_
#define TFS_CLIENT_TFSLARGEFILE_H_

#include <stdint.h>

#include "tfs_file.h"
#include "local_key.h"

namespace tfs
{
  namespace client
  {
    class TfsLargeFile : public TfsFile
    {
    public:
      TfsLargeFile();
      virtual ~TfsLargeFile();

      virtual int open(const char* file_name, const char* suffix, int flags, ... );
      virtual int read(void* buf, size_t count);
      virtual int write(const void* buf, size_t count);
      virtual off_t lseek(off_t offset, int whence);
      virtual ssize_t pread(void* buf, size_t count, off_t offset);
      virtual ssize_t pwrite(const void* buf, size_t count, off_t offset);
      virtual int close();

    private:
      static const int64_t SEGMENT_SIZE = 1 << 10;
      static const int64_t BATCH_COUNT = 10;
      static const int64_t BATCH_SIZE = SEGMENT_SIZE * BATCH_COUNT;

      int batch_open(const int64_t count, std::multimap<uint32_t, common::VUINT64>& segments);
      int process(const InnerFilePhase file_phase);

      //int do_async_request(const InnerFilePhase file_phase, const int64_t wait_id);
      //int do_async_response(const InnerFilePhase file_phase, /*response*/);
    private:
      LocalKey local_key_;
      std::vector<TfsFile*> current_files_;
    };
  }
}
#endif  // TFS_CLIENT_TFSLARGEFILE_H_
