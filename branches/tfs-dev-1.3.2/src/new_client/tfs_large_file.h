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
      virtual int64_t read(void* buf, int64_t count);
      virtual int64_t write(const void* buf, int64_t count);
      virtual int64_t lseek(int64_t offset, int whence);
      virtual int64_t pread(void* buf, int64_t count, int64_t offset);
      virtual int64_t pwrite(const void* buf, int64_t count, int64_t offset);
      virtual int close();

    protected:
      virtual int get_segment_for_read(int64_t offset, char* buf, int64_t count);
      virtual int get_segment_for_write(int64_t offset, const char* buf, int64_t count);
      virtual int read_process();
      virtual int write_process();
      virtual int close_process();

    private:
      int update_key();
      int flush_key();
      int upload_key();
      int remove_key();

    private:
      static const int64_t BATCH_COUNT = 10;
      static const int64_t BATCH_SIZE = SEGMENT_SIZE * BATCH_COUNT;

    private:
      LocalKey local_key_;
    };
  }
}
#endif  // TFS_CLIENT_TFSLARGEFILE_H_
