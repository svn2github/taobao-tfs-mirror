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
      LocalKey local_key_;
    };
  }
}
#endif  // TFS_CLIENT_TFSLARGEFILE_H_
