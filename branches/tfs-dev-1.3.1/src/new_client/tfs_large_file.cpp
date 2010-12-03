#include "tfs_large_file.h"

using namespace tfs::client;
using namespace tfs::common;
using namespace tfs::message;


TfsLargeFile::TfsLargeFile()
{
}

TfsLargeFile::~TfsLargeFile()
{
}

int TfsLargeFile::open(const char* file_name, const char *suffix, int flags, ... )
{
  int ret = TFS_ERROR;

  flag_ = flags;
  va_list args;
  va_start(args, flags);
  char* local_key = va_arg(args, char*);
  if (!local_key)
  {
    TBSYS_LOG(ERROR, "open with large mode occur null key");
    return ret;
  }
  ret = local_key_.initialize(local_key);
  if (ret != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "initialize local key fail, ret: %d", ret);
  }
  return ret;
}

int TfsLargeFile::read(void* buf, size_t count)
{

}

int TfsLargeFile::write(const void* buf, size_t count)
{

}

off_t TfsLargeFile::lseek(off_t offset, int whence)
{

}

ssize_t TfsLargeFile::pread(void* buf, size_t count, off_t offset)
{

}

ssize_t TfsLargeFile::pwrite(const void* buf, size_t count, off_t offset)
{

}

int TfsLargeFile::close()
{

}
