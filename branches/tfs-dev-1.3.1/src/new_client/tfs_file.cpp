#include "tfs_file.h"

using namespace tfs::client;
using namespace tfs::common;
using namespace tfs::message;

TfsFile::TfsFile() : tfs_session_(NULL), flag_(-1), offset_(0)
{
}

TfsFile::~TfsFile()
{
}

int TfsFile::open_ex(const char* file_name, const char* suffix, const int32_t mode)
{

}

ssize_t TfsFile::read_ex(void* buf, size_t count)
{

}

ssize_t TfsFile::write_ex(const void* buf, size_t count)
{

}

off_t TfsFile::lseek_ex(off_t offset, int whence)
{

}

ssize_t TfsFile::pread_ex(void* buf, size_t count, off_t offset)
{

}

ssize_t TfsFile::pwrite_ex(const void* buf, size_t count, off_t offset)
{

}

int TfsFile::close_ex()
{

}

void TfsFile::set_session(TfsSession* tfs_session)
{
  tfs_session_ = tfs_session;
}

const char* get_file_name()
{
  
}
