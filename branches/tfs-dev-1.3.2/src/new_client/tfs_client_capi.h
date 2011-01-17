#ifndef TFS_CLIENT_C_API_H_
#define TFS_CLIENT_C_API_H_

#include "common/define.h"

#if __cplusplus
extern "C"
{
#endif

   int t_initialize(const char* ns_addr, const int32_t cache_time = tfs::common::DEFAULT_BLOCK_CACHE_TIME,
                      const int32_t cache_items = tfs::common::DEFAULT_BLOCK_CACHE_ITEMS);

   int t_open(const char* file_name, const char* suffix, const int flags, const char* key = NULL);
   int64_t t_read(const int fd, void* buf, const int64_t count);
   int64_t t_write(const int fd, const void* buf, const int64_t count);
   int64_t t_lseek(const int fd, const int64_t offset, const int whence);
   int64_t t_pread(const int fd, void* buf, const int64_t count, const int64_t offset);
   int64_t t_pwrite(const int fd, const void* buf, const int64_t count, const int64_t offset);
   int t_fstat(const int fd, tfs::common::FileStat* buf, const int mode = tfs::common::NORMAL_STAT);
   int t_close(const int fd, char* tfs_name, const int32_t len);

#if __cplusplus
}
#endif
#endif
