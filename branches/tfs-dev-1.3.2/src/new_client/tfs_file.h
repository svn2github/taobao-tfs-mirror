#ifndef TFS_CLIENT_TFSFILE_H_
#define TFS_CLIENT_TFSFILE_H_

#include <sys/types.h>
#include <fcntl.h>

#include "common/error_msg.h"
#include "common/func.h"
#include "message/client.h"
#include "message/client_pool.h"
#include "fsname.h"

namespace tfs
{
  namespace client
  {
    enum InnerFilePhase
    {
      FILE_PHASE_CREATE_FILE = 1,
      FILE_PHASE_WRITE_DATA,
      FILE_PHASE_CLOSE_FILE
    };

    class TfsSession;
    class TfsFile
    {
    public:
      TfsFile();
      TfsFile(uint32_t block_id, common::VUINT64& ds_list);
      virtual ~TfsFile();

      // virtual level operation
      virtual int open(const char* file_name, const char *suffix, int flags, ... ) = 0;
      virtual int read(void* buf, size_t count) = 0;
      virtual int write(const void* buf, size_t count) = 0;
      virtual off_t lseek(off_t offset, int whence) = 0;
      virtual ssize_t pread(void* buf, size_t count, off_t offset) = 0;
      virtual ssize_t pwrite(const void* buf, size_t count, off_t offset) = 0;
      virtual int close() = 0;

      const char* get_file_name();
      void set_session(TfsSession* tfs_session);

    protected:
      // common operation
      int open_ex(const char* file_name, const char *suffix, const int32_t mode);
      ssize_t read_ex(void* buf, size_t count);
      ssize_t write_ex(const void* buf, size_t count);
      off_t lseek_ex(off_t offset, int whence);
      ssize_t pread_ex(void* buf, size_t count, off_t offset);
      ssize_t pwrite_ex(const void* buf, size_t count, off_t offset);
      int close_ex();
      
    protected:
      static const int64_t WAIT_TIME_OUT = 5000;

    protected:
      FSName fsname_;
      TfsSession* tfs_session_;
      int32_t flag_;
      int32_t offset_;
      common::VUINT64 ds_list_;
      const char* write_buf;
    };
  }
}
#endif  // TFS_CLIENT_TFSFILE_H_
