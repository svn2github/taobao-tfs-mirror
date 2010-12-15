#ifndef TFS_CLIENT_TFSFILE_H_
#define TFS_CLIENT_TFSFILE_H_

#include <sys/types.h>
#include <fcntl.h>

#include "common/error_msg.h"
#include "common/func.h"
#include "common/client_manager.h"
#include "message/client.h"
#include "message/client_pool.h"

#include "tfs_session.h"
#include "fsname.h"
#include "local_key.h"

namespace tfs
{
  namespace client
  {
    enum InnerFilePhase
    {
      FILE_PHASE_CREATE_FILE = 1,
      FILE_PHASE_WRITE_DATA,
      FILE_PHASE_CLOSE_FILE,
      FILE_PHASE_READ_FILE
    };

    enum
    {
      TFS_FILE_OPEN_YES = 0,
      TFS_FILE_OPEN_NO
    };

    enum
    {
      TFS_FILE_EOF_YES = 0,
      TFS_FILE_EOF_NO
    };

    class TfsSession;
    class TfsFile
    {
    public:
      TfsFile();
      TfsFile(std::vector<SegmentData*>& seg_list);
      TfsFile(uint32_t block_id, common::VUINT64& ds_list);
      virtual ~TfsFile();

      // virtual level operation
      virtual int open(const char* file_name, const char *suffix, int flags, ... );
      virtual int read(void* buf, size_t count);
      virtual int write(const void* buf, size_t count);
      virtual off_t lseek(off_t offset, int whence);
      virtual ssize_t pread(void* buf, size_t count, off_t offset);
      virtual ssize_t pwrite(const void* buf, size_t count, off_t offset);
      virtual int close();

      const char* get_file_name();
      void set_session(TfsSession* tfs_session);
      int process(const InnerFilePhase file_phase);

    protected:
      // common operation
      int open_ex(const char* file_name, const char *suffix, const int32_t mode);
      ssize_t read_ex(void* buf, size_t count);
      ssize_t write_ex(const void* buf, size_t count);
      off_t lseek_ex(off_t offset, int whence);
      ssize_t pread_ex(void* buf, size_t count, off_t offset);
      ssize_t pwrite_ex(const void* buf, size_t count, off_t offset);
      int close_ex();

      int connect_ds();
      int create_filename();

    private:
      int do_async_request(const InnerFilePhase file_phase, const int64_t wait_id, const int32_t index);
      int do_async_response(const InnerFilePhase file_phase, tbnet::Packet* packet, const int32_t index);

      int async_req_create_file(const int64_t wait_id, const int32_t index);
      int async_rsp_create_file(tbnet::Packet* packet, const int32_t index);

      int async_req_read_file(const int64_t wait_id, const int32_t index);
      int async_rsp_read_file(tbnet::Packet* packet, const int32_t index);

      int async_req_write_data(const int64_t wait_id, const int32_t index);
      int async_rsp_write_data(tbnet::Packet* packet, const int32_t index);

      int async_req_close_file(const int64_t wait_id, const int32_t index);
      int async_rsp_close_file(tbnet::Packet* packet, const int32_t index);

    protected:
      static const int64_t WAIT_TIME_OUT = 5000;

    protected:
      FSName fsname_;
      int32_t flags_;
      int32_t is_open_;
      int32_t eof_;
      int64_t offset_;
      //sync flag
      int32_t option_flag_;
      //uint64_t file_number_;
      //int32_t pri_ds_index_;
      //int32_t last_elect_ds_id_;
      //message::Client* client_;
      //common::VUINT64 ds_list_;
      TfsSession* tfs_session_;
      char error_message_[common::ERR_MSG_SIZE];
      std::vector<SegmentData*> processing_seg_list_;
    };
  }
}
#endif  // TFS_CLIENT_TFSFILE_H_
