#ifndef TFS_CLIENT_TFSFILE_H_
#define TFS_CLIENT_TFSFILE_H_

#include <sys/types.h>
#include <fcntl.h>

#include "common/error_msg.h"
#include "common/func.h"
#include "common/lock.h"
#include "common/client_define.h"
#include "message/client_manager.h"
#include "tfs_session.h"
#include "fsname.h"
#include "local_key.h"

namespace tfs
{
  namespace client
  {
    enum InnerFilePhase
    {
      FILE_PHASE_OPEN_FILE = 0,
      FILE_PHASE_CREATE_FILE,
      FILE_PHASE_WRITE_DATA,
      FILE_PHASE_CLOSE_FILE,
      FILE_PHASE_READ_FILE,
      FILE_PHASE_STAT_FILE,
      FILE_PHASE_UNLINK_FILE
    };

    struct PhaseStatus
    {
      InnerFilePhase pre_phase_;
      SegmentStatus status_;
    };

    // CAUTION: depend on InnerFilePhase member sequence, maybe change to map
    const static PhaseStatus phase_status[] = {
      {FILE_PHASE_OPEN_FILE, SEG_STATUS_OPEN_OVER}, // dummy open.
      {FILE_PHASE_OPEN_FILE, SEG_STATUS_CREATE_OVER}, // create.
      {FILE_PHASE_CREATE_FILE, SEG_STATUS_BEFORE_CLOSE_OVER}, // write
      {FILE_PHASE_WRITE_DATA, SEG_STATUS_ALL_OVER}, // close. just read write stat unlink is same previous phase
      {FILE_PHASE_OPEN_FILE, SEG_STATUS_ALL_OVER}, // read.
      {FILE_PHASE_OPEN_FILE, SEG_STATUS_ALL_OVER}, // stat.
      {FILE_PHASE_OPEN_FILE, SEG_STATUS_ALL_OVER} // unlink.
    };

    enum
    {
      TFS_FILE_OPEN_YES = 0,
      TFS_FILE_OPEN_NO
    };

    class TfsFile
    {
    public:
      TfsFile();
      virtual ~TfsFile();

      // virtual interface
      virtual int open(const char* file_name, const char* suffix, const int flags, ... ) = 0;
      virtual int64_t read(void* buf, int64_t count) = 0;
      virtual int64_t write(const void* buf, int64_t count) = 0;
      virtual int64_t lseek(int64_t offset, int whence) = 0;
      virtual int64_t pread(void* buf, int64_t count, int64_t offset) = 0;
      virtual int64_t pwrite(const void* buf, int64_t count, int64_t offset) = 0;
      virtual int fstat(TfsFileStat* file_info, const TfsStatFlag mode = NORMAL_STAT) = 0;
      virtual int close() = 0;
      virtual int unlink(const char* file_name, const char* suffix, const TfsUnlinkType action) = 0;

      const char* get_file_name();
      void set_session(TfsSession* tfs_session);

    protected:
      // virtual level operation
      virtual int get_segment_for_read(int64_t offset, char* buf, int64_t count) = 0;
      virtual int get_segment_for_write(int64_t offset, const char* buf, int64_t count) = 0;
      virtual int get_size_for_rw(const int64_t check_size, const int64_t count, int64_t& cur_size, bool& not_end) = 0;
      virtual int read_process() = 0;
      virtual int write_process() = 0;
      virtual int finish_write_process(int status) = 0;
      virtual int close_process() = 0;
      virtual int unlink_process() = 0;

    protected:
      // common operation
      void destroy_seg();
      int get_meta_segment(const int64_t offset, char* buf, const int64_t count);
      int process(const InnerFilePhase file_phase);
      int32_t finish_read_process(int status, int64_t& read_size);

      int open_ex(const char* file_name, const char *suffix, const int32_t flags);
      int64_t read_ex(void* buf, const int64_t count, const int64_t offset, const bool modify = true);
      int64_t write_ex(const void* buf, int64_t count, int64_t offset, bool modify = true);
      int64_t lseek_ex(int64_t offset, int whence);
      int64_t pread_ex(void* buf, int64_t count, int64_t offset);
      int64_t pwrite_ex(const void* buf, int64_t count, int64_t offset);
      int fstat_ex(common::FileInfo* file_info, const TfsStatFlag mode);
      int close_ex();
      int get_size_for_rw_ex(const int64_t check_size, const int64_t count, int64_t& cur_size, bool& not_end, const int64_t per_size);

    private:
      int do_async_request(const InnerFilePhase file_phase, const int64_t wait_id, const int32_t index);
      int do_async_response(const InnerFilePhase file_phase, message::Message* packet, const int32_t index);

      int async_req_create_file(const int64_t wait_id, const int32_t index);
      int async_rsp_create_file(message::Message* packet, const int32_t index);

      int async_req_read_file(const int64_t wait_id, const int32_t index);
      int async_rsp_read_file(message::Message* packet, const int32_t index);

      int async_req_write_data(const int64_t wait_id, const int32_t index);
      int async_rsp_write_data(message::Message* packet, const int32_t index);

      int async_req_close_file(const int64_t wait_id, const int32_t index);
      int async_rsp_close_file(message::Message* packet, const int32_t index);

      int async_req_stat_file(const int64_t wait_id, const int32_t index);
      int async_rsp_stat_file(message::Message* packet, const int32_t index);

      int async_req_unlink_file(const int64_t wait_id, const int32_t index);
      int async_rsp_unlink_file(message::Message* packet, const int32_t index);

    public:
      common::RWLock rw_lock_;

    protected:
      static const int64_t WAIT_TIME_OUT = 3000000;
      static const int64_t CLIENT_TRY_COUNT = 3;

      static const int64_t BATCH_COUNT = 8;
      static const int64_t BATCH_SIZE = common::SEGMENT_SIZE * BATCH_COUNT;

    protected:
      FSName fsname_;
      int32_t flags_;
      int32_t is_open_;
      int32_t eof_;
      int64_t offset_;
      SegmentData* meta_seg_;
      int32_t option_flag_; //sync flag
      TfsSession* tfs_session_;
      SEG_DATA_LIST processing_seg_list_;
    };
  }
}
#endif  // TFS_CLIENT_TFSFILE_H_
