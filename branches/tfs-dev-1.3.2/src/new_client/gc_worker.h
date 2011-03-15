#ifndef TFS_CLIENT_GCWORKER_H_
#define TFS_CLIENT_GCWORKER_H_

#include "tfs_client_api.h"
#include "local_key.h"
#include "gc_file.h"
#include <Timer.h>

namespace tfs
{
  namespace client
  {
    enum GcType
    {
      GC_EXPIRED_LOCAL_KEY = 0,
      GC_GARBAGE_FILE
    };

    class GcWorker : public tbutil::TimerTask
    {
    public:
      GcWorker();
      ~GcWorker();

    public:
      virtual void runTimerTask();
      int destroy();

    private:
      int start_gc(GcType gc_type);
      int get_expired_file(const char* path);
      int check_file(const char* path, const char* file, time_t now);
      int check_lock(const char* file);
      int do_gc(GcType gc_type);

      template<class T> int do_gc_ex(T& meta, const char* file_name, const char* addr);
      template<class T> int do_unlink(T& seg_info, const char* addr);

    private:
      DISALLOW_COPY_AND_ASSIGN(GcWorker);
      bool destroy_;
      TfsClient* tfs_client_;
      LocalKey local_key_;
      GcFile gc_file_;
      std::vector<std::string> file_;
    };
    typedef tbutil::Handle<GcWorker> GcWorkerPtr;

    class GcManager
    {
      public:
        GcManager();
        ~GcManager();

      public:
        int initialize(tbutil::TimerPtr timer, const int64_t schedule_interval_s);
        int wait_for_shut_down();
        int destroy();
        int reset_schedule_interval(const int64_t schedule_interval_s);

      private:
        DISALLOW_COPY_AND_ASSIGN(GcManager);
      private:
        bool destroy_;
        tbutil::TimerPtr timer_;
        GcWorkerPtr gc_worker_;
    };

  }
}

#endif
