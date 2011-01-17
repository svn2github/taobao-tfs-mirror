#ifndef TFS_CLIENT_GCWORKER_H_
#define TFS_CLIENT_GCWORKER_H_

#include "tfs_client_api.h"
#include "local_key.h"
#include "gc_file.h"

namespace tfs
{
  namespace client
  {
    enum GcType
    {
      GC_EXPIRED_LOCAL_KEY = 0,
      GC_GARBAGE_FILE
    };

    const int32_t GC_EXPIRED_TIME = 86400; // 10 days

    class GcWorker
    {
    public:
      GcWorker();
      ~GcWorker();

      static void* start(void* arg);
      int do_start();

    private:
      int start_gc(GcType gc_type);
      int get_expired_file(const char* path);
      int check_file(const char* path, const char* file, time_t now);
      int check_lock(const char* file);
      int do_gc(GcType gc_type);

      template<class T> int do_gc_ex(T& meta, const char* file_name, const char* addr);
      template<class T> int do_unlink(T& seg_info, const char* addr);

    private:
      TfsClient* tfs_client_;
      LocalKey local_key_;
      GcFile gc_file_;
      std::vector<std::string> file_;
    };
  }
}

#endif
