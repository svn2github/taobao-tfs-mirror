#ifndef TFS_DATASERVER_CHECKWORKER_H_
#define TFS_DATASERVER_CHECKWORKER_H_

#include <Timer.h>
#include "client/fsname.h"
namespace tfs
{
  namespace dataserver
  {
    class DataService;
    class CheckWorker : public tbutil::TimerTask
    {
      public:
        CheckWorker(DataService* dataservice);
        ~CheckWorker();
        void runTimerTask();
      public:
        bool destroy_;
      private:
        int do_check();
        DataService* dataservice_;
        static const uint32_t READ_STAT_LOG_BUFFER_LEN = 100;
    };
    typedef tbutil::Handle<CheckWorker> CheckWorkerPtr;

    class CheckManager
    {
      public:
        CheckManager();
        ~CheckManager();
        int initialize(tbutil::TimerPtr timer, const int64_t schedule_interval, DataService* dataservice);
        int wait_for_shut_down();
        int destroy();
      private:
        CheckWorkerPtr check_worker_;
        tbutil::TimerPtr timer_;
    };
  }
}

#endif

