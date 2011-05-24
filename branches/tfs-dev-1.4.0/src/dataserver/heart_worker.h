#ifndef TFS_DATASERVER_HEARTWORKER_H_
#define TFS_DATASERVER_HEARTWORKER_H_

#include <Timer.h>
namespace tfs
{
  namespace dataserver
  {
    class DataService;
    class HeartWorker : public tbutil::TimerTask
    {
      public:
        HeartWorker(DataService* dataservice);
        ~HeartWorker();
        void runTimerTask();
        int stop_heart();
      private:
        void* do_heart();
        int run_heart();
        void send_blocks_to_ns(const int32_t who);
      private:
        DataService* dataservice_;
    };
    typedef tbutil::Handle<HeartWorker> HeartWorkerPtr;

    class HeartManager
    {
      public:
        HeartManager();
        ~HeartManager();
        int initialize(tbutil::TimerPtr timer, const int64_t schedule_interval, DataService* dataservice);
        int wait_for_shut_down();
        int destroy();
      private:
        tbutil::TimerPtr timer_;
        HeartWorkerPtr heart_worker_;
    };
  }
}

#endif

