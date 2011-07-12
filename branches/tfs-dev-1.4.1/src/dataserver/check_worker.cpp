#include "dataservice.h"
#include "check_worker.h"

namespace tfs
{
  namespace dataserver
  {
    using namespace common;
    using namespace client;
    CheckManager::CheckManager()
    {
    }

    CheckManager::~CheckManager()
    {
    }

    int CheckManager::initialize(tbutil::TimerPtr timer, const int64_t schedule_interval, DataService* dataservice)
    {
      timer_ = timer;
      if (dataservice == NULL)
      {
        return TFS_ERROR;
      }
      if (schedule_interval > 0)
      {
        check_worker_ = new CheckWorker(dataservice);
        timer_->scheduleRepeated(check_worker_, tbutil::Time::microSeconds(schedule_interval));
      }
      return TFS_SUCCESS;
    }
    int CheckManager::wait_for_shut_down()
    {
      if ((0 != timer_) && (0 != check_worker_))
      {
        timer_->cancel(check_worker_);
      }
      return TFS_SUCCESS;
    }

    int CheckManager::destroy()
    {
      if (0 != check_worker_)
      {
        check_worker_->destroy_ = true;
      }
      return TFS_SUCCESS;
    }

    CheckWorker::CheckWorker(DataService* dataservice)
    {
      dataservice_ = dataservice;
    }

    CheckWorker::~CheckWorker()
    {
    }

    void CheckWorker::runTimerTask()
    {
      this->do_check();
    }

    int CheckWorker::do_check()
    {
      static int32_t last_rlog = 0;
      tzset();
      static int zonesec = 86400 + timezone;
      //check datafile
      dataservice_->data_management_.gc_data_file();
      if (destroy_)
        return TFS_SUCCESS;

      //check repair block
      dataservice_->block_checker_.consume_repair_task();
      if (destroy_)
        return TFS_SUCCESS;

      //check clonedblock
      dataservice_->repl_block_->expire_cloned_block_map();
      if (destroy_)
        return TFS_SUCCESS;

      // check compact block
      dataservice_->compact_block_->expire_compact_block_map();
      if (destroy_)
        return TFS_SUCCESS;

      int32_t current_time = time(NULL);
      // check log: write a new log everyday and expire error block
      if (current_time % 86400 >= zonesec && current_time % 86400 < zonesec + 300 && last_rlog < current_time - 600)
      {
        last_rlog = current_time;
        TBSYS_LOGGER.rotateLog(SYSPARAM_DATASERVER.log_file_.c_str());
        dataservice_->READ_STAT_LOGGER.rotateLog(SYSPARAM_DATASERVER.read_stat_log_file_.c_str());
        dataservice_->WRITE_STAT_LOGGER.rotateLog(SYSPARAM_DATASERVER.write_stat_log_file_.c_str());

        // expire error block
        dataservice_->block_checker_.expire_error_block();
      }
      if (destroy_)
        return TFS_SUCCESS;

      // check stat
      dataservice_->count_mutex_.lock();
      dataservice_->visit_stat_.check_visit_stat();
      dataservice_->count_mutex_.unlock();

      if (dataservice_->read_stat_buffer_.size() >= READ_STAT_LOG_BUFFER_LEN)
      {
        int64_t time_start = tbsys::CTimeUtil::getTime();
        TBSYS_LOG(INFO, "---->START DUMP READ INFO. buffer size: %u, start time: %" PRI64_PREFIX "d", dataservice_->read_stat_buffer_.size(), time_start);
        dataservice_->read_stat_mutex_.lock();
        int32_t per_log_size = FILE_NAME_LEN + 2; //two space
        char read_log_buffer[READ_STAT_LOG_BUFFER_LEN * per_log_size + 1];
        int32_t loops = dataservice_->read_stat_buffer_.size() / READ_STAT_LOG_BUFFER_LEN;
        int32_t remain = dataservice_->read_stat_buffer_.size() % READ_STAT_LOG_BUFFER_LEN;
        int32_t index = 0, offset;
        int32_t inner_loop = 0;
        //flush all record to log
        for (int32_t i = 0; i <= loops; ++i)
        {
          memset(read_log_buffer, 0, READ_STAT_LOG_BUFFER_LEN * per_log_size + 1);
          offset = 0;
          if (i == loops)
          {
            inner_loop = remain;
          }
          else
          {
            inner_loop = READ_STAT_LOG_BUFFER_LEN;
          }

          for (int32_t j = 0; j < inner_loop; ++j)
          {
            index = i * READ_STAT_LOG_BUFFER_LEN + j;
            FSName fs_name;
            fs_name.set_block_id(dataservice_->read_stat_buffer_[index].first);
            fs_name.set_file_id(dataservice_->read_stat_buffer_[index].second);
            /* per_log_size + 1: add null */
            snprintf(read_log_buffer + offset, per_log_size + 1, "  %s", fs_name.get_name());
            offset += per_log_size;
          }
          read_log_buffer[offset] = '\0';
          READ_STAT_LOG(INFO, "%s", read_log_buffer);
        }
        dataservice_->read_stat_buffer_.clear();
        dataservice_->read_stat_mutex_.unlock();
        int64_t time_end = tbsys::CTimeUtil::getTime();
        TBSYS_LOG(INFO, "Dump read info.end time: %" PRI64_PREFIX "d. Cost Time: %" PRI64_PREFIX "d\n", time_end, time_end - time_start);
      }
      if (destroy_)
        return TFS_SUCCESS;
      dataservice_->data_management_.remove_data_file();

      return TFS_SUCCESS;
    }
  }
}
