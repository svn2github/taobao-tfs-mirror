#include "dataservice.h"
#include "heart_worker.h"
#include "common/func.h"

namespace tfs
{
  namespace dataserver
  {
    using namespace common;
    using namespace message;

    HeartManager::HeartManager() :
      timer_(0),
      heart_worker_(0)
    {
    }

    HeartManager::~HeartManager()
    {
    }

    int HeartManager::initialize(tbutil::TimerPtr timer, int64_t schedule_interval, DataService* dataservice)
    {
      timer_ = timer;
      if (schedule_interval > 0)
      {
        heart_worker_ = new HeartWorker(dataservice);
        timer_->scheduleRepeated(heart_worker_, tbutil::Time::microSeconds(schedule_interval));
      }
      return TFS_SUCCESS;
    }

    int HeartManager::wait_for_shut_down()
    {
      if ((0 != timer_) && (0 != heart_worker_))
      {
        timer_->cancel(heart_worker_);
      }
      return TFS_SUCCESS;
    }

    int HeartManager::destroy()
    {
      if (0 != heart_worker_)
      {
        heart_worker_->stop_heart();
      }
      return TFS_SUCCESS;
    }

    HeartWorker::HeartWorker(DataService* dataservice)
    {
      dataservice_ = dataservice;
    }

    HeartWorker::~HeartWorker()
    {
    }

    void HeartWorker::runTimerTask()
    {
      this->run_heart();
    }

    int HeartWorker::run_heart()
    {
      //sleep for a while, waiting for listen port establish
      dataservice_->data_management_.get_ds_filesystem_info(dataservice_->data_server_info_.block_count_,
        dataservice_->data_server_info_.use_capacity_,
        dataservice_->data_server_info_.total_capacity_);
      dataservice_->data_server_info_.current_load_ = Func::get_load_avg();
      dataservice_->data_server_info_.current_time_ = time(NULL);
      if (DATASERVER_STATUS_DEAD == dataservice_->data_server_info_.status_)
      {
        return TFS_ERROR;
      }
      send_blocks_to_ns(0);
      send_blocks_to_ns(1);


      dataservice_->cpu_metrics_.summary();
      return TFS_SUCCESS;
    }

    int HeartWorker::stop_heart()
    {
      TBSYS_LOG(INFO, "stop heartbeat...");
      dataservice_->data_server_info_.status_ = DATASERVER_STATUS_DEAD;
      send_blocks_to_ns(0);
      send_blocks_to_ns(1);
      return TFS_SUCCESS;
    }

    void HeartWorker::send_blocks_to_ns(const int32_t who)
    {
      if (0 != who && 1 != who)
        return;

      // ns who is not set
      if (!dataservice_->set_flag_[who])
        return;

      bool reset_need_send_blockinfo_flag = dataservice_->data_management_.get_all_logic_block_size() <= 0;
      SetDataserverMessage req_sds_msg;
      req_sds_msg.set_ds(&dataservice_->data_server_info_);
      if (dataservice_->need_send_blockinfo_[who])
      {
        req_sds_msg.set_has_block(HAS_BLOCK_FLAG_YES);

        list<LogicBlock*> logic_block_list;
        dataservice_->data_management_.get_all_logic_block(logic_block_list);
        for (list<LogicBlock*>::iterator lit = logic_block_list.begin(); lit != logic_block_list.end(); ++lit)
        {
          TBSYS_LOG(DEBUG, "send block to ns: %d, blockid: %u\n", who, (*lit)->get_logic_block_id());
          req_sds_msg.add_block((*lit)->get_block_info());
        }
      }

      Message *message = dataservice_->hb_client_[who]->call(&req_sds_msg);
      if (NULL != message)
      {
        if (RESP_HEART_MESSAGE == message->get_message_type())
        {
          RespHeartMessage* resp_hb_msg = dynamic_cast<RespHeartMessage*>(message);
          dataservice_->need_send_blockinfo_[who] = 0;
          if (resp_hb_msg->get_status() == HEART_NEED_SEND_BLOCK_INFO)
          {
            TBSYS_LOG(DEBUG, "nameserver %d ask for send block\n", who + 1);
            dataservice_->need_send_blockinfo_[who] = 1;
          }
          else if (resp_hb_msg->get_status() == HEART_EXP_BLOCK_ID)
          {
            TBSYS_LOG(INFO, "nameserver %d ask for expire block\n", who + 1);
            dataservice_->data_management_.add_new_expire_block(resp_hb_msg->get_expire_blocks(), NULL, resp_hb_msg->get_new_blocks());
          }

          int32_t old_sync_mirror_status = dataservice_->sync_mirror_status_;
          dataservice_->sync_mirror_status_ = resp_hb_msg->get_sync_mirror_status();

          if (old_sync_mirror_status != dataservice_->sync_mirror_status_)
          {
           //has modified
            if ((dataservice_->sync_mirror_status_ & 1))
            {
              TBSYS_LOG(ERROR, "sync pause.");
              dataservice_->sync_mirror_->set_pause(1);
            }
            if ((dataservice_->sync_mirror_status_ & 3) == 2)
            {
              TBSYS_LOG(ERROR, "sync start.");
              dataservice_->sync_mirror_->set_pause(0);
            }
            if ((dataservice_->sync_mirror_status_ & 4))
            {
              TBSYS_LOG(ERROR, "sync disable log.");
              dataservice_->sync_mirror_->disable_log();
            }
            if ((dataservice_->sync_mirror_status_ & 12) == 8)
            {
              TBSYS_LOG(ERROR, "sync reset log.");
              dataservice_->sync_mirror_->reset_log();
            }
            if ((dataservice_->sync_mirror_status_ & 16))
            {
              TBSYS_LOG(ERROR, "sync set slave ip.");
              dataservice_->sync_mirror_->reload_slave_ip();
            }
          }

        }
        tbsys::gDelete(message);
      }
      else
      {
        TBSYS_LOG(ERROR, "Message is NULL");
        dataservice_->hb_client_[who]->disconnect();
      }
    }
  }
}
