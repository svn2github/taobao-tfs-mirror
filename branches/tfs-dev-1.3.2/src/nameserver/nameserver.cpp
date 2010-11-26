/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   qushan<qushan@taobao.com>
 *      - modify 2009-03-27
 *   duanfei <duanfei@taobao.com>
 *      - modify 2010-04-23
 *
 */
#include <Memory.hpp>
#include "common/error_msg.h"
#include "nameserver.h"
#include "common/config_item.h"
#include "message/client_pool.h"
#include <iterator>

using namespace tfs::common;
using namespace tfs::message;

namespace tfs
{
  namespace nameserver
  {

    OwnerCheckTimerTask::OwnerCheckTimerTask(NameServer* fsnm) :
      fs_name_system_(fsnm)
    {
      main_task_queue_size_ = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_TASK_MAX_QUEUE_SIZE, 100);
      int32_t percent_size = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_TASK_PRECENT_SEC_SIZE, 1);
      owner_check_time_ = tbutil::Time::microSeconds(main_task_queue_size_ * percent_size * 1000);
      max_owner_check_time_ = owner_check_time_ * 4;
      NsRuntimeGlobalInformation* ngi = fs_name_system_->get_ns_global_info();
      int64_t now = tbsys::CTimeUtil::getTime();
      ngi->last_owner_check_time_ = tbutil::Time::microSeconds(now);
      ngi->last_push_owner_check_packet_time_ = tbutil::Time::microSeconds(now) + owner_check_time_;
TBSYS_LOG    (INFO, "ownerchecktime(%"PRI64_PREFIX"d)(us), maxownerchecktime(%"PRI64_PREFIX"u)(us)", owner_check_time_.toMicroSeconds(),
        max_owner_check_time_.toMicroSeconds());
  }

  OwnerCheckTimerTask::~OwnerCheckTimerTask()
  {

  }

  void OwnerCheckTimerTask::runTimerTask()
  {
    bool bret = false;
    NsRuntimeGlobalInformation* ngi = fs_name_system_->get_ns_global_info();
    do
    {
      ngi->dump(TBSYS_LOG_LEVEL(DEBUG));
      if (ngi->owner_status_ < NS_STATUS_INITIALIZED)
      return;

      if (ngi->last_push_owner_check_packet_time_ > ngi->last_owner_check_time_ + max_owner_check_time_)
      {
        TBSYS_LOG(
            INFO,
            "last push owner check packet time(%"PRI64_PREFIX"d)(us) > max owner check time(%"PRI64_PREFIX"d)(us), nameserver dead, modify owner status(uninitialize)",
            ngi->last_push_owner_check_packet_time_.toMicroSeconds(),
            (ngi->last_owner_check_time_ + max_owner_check_time_).toMicroSeconds());
        tbutil::Mutex::Lock lock(*ngi);
        ngi->owner_status_ = NS_STATUS_UNINITIALIZE;//modify owner status
        return;
      }

      if ((ngi->last_push_owner_check_packet_time_ > ngi->last_owner_check_time_ + owner_check_time_) // >= 1
          && (ngi->last_push_owner_check_packet_time_ <= ngi->last_owner_check_time_ + max_owner_check_time_)) // <= 4

      {
        tbutil::Mutex::Lock lock(*ngi);
        ngi->last_push_owner_check_packet_time_ = tbutil::Time::microSeconds(tbsys::CTimeUtil::getTime());
        TBSYS_LOG(
            DEBUG,
            "last push owner check packet time(%"PRI64_PREFIX"d)(us) >= one check time(%"PRI64_PREFIX"d)(us) or last push owner check packet time(%"PRI64_PREFIX"d)(us) <= four check time(%"PRI64_PREFIX"d)(us) must be return...",
            ngi->last_push_owner_check_packet_time_.toMicroSeconds(),
            (ngi->last_owner_check_time_ + owner_check_time_).toMicroSeconds(),
            ngi->last_push_owner_check_packet_time_.toMicroSeconds(),
            (ngi->last_owner_check_time_ + max_owner_check_time_).toMicroSeconds());
        return;
      }

      OwnerCheckMessage* message = new OwnerCheckMessage();
      bret = fs_name_system_->get_packet_queue_thread()->push(message, main_task_queue_size_, false);
      TBSYS_LOG(DEBUG, "push owner check packet in taskqueue, bret(%d)", bret);
      tbutil::Mutex::Lock lock(*ngi);
      ngi->last_push_owner_check_packet_time_= tbutil::Time::microSeconds(tbsys::CTimeUtil::getTime());
    }
    while (!bret && ngi->owner_status_ == NS_STATUS_INITIALIZED);
  }

  NameServer::NameServer() :
  meta_mgr_(this), timer_(new tbutil::Timer()), master_slave_heart_mgr_(&meta_mgr_, timer_), main_task_queue_size_(
      100), replicate_thread_(meta_mgr_), compact_thread_(meta_mgr_), redundant_thread_(meta_mgr_), heart_mgr_(
      meta_mgr_, replicate_thread_), pass_mgr_(meta_mgr_)
  {
    memset(&ns_global_info_, 0, sizeof(ns_global_info_));
  }

  NameServer::~NameServer()
  {
  }

  // run threads
  void NameServer::run(tbsys::CThread *thread, void *)
  {
    if (thread == &check_block_thread_)
    do_check_blocks();
    else if (thread == &check_ds_thread_)
    do_check_ds();
    else if (thread == &balance_thread_)
    do_balance();
    else if (thread == &time_out_thread_)
    do_time_out();
  }

  // start nameserver service
  int NameServer::start()
  {
    int ret = TFS_SUCCESS;

    // initialize ns_global_info_
    if (initialize_ns_global_info() != TFS_SUCCESS)
    return EXIT_GENERAL_ERROR;

    // listen to the port, get packets
    streamer_.set_packet_factory(&msg_factory_);
    CLIENT_POOL.init_with_transport(&transport_);

    int32_t server_port = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_PORT);
    char spec[SPEC_LEN];
    sprintf(spec, "tcp::%d", server_port);
    if (transport_.listen(spec, &streamer_, this) == NULL)
    {
      TBSYS_LOG(ERROR, "listen port failed(%d)", server_port);
      return EXIT_NETWORK_ERROR;
    }
    //start thread for handling heartbeat from the other ns
    master_slave_heart_mgr_.initialize();

    transport_.start();

    //send msg to peer
    //if we're the slave ns & send msg to master failed,we must wait.
    //if we're the master ns, just ignore this failure.
    ret = get_peer_role();
    //ok,now we can check the role of peer
    if ((ret != TFS_SUCCESS)
        ||(ns_global_info_.owner_role_ == ns_global_info_.other_side_role_))
    {
      TBSYS_LOG(ERROR, "iret != TFS_SUCCESS or owner role(%s) == other side role(%s), must be exit...",
          ns_global_info_.owner_role_ == NS_ROLE_MASTER ? "master" : "slave", ns_global_info_.other_side_role_
          == NS_ROLE_MASTER ? "master" : "slave");
      return EXIT_GENERAL_ERROR;
    }

    int32_t block_chunk_num = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_BLOCK_CHUNK_NUM, 13);
    if ((ret = meta_mgr_.initialize(block_chunk_num)) != TFS_SUCCESS)
    return ret;

    // start thread for handling main task
    // start threads for handling heartbeat from ds.
    initialize_handle_task_and_heart_threads();

    //if we're the master ns,we can start service now.change status to INITIALIZED.
    //TODO lock
    if (ns_global_info_.owner_role_ == NS_ROLE_MASTER)
    {
      ns_global_info_.owner_status_ = NS_STATUS_INITIALIZED;
    }
    else
    { //if we're the slave ns, we must sync data from the master ns.
      ns_global_info_.owner_status_ = NS_STATUS_ACCEPT_DS_INFO;
      if (wait_for_ds_report() != TFS_SUCCESS)//in wait_for_ds_report,someone killed me
      return EXIT_GENERAL_ERROR; //the signal handler have already called stop()
      ns_global_info_.owner_status_ = NS_STATUS_INITIALIZED;
    }

    // start threads
    initialize_handle_threads();

    //start heartbeat loop
    master_heart_task_ = new MasterHeartTimerTask(&meta_mgr_);
    ret = timer_->scheduleRepeated(master_heart_task_, tbutil::Time::seconds(SYSPARAM_NAMESERVER.heart_interval_));
    if (ret < 0)
    return EXIT_GENERAL_ERROR;
    slave_heart_task_ = new SlaveHeartTimerTask(&meta_mgr_, timer_);
    ret = timer_->scheduleRepeated(slave_heart_task_, tbutil::Time::seconds(SYSPARAM_NAMESERVER.heart_interval_));
    if (ret < 0)
    return EXIT_GENERAL_ERROR;
    int32_t percent_size = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_TASK_PRECENT_SEC_SIZE, 1);
    int32_t owner_check_interval = main_task_queue_size_ * percent_size * 1000;
    owner_check_task_ = new OwnerCheckTimerTask(this);
    ret = timer_->scheduleRepeated(owner_check_task_, tbutil::Time::milliSeconds(owner_check_interval));
    if (ret < 0)
    return EXIT_GENERAL_ERROR;
    TBSYS_LOG(DEBUG, "owner_check_interval(%d)(us)", owner_check_interval);
    check_owner_is_master_task_ = new CheckOwnerIsMasterTimerTask(&meta_mgr_);
    ret = timer_->scheduleRepeated(check_owner_is_master_task_, tbutil::Time::seconds(
            SYSPARAM_NAMESERVER.heart_interval_));
    if (ret < 0)
    return EXIT_GENERAL_ERROR;
    return TFS_SUCCESS;
  }

  // stop service
  int NameServer::stop()
  {
    if (ns_global_info_.destroy_flag_ == NS_DESTROY_FLAGS_YES)
    {
      return TFS_SUCCESS;
    }

    transport_.stop();
    {
      tbutil::Mutex::Lock lock(ns_global_info_);
      ns_global_info_.owner_status_ = NS_STATUS_UNINITIALIZE;
    }
    ns_global_info_.destroy_flag_ = NS_DESTROY_FLAGS_YES;

    timer_->cancel(master_heart_task_);
    timer_->cancel(slave_heart_task_);
    timer_->cancel(owner_check_task_);
    timer_->cancel(check_owner_is_master_task_);
    pass_mgr_.destroy();
    heart_mgr_.stop(true);
    master_slave_heart_mgr_.destroy();
    meta_mgr_.destroy();
    replicate_thread_.destroy();
    compact_thread_.stop();
    main_task_queue_thread_.stop();
    timer_->destroy();
    return TFS_SUCCESS;
  }

  int NameServer::wait()
  {
    heart_mgr_.wait();
    master_slave_heart_mgr_.wait_for_shut_down();
    meta_mgr_.wait_for_shut_down();

    // join thread
    check_ds_thread_.join();
    check_block_thread_.join();
    balance_thread_.join();
    time_out_thread_.join();

    replicate_thread_.wait_for_shut_down();
    compact_thread_.wait();
    main_task_queue_thread_.wait();
    transport_.wait();

    int ret = 0;
    if ((ret = meta_mgr_.save()) != TFS_SUCCESS)
    {
      return ret;
    }
    return ret;
  }

  // check the dead ds list and the writable ds list
  int NameServer::check_ds(const time_t now)
  {
    VUINT64 dead_ds_list, write_ds_list;
    int32_t expire_time = now - SYSPARAM_NAMESERVER.ds_dead_time_ * 4;
    // get the list of the dead ds and available ds.
    meta_mgr_.get_block_ds_mgr().check_ds(SYSPARAM_NAMESERVER.ds_dead_time_, dead_ds_list, write_ds_list);

    // check the status of the dead ds
    // if the info is null or ds is still alive, then skip
    // else mark the ds and exclude it from the balance
    const uint32_t dead_ds_list_size = dead_ds_list.size();
    uint64_t ds_id = 0;
    ServerCollect* server_collect = NULL;
    DataServerStatInfo* ds_stat_info = NULL;
    for (uint32_t i = 0; i < dead_ds_list_size; ++i)
    {
      ds_id = dead_ds_list[i];
      server_collect = meta_mgr_.get_block_ds_mgr().get_ds_collect(ds_id);
      if (server_collect == NULL)
      continue;

      ds_stat_info = server_collect->get_ds();
      if (ds_stat_info->last_update_time_ > expire_time
          && test_server_alive(ds_id) == TFS_SUCCESS)
      {
        continue;
      }

      if (ns_global_info_.destroy_flag_ == NS_DESTROY_FLAGS_YES)
      break;

      TBSYS_LOG(INFO, "dataserver:(%s) is down", tbsys::CNetUtil::addrToString(ds_id).c_str());

      meta_mgr_.leave_ds(ds_id);

      if (ns_global_info_.owner_role_ == NS_ROLE_MASTER)
      replicate_thread_.inc_stop_balance_count();
    }

    if (write_ds_list.size() == 0)
    return TFS_SUCCESS;

    // select a writable ds randomly, and check its available blocks which were used for write primary.
    srand(now);
    const uint32_t write_ds_list_size = write_ds_list.size();
    int32_t start_index = rand() % write_ds_list_size;
    for (uint32_t i = 0; i < write_ds_list_size; ++i)
    {
      ds_id = write_ds_list[start_index++ % write_ds_list_size];
      meta_mgr_.check_primary_writable_block(ds_id, SYSPARAM_NAMESERVER.add_primary_block_count_, true);
      if ((i >= 0x0A) | (ns_global_info_.destroy_flag_ == NS_DESTROY_FLAGS_YES))
      break;
    }
    return TFS_SUCCESS;
  }

  /*
   * Thread Function do_check_ds
   * check DataServerStatInfo heart beat message, see if lost connection.
   * check DataServerStatInfo primary write blocks, add some new block if lack.
   */
  int NameServer::do_check_ds()
  {
    Func::sleep(SYSPARAM_NAMESERVER.safe_mode_time_, reinterpret_cast<int32_t*> (&ns_global_info_.destroy_flag_));

    while (ns_global_info_.destroy_flag_ == NS_DESTROY_FLAGS_NO)
    {
      check_ds( time(NULL));
      Func::sleep(SYSPARAM_NAMESERVER.heart_interval_, reinterpret_cast<int32_t*> (&ns_global_info_.destroy_flag_));
    }
    return TFS_SUCCESS;
  }

  /*
   * Thread Function do_time_out
   * check timeout tasks.
   */
  int NameServer::do_time_out()
  {
    Func::sleep(SYSPARAM_NAMESERVER.safe_mode_time_ * 10, reinterpret_cast<int32_t*> (&ns_global_info_.destroy_flag_));
    while (ns_global_info_.destroy_flag_ == NS_DESTROY_FLAGS_NO)
    {
      if (ns_global_info_.owner_role_ == NS_ROLE_MASTER)
      compact_thread_.check_time_out();
      Func::sleep(SYSPARAM_NAMESERVER.compact_check_interval_,
          reinterpret_cast<int32_t*> (&ns_global_info_.destroy_flag_));
    }
    return TFS_SUCCESS;
  }

  // check whether it's the time to do task
  static bool check_task_interval(time_t now_time, time_t& last_check_time, time_t interval)
  {
    if (now_time - last_check_time >= interval)
    {
      last_check_time = now_time;
      return true;
    }
    return false;
  }

  // check whether the blocks need to be replicate, compact or redundant
  int NameServer::do_check_blocks()
  {
    Func::sleep(SYSPARAM_NAMESERVER.safe_mode_time_ * 10, reinterpret_cast<int32_t*> (&ns_global_info_.destroy_flag_));

    const int32_t CHECK_TASK = 4;
    time_t last_check_time[CHECK_TASK] = { time(NULL) };
    bool need_check[CHECK_TASK] = { false };
    int32_t check_interval[CHECK_TASK] =
    {
      SYSPARAM_NAMESERVER.heart_interval_,
      SYSPARAM_NAMESERVER.replicate_check_interval_,
      SYSPARAM_NAMESERVER.compact_check_interval_,
      SYSPARAM_NAMESERVER.redundant_check_interval_
    };

    VUINT32 lose_blocks;
    VUINT32 compact_blocks;
    VUINT32 redundant_blocks;
    time_t now_time = 0;
    uint32_t server_size = 0;
    uint32_t max_repl_plan_blocks = 0;
    bool need_check_replica = false;

    while (ns_global_info_.destroy_flag_ == NS_DESTROY_FLAGS_NO)
    {
      Func::sleep(SYSPARAM_NAMESERVER.heart_interval_, reinterpret_cast<int32_t*> (&ns_global_info_.destroy_flag_));

      if (ns_global_info_.destroy_flag_ == NS_DESTROY_FLAGS_YES)
      break;

      now_time = time(NULL);

      meta_mgr_.checkpoint();

      if (ns_global_info_.owner_role_ != NS_ROLE_MASTER)
      continue;

      // wait for ds report
      if ((ns_global_info_.switch_time_ != 0) && (time(NULL) < static_cast<time_t> ((ns_global_info_.switch_time_
                      + SYSPARAM_NAMESERVER.safe_mode_time_))))
      {
        float wait_for_ds_report_time = static_cast<float> ((ns_global_info_.switch_time_
                + SYSPARAM_NAMESERVER.safe_mode_time_) - time(NULL));
        TBSYS_LOG(INFO, "not check blcok , wait(%f) for ds report...", wait_for_ds_report_time);
        Func::sleep(wait_for_ds_report_time, reinterpret_cast<int32_t*> (&ns_global_info_.destroy_flag_));
      }

      // calc time interval
      need_check_replica = false;

      for (int32_t i = 0; i < CHECK_TASK; ++i)
      {
        need_check[i] = check_task_interval(now_time, last_check_time[i], check_interval[i]);
        if (i != 0 && need_check[i])
        {
          need_check_replica = true;
        }
      }

      // check all dead servers lost heartbeat
      if (!need_check_replica)
      	continue;

      // reset check parameters
      server_size = meta_mgr_.get_block_ds_mgr().get_alive_ds_size();
      max_repl_plan_blocks = server_size * SYSPARAM_NAMESERVER.replicate_max_count_per_server_;

      lose_blocks.clear();
      compact_blocks.clear();
      redundant_blocks.clear();

      // start scanner
      ScannerManager::Scanner
      replicate_scanner(need_check[1], max_repl_plan_blocks, replicate_thread_, lose_blocks);
      ScannerManager::Scanner compact_scanner(need_check[2] && compact_thread_.is_compacting_time(), server_size,
          compact_thread_, compact_blocks);
      ScannerManager::Scanner redundant_scanner(need_check[3], server_size, redundant_thread_, redundant_blocks);

      pass_mgr_.add_scanner(1, &replicate_scanner);
      pass_mgr_.add_scanner(2, &compact_scanner);
      pass_mgr_.add_scanner(3, &redundant_scanner);
      pass_mgr_.run();

      TBSYS_LOG(DEBUG, "build plan : lose (%u), compact (%u), redundant (%u)", lose_blocks.size(), compact_blocks.size(), redundant_blocks.size());

      if ((meta_mgr_.get_lease_clerk().if_need_clear())
          && (need_check[2]))
      {
        meta_mgr_.get_lease_clerk().clear();
      }
    }
    return TFS_SUCCESS;
  }

  // do balance
  int NameServer::do_balance()
  {
    Func::sleep(SYSPARAM_NAMESERVER.safe_mode_time_ * 20, reinterpret_cast<int32_t*> (&ns_global_info_.destroy_flag_));

    while (ns_global_info_.destroy_flag_ == NS_DESTROY_FLAGS_NO)
    {
      if (ns_global_info_.owner_role_ == NS_ROLE_MASTER)
      {
        // wait for ds report
        if ((ns_global_info_.switch_time_ != 0) && (time(NULL) < static_cast<time_t> (ns_global_info_.switch_time_
                    + SYSPARAM_NAMESERVER.safe_mode_time_)))
        {
          float wait_for_ds_report_time = static_cast<float> ((ns_global_info_.switch_time_
                  + SYSPARAM_NAMESERVER.safe_mode_time_) - time(NULL));
          TBSYS_LOG(INFO, "not balance, wait(%f) for ds report...", wait_for_ds_report_time);
          Func::sleep(wait_for_ds_report_time, reinterpret_cast<int32_t*> (&ns_global_info_.destroy_flag_));
        }
        replicate_thread_.balance();
      }
      Func::sleep(SYSPARAM_NAMESERVER.balance_check_interval_,
          reinterpret_cast<int32_t*> (&ns_global_info_.destroy_flag_));
    }
    return TFS_SUCCESS;
  }

  tbnet::IPacketHandler::HPRetCode NameServer::handlePacket(tbnet::Connection *connection, tbnet::Packet *packet)
  {

    // parser packet
    if (!packet->isRegularPacket())
    {
      TBSYS_LOG(ERROR, "controlpacket, cmd(%d)", ((tbnet::ControlPacket*) packet)->getCommand());
      return tbnet::IPacketHandler::FREE_CHANNEL;
    }
    Message *bp = dynamic_cast<Message*> (packet);
    bp->set_connection(connection);
    bp->setExpireTime(MAX_RESPONSE_TIME);
    bp->set_direction(bp->get_direction() | DIRECTION_RECEIVE);

    int32_t pcode = bp->getPCode();

    // packets filter, check the packet is allowed or not.
    // is master & status == INITIALIZED ,all msg is allowed.
    // is master & status != INITIALIZED ,ns' heart msg is allowed.
    // is slave & status == INITIALIZED || ACCEPT_DS_INFO, ns & ds' heart msg are allowed.
    // is slave & status != INITIALIZED && ACCEPT_DS_INFO, ns' heart msg is allowed.
    if (pcode == OWNER_CHECK_MESSAGE)
    goto PASS;

    if ((ns_global_info_.owner_status_ < NS_STATUS_UNINITIALIZE)
        || (ns_global_info_.owner_status_> NS_STATUS_INITIALIZED))
    {
      TBSYS_LOG(WARN,"status is incorrect");
      ns_global_info_.dump(TBSYS_LOG_LEVEL(INFO));
      goto NOT_ALLOWED;
    }

    if (ns_global_info_.owner_status_ == NS_STATUS_UNINITIALIZE)
    {
      if (pcode == MASTER_AND_SLAVE_HEART_MESSAGE)
      goto PASS;
    }
    else if ((ns_global_info_.owner_status_ == NS_STATUS_INITIALIZED) || (ns_global_info_.owner_status_
            == NS_STATUS_ACCEPT_DS_INFO))
    {
      if (pcode == GET_SERVER_STATUS_MESSAGE || pcode == GET_BLOCK_INFO_MESSAGE) //for convenience
      goto PASS;
      if (ns_global_info_.owner_role_ == NS_ROLE_MASTER)
      {
        assert(ns_global_info_.owner_status_ != NS_STATUS_ACCEPT_DS_INFO);
        goto PASS;
      }
      else if (ns_global_info_.owner_role_ == NS_ROLE_SLAVE)
      {
        if (pcode == MASTER_AND_SLAVE_HEART_MESSAGE || pcode == SET_DATASERVER_MESSAGE || pcode == OPLOG_SYNC_MESSAGE
            || pcode == REPLICATE_BLOCK_MESSAGE || pcode == BLOCK_COMPACT_COMPLETE_MESSAGE || pcode
            == GET_BLOCK_LIST_MESSAGE || pcode == HEARTBEAT_AND_NS_HEART_MESSAGE)
        {
          goto PASS;
        }
      }
    }

    NOT_ALLOWED:
    TBSYS_LOG(DEBUG, "the msg(%d) will be ignored", pcode);
    bp->free();
    return tbnet::IPacketHandler::FREE_CHANNEL;

    // push the packet to the right threads.
    PASS:
    switch (pcode)
    {
      case SET_DATASERVER_MESSAGE:
      heart_mgr_.push(bp);
      break;
      case MASTER_AND_SLAVE_HEART_MESSAGE:
      case HEARTBEAT_AND_NS_HEART_MESSAGE:
      TBSYS_LOG(DEBUG, "received a msg of master_and_slave_heart_message");
      master_slave_heart_mgr_.push(bp);
      break;
      case OPLOG_SYNC_MESSAGE:
			TBSYS_LOG(DEBUG, "received a msg of oplog sync msg");
      meta_mgr_.get_oplog_sync_mgr()->push(bp);
      break;
      default:
      if (!main_task_queue_thread_.push(bp, main_task_queue_size_, false))
      {
        MessageFactory::send_error_message(bp, TBSYS_LOG_LEVEL(ERROR), STATUS_MESSAGE_ERROR,
            ns_global_info_.owner_ip_port_, "task message beyond max queue size, discard.");
        bp->free();
      }
      break;
    }
    return tbnet::IPacketHandler::KEEP_CHANNEL;
  }

  // handle the main task queue
  bool NameServer::handlePacketQueue(tbnet::Packet *packet, void *)
  {
    Message *message = dynamic_cast<Message*> (packet);
    int ret = TFS_SUCCESS;
    switch (message->get_message_type())
    {
      case GET_BLOCK_INFO_MESSAGE:
      ret = get_block_info(message);
      break;
      case BLOCK_WRITE_COMPLETE_MESSAGE:
      ret = write_complete(message);
      break;
      case REPLICATE_BLOCK_MESSAGE:
      ret = replicate_thread_.handle_replicate_complete(dynamic_cast<ReplicateBlockMessage*> (message));
      break;
      case BLOCK_COMPACT_COMPLETE_MESSAGE:
      ret = compact_thread_.handle_complete_msg(dynamic_cast<CompactBlockCompleteMessage*> (message));
      break;
      case UPDATE_BLOCK_INFO_MESSAGE:
      ret = update_block_info(message);
      break;
      case GET_SERVER_STATUS_MESSAGE:
      ret = get_ds_status(message);
      break;
      case REPLICATE_INFO_MESSAGE:
      ret = get_replicate_info_msg(message);
      break;
      case CLIENT_CMD_MESSAGE:
      ret = do_control_cmd(message);
      break;
      case STATUS_MESSAGE:
      if (dynamic_cast<StatusMessage*> (message)->get_status() == STATUS_MESSAGE_PING)
      {
        message->reply_message(new StatusMessage(STATUS_MESSAGE_PING));
      }
      else
      {
        ret = EXIT_GENERAL_ERROR;
      }
      break;
      case OWNER_CHECK_MESSAGE:
      ret = owner_check(message);
      break;
      case GET_BLOCK_LIST_MESSAGE:
      ret = get_block_list(message);
      break;
      default:
      ret = EXIT_UNKNOWN_MSGTYPE;
      break;
    }

    if (ret != TFS_SUCCESS)
    {
      MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), ret, ns_global_info_.owner_ip_port_,
          "execute message failed");
    }
    return true;
  }

  int NameServer::get_block_info(Message *msg)
  {
    GetBlockInfoMessage* message = dynamic_cast<GetBlockInfoMessage*> (msg);
    if (meta_mgr_.get_block_ds_mgr().get_ds_size() <= 0)
    {
      return MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), EXIT_NO_DATASERVER,
          ns_global_info_.owner_ip_port_, "not found dataserver, dataserver size equal 0");
    }

    SetBlockInfoMessage *result_msg = new SetBlockInfoMessage();
    uint32_t block_id = message->get_block_id();
    int32_t mode = message->get_mode();

    VUINT64 ds_list;
    uint32_t lease_id = 0;
    int32_t version = 0;
    int32_t ret = TFS_SUCCESS;
    if (mode & BLOCK_READ)
    {
      TBSYS_LOG(DEBUG, "read block info, block(%u), mode(%d)", block_id, mode);
      ret = meta_mgr_.read_block_info(block_id, ds_list);
      if (ret == TFS_SUCCESS)
      result_msg->set_read_block_ds(block_id, &ds_list);
    }
    else
    {
      if (ns_global_info_.owner_role_ == NS_ROLE_SLAVE)
      goto out;
      // BLOCK_WRITE | BLOCK_CREATE | BLOCK_NEWBLK | BLOCK_NOLEASE
      // check this block if doing any operations like replicating, moving, compacting...
      if (block_id != 0 && !(mode & BLOCK_NOLEASE))
      {
        if ((replicate_thread_.get_executor().is_replicating_block(block_id))
            || (compact_thread_.is_compacting_block(block_id)))
        {
          TBSYS_LOG(ERROR, "get block info block(%u), mode(%d) is busy", block_id ,mode);
          ret = EXIT_BLOCK_BUSY;
          goto out;
        }
      }
      ret = meta_mgr_.write_block_info(block_id, mode, lease_id, version, ds_list);
      TBSYS_LOG(DEBUG, "get block info: block(%u) mode(%d) lease(%u), version(%d), dataserver size(%u), result(%d)",
          block_id, mode, lease_id, version, ds_list.size(), ret);
      if (ret == TFS_SUCCESS)
      result_msg->set_write_block_ds(block_id, &ds_list, version, lease_id);
    }

    out:
    if (ret != TFS_SUCCESS)
    {
      tbsys::gDelete(result_msg);
      return MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), ret, ns_global_info_.owner_ip_port_,
          "got error, when get block(%u) mode(%d), result(%d) information", block_id, mode, ret);
    }

    message->reply_message(result_msg);

    return TFS_SUCCESS;
  }

  /**
   * a write operation completed, commit to nameserver and update block's verion
   */
  int NameServer::write_complete(Message* msg)
  {
    BlockWriteCompleteMessage* message = dynamic_cast<BlockWriteCompleteMessage*> (msg);
    BlockInfo *blk = message->get_block();
    uint64_t ds_id = message->get_server_id();
    uint32_t lease_id = message->get_lease_id();
    WriteCompleteStatus status = message->get_success();
    UnlinkFlag unlink_flag = message->get_unlink_flag();

    TBSYS_LOG(DEBUG, "write commit: block(%u) dataserver(%s) version(%d), file_count(%d)"
        "size(%d) delfile_count(%d), del_size(%d), seq_no(%u), lease(%u), unlink(%s), status(%s)", blk->block_id_,
        tbsys::CNetUtil::addrToString(ds_id).c_str(), blk->version_, blk->file_count_, blk->size_,
        blk->del_file_count_, blk->del_size_, blk->seq_no_, lease_id,
        unlink_flag == UNLINK_FLAG_NO ? "no" : unlink_flag == UNLINK_FLAG_YES ? "yes" : "unknow",
        status == WRITE_COMPLETE_STATUS_YES ? "yes" : "no");

    bool need_add_new_block = false;
    std::string errmsg;
    int ret = meta_mgr_.write_commit(*blk, ds_id, lease_id, unlink_flag, status, need_add_new_block, errmsg);
    message->reply_message(new StatusMessage(ret, const_cast<char*> (errmsg.c_str())));

    // add new block, when block filled complete
    if (need_add_new_block)
    {
      int need_add_new_block_count = meta_mgr_.check_primary_writable_block(ds_id, 1, true);
      TBSYS_LOG(INFO, "need add new block, count(%d)", need_add_new_block_count);
    }
    return ret;
  }

  int NameServer::update_block_info(Message* msg)
  {
    UpdateBlockInfoMessage* message = dynamic_cast<UpdateBlockInfoMessage*> (msg);
    uint32_t block_id = message->get_block_id();
    BlockInfo* block_info = message->get_block();
    uint64_t dest_ds_id = message->get_server_id();
    int32_t repair_flag = message->get_repair();

    if (block_id == 0)
    {
      TBSYS_LOG(WARN, "block(%u) not found", block_id);
      return EXIT_BLOCK_NOT_FOUND;
    }

    LayoutManager& block_ds_map = meta_mgr_.get_block_ds_mgr();
    if (repair_flag == UPDATE_BLOCK_NORMAL)
    {
      if (block_info == NULL)
      {
        MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), EXIT_BLOCK_NOT_FOUND,
            ns_global_info_.owner_ip_port_, "repair block(%u) blockinfo object is null", block_id);
        return TFS_SUCCESS;
      }
      TBSYS_LOG(DEBUG, "block repair new block info(%u)(%d)", block_id, block_info->version_);
      int ret = meta_mgr_.update_block_info(*block_info, dest_ds_id, true);
      if (ret == TFS_SUCCESS)
      {
        message->reply_message(new StatusMessage(ret, "update block info successful"));
      }
      else
      {
        TBSYS_LOG(ERROR, "new block info version lower than meta,cannot update");
        message->reply_message(new StatusMessage(ret, "new block info version lower than meta, cannot update"));
      }
      return ret;
    }

    BlockChunkPtr ptr = block_ds_map.get_block_chunk(block_id);
    ptr->mutex_.rdlock();

    BlockCollect* block_collect = ptr->find(block_id);
    if (block_collect == NULL)
    {
      ptr->mutex_.unlock();
      MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), EXIT_BLOCK_NOT_FOUND,
          ns_global_info_.owner_ip_port_, "repair block, block collect not found by block id(%u)", block_id);
      return TFS_SUCCESS;
    }

    // need repair this block;
    VUINT64 ds_list = *(block_collect->get_ds());
    ptr->mutex_.unlock();
    if ((repair_flag == UPDATE_BLOCK_MISSING)
        && (ds_list.size() >= static_cast<uint32_t> (SYSPARAM_NAMESERVER.min_replication_)))
    {
      MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), EXIT_BLOCK_NOT_FOUND,
          ns_global_info_.owner_ip_port_, "already got block(%u) replica(%u)", block_id, ds_list.size());
      return TFS_SUCCESS;
    }

    if (ds_list.size() == 0)
    {
      MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), EXIT_NO_DATASERVER,
          ns_global_info_.owner_ip_port_, "repair block(%u) no any dataserver hold it", block_id);
      return TFS_SUCCESS;
    }

    uint64_t source_ds_id = 0;
    for (uint32_t i = 0; i < ds_list.size(); ++i)
    {
      if (ds_list.at(i) != dest_ds_id)
      {
        source_ds_id = ds_list.at(i);
        break;
      }
    }

    if (source_ds_id == 0)
    {
      MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), EXIT_NO_DATASERVER,
          ns_global_info_.owner_ip_port_, "repair block(%u) no any other dataserver(%u) hold a correct replica", block_id, ds_list.size());
      return TFS_SUCCESS;
    }

    message->reply_message(new StatusMessage(STATUS_MESSAGE_REMOVE));
    meta_mgr_.get_lease_clerk().cancel_lease(block_id);
    block_ds_map.release_ds_relation(block_id, dest_ds_id);
    return replicate_thread_.get_executor().send_replicate_cmd(source_ds_id, dest_ds_id, block_id, REPLICATE_BLOCK_MOVE_FLAG_NO);
  }

  int NameServer::get_ds_status(Message *msg)
  {
    GetServerStatusMessage* message = dynamic_cast<GetServerStatusMessage*> (msg);
    SetServerStatusMessage *resp = new SetServerStatusMessage();
    int32_t type = message->get_status_type();
    resp->set_from_row(message->get_from_row());
    resp->set_return_row(message->get_return_row());
    resp->set_block_server_map(type, &meta_mgr_.get_block_ds_mgr());
    resp->set_server_map(type, meta_mgr_.get_block_ds_mgr().get_ds_map());
    resp->set_wblock_list(type, &meta_mgr_.get_block_ds_mgr().get_writable_block_list());
    message->reply_message(resp);
    return TFS_SUCCESS;
  }

  int NameServer::get_block_list(Message* msg)
  {
    GetBlockListMessage* message = dynamic_cast<GetBlockListMessage*> (msg);
    GetBlockListMessage* resp = new GetBlockListMessage();
    resp->set_request_flag(0);
    resp->set_write_flag(message->get_write_flag());
    resp->set_start_block_id(message->get_start_block_id());
    resp->set_start_inclusive(message->get_start_inclusive());
    resp->set_end_block_id(message->get_end_block_id());
    resp->set_read_count(message->get_read_count());
    resp->set_return_count(message->get_return_count());
    resp->set_start_block_chunk(message->get_start_block_chunk());
    resp->set_next_block_chunk(message->get_next_block_chunk());
    resp->set_block_server_map(&meta_mgr_.get_block_ds_mgr());
    message->reply_message(resp);
    return TFS_SUCCESS;
  }

  int NameServer::get_replicate_info_msg(Message* msg)
  {
    ReplicateInfoMessage* message = dynamic_cast<ReplicateInfoMessage*> (msg);
    ReplicateInfoMessage * rim = new ReplicateInfoMessage();
    replicate_thread_.handle_replicate_info_msg(rim);
    message->reply_message(rim);
    return TFS_SUCCESS;
  }

  /**
   * 1 - m_maxUseCapacityRatio
   * 2 - m_minReplication
   * 3 - m_maxReplication
   * 4 - m_dsDeadTime
   * 5 - m_heartInterval
   * 6 - m_replCheckInterval
   * 7 - m_balanceCheckInterval
   * 8 - pauseReplication_
   * 9 - m_replWaitTime
   * 10 - compactHourRange
   * 11 - m_syncMirrorStatus
   */
  int NameServer::set_runtime_param(const uint32_t index, const uint64_t value, char *retstr)
  {
    retstr[0] = '\0';
    int32_t offset = (index & 0x0FFFFFFF);
    int32_t set = (index & 0xF0000000);
    int32_t *param[] =
    {
      &SYSPARAM_NAMESERVER.min_replication_,
      &SYSPARAM_NAMESERVER.max_replication_,
      &SYSPARAM_NAMESERVER.max_write_file_count_,
      &SYSPARAM_NAMESERVER.max_use_capacity_ratio_,
      &SYSPARAM_NAMESERVER.ds_dead_time_,
      &SYSPARAM_NAMESERVER.heart_interval_,
      &SYSPARAM_NAMESERVER.replicate_check_interval_,
      &SYSPARAM_NAMESERVER.balance_check_interval_,
      reinterpret_cast<int32_t*> (replicate_thread_.get_pause_flag()),
      &SYSPARAM_NAMESERVER.replicate_wait_time_,
      &SYSPARAM_NAMESERVER.replicate_max_time_,
      &SYSPARAM_NAMESERVER.replicate_max_count_per_server_,
      &SYSPARAM_NAMESERVER.redundant_check_interval_,
      &SYSPARAM_NAMESERVER.compact_time_lower_,
      &SYSPARAM_NAMESERVER.compact_time_upper_,
      &SYSPARAM_NAMESERVER.compact_delete_ratio_,
      &SYSPARAM_NAMESERVER.compact_max_load_,
      &SYSPARAM_NAMESERVER.compact_preserve_time_,
      &SYSPARAM_NAMESERVER.compact_check_interval_,
      &SYSPARAM_NAMESERVER.cluster_index_
    };
    static const char *paramstr[] =
    {
      "minReplication",
      "maxReplication",
      "maxWriteFileCount",
      "maxUseCapacityRatio",
      "dsDeadTime",
      "heartInterval",
      "replCheckInterval",
      "balanceCheckInterval",
      "pauseReplication",
      "replWaitTime",
      "replicateMaxTime",
      "replicateMaxCountPerServer",
      "redundantCheckInterval",
      "compactTimeLower",
      "compactTimeUpper",
      "compactDeleteRatio",
      "compactMaxLoad",
      "compactPreserverTime",
      "compactCheckInterval",
      "clusterIndex"

    };
    int32_t size = sizeof(param) / sizeof(int32_t*);
    if (offset < 1 || offset > size)
    {
      sprintf(retstr, "index (%d) invalid.", offset);
      TBSYS_LOG(ERROR, "index(%d) invalid.", offset);
      return TFS_SUCCESS;
    }
    int32_t *current_value = param[index - 1];
    if (set)
    {
      *current_value = (int32_t)(value & 0xFFFFFFFF);
    }
    else
    {
      sprintf(retstr, "%d", *current_value);
    }
    TBSYS_LOG(INFO, "index(%d) set(%d) name(%s) value(%d)", offset, set, paramstr[index - 1], *current_value);
    return TFS_SUCCESS;
  }

  int NameServer::owner_check(Message*)
  {
    tbutil::Mutex::Lock lock(ns_global_info_);
    ns_global_info_.last_owner_check_time_ = tbutil::Time::microSeconds(tbsys::CTimeUtil::getTime());
    TBSYS_LOG(DEBUG, "now(%s)", ns_global_info_.last_owner_check_time_.toDateTime().c_str());
    TBSYS_LOG(INFO, "owner check time(%" PRI64_PREFIX "u: %s)", ns_global_info_.last_owner_check_time_.toMicroSeconds(),
        ns_global_info_.last_owner_check_time_.toDateTime().c_str());
    return TFS_SUCCESS;
  }

  /*
   * expire blocks on DataServerStatInfo only post expire message to ds, dont care result.
   * @param [in] ds_id  DataServerStatInfo id the one who post to.
   * @param [in] block_id block id, the one need expired.
   * @return TFS_SUCCESS success.
   */
  int NameServer::rm_block_from_ds(const uint64_t ds_id, const uint32_t block_id)
  {
    TBSYS_LOG(INFO, "remove  block (%u) on server (%s)", block_id, tbsys::CNetUtil::addrToString(ds_id).c_str());
    RemoveBlockMessage rbmsg;
    rbmsg.add_remove_id(block_id);
    return post_message_to_server(ds_id, &rbmsg);
  }

  int NameServer::rm_block_from_ds(const uint64_t ds_id, const vector<uint32_t>& block_ids)
  {
    TBSYS_LOG(INFO, "remove  block count (%u) on server (%s)", block_ids.size(),
        tbsys::CNetUtil::addrToString(ds_id).c_str());
    RemoveBlockMessage rbmsg;
    rbmsg.set_remove_list(block_ids);
    return post_message_to_server(ds_id, &rbmsg);
  }

  int NameServer::do_control_cmd(Message* msg)
  {
    ClientCmdMessage* message = dynamic_cast<ClientCmdMessage*> (msg);
    StatusMessage *resp = new StatusMessage(STATUS_MESSAGE_ERROR, "unknown client cmd.");
    int32_t type = message->get_type();
    uint64_t from_ds_id = message->get_from_server_id();
    do
    {
      if (type == CLIENT_CMD_LOADBLK)
      {
        uint32_t block_id = message->get_block_id();
        BlockCollect *block_collect = meta_mgr_.get_block_ds_mgr().get_block_collect(block_id);
        if (block_collect == NULL)
        {
          resp->set_message(STATUS_MESSAGE_ERROR, "block no exist");
          TBSYS_LOG(ERROR, "block_id(%u) no exist", block_id);
          break;
        }
        if (block_collect->get_ds()->size() != 0)
        {
          resp->set_message(STATUS_MESSAGE_ERROR, "block no lose.");
          TBSYS_LOG(ERROR, "block_id(%u) no lose.", block_id);
          break;
        }
        uint64_t ds_id = message->get_server_id();
        ServerCollect *server_collect = meta_mgr_.get_block_ds_mgr().get_ds_collect(ds_id);
        if (server_collect == NULL)
        {
          resp->set_message(STATUS_MESSAGE_ERROR, "server no exist");
          TBSYS_LOG(ERROR, "server(%s) no exist.", tbsys::CNetUtil::addrToString(ds_id).c_str());
          break;
        }
        if (send_message_to_server(ds_id, message, NULL) != TFS_SUCCESS)
        {
          resp->set_message(STATUS_MESSAGE_ERROR, "send message to server failure.");
          break;
        }
        meta_mgr_.get_block_ds_mgr().build_ds_block_relation(block_id, ds_id, false);
        resp->set_message(STATUS_MESSAGE_OK);
        TBSYS_LOG(WARN, "client(%s) exectue CLIENT_CMD_LOADBLK server(%s), block_id(%u)", tbsys::CNetUtil::addrToString(
                from_ds_id).c_str(), tbsys::CNetUtil::addrToString(ds_id).c_str(), block_id);
      }
      else if (type == CLIENT_CMD_EXPBLK)
      {
        uint32_t block_id = message->get_block_id();
        uint64_t ds_id = message->get_server_id();

        BlockChunkPtr ptr = meta_mgr_.get_block_ds_mgr().get_block_chunk(block_id);
        ptr->mutex_.wrlock();
        BlockCollect *block_collect = ptr->find(block_id);
        if (block_collect == NULL)
        {
          ptr->mutex_.unlock();
          resp->set_message(STATUS_MESSAGE_ERROR, "block no exist");
          TBSYS_LOG(ERROR, "block(%u) no exist", block_id);
          break;
        }
        if (block_collect->get_ds()->size() == 0 && ds_id == 0)
        {
          ptr->remove(block_id);
          ptr->mutex_.unlock();
          resp->set_message(STATUS_MESSAGE_OK);
          break;
        }
        ptr->mutex_.unlock();

        ServerCollect *server_collect = meta_mgr_.get_block_ds_mgr().get_ds_collect(ds_id);
        if (server_collect == NULL)
        {
          resp->set_message(STATUS_MESSAGE_ERROR, "server no exist");
          TBSYS_LOG(ERROR, "server(%s) no exist.", tbsys::CNetUtil::addrToString(ds_id).c_str());
          break;
        }
        if (rm_block_from_ds(ds_id, block_id) == TFS_SUCCESS)
        {
          //meta_mgr_.get_block_ds_mgr().lock();
          meta_mgr_.get_block_ds_mgr().release_ds_relation(block_id, ds_id);
          //meta_mgr_.get_block_ds_mgr().unlock();
        }
        resp->set_message(STATUS_MESSAGE_OK);
        TBSYS_LOG(WARN, "client(%s) exectue CLIENT_CMD_EXPBLK server(%s), block_id(%u)", tbsys::CNetUtil::addrToString(
                from_ds_id).c_str(), tbsys::CNetUtil::addrToString(ds_id).c_str(), block_id);
      }
      else if (type == CLIENT_CMD_COMPACT)
      {
        uint32_t block_id = message->get_block_id();
        BlockCollect *block_collect = meta_mgr_.get_block_ds_mgr().get_block_collect(block_id);
        if (block_collect == NULL)
        {
          resp->set_message(STATUS_MESSAGE_ERROR, "block no exist");
          TBSYS_LOG(ERROR, "block_id(%u) no exist", block_id);
          break;
        }
        const VUINT64 *servers = block_collect->get_ds();
        compact_thread_.send_compact_cmd(*servers, block_id);
        resp->set_message(STATUS_MESSAGE_OK);
        TBSYS_LOG(WARN, "client(%s) execute CLIENT_CMD_COMPACT block_id(%u)",
            tbsys::CNetUtil::addrToString(from_ds_id).c_str(), block_id);
      }
      else if (type == CLIENT_CMD_IMMEDIATELY_REPL)
      {
        uint32_t block_id = message->get_block_id();
        uint32_t replicate_move_flag = message->get_version();
        uint64_t dest_ds_id = message->get_server_id();
        uint64_t src_ds_id = message->get_from_server_id();

        TBSYS_LOG(INFO, "immediately replicate block(%u) replicate block move flag(%s) src(%s) dest(%s)", block_id,
            replicate_move_flag == REPLICATE_BLOCK_MOVE_FLAG_NO ? "no" : "yes", tbsys::CNetUtil::addrToString(
                src_ds_id).c_str(), tbsys::CNetUtil::addrToString(dest_ds_id).c_str());

        BlockCollect* block_collect = meta_mgr_.get_block_ds_mgr().get_block_collect(block_id);
        if (block_collect == NULL)
        {
          TBSYS_LOG(ERROR, "replicate block(%u) does not exist", block_id);
          resp->set_message(EXIT_BLOCK_NOT_FOUND);
          break;
        }
        if ((replicate_move_flag == REPLICATE_BLOCK_MOVE_FLAG_NO)
            ||(replicate_move_flag == REPLICATE_BLOCK_MOVE_FLAG_YES))
        {
          if (src_ds_id == 0 || dest_ds_id == 0 || src_ds_id == dest_ds_id)
          {
            TBSYS_LOG(ERROR, "immediately replicate block id(%u) replicate_move_flag(%s) src(%s) or dest(%s) is illegal",
                block_id, replicate_move_flag == REPLICATE_BLOCK_MOVE_FLAG_NO ? "no" : "yes", tbsys::CNetUtil::addrToString(src_ds_id).c_str(),
                tbsys::CNetUtil::addrToString(dest_ds_id).c_str());
            break;
          }
          replicate_thread_.get_executor().send_replicate_cmd(src_ds_id, dest_ds_id, block_id, replicate_move_flag);
        }
        else
        {
          TBSYS_LOG(INFO, "immediately replicate block(%u)", block_id);
          replicate_thread_.emergency_replicate(block_id);
        }
        resp->set_message(STATUS_MESSAGE_OK);
      }
      else if (type == CLIENT_CMD_REPAIR_GROUP)
      {
        uint32_t block_id = message->get_block_id();
        if (block_id > 0)
        {
          redundant_thread_.check_group_block(meta_mgr_.get_block_ds_mgr().get_block_collect(block_id));
        }
        else
        {
          //m_checkGroupBlockTime = 0;
        }
        resp->set_message(STATUS_MESSAGE_OK);
        TBSYS_LOG(INFO, "client(%s) execute CLIENT_CMD_REPAIR_GROUP block(%u)",
            tbsys::CNetUtil::addrToString(from_ds_id).c_str(), block_id);
      }
      else if (type == CLIENT_CMD_SET_PARAM)
      {
        uint32_t index = message->get_block_id();
        uint64_t value = message->get_server_id();
        char retstr[256];
        set_runtime_param(index, value, retstr);
        resp->set_message(STATUS_MESSAGE_OK, retstr);
      }
      else if (type == CLIENT_CMD_CLEAR_REPL_INFO)
      {
        TBSYS_LOG(INFO, "client(%s) exectue CLIENT_CMD_CLEAR_REPL_INFO", tbsys::CNetUtil::addrToString(from_ds_id).c_str());
        replicate_thread_.get_executor().clear_replicating_info();
        resp->set_message(STATUS_MESSAGE_OK);
      }
    }
    while (0);
    message->reply_message(resp);
    return TFS_SUCCESS;
  }

  //initalize nameserver infomation
  int NameServer::initialize_ns_global_info()
  {
    // get namesever ip list and port from CONF FILE
    const char* ns_ip = CONFIG.get_string_value(CONFIG_NAMESERVER, CONF_IP_ADDR_LIST);
    int32_t ns_port = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_PORT);
    if (ns_ip == NULL || ns_port <= 0)
    {
      TBSYS_LOG(ERROR, "initialize ns ip is null or ns port <= 0, must be exit");
      return EXIT_GENERAL_ERROR;
    }
    TBSYS_LOG(DEBUG, "ns list(%s),ns port(%d)", ns_ip, ns_port);
    vector < uint64_t > ns_ip_list;
    char buffer[256];
    strncpy(buffer, ns_ip, 256);
    char *t = NULL;
    char *s = buffer;
    while ((t = strsep(&s, "|")) != NULL)
    {
      ns_ip_list.push_back(Func::str_to_addr(t, ns_port));
    }
    if (ns_ip_list.size() != 2)
    {
      TBSYS_LOG(DEBUG, "must have two ns,check your ns' list");
      return EXIT_GENERAL_ERROR;
    }

    // get local ip from CONF FILE
    const char *dev_name = CONFIG.get_string_value(CONFIG_NAMESERVER, CONF_DEV_NAME);
    uint32_t local_ip = Func::get_local_addr(dev_name);
    if (dev_name == NULL || local_ip == 0)
    {
      TBSYS_LOG(ERROR, "get dev name is null or local ip == 0 , must be exit");
      return EXIT_GENERAL_ERROR;
    }

    uint64_t local_ns_id = 0;
    IpAddr* adr = (IpAddr*) (&local_ns_id);
    adr->ip_ = local_ip;
    adr->port_ = ns_port;

    if (find(ns_ip_list.begin(), ns_ip_list.end(), local_ns_id) == ns_ip_list.end())
    {
      TBSYS_LOG(ERROR, "local ip(%s) not in ip_list(%s) , must be exit",
          tbsys::CNetUtil::addrToString(local_ns_id).c_str(), ns_ip);
      return EXIT_GENERAL_ERROR;
    }

    // set ns_global_info
    for (vector<uint64_t>::iterator it = ns_ip_list.begin(); it != ns_ip_list.end(); ++it)
    {
      if (local_ns_id == *it)
      ns_global_info_.owner_ip_port_ = *it;
      else
      ns_global_info_.other_side_ip_port_ = *it;
    }

    ns_global_info_.switch_time_ = 0;
    ns_global_info_.owner_status_ = NS_STATUS_UNINITIALIZE;

    const char *ip = CONFIG.get_string_value(CONFIG_NAMESERVER, CONF_IP_ADDR);
    ns_global_info_.vip_ = Func::get_addr(ip);

    if (Func::is_local_addr(ns_global_info_.vip_))
    ns_global_info_.owner_role_ = NS_ROLE_MASTER;
    else
    ns_global_info_.owner_role_ = NS_ROLE_SLAVE;

    TBSYS_LOG(DEBUG, "i %s the master server", ns_global_info_.owner_role_ == NS_ROLE_MASTER ? "am" : "am not");

    ns_global_info_.other_side_role_ = NS_ROLE_NONE;
    return TFS_SUCCESS;

  }

  // send the message to the other side nameserver
  // check the other side role, update ns_global_info
  int NameServer::get_peer_role()
  {
    // set the MasterAndSlaveHeartMessage
    MasterAndSlaveHeartMessage master_slave_msg;
    master_slave_msg.set_ip_port(ns_global_info_.owner_ip_port_);
    master_slave_msg.set_role(ns_global_info_.owner_role_);
    master_slave_msg.set_status(ns_global_info_.owner_status_);
    master_slave_msg.set_flags(HEART_GET_DATASERVER_LIST_FLAGS_NO);
    int32_t ret = TFS_SUCCESS;
    Message *ret_msg = NULL;
    MasterAndSlaveHeartResponseMessage *response = NULL;
    while (true)
    {
      TBSYS_LOG(DEBUG, "get peers(%s) role, owner role(%s), other side role(%s)...", tbsys::CNetUtil::addrToString(
              ns_global_info_.other_side_ip_port_).c_str(), ns_global_info_.owner_role_ == NS_ROLE_MASTER ? "master"
          : "slave", ns_global_info_.other_side_role_ == NS_ROLE_MASTER ? "master" : "slave");

      // send message
      ret = send_message_to_server(ns_global_info_.other_side_ip_port_, &master_slave_msg, &ret_msg);
      if (ret != TFS_SUCCESS)
      {
        ret = EXIT_GENERAL_ERROR;
        goto out;
      }
      if (ret_msg->get_message_type() != MASTER_AND_SLAVE_HEART_RESPONSE_MESSAGE)
      {
        ret = EXIT_GENERAL_ERROR;
        tbsys::gDelete(ret_msg);
        goto out;
      }

      //check whether the address of the other ns is corresponding with the conf.
      response = NULL;
      response = dynamic_cast<MasterAndSlaveHeartResponseMessage *> (ret_msg);
      ns_global_info_.other_side_role_ = static_cast<NsRole> (response->get_role());
      if (ns_global_info_.other_side_ip_port_ != response->get_ip_port())
      {
        TBSYS_LOG(WARN, "peer's id in config file is incorrect?");
        ns_global_info_.other_side_ip_port_ = response->get_ip_port();
      }

      // if we are the master, and the other ns is ready, then mark the Sync data flag.
      {
        tbutil::Mutex::Lock lock(ns_global_info_);
        ns_global_info_.other_side_status_ = static_cast<NsStatus> (response->get_status());
        if ((ns_global_info_.other_side_status_ == NS_STATUS_INITIALIZED) && (ns_global_info_.owner_role_
                == NS_ROLE_MASTER))
        {
          ns_global_info_.sync_oplog_flag_ = NS_SYNC_DATA_FLAG_YES;
        }
      }

      tbsys::gDelete(ret_msg);

      out:

      if (ns_global_info_.destroy_flag_ == NS_DESTROY_FLAGS_YES)
      {
        TBSYS_LOG(DEBUG, "maybe someone killed me...");
        return ret;
      }

      if (ns_global_info_.owner_role_ == NS_ROLE_SLAVE)
      {
        if (Func::is_local_addr(ns_global_info_.vip_))
        {
          tbutil::Mutex::Lock lock(ns_global_info_);
          ns_global_info_.owner_role_ = NS_ROLE_MASTER;
        }
      }

      if (ns_global_info_.owner_role_ == NS_ROLE_MASTER)
      return TFS_SUCCESS;

      //send failed or recv msg error
      //if we're the slave ns,retry
      if (ret != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "wait master done, retry...");
        usleep(500000); //500ms
        continue;
      }
      return ret;
    }
    return ret;
  }

  // start threads
  void NameServer::initialize_handle_threads()
  {
    check_ds_thread_.start(this, NULL);
    check_block_thread_.start(this, NULL);
    balance_thread_.start(this, NULL);
    time_out_thread_.start(this, NULL);
    replicate_thread_.initialize();
    compact_thread_.start();
  }

  void NameServer::initialize_handle_task_and_heart_threads()
  {
    // start thread for main task queue
    int32_t thead_count = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_THREAD_COUNT, 1);
    main_task_queue_thread_.setThreadParameter(thead_count, this, NULL);
    main_task_queue_thread_.start();

    // start thread for handling heartbeat from ds
    int32_t heart_thread_count = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_HEART_THREAD_COUNT, 2);
    int32_t heart_max_queue_size = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_HEART_MAX_QUEUE_SIZE, 10);
    heart_mgr_.initialize(heart_thread_count, heart_max_queue_size);

    main_task_queue_size_ = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_TASK_MAX_QUEUE_SIZE, 100);
    TBSYS_LOG(INFO, "fsnamesystem::start: %s", tbsys::CNetUtil::addrToString(ns_global_info_.owner_ip_port_).c_str());
  }

  // check the ds list in each ns
  int NameServer::wait_for_ds_report()
  {
    MasterAndSlaveHeartMessage master_slave_msg;
    master_slave_msg.set_ip_port(ns_global_info_.owner_ip_port_);
    master_slave_msg.set_role(ns_global_info_.owner_role_);
    master_slave_msg.set_status(ns_global_info_.owner_status_);
    master_slave_msg.set_flags(HEART_GET_DATASERVER_LIST_FLAGS_YES);
    int32_t ret = TFS_SUCCESS;
    int percent = 0;
    Message *ret_msg = NULL;
    time_t end_report_time = time(NULL) + SYSPARAM_NAMESERVER.safe_mode_time_;
    while (true)
    {
      ret = send_message_to_server(ns_global_info_.other_side_ip_port_, &master_slave_msg, &ret_msg);
      if (ret != TFS_SUCCESS)
      goto out;

      if (ret_msg->get_message_type() == MASTER_AND_SLAVE_HEART_RESPONSE_MESSAGE)
      {
        MasterAndSlaveHeartResponseMessage *response = NULL;
        response = dynamic_cast<MasterAndSlaveHeartResponseMessage *> (ret_msg);
        VUINT64 peer_list(response->get_ds_list()->begin(), response->get_ds_list()->end());
        const SERVER_MAP* local_ds_map = meta_mgr_.get_block_ds_mgr().get_ds_map();
        VUINT64 local_list;
        GetAliveDataServerList store(local_list);
        std::for_each(local_ds_map->begin(), local_ds_map->end(), store);

        TBSYS_LOG(DEBUG, "local size:%u,peer size:%u", local_list.size(), peer_list.size());
        if (peer_list.size() == 0)
        {
          tbsys::gDelete(response);
          break;
        }

        percent = (int) (((double) local_list.size() / (double) peer_list.size()) * 100);
        if (percent < 90)
        {
          tbsys::gDelete(response);
          break;
        }
        tbsys::gDelete(response);
        break;
      }

      tbsys::gDelete(ret_msg);
      out:

      if (time(NULL) > end_report_time)
      break;

      usleep(500000); //500ms

      if (ns_global_info_.destroy_flag_ == NS_DESTROY_FLAGS_YES)
      return EXIT_GENERAL_ERROR;
    }
    return TFS_SUCCESS;
  }
}
}

