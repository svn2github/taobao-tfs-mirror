/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: dataservice.cpp 1000 2011-11-03 02:40:09Z mingyan.zc@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   qushan<qushan@taobao.com>
 *      - modify 2009-03-27
 *   linqing <linqing.zyd@taobao.com>
 *      - modify 2012-12-12
 *
 */
#include "dataservice.h"
#include "ds_define.h"

#include <Memory.hpp>
#include "common/new_client.h"
#include "common/client_manager.h"
#include "common/func.h"
#include "common/config_item.h"
#include "common/directory_op.h"
#include "new_client/fsname.h"

namespace tfs
{
  namespace dataserver
  {
    using namespace std;
    using namespace tfs::common;
    using namespace tfs::client;
    using namespace tfs::message;
    DataService::DataService():
        op_manager_(*this),
        lease_manager_(NULL),
        data_helper_(*this),
        task_manager_(*this),
        block_manager_(NULL),
        client_request_server_(*this),
        writable_block_manager_(*this),
        check_manager_(*this),
        sync_manager_(NULL),
        migrate_manager_(NULL),
        timeout_thread_(0),
        task_thread_(0),
        check_thread_(0)
    {

    }

    DataService::~DataService()
    {
      tbsys::gDelete(sync_manager_);
      tbsys::gDelete(migrate_manager_);
      tbsys::gDelete(lease_manager_);
      tbsys::gDelete(block_manager_);
      timeout_thread_ = 0;
      task_thread_ = 0;
    }

    int DataService::parse_common_line_args(int argc, char* argv[], std::string& errmsg)
    {
      char buf[256] = {'\0'};
      int32_t index = 0;
      while ((index = getopt(argc, argv, "i:")) != EOF)
      {
        switch (index)
        {
          case 'i':
            server_index_ = optarg;
            break;
          default:
            snprintf(buf, 256, "%c invalid parameter", index);
            break;
        }
      }
      int32_t ret = server_index_.empty() ? TFS_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS != ret)
      {
        snprintf(buf, 256, "server index in empty, invalid parameter");
      }
      errmsg.assign(buf);
      return ret;
    }

    int DataService::get_listen_port() const
    {
      int32_t port = -1;
      int32_t base_port = TBSYS_CONFIG.getInt(CONF_SN_PUBLIC, CONF_PORT, 0);
      if (base_port >= 1024 || base_port <= 65535)
      {
        port = SYSPARAM_DATASERVER.get_real_ds_port(base_port, server_index_);
        if (port < 1024 || base_port > 65535)
        {
          port = -1;
        }
      }
      return port;
    }

    const char* DataService::get_log_file_path()
    {
      const char* log_file_path = NULL;
      const char* work_dir = get_work_dir();
      if (work_dir != NULL)
      {
        log_file_path_ = work_dir;
        log_file_path_ += "/logs/dataserver";
        log_file_path_ = SYSPARAM_DATASERVER.get_real_file_name(log_file_path_, server_index_, "log");
        log_file_path = log_file_path_.c_str();
      }
      return log_file_path;
    }

    const char* DataService::get_pid_file_path()
    {
      const char* pid_file_path = NULL;
      const char* work_dir = get_work_dir();
      if (work_dir != NULL)
      {
        pid_file_path_ = work_dir;
        pid_file_path_ += "/logs/dataserver";
        pid_file_path_ = SYSPARAM_DATASERVER.get_real_file_name(pid_file_path_, server_index_, "pid");
        pid_file_path = pid_file_path_.c_str();
      }
      return pid_file_path;
    }

    string DataService::get_real_work_dir()
    {
      const char* work_dir = get_work_dir();
      string real_work_dir = "";
      if (NULL != work_dir)
      {
        real_work_dir = string(work_dir) + "/dataserver_" + server_index_;
      }
      return real_work_dir;
    }

    int DataService::initialize(int argc, char* argv[])
    {
      UNUSED(argc);
      srandom(time(NULL));
      int32_t ret = SYSPARAM_DATASERVER.initialize(config_file_, server_index_);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "load dataserver parameter failed: %d", ret);
        ret = EXIT_GENERAL_ERROR;
      }

      if (TFS_SUCCESS == ret)
      {
        const int32_t server_index = atoi(server_index_.c_str());
        DsRuntimeGlobalInformation& instance = DsRuntimeGlobalInformation::instance();
        instance.information_.type_ = 0 == server_index ? DATASERVER_DISK_TYPE_SYSTEM : DATASERVER_DISK_TYPE_FULL;
      }

      if (TFS_SUCCESS == ret)
      {
        //create work directory
        string work_dir = get_real_work_dir();
        ret = work_dir.empty() ? EXIT_GENERAL_ERROR: TFS_SUCCESS;
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "%s not set work dir, must be exist", argv[0]);
        }
        if (TFS_SUCCESS == ret)
        {
          string storage_dir = work_dir + string("/storage");
          if (!DirectoryOp::create_full_path(storage_dir.c_str()))
          {
            ret = EXIT_GENERAL_ERROR;
            TBSYS_LOG(ERROR, "create directory %s error: %s", storage_dir.c_str(), strerror(errno));
          }
          if ( TFS_SUCCESS == ret)
          {
            storage_dir = work_dir + string("/tmp");
            if (!DirectoryOp::create_full_path(storage_dir.c_str()))
            {
              ret = EXIT_GENERAL_ERROR;
              TBSYS_LOG(ERROR, "create directory %s error: %s", storage_dir.c_str(), strerror(errno));
            }
          }
          if (TFS_SUCCESS == ret)
          {
            storage_dir = work_dir + string("/mirror");
            if (!DirectoryOp::create_full_path(storage_dir.c_str()))
            {
              ret = EXIT_GENERAL_ERROR;
              TBSYS_LOG(ERROR, "create directory %s error: %s", storage_dir.c_str(), strerror(errno));
            }
          }
        }
      }

      //set name server ip
      std::vector<uint64_t> ns_ip_port;
      if (TFS_SUCCESS == ret)
      {
        ret = initialize_nameserver_ip_addr_(ns_ip_port);
      }

      //check dev & ip
      if (TFS_SUCCESS == ret)
      {
        const char* ip_addr = get_ip_addr();
        if (NULL == ip_addr)//get ip addr
        {
          ret =  EXIT_CONFIG_ERROR;
          TBSYS_LOG(ERROR, "%s", "dataserver not set ip_addr");
        }

        if (TFS_SUCCESS == ret)
        {
          const char *dev_name = get_dev();
          if (NULL == dev_name)//get dev name
          {
            ret =  EXIT_CONFIG_ERROR;
            TBSYS_LOG(ERROR, "%s","dataserver not set dev_name");
          }
          else
          {
            uint32_t ip_addr_id = tbsys::CNetUtil::getAddr(ip_addr);
            uint32_t local_ip   = Func::get_local_addr(dev_name);
            if (local_ip != ip_addr_id)
            {
              TBSYS_LOG(WARN, "ip '%s' is not local ip, local ip: %s",ip_addr, tbsys::CNetUtil::addrToString(local_ip).c_str());
              ret = EXIT_CONFIG_ERROR;
            }
          }
        }
      }

      //start clientmanager
      if (TFS_SUCCESS == ret)
      {
        NewClientManager::get_instance().destroy();
        assert(NULL != get_packet_streamer());
        assert(NULL != get_packet_factory());
        BasePacketStreamer* packet_streamer = dynamic_cast<BasePacketStreamer*>(get_packet_streamer());
        BasePacketFactory* packet_factory   = dynamic_cast<BasePacketFactory*>(get_packet_factory());
        ret = NewClientManager::get_instance().initialize(packet_factory, packet_streamer,
                NULL, &BaseService::golbal_async_callback_func, this);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "start client manager failed, must be exit!!!");
          ret = EXIT_NETWORK_ERROR;
        }
      }

      if (TFS_SUCCESS == ret)
      {
        //init block manager
        string sb_path = string(SYSPARAM_FILESYSPARAM.mount_name_) + SUPERBLOCK_NAME;
        block_manager_ = new (std::nothrow) BlockManager(sb_path);
        assert(NULL != block_manager_);

      }

      //load all blocks
      if (TFS_SUCCESS == ret)
      {
        ret = get_block_manager().bootstrap(SYSPARAM_FILESYSPARAM);
        if (TFS_SUCCESS != ret)
          TBSYS_LOG(ERROR, "load all blocks failed, ret: %d", ret);
      }

      // init lease manager
      if (TFS_SUCCESS == ret)
      {
        lease_manager_ = new (std::nothrow) LeaseManager(*this, ns_ip_port);
        assert(NULL != lease_manager_);
        ret = lease_manager_->initialize();
      }

      if (TFS_SUCCESS == ret)
      {
        srandom(time(NULL));
      }

      // init heartbeat
      //if (TFS_SUCCESS == ret)
      //{
      //  heart_manager_    = new (std::nothrow)DataServerHeartManager(*this, ns_ip_port);
      //  assert(NULL != heart_manager_);
      //  ret = heart_manager_->initialize();
      //}

      // init data_manager/lease_manager
      // lease manager need global info, should init after heart manager
      if (TFS_SUCCESS == ret)
      {
        op_manager_.initialize();
      }

      if (TFS_SUCCESS == ret)
      {
        task_thread_      = new (std::nothrow)RunTaskThreadHelper(*this);
        assert(0 != task_thread_);
        timeout_thread_  = new (std::nothrow)TimeoutThreadHelper(*this);
        assert(0 != timeout_thread_);
        check_thread_ = new (std::nothrow)RunCheckThreadHelper(*this);
        assert(0 != check_thread_);
      }

      // sync mirror should init after bootstrap
      if (TFS_SUCCESS == ret)
      {
        const int32_t limit = TBSYS_CONFIG.getInt(CONF_SN_DATASERVER, CONF_SYNC_FILE_ENTRY_QUEUE_LIMIT, 100 * 30 * 60);
        const char* ratio = TBSYS_CONFIG.getString(CONF_SN_DATASERVER, CONF_SYNC_FILE_ENTRY_QUEUE_WARN_RATIO,"0.8");
        float warn_ratio  = strtof(ratio, NULL);
        const char* str_dest_addr = TBSYS_CONFIG.getString(CONF_SN_DATASERVER, CONF_SYNC_FILE_ENTRY_DEST_ADDR, NULL);
        uint64_t dest_addr = INVALID_SERVER_ID;
        if (NULL != str_dest_addr)
        {
          std::vector<string> vec;
          common::Func::split_string(str_dest_addr, ':', vec);
          ret = vec.size() == 2U ? TFS_SUCCESS : EXIT_SYSTEM_PARAMETER_ERROR;
          if (TFS_SUCCESS == ret)
          {
            dest_addr = tbsys::CNetUtil::strToAddr(vec[0].c_str(), atoi(vec[0].c_str()));
          }
        }
        if (TFS_SUCCESS == ret && INVALID_SERVER_ID != dest_addr)
        {
          DsRuntimeGlobalInformation& instance = DsRuntimeGlobalInformation::instance();
          sync_manager_ = new (std::nothrow)SyncManager(dest_addr, instance.information_.id_, instance.ns_vip_port_, limit, warn_ratio);
          assert(NULL != sync_manager_);
        }
        if (INVALID_SERVER_ID != dest_addr)
        {
          TBSYS_LOG(INFO, "start sync file queue %s, queue limit: %d, queue warn ratio: %f, dest_addr: %s",
            TFS_SUCCESS == ret ? "successful" : "failed", limit, warn_ratio, NULL != str_dest_addr ? str_dest_addr : "null");
        }
      }

      // init migrate
      if (TFS_SUCCESS == ret)
      {
        const char* str_dest_addr = TBSYS_CONFIG.getString(CONF_SN_DATASERVER, CONF_MIGRATE_SERVER_ADDR, NULL);
        uint64_t migrate_addr = common::INVALID_SERVER_ID;
        if (NULL != str_dest_addr)
        {
          std::vector<string> vec;
          common::Func::split_string(str_dest_addr, ':', vec);
          ret = vec.size() == 2U ? TFS_SUCCESS : EXIT_SYSTEM_PARAMETER_ERROR;
          if (TFS_SUCCESS == ret)
          {
            migrate_addr = tbsys::CNetUtil::strToAddr(vec[0].c_str(), atoi(vec[0].c_str()));
          }
        }
        if (TFS_SUCCESS == ret && INVALID_SERVER_ID != migrate_addr)
        {
          DsRuntimeGlobalInformation& instance = DsRuntimeGlobalInformation::instance();
          migrate_manager_ = new (std::nothrow)MigrateManager(migrate_addr, instance.information_.id_);
          assert(NULL != migrate_manager_);
          migrate_manager_->initialize();
        }

        if (INVALID_SERVER_ID != migrate_addr)
        {
          TBSYS_LOG(INFO, "start migrate heartbeat %s, migrate serveraddr: %s",
            TFS_SUCCESS == ret ? "successful" : "failed", NULL != str_dest_addr ? str_dest_addr : "null");
        }
      }

      return ret;
    }

    int DataService::initialize_nameserver_ip_addr_(std::vector<uint64_t>& ns_ip_port)
    {
      IpAddr* addr = NULL;
      int32_t ret = (SYSPARAM_DATASERVER.local_ns_ip_.empty()) ? EXIT_SYSTEM_PARAMETER_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {

        DsRuntimeGlobalInformation& instance = DsRuntimeGlobalInformation::instance();
        addr = reinterpret_cast<IpAddr*> (&instance.ns_vip_port_);
        addr->ip_ =  Func::get_addr(SYSPARAM_DATASERVER.local_ns_ip_.c_str());
        addr->port_ =  SYSPARAM_DATASERVER.local_ns_port_;
        ret = (0 == addr->ip_) ? EXIT_SYSTEM_PARAMETER_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS != ret)
          TBSYS_LOG(ERROR, "nameserver ip addrr is invalid, %s", SYSPARAM_DATASERVER.local_ns_ip_.c_str());
      }

      if (TFS_SUCCESS == ret)
      {
        ret = SYSPARAM_DATASERVER.ns_addr_list_.empty() ? EXIT_SYSTEM_PARAMETER_ERROR: TFS_SUCCESS;
        if (TFS_SUCCESS != ret)
          TBSYS_LOG(ERROR, "nameserver real ip addrr is invalid, %s", SYSPARAM_DATASERVER.ns_addr_list_.c_str());
      }

      if (TFS_SUCCESS == ret)
      {
        const int32_t BUF_LEN = 512;
        char buffer[BUF_LEN] = {'\0'};
        strncpy(buffer, SYSPARAM_DATASERVER.ns_addr_list_.c_str(), BUF_LEN);
        char* t = NULL, *s = buffer;
        while (NULL != (t = strsep(&s, "|")))
        {
          uint32_t ip = Func::get_addr(t);
          ret = (0 == ip) ? EXIT_SYSTEM_PARAMETER_ERROR : TFS_SUCCESS;
          if (TFS_SUCCESS != ret)
            TBSYS_LOG(ERROR, "nameserver real ip addrr: %s is invalid...", t);
          else
            ns_ip_port.push_back(Func::str_to_addr(t, (addr->port_ + 1)));
        }
      }

      if (TFS_SUCCESS == ret)
      {
        int32_t size = ns_ip_port.size();
        ret = ns_ip_port.empty() || size > MAX_SINGLE_CLUSTER_NS_NUM ? EXIT_SYSTEM_PARAMETER_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS != ret)
          TBSYS_LOG(WARN, "must have one ns, check your ns' list, %s",  SYSPARAM_DATASERVER.ns_addr_list_.c_str());
      }
      return ret;
    }

    int DataService::destroy_service()
    {
      DsRuntimeGlobalInformation::instance().destroy();

      if (NULL != sync_manager_)
        sync_manager_->destroy();

      if (NULL != migrate_manager_)
        migrate_manager_->destroy();

      if (NULL != lease_manager_)
         lease_manager_->destroy();

      if (0 != task_thread_)
      {
        get_task_manager().stop_task();
        task_thread_->join();
      }

      if (0 != timeout_thread_)
      {
        timeout_thread_->join();
      }

      if (0 != check_thread_)
      {
        check_thread_->join();
      }

      return TFS_SUCCESS;
    }

    void DataService::rotate_(time_t& last_rotate_log_time, time_t now, time_t zonesec)
    {
      if ((now % 86400 >= zonesec)
          && (now % 86400 < zonesec + 300)
          && (last_rotate_log_time < now - 600))
      {
        last_rotate_log_time = now;
        TBSYS_LOGGER.rotateLog(NULL);
      }
    }

    void DataService::dump_stat_(time_t now)
    {
      int32_t interval = std::max(SYSPARAM_DATASERVER.dump_vs_interval_, 60);
      if (now % interval == 0)
      {
        static int64_t last_write_bytes[2] = {0};
        static int64_t last_write_file_count[2] = {0};
        static int64_t last_read_bytes[2] = {0};
        static int64_t last_read_file_count[2] = {0};
        static int64_t last_stat_file_count[2] = {0};
        static int64_t last_unlink_file_count[2] = {0};

        DataServerStatInfo& info = DsRuntimeGlobalInformation::instance().information_;
        char buf[1024];
        const char header_fmt[] = "\t%-12s%12s%18s%12s%18s\n";
        const char fmt[] = "\t%-12s%12"PRI64_PREFIX"d%18"PRI64_PREFIX"d%12"PRI64_PREFIX"d%18"PRI64_PREFIX"d\n";
        int pos = 0;
        pos += sprintf(buf + pos, header_fmt,
            "Oper-Type", "Succ-Count", "Succ-Bytes(KB)", "Fail-Count", "Fail-Bytes(KB)");

        // dump total access info
        pos += sprintf(buf + pos, fmt, "total-read",
            info.read_file_count_[0], info.read_bytes_[0] / 1024,
            info.read_file_count_[1], info.read_bytes_[1] / 1024);
        pos += sprintf(buf + pos, fmt, "total-write",
            info.write_file_count_[0], info.write_bytes_[0] / 1024,
            info.write_file_count_[1], info.write_bytes_[1] / 1024);
        pos += sprintf(buf + pos, fmt, "total-stat",
            info.stat_file_count_[0], 0L,
            info.stat_file_count_[1], 0L);
        pos += sprintf(buf + pos, fmt, "total-unlink",
            info.unlink_file_count_[0], 0L,
            info.unlink_file_count_[1], 0L);

        // dump last minute access info
        pos += sprintf(buf + pos, fmt, "last-read",
            info.read_file_count_[0] - last_read_file_count[0],
            (info.read_bytes_[0] - last_read_bytes[0]) / 1024,
            info.read_file_count_[1] - last_read_file_count[1],
            (info.read_bytes_[1] - last_read_bytes[1]) / 1024);
        pos += sprintf(buf + pos, fmt, "last-write",
            info.write_file_count_[0] - last_write_file_count[0],
            (info.write_bytes_[0] - last_write_bytes[0]) / 1024,
            info.write_file_count_[1] - last_write_file_count[1],
            (info.write_bytes_[1] - last_write_bytes[1]) / 1024);
        pos += sprintf(buf + pos, fmt, "last-stat",
            info.stat_file_count_[0] - last_stat_file_count[0], 0L,
            info.stat_file_count_[1] - last_stat_file_count[1], 0L);
        pos += sprintf(buf + pos, fmt, "last_unlink",
            info.unlink_file_count_[0] - last_unlink_file_count[0], 0L,
            info.unlink_file_count_[1] - last_unlink_file_count[1], 0L);

        TBSYS_LOG(INFO, "DUMP ACCESS STAT BEGIN\n%s", buf);
        TBSYS_LOG(INFO, "DUMP ACCESS STAT END");

        // update last info for next dump
        last_write_file_count[0] = info.write_file_count_[0];
        last_write_file_count[1] = info.write_file_count_[1];
        last_write_bytes[0] = info.write_bytes_[0];
        last_write_bytes[1] = info.write_bytes_[1];
        last_read_file_count[0] = info.read_file_count_[0];
        last_read_file_count[1] = info.read_file_count_[1];
        last_read_bytes[0] = info.read_bytes_[0];
        last_read_bytes[1] = info.read_bytes_[1];
        last_stat_file_count[0] = info.stat_file_count_[0];
        last_stat_file_count[1] = info.stat_file_count_[1];
        last_unlink_file_count[0] = info.unlink_file_count_[0];
        last_unlink_file_count[1] = info.unlink_file_count_[1];
      }
    }

    void DataService::timeout_()
    {
      tzset();
      const int32_t MAX_SLEEP_TIME_US = 1 * 1000 * 1000;//1s
      time_t zonesec = 86400 + timezone, now = 0, last_rotate_log_time = 0;
      while (!DsRuntimeGlobalInformation::instance().is_destroyed())
      {
        now = time(NULL);
        //rotate log
        rotate_(last_rotate_log_time, now, zonesec);

        // dump access stat
        dump_stat_(now);

        //check datafile
        op_manager_.timeout(Func::get_monotonic_time());

        if (NULL != block_manager_)
          block_manager_->timeout(Func::get_monotonic_time());

        task_manager_.expire_task();
        lease_manager_->timeout(Func::get_monotonic_time());

        usleep(MAX_SLEEP_TIME_US);
      }
    }

    void DataService::run_task_()
    {
      task_manager_.run_task();
    }

    void DataService::run_check_()
    {
      check_manager_.run_check();
    }

    int DataService::callback(common::NewClient* client)
    {
      int32_t ret = NULL != client ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        tbnet::Packet* packet = client->get_source_msg();
        ret = (NULL != packet)? TFS_SUCCESS: TFS_ERROR;
        if (TFS_SUCCESS == ret)
        {
          int32_t pcode = packet->getPCode();
          if (WRITE_FILE_MESSAGE_V2 == pcode ||
              CLOSE_FILE_MESSAGE_V2 == pcode ||
              UNLINK_FILE_MESSAGE_V2 == pcode)
          {
            ret = client_request_server_.callback(client);
          }
          else if (DS_APPLY_BLOCK_MESSAGE == pcode ||
              DS_GIVEUP_BLOCK_MESSAGE == pcode)
          {
            ret = writable_block_manager_.callback(client);
          }
          else if (REPORT_CHECK_BLOCK_RESPONSE_MESSAGE == pcode)
          {
            TBSYS_LOG(INFO, "report check result succuss");
          }
          else
          {
            TBSYS_LOG(ERROR, "callback handle error message pcode: %d", pcode);
          }
        }
      }
      return ret;
    }

    tbnet::IPacketHandler::HPRetCode DataService::handlePacket(tbnet::Connection *connection, tbnet::Packet *packet)
    {
      tbnet::IPacketHandler::HPRetCode hret = tbnet::IPacketHandler::FREE_CHANNEL;
      bool bret = NULL != connection && NULL != packet;
      if (bret)
      {
        TBSYS_LOG(DEBUG, "receive pcode : %d", packet->getPCode());
        if (!packet->isRegularPacket())
        {
          bret = false;
          TBSYS_LOG(WARN, "control packet, pcode: %d", dynamic_cast<tbnet::ControlPacket*>(packet)->getCommand());
        }
        if (bret)
        {
          BasePacket* bpacket = dynamic_cast<BasePacket*>(packet);
          bpacket->set_connection(connection);
          bpacket->setExpireTime(MAX_RESPONSE_TIME);
          bpacket->set_direction(static_cast<DirectionStatus>(bpacket->get_direction()|DIRECTION_RECEIVE));

          if (bpacket->is_enable_dump())
          {
            bpacket->dump();
          }
          if (!DsRuntimeGlobalInformation::instance().is_destroyed())
          {
            bret = push(bpacket, false);
            if (bret)
              hret = tbnet::IPacketHandler::KEEP_CHANNEL;
            else
            {
              bpacket->reply_error_packet(TBSYS_LOG_LEVEL(ERROR),EXIT_WORK_QUEUE_FULL, "peer: %s, local: %s. task message beyond max queue size, discard", tbsys::CNetUtil::addrToString(bpacket->get_connection()->getPeerId()).c_str(), get_ip_addr());
              bpacket->free();
            }
          }
          else
          {
            bpacket->reply_error_packet(TBSYS_LOG_LEVEL(WARN), STATUS_MESSAGE_ACCESS_DENIED,
                "you client %s access been denied. msgtype: %d", tbsys::CNetUtil::addrToString(
                  connection->getPeerId()).c_str(), packet->getPCode());
            // packet denied, must free
            bpacket->free();
          }
        }
      }
      return hret;
    }

    bool DataService::handlePacketQueue(tbnet::Packet* packet, void* args)
    {
      bool bret = BaseService::handlePacketQueue(packet, args);
      if (bret)
      {
        int32_t pcode = packet->getPCode();
        int32_t ret = LOCAL_PACKET == pcode ? TFS_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          switch (pcode)
          {
            case LIST_BLOCK_MESSAGE:
              ret = list_blocks(dynamic_cast<ListBlockMessage*>(packet));
              break;
            case REPLICATE_BLOCK_MESSAGE:
            case COMPACT_BLOCK_MESSAGE:
            case DS_COMPACT_BLOCK_MESSAGE:
            case DS_REPLICATE_BLOCK_MESSAGE:
            case RESP_DS_COMPACT_BLOCK_MESSAGE:
            case RESP_DS_REPLICATE_BLOCK_MESSAGE:
            case REQ_EC_MARSHALLING_MESSAGE:
            case REQ_EC_REINSTATE_MESSAGE:
            case REQ_EC_DISSOLVE_MESSAGE:
            case NS_REQ_RESOLVE_BLOCK_VERSION_CONFLICT_MESSAGE:
              ret = task_manager_.handle(dynamic_cast<BaseTaskMessage*>(packet));
              break;
            case GET_BLOCK_INFO_MESSAGE_V2:
              ret = get_block_info(dynamic_cast<GetBlockInfoMessageV2*>(packet));
              break;
            case GET_SERVER_STATUS_MESSAGE:
              ret = get_server_status(dynamic_cast<GetServerStatusMessage*>(packet));
              break;
            case STATUS_MESSAGE:
              ret = get_ping_status(dynamic_cast<StatusMessage*>(packet));
              break;
            case CLIENT_CMD_MESSAGE:
              ret = client_command(dynamic_cast<ClientCmdMessage*>(packet));
              break;
            case REQ_CALL_DS_REPORT_BLOCK_MESSAGE:
            case STAT_FILE_MESSAGE_V2:
            case READ_FILE_MESSAGE_V2:
            case WRITE_FILE_MESSAGE_V2:
            case CLOSE_FILE_MESSAGE_V2:
            case UNLINK_FILE_MESSAGE_V2:
            case NEW_BLOCK_MESSAGE_V2:
            case REMOVE_BLOCK_MESSAGE_V2:
            case READ_RAWDATA_MESSAGE_V2:
            case WRITE_RAWDATA_MESSAGE_V2:
            case READ_INDEX_MESSAGE_V2:
            case WRITE_INDEX_MESSAGE_V2:
            case QUERY_EC_META_MESSAGE:
            case COMMIT_EC_META_MESSAGE:
            case GET_ALL_BLOCKS_HEADER_MESSAGE:
              ret = client_request_server_.handle(packet);
              break;
            case REQ_CHECK_BLOCK_MESSAGE:
            case REPORT_CHECK_BLOCK_MESSAGE:
              ret = check_manager_.handle(packet);
              break;
            default:
              TBSYS_LOG(ERROR, "process packet pcode: %d\n", pcode);
              ret = TFS_ERROR;
              break;
          }
          if (common::TFS_SUCCESS != ret)
          {
            common::BasePacket* msg = dynamic_cast<common::BasePacket*>(packet);
            msg->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), ret, "execute message failed");
          }
        }
      }
      return bret;
    }

    int DataService::list_blocks(ListBlockMessage* message)
    {
      int32_t list_type = message->get_block_type();
      RespListBlockMessage* resp_lb_msg = new (std::nothrow) RespListBlockMessage();
      assert(NULL != resp_lb_msg);

      if (list_type & LB_BLOCK)
      {
        common::VUINT64* blocks = resp_lb_msg->get_blocks();
        get_block_manager().get_all_block_ids(*blocks);
      }

      if (list_type & LB_PAIRS)
      {
        map<uint64_t, std::vector<int32_t> >* pairs = resp_lb_msg->get_pairs();
        get_block_manager().get_all_logic_block_to_physical_block(*pairs);
      }

      if (list_type & LB_INFOS)
      {
        vector<common::BlockInfoV2>* infos = resp_lb_msg->get_infos();
        get_block_manager().get_all_block_info(*infos);
      }

      resp_lb_msg->set_status_type(list_type);
      return message->reply(resp_lb_msg);
    }

    int DataService::get_block_info(GetBlockInfoMessageV2 *message)
    {
      uint64_t block_id = message->get_block_id();
      BlockInfoV2 block_info;
      int ret = get_block_manager().get_block_info(block_info, block_id);
      if (TFS_SUCCESS != ret)
      {
        ret = message->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), ret,
            "block is not exist, blockid: %"PRI64_PREFIX"u, ret: %d", block_id, ret);
      }
      else
      {
        UpdateBlockInfoMessageV2* resp_ubi_msg = new (std::nothrow) UpdateBlockInfoMessageV2();
        assert(NULL != resp_ubi_msg);
        resp_ubi_msg->set_block_info(block_info);
        ret = message->reply(resp_ubi_msg);
      }
      return ret;
    }

    int DataService::get_server_status(GetServerStatusMessage *message)
    {
      int ret = TFS_SUCCESS;
      int32_t type = message->get_status_type();

      if (GSS_MAX_VISIT_COUNT == type)
      {
        // no longer has MAX_VISIT_COUNT information
      }
      else if (GSS_BLOCK_FILE_INFO == type)
      {
        uint64_t block_id = message->get_return_row();
        uint64_t attach_block_id = block_id;
        //get block file list
        BlockFileInfoMessage* resp_bfi_msg = new (std::nothrow) BlockFileInfoMessage();
        assert(NULL != resp_bfi_msg);
        FILE_INFO_LIST& fileinfos = resp_bfi_msg->get_fileinfo_list();
        ret = get_block_manager().traverse(fileinfos, block_id, attach_block_id);
        if (TFS_SUCCESS != ret)
        {
          tbsys::gDelete(resp_bfi_msg);
          ret = message->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), ret,
              "GSS_BLOCK_FILE_INFO fail, blockid: %"PRI64_PREFIX"u, ret: %d", block_id, ret);
        }
        else
        {
          ret = message->reply(resp_bfi_msg);
        }
      }
      else if (GSS_BLOCK_FILE_INFO_V2 == type)
      {
        uint64_t block_id = message->get_return_row();
        uint64_t attach_block_id = message->get_from_row();
        //get block file list
        IndexHeaderV2 header;
        BlockFileInfoMessageV2* resp_bfi_msg = new (std::nothrow) BlockFileInfoMessageV2();
        assert(NULL != resp_bfi_msg);
        FILE_INFO_LIST_V2* fileinfos = resp_bfi_msg->get_fileinfo_list();
        ret = get_block_manager().traverse(header, *fileinfos, block_id, attach_block_id);
        if (TFS_SUCCESS != ret)
        {
          tbsys::gDelete(resp_bfi_msg);
          ret = message->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), ret,
              "GSS_BLOCK_FILE_INFO fail, blockid: %"PRI64_PREFIX"u, ret: %d", block_id, ret);
        }
        else
        {
          ret = message->reply(resp_bfi_msg);
        }
      }
      else if (GSS_BLOCK_RAW_META_INFO == type)
      {
        // meta will be included in FileInfoV2
      }
      else if (GSS_BLOCK_STATISTIC_VISIT_INFO == type)
      {
        bool reset = message->get_from_row();
        BlockStatisticVisitInfoMessage* reply_msg = new (std::nothrow) BlockStatisticVisitInfoMessage();
        assert(NULL != reply_msg);
        ret = get_block_manager().get_all_block_statistic_visit_info(reply_msg->get_block_statistic_visit_maps(), reset);
        if (TFS_SUCCESS != ret)
        {
          tbsys::gDelete(reply_msg);
          ret = message->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), ret,"GSS_BLOCK_STATISTIC_VISIT_INFO fail, ret: %d", ret);
        }
        else
        {
          ret = message->reply(reply_msg);
        }
      }
      return ret;
    }

    int DataService::get_ping_status(StatusMessage* message)
    {
      int ret = TFS_SUCCESS;
      if (STATUS_MESSAGE_PING == message->get_status())
      {
        StatusMessage *statusmessage = new StatusMessage(STATUS_MESSAGE_PING);
        message->reply(statusmessage);
      }
      else
      {
        ret = TFS_ERROR;
      }
      return ret;
    }

    int32_t DataService::client_command(ClientCmdMessage* message)
    {
      StatusMessage* resp = new StatusMessage(STATUS_MESSAGE_ERROR, "unknown client cmd.");
      int32_t type = message->get_cmd();
      uint64_t from_server_id = message->get_value2();
      do
      {
        // load a lost block
        if (CLIENT_CMD_SET_PARAM == type)
        {
          uint32_t block_id = message->get_value3();
          uint64_t server_id = message->get_value1();
          TBSYS_LOG(DEBUG, "set run param block_id: %u, server: %" PRI64_PREFIX "u, from: %s", block_id, server_id,
              tbsys::CNetUtil::addrToString(from_server_id).c_str());
        }
      }
      while (0);
      message->reply(resp);
      return TFS_SUCCESS;
    }

    void DataService::TimeoutThreadHelper::run()
    {
      service_.timeout_();
    }

    void DataService::RunTaskThreadHelper::run()
    {
      service_.run_task_();
    }

    void DataService::RunCheckThreadHelper::run()
    {
      service_.run_check_();
    }



    int ds_async_callback(common::NewClient* client)
    {
      DataService* service = dynamic_cast<DataService*>(BaseMain::instance());
      int32_t ret = NULL != service ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = service->callback(client);
      }
      return ret;
    }
  }/** end namespace dataserver **/
}/** end namespace tfs **/
