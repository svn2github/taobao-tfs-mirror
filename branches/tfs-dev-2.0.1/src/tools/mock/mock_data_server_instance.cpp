/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: mock_data_server_instance.cpp 451 2011-06-08 09:47:31Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 *
 */

#include <tbsys.h>

#include "common/status_message.h"
#include "common/config_item.h"
#include "common/client_manager.h"

#include "message/message_factory.h"

#include "mock_data_server_instance.h"

using namespace tbsys;
using namespace tfs::mock;
using namespace tfs::message;
using namespace tfs::common;
static FileInfo gfile_info;
static const int8_t BUF_LEN = 32;
static char BUF[BUF_LEN] = {'1'};

static int ns_async_callback(NewClient* client);

MockDataService::MockDataService():
  ns_ip_port_(0),
  need_send_block_to_ns_(HAS_BLOCK_FLAG_YES)
{
  memset(&information_, 0, sizeof(information_));
  memset(&gfile_info, 0, sizeof(gfile_info));
  gfile_info.size_ = BUF_LEN;
  gfile_info.crc_  = 0;
  gfile_info.id_ = 0xff;
  gfile_info.crc_  = Func::crc(gfile_info.crc_, BUF, BUF_LEN);
}

MockDataService::~MockDataService()
{

}

int MockDataService::parse_common_line_args(int argc, char* argv[])
{
  int32_t i = 0;
  while ((i = getopt(argc, argv, "l:")) != EOF)
  {
    switch (i)
    {
      case 'l':
        server_index_ = optarg;
        break;
      case 'c':
        information_.total_capacity_ = atoi(optarg);
        break;
      default:
        TBSYS_LOG(ERROR, "%s", "invalid parameter");
        break;
    }
  }
  return server_index_.empty() && information_.total_capacity_ > 0 ? TFS_ERROR : TFS_SUCCESS;
}

int32_t MockDataService::get_listen_port() const
{
  int32_t port = get_port() + atoi(server_index_.c_str());
  return port <= 1024 || port > 65535 ? -1 : port;
}

int32_t MockDataService::get_ns_port() const
{
  int32_t port = TBSYS_CONFIG.getInt(CONF_SN_MOCK_DATASERVER, CONF_PORT, -1);
  return port <= 1024 || port > 65535 ? -1 : port;
}

const char* MockDataService::get_log_file_path()
{
  const char* log_file_path = NULL;
  const char* work_dir = get_work_dir();
  if (work_dir != NULL)
  {
    log_file_path_ = work_dir;
    log_file_path_ += "/logs/moc_dataserver_"+server_index_;
    log_file_path_ += ".log";
    log_file_path = log_file_path_.c_str();
  }
  return log_file_path;
}
 

int MockDataService::keepalive()
{
  {
    RWLock::Lock lock(blocks_mutex_, WRITE_LOCKER);
    information_.current_time_ = time(NULL);
    information_.current_load_ = Func::get_load_avg();
    information_.block_count_  = blocks_.size();
  }

  SetDataserverMessage msg;
  msg.set_ds(&information_);
  if (need_send_block_to_ns_ == HAS_BLOCK_FLAG_YES)
  {
    RWLock::Lock lock(blocks_mutex_, READ_LOCKER);
    std::map<uint32_t, BlockEntry>::iterator iter = blocks_.begin();
    for (; iter != blocks_.end(); ++iter)
    {
      msg.add_block(&iter->second.info_);
    }
  }

  TBSYS_LOG(DEBUG, "keepalive, need_send_block_to_ns_(%d)", need_send_block_to_ns_);

  NewClient* client = NewClientManager::get_instance().create_client();
  tbnet::Packet* result = NULL;
  int32_t iret = send_msg_to_server(ns_ip_port_, client, &msg, result);
  if (iret == TFS_SUCCESS)
  {
    if (result->getPCode() == RESP_HEART_MESSAGE)
    {
      RespHeartMessage* message = dynamic_cast<RespHeartMessage*>(result);
      need_send_block_to_ns_ = HAS_BLOCK_FLAG_NO;
      if (message->get_status() == HEART_NEED_SEND_BLOCK_INFO)
      {
        need_send_block_to_ns_ = HAS_BLOCK_FLAG_YES;
      }
    }
    else
    {
      TBSYS_LOG(WARN, "message is invalid, pcode(%d)", result->getPCode());
    }
  }
  else
  {
    TBSYS_LOG(ERROR, "%s", "message is null or iret(%d) !=  TFS_SUCCESS");
  }
  NewClientManager::get_instance().destroy_client(client);
  return iret;
}

/** handle single packet */
bool MockDataService::handlePacketQueue(tbnet::Packet *packet, void *args)
{
  bool bret = BaseService::handlePacketQueue(packet, args);
  if (bret)
  {
    int32_t pcode = packet->getPCode();
    int32_t iret = LOCAL_PACKET == pcode ? TFS_ERROR : common::TFS_SUCCESS;
    if (TFS_SUCCESS == iret)
    {
      TBSYS_LOG(DEBUG, "PCODE: %d", pcode);
      common::BasePacket* msg = dynamic_cast<common::BasePacket*>(packet);
      switch( pcode)
      {
        case CREATE_FILENAME_MESSAGE:
          iret = create_file_number(msg);
          break;
        case WRITE_DATA_MESSAGE:
          iret = write(msg);
          break;
        case READ_DATA_MESSAGE:
          iret = read(msg);
          break;
        case READ_DATA_MESSAGE_V2:
          iret = readv2(msg);
          break;
        case CLOSE_FILE_MESSAGE:
          iret = close(msg);
          break;
        case NEW_BLOCK_MESSAGE:
          iret = new_block(msg);
          break;
        case FILE_INFO_MESSAGE:
          iret = get_file_info(msg);
          break;

        default:
          iret = EXIT_UNKNOWN_MSGTYPE;
          TBSYS_LOG(ERROR, "unknown msg type: %d", pcode);
          break;
      }
      if (common::TFS_SUCCESS != iret)
      {
        msg->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), iret, "execute message failed");
      }
    }
  }
  return bret;
}

int MockDataService::initialize(int argc, char* argv[])
{
  UNUSED(argc);
  UNUSED(argv);
  int32_t iret = get_ns_port() > 0 ? TFS_SUCCESS : TFS_ERROR;
  if (TFS_SUCCESS == iret)
  {
    ns_ip_port_ = tbsys::CNetUtil::strToAddr(get_ip_addr(), get_ns_port());

    IpAddr* adr = reinterpret_cast<IpAddr*>(&information_.id_);
    adr->ip_ = Func::get_local_addr(get_dev());
    adr->port_ = get_listen_port();

    int32_t heart_interval = TBSYS_CONFIG.getInt(CONF_SN_MOCK_DATASERVER,CONF_HEART_INTERVAL,2);
    KeepaliveTimerTaskPtr task = new KeepaliveTimerTask(*this);
    get_timer()->scheduleRepeated(task, tbutil::Time::seconds(heart_interval));
  }
  return iret;
}

int MockDataService::callback(common::NewClient* client)
{
  int32_t iret = NULL != client ? TFS_SUCCESS : TFS_ERROR;
  if (TFS_SUCCESS == iret)
  {
    NewClient::RESPONSE_MSG_MAP* sresponse = client->get_success_response();
    NewClient::RESPONSE_MSG_MAP* fresponse = client->get_fail_response();
    iret = NULL != sresponse && fresponse != NULL ? TFS_SUCCESS : TFS_ERROR;
    if (TFS_SUCCESS == iret)
    {
      tbnet::Packet* packet = client->get_source_msg();
      assert(NULL != packet);
      bool all_success = sresponse->size() == client->get_send_id_sign().size();
      BasePacket* bpacket= dynamic_cast<BasePacket*>(packet);
      StatusMessage* reply_msg =  all_success ?
        new StatusMessage(STATUS_MESSAGE_OK, "write data complete"):
        new StatusMessage(STATUS_MESSAGE_ERROR, "write data fail");
      iret = bpacket->reply(reply_msg);
    }
  }
  return iret;

}

int MockDataService::write(common::BasePacket* msg)
{
  int32_t iret = NULL != msg ? TFS_SUCCESS : TFS_ERROR;
  if (TFS_SUCCESS == iret)
  {
    WriteDataMessage* message = dynamic_cast<WriteDataMessage*>(msg);
    WriteDataInfo write_info = message->get_write_info();
    uint32_t lease_id = message->get_lease_id();
    int32_t version = message->get_block_version();
    write_info.length_  = MAX_WRITE_FILE_SIZE != 0 ? MAX_WRITE_FILE_SIZE : write_info.length_;
    {
      RWLock::Lock lock(blocks_mutex_, WRITE_LOCKER);
      std::map<uint32_t, BlockEntry>::iterator iter = blocks_.find(write_info.block_id_);
      if (iter == blocks_.end())
      {
        iret = message->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), iret, "create file failed. blockid: %u, fileid: %" PRI64_PREFIX "u.", write_info.block_id_);
      }
      if (TFS_SUCCESS == iret)
      {
        iter->second.info_.size_ += write_info.length_;
      }
    }
    if (TFS_SUCCESS == iret)
    {
      {
        RWLock::Lock lock(infor_mutex_, WRITE_LOCKER);
        information_.use_capacity_ += write_info.length_;
      }

      if (Master_Server_Role == write_info.is_server_)
      {
        message->set_server(Slave_Server_Role);
        message->set_lease_id(lease_id);
        message->set_block_version(version);
        iret = this->post_message_to_server(message, message->get_ds_list());
        if ( 0x01 == iret)
        {
          iret = message->reply(new StatusMessage(STATUS_MESSAGE_OK));
        }
        else
        {
          iret = message->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), iret,
            "write data fail to other dataserver (send): blockid: %u, fileid: %" PRI64_PREFIX "u, datalen: %d, ret: %d",
            write_info.block_id_, write_info.file_id_, write_info.length_, iret);
        }
      }
      else
      {
        iret = message->reply(new StatusMessage(STATUS_MESSAGE_OK));
      }
    }
  }
  return iret;
}

int MockDataService::read(common::BasePacket* msg)
{
  int32_t iret = NULL != msg ? TFS_SUCCESS : TFS_ERROR;
  if (TFS_SUCCESS == iret)
  {
    ReadDataMessage* message = dynamic_cast<ReadDataMessage*>(msg);
    RespReadDataMessage* rmsg = new RespReadDataMessage();
    char* data = rmsg->alloc_data(BUF_LEN);
    memcpy(data, BUF, BUF_LEN);
    rmsg->set_length(BUF_LEN);
    iret = message->reply(rmsg);
    TBSYS_LOG(DEBUG, "read msg");
  }
  return iret;
}

int MockDataService::readv2(common::BasePacket* msg)
{
  int32_t iret = NULL != msg ? TFS_SUCCESS : TFS_ERROR;
  if (TFS_SUCCESS == iret)
  {
    ReadDataMessageV2* message = dynamic_cast<ReadDataMessageV2*>(msg);
    uint32_t block_id = message->get_block_id();
    RespReadDataMessageV2* rmsg = new RespReadDataMessageV2();
    RWLock::Lock lock(blocks_mutex_, READ_LOCKER);
    std::map<uint32_t, BlockEntry>::iterator iter = blocks_.find(block_id);
    if (iter == blocks_.end())
    {
      rmsg->set_length(0);
      iret = message->reply(rmsg);
    }
    else
    {
      char* data = rmsg->alloc_data(BUF_LEN);
      memcpy(data, BUF, BUF_LEN);
      rmsg->set_length(BUF_LEN);
      rmsg->set_file_info(&gfile_info);
      iret = message->reply(rmsg);
    }
  }
  return iret;
}

int MockDataService::close(common::BasePacket* msg)
{
  int32_t iret = NULL != msg ? TFS_SUCCESS : TFS_ERROR;
  if (TFS_SUCCESS == iret)
  {
    CloseFileMessage* message = dynamic_cast<CloseFileMessage*>(msg);
    CloseFileInfo info = message->get_close_file_info();
    uint32_t lease_id = message->get_lease_id();
    RWLock::Lock lock(blocks_mutex_, WRITE_LOCKER);
    std::map<uint32_t, BlockEntry>::iterator iter = blocks_.find(info.block_id_);
    if (iter == blocks_.end())
    {
       iret = message->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), iret,
          "close write file failed. block is not exist. blockid: %u, fileid: %" PRI64_PREFIX "u.", info.block_id_, info.file_id_);
    }
    else
    {
      if (CLOSE_FILE_SLAVER != info.mode_)
      {
        message->set_mode(CLOSE_FILE_SLAVER);
        message->set_block(&iter->second.info_);
        TBSYS_LOG(DEBUG, "blockid: %u", info.block_id_);
        iret = send_message_to_slave(message, message->get_ds_list());
        if (iret != TFS_SUCCESS)
        {
          iret = commit_to_nameserver(iter, info.block_id_, lease_id, TFS_ERROR);
        }
        else
        {
          iret = commit_to_nameserver(iter, info.block_id_, lease_id, TFS_SUCCESS);
        }
        if (iret == TFS_SUCCESS)
        {
          iret = message->reply(new StatusMessage(STATUS_MESSAGE_OK));
        }
        else
        {
          TBSYS_LOG(ERROR, "dataserver commit fail, block_id: %u, file_id: %"PRI64_PREFIX"u, iret: %d",
              info.block_id_, info.file_id_, iret);
          iret = message->reply(new StatusMessage(STATUS_MESSAGE_ERROR));
        }
      }
      else
      {
        const BlockInfo* copyblk = message->get_block();
        if (NULL != copyblk)
        {
          iter->second.info_.seq_no_ = copyblk->seq_no_;
        }
        iret = message->reply(new StatusMessage(STATUS_MESSAGE_OK));
      }
    }
  }
  return iret;
}

int MockDataService::create_file_number(common::BasePacket* msg)
{
  int32_t iret = NULL != msg ? TFS_SUCCESS : TFS_ERROR;
  if (TFS_SUCCESS == iret)
  {
    CreateFilenameMessage* message = dynamic_cast<CreateFilenameMessage*>(msg);
    uint32_t block_id =  message->get_block_id();
    uint64_t file_id  = message->get_file_id();
    RWLock::Lock lock(blocks_mutex_, WRITE_LOCKER);
    std::map<uint32_t, BlockEntry>::iterator iter = blocks_.find(block_id);
    if (iter == blocks_.end())
    {
      TBSYS_LOG(DEBUG, "create file number failed, blockid : %u", block_id);
      iret = message->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), iret, 
            "create file failed. blockid: %u, fileid: %" PRI64_PREFIX "u.", block_id, file_id);
    }
    else
    {
      file_id = ++iter->second.file_id_factory_;
      RespCreateFilenameMessage* rmsg = new RespCreateFilenameMessage();
      rmsg->set_block_id(block_id);
      rmsg->set_file_id(file_id);
      rmsg->set_file_number(file_id);
      iret = message->reply(rmsg);
    }
  }
  return iret;
}

int MockDataService::new_block(common::BasePacket* msg)
{
  int32_t iret = NULL != msg ? TFS_SUCCESS : TFS_ERROR;
  if (TFS_SUCCESS == iret)
  {
    NewBlockMessage* message = dynamic_cast<NewBlockMessage*>(msg);
    const VUINT32* blocks = message->get_new_blocks();
    VUINT32::const_iterator iter = blocks->begin();
    for (; iter != blocks->end(); ++iter)
    {
      BlockEntry entry;
      entry.info_.block_id_ = (*iter);
      entry.info_.version_ = 1;
      std::pair<std::map<uint32_t, BlockEntry>::iterator, bool> res =
        blocks_.insert(std::map<uint32_t, BlockEntry>::value_type((*iter), entry));
      if (!res.second)
      {
        TBSYS_LOG(WARN, "block(%u) exist", (*iter));
      }
      TBSYS_LOG(INFO, "add new block(%u)", (*iter));
    }
    iret = message->reply(new StatusMessage(STATUS_MESSAGE_OK));
  }
  return iret;
}

int MockDataService::get_file_info(BasePacket* msg)
{
  int32_t iret = NULL != msg ? TFS_SUCCESS : TFS_ERROR;
  if (TFS_SUCCESS == iret)
  {
    FileInfoMessage* message = dynamic_cast<FileInfoMessage*>(msg);
    RespFileInfoMessage* rmsg = new RespFileInfoMessage();
    rmsg->set_file_info(&gfile_info);
    iret = message->reply(rmsg);
  }
  return iret;
}

int MockDataService::post_message_to_server(BasePacket* message, const VUINT64& ds_list)
{
  int32_t iret = NULL != message && !ds_list.empty() ? TFS_SUCCESS : TFS_ERROR;
  if (TFS_SUCCESS == iret)
  {
    VUINT64 result;
    VUINT64::const_iterator iter = ds_list.begin();
    for (; iter != ds_list.end(); ++iter)
    {
      if ((*iter) != information_.id_)
      {
        result.push_back((*iter));
      }
    }
    if (!result.empty())
    {
      NewClient* client = NewClientManager::get_instance().create_client();
      iret = client->async_post_request(result, message, ns_async_callback);
      NewClientManager::get_instance().destroy_client(client);
    }
  }
  return iret;
}

int MockDataService::send_message_to_slave(BasePacket* message, const VUINT64& ds_list)
{
  int32_t iret = NULL != message && !ds_list.empty() ? TFS_SUCCESS : TFS_ERROR;
  if (TFS_SUCCESS == iret)
  {
    uint8_t send_id = 0;
    NewClient* client = NewClientManager::get_instance().create_client();
    VUINT64::const_iterator iter = ds_list.begin();
    for (; iter  != ds_list.end(); ++iter)
    {
      if ((*iter) == information_.id_)
      {
        continue;
      }
      client->post_request((*iter), message, send_id);
      if (TFS_SUCCESS != iret)
      {
        TBSYS_LOG(ERROR, "send message to server to dataserver(%s) fail.", tbsys::CNetUtil::addrToString((*iter)).c_str());
        break;
      }
    }
    NewClientManager::get_instance().destroy_client(client);
  }
  return iret;
}

int MockDataService::commit_to_nameserver(std::map<uint32_t, BlockEntry>::iterator iter, uint32_t block_id, uint32_t lease_id, int32_t status, common::UnlinkFlag flag)
{
  UNUSED(status);
  UNUSED(block_id);
  int32_t iret = iter == blocks_.end() ? TFS_ERROR : TFS_SUCCESS;
  if (TFS_SUCCESS == iret)
  {
    BlockInfo info;
    memcpy(&info, &iter->second.info_, sizeof(info));
    ++info.version_;
    BlockWriteCompleteMessage rmsg;
    rmsg.set_block(&info);
    rmsg.set_server_id(information_.id_);
    rmsg.set_lease_id(lease_id);
    rmsg.set_success(WRITE_COMPLETE_STATUS_YES);
    rmsg.set_unlink_flag(flag);
    int32_t ret = STATUS_MESSAGE_ERROR;
    iret = send_msg_to_server(ns_ip_port_, &rmsg, ret);
    if (TFS_SUCCESS == iret)
    {
      iret = STATUS_MESSAGE_OK == ret ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        iter->second.info_.version_++;
      }
    }
  }
  return iret;
}

KeepaliveTimerTask::KeepaliveTimerTask(MockDataService& instance)
  :instance_(instance)
{

}

KeepaliveTimerTask::~KeepaliveTimerTask()
{

}

void KeepaliveTimerTask::runTimerTask()
{
  instance_.keepalive();
}

int ns_async_callback(NewClient* client)
{
  MockDataService* service = dynamic_cast<MockDataService*>(BaseMain::instance());
  int32_t iret = NULL != service ? TFS_SUCCESS : TFS_ERROR;
  if (TFS_SUCCESS == iret)
  {
    iret = service->callback(client);
  }
  return iret;
}




