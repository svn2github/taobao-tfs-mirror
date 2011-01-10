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
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 *
 */
#include "common/define.h"
#include "common/interval.h"
#include "mock_data_server_instance.h"
#include "message/client_pool.h"
#include "common/config.h"
#include "common/config_item.h"
#include "common/error_msg.h"
#include "common/func.h"

using namespace tfs::mock;
using namespace tfs::message;
using namespace tfs::common;
using namespace tbnet;
using namespace tbsys;
using namespace tbutil;
static FileInfo gfile_info;
static const int8_t BUF_LEN = 32;
static char BUF[BUF_LEN] = {'1'};

MockDataServerInstance::MockDataServerInstance(int32_t max_write_file_size):
  timer_(0),
  ns_ip_port_(0),
  need_send_block_to_ns_(HAS_BLOCK_FLAG_YES),
  MAX_WRITE_FILE_SIZE(max_write_file_size)
{
  memset(&information_, 0, sizeof(information_));
  memset(&gfile_info, 0, sizeof(gfile_info));
  gfile_info.size_ = BUF_LEN;
  gfile_info.crc_  = 0;
  gfile_info.id_ = 0xff;
  gfile_info.crc_  = Func::crc(gfile_info.crc_, BUF, BUF_LEN);
}

MockDataServerInstance::~MockDataServerInstance()
{

}

int MockDataServerInstance::keepalive()
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

  Message* result = NULL;
  int iret = send_message_to_server(ns_ip_port_, &msg, &result);
  if (iret == TFS_SUCCESS && result != NULL)
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
    tbsys::gDelete(result);
  }
  else
  {
    TBSYS_LOG(ERROR, "%s", "message is null or iret(%d) !=  TFS_SUCCESS");
  }
  return TFS_SUCCESS;
}

tbnet::IPacketHandler::HPRetCode MockDataServerInstance::handlePacket(tbnet::Connection* conn, tbnet::Packet* packet)
{
  bool bret = conn != NULL && packet != NULL;
  if (!bret)
  {
    TBSYS_LOG(ERROR, "%s", "invalid packet");
    return tbnet::IPacketHandler::FREE_CHANNEL;
  }

  if (!packet->isRegularPacket())
  {
    TBSYS_LOG(INFO, "control packet cmd(%d)", ((tbnet::ControlPacket*) packet)->getCommand());
    return tbnet::IPacketHandler::FREE_CHANNEL;
  }
  Message* msg = dynamic_cast<Message*>(packet);
  msg->set_connection(conn);
  msg->setExpireTime(MAX_RESPONSE_TIME);
  msg->set_direction(msg->get_direction() | DIRECTION_RECEIVE);
  if (!main_work_queue_.push(msg))
  {
    MessageFactory::send_error_message(msg, TBSYS_LOG_LEVEL(WARN),
      STATUS_MESSAGE_ERROR, information_.id_, 
      "TASK message beyond max queue size, discard." );
    msg->free();
  }
  return tbnet::IPacketHandler::KEEP_CHANNEL;
}

bool MockDataServerInstance::handlePacketQueue(tbnet::Packet *packet, void *args)
{
  Message* msg = dynamic_cast<Message*>(packet);
  int32_t pcode = msg->getPCode();
  int32_t iret  = TFS_ERROR;
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
    break;
  }
  if (iret != TFS_SUCCESS)
  {
    TBSYS_LOG(DEBUG, "pcode(%d)", msg->getPCode());
    MessageFactory::send_error_message(msg, TBSYS_LOG_LEVEL(ERROR), iret, information_.id_, 
      "execute message failed");
  }
  return true;
}

int MockDataServerInstance::initialize(int32_t port, int64_t capacity, const std::string& work_index, const std::string& conf)
{
  int iret = SysParam::instance().load_mock_dataserver(conf);
  if (iret != TFS_SUCCESS)
  {
    fprintf(stderr, "load config file(%s) failed\n", conf.c_str());
    return EXIT_GENERAL_ERROR;
  }
 
  information_.total_capacity_ = capacity;
  char* ns_ip = CONFIG.get_string_value(CONFIG_NAMESERVER, CONF_IP_ADDR);
  if (ns_ip == NULL)
  {
    TBSYS_LOG(ERROR, "%s", "'ns_ip' is invalid");
    return TFS_ERROR;
  }
  int32_t ns_port = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_PORT);
  ns_ip_port_ = tbsys::CNetUtil::strToAddr(ns_ip, ns_port);
  
  IpAddr* adr = reinterpret_cast<IpAddr*>(&information_.id_);
  adr->ip_ = Func::get_local_addr(SYSPARAM_MOCK_DATASERVER.dev_name_.c_str());
  adr->port_ = port;
  //listen to the port, get packets
  streamer_.set_packet_factory(&msg_factory_);
  CLIENT_POOL.init_with_transport(&transport_);

  char spec[SPEC_LEN];
  memset(spec, 0, sizeof(spec));
  snprintf(spec, SPEC_LEN, "tcp::%d", port);

  if (transport_.listen(spec, &streamer_, this) == NULL)
  {
    TBSYS_LOG(ERROR, "listen port fail, port(%d)", port);
    return EXIT_NETWORK_ERROR;
  }

  transport_.start();

  timer_ = new Timer();

  int32_t heart_interval = CONFIG.get_int_value(CONFIG_PUBLIC,CONF_HEART_INTERVAL,2);
  KeepaliveTimerTaskPtr task = new KeepaliveTimerTask(*this);
  timer_->scheduleRepeated(task, tbutil::Time::seconds(heart_interval));

  int32_t thread_count = CONFIG.get_int_value(CONFIG_MOCK_DATASERVER, CONF_THREAD_COUNT, 4);
  main_work_queue_.setThreadParameter(thread_count, this, NULL);
  main_work_queue_.start();
  
  return TFS_SUCCESS;
}

int MockDataServerInstance::wait_for_shut_down()
{
  transport_.wait();
  main_work_queue_.wait();
  return TFS_SUCCESS;
}

bool MockDataServerInstance::destroy()
{
  if (timer_ != 0)
  {
    timer_->destroy();
  }

  transport_.stop();
  main_work_queue_.stop();
  return true;
}

int MockDataServerInstance::write(message::Message* msg)
{
  bool bret = msg != NULL;
  if (bret)
  {
    int32_t iret = TFS_ERROR;
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
        MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), information_.id_,
              "create file failed. blockid: %u, fileid: %" PRI64_PREFIX "u.", write_info.block_id_, write_info.file_id_); 
        return TFS_SUCCESS;
      }
      iter->second.info_.size_ += write_info.length_;
    }

    {
      RWLock::Lock lock(infor_mutex_, WRITE_LOCKER);
      information_.use_capacity_ += write_info.length_;
    }
 
    if (Master_Server_Role == write_info.is_server_)
    {
      message->set_server(Slave_Server_Role);
      message->set_lease_id(lease_id);
      message->set_block_version(version);
      iret = post_message_to_server(message, message->get_ds_list());
      if (iret == 0)
      {
        message->reply_message(new StatusMessage(STATUS_MESSAGE_OK));
      }
      else
      {
        MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), information_.id_,
          "write data fail to other dataserver (send): blockid: %u, fileid: %" PRI64_PREFIX "u, datalen: %d",
          write_info.block_id_, write_info.file_id_, write_info.length_);                                    
      }
    }
    else
    {
      message->reply_message(new StatusMessage(STATUS_MESSAGE_OK));
    }
  }
  return !bret ? TFS_ERROR : TFS_SUCCESS;
}

int MockDataServerInstance::read(message::Message* msg)
{
  bool bret = msg != NULL;
  if (bret)
  {
    ReadDataMessage* message = dynamic_cast<ReadDataMessage*>(msg);
    RespReadDataMessage* rmsg = new RespReadDataMessage();
    char* data = rmsg->alloc_data(BUF_LEN);
    memcpy(data, BUF, BUF_LEN);
    rmsg->set_length(BUF_LEN);
    message->reply_message(rmsg);
    TBSYS_LOG(DEBUG, "read msg");
  }
  return !bret ? TFS_ERROR : TFS_SUCCESS;
}

int MockDataServerInstance::readv2(message::Message* msg)
{
  bool bret = msg != NULL;
  if (bret)
  {
    ReadDataMessageV2* message = dynamic_cast<ReadDataMessageV2*>(msg);
    uint32_t block_id = message->get_block_id();
    RespReadDataMessageV2* rmsg = new RespReadDataMessageV2();
    RWLock::Lock lock(blocks_mutex_, READ_LOCKER);
    std::map<uint32_t, BlockEntry>::iterator iter = blocks_.find(block_id);
    if (iter == blocks_.end())
    {
      rmsg->set_length(0);
      message->reply_message(rmsg);
      return TFS_SUCCESS;
    }

    char* data = rmsg->alloc_data(BUF_LEN);
    memcpy(data, BUF, BUF_LEN);
    rmsg->set_length(BUF_LEN);
    rmsg->set_file_info(&gfile_info);
    message->reply_message(rmsg);
  }
  return !bret ? TFS_ERROR : TFS_SUCCESS;
}

int MockDataServerInstance::close(message::Message* msg)
{
  bool bret = msg != NULL;
  if (bret)
  {
    CloseFileMessage* message = dynamic_cast<CloseFileMessage*>(msg);
    CloseFileInfo info = message->get_close_file_info();
    uint32_t lease_id = message->get_lease_id();

    {
      RWLock::Lock lock(blocks_mutex_, WRITE_LOCKER);
      std::map<uint32_t, BlockEntry>::iterator iter = blocks_.find(info.block_id_);
      if (iter == blocks_.end())
      {
        MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), information_.id_,
              "close write file failed. block is not exist. blockid: %u, fileid: %" PRI64_PREFIX "u.", info.block_id_, info.file_id_); 
        return TFS_SUCCESS;
      }
      if (CLOSE_FILE_SLAVER != info.mode_)
      {
        message->set_mode(CLOSE_FILE_SLAVER);
        message->set_block(&iter->second.info_);
        TBSYS_LOG(DEBUG, "block id(%u)", info.block_id_);
        int iret = send_message_to_slave(message, message->get_ds_list());
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
          message->reply_message(new StatusMessage(STATUS_MESSAGE_OK));
        }
        else
        {
          TBSYS_LOG(ERROR, "dataserver commit fail, block_id: %u, file_id: %"PRI64_PREFIX"u, iret: %d",
            info.block_id_, info.file_id_, iret);
          message->reply_message(new StatusMessage(STATUS_MESSAGE_ERROR));
        }
      }
      else
      {
        BlockInfo* copyblk = message->get_block();                                             
        if (NULL != copyblk)
        {
          iter->second.info_.seq_no_ = copyblk->seq_no_;
        }
        message->reply_message(new StatusMessage(STATUS_MESSAGE_OK));
      }
    }
 
  }
  return !bret ? TFS_ERROR : TFS_SUCCESS;
}

int MockDataServerInstance::create_file_number(message::Message* msg)
{
  bool bret = msg != NULL;
  if (bret)
  {
    CreateFilenameMessage* message = dynamic_cast<CreateFilenameMessage*>(msg);
    uint32_t block_id =  message->get_block_id();
    uint64_t file_id  = message->get_file_id();
    RWLock::Lock lock(blocks_mutex_, WRITE_LOCKER);
    std::map<uint32_t, BlockEntry>::iterator iter = blocks_.find(block_id);
    if (iter == blocks_.end())
    {
      MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), information_.id_,
            "create file failed. blockid: %u, fileid: %" PRI64_PREFIX "u.", block_id, file_id); 
      return TFS_SUCCESS;
    }
    file_id = ++iter->second.file_id_factory_;
    RespCreateFilenameMessage* rmsg = new RespCreateFilenameMessage();
    rmsg->set_block_id(block_id);
    rmsg->set_file_id(file_id);
    rmsg->set_file_number(file_id);
    message->reply_message(rmsg);
  }
  return !bret ? TFS_ERROR : TFS_SUCCESS;
}

int MockDataServerInstance::new_block(message::Message* msg)
{
  bool bret = msg != NULL;
  if (bret)
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
    message->reply_message(new StatusMessage(STATUS_MESSAGE_OK));
  }
  return !bret ? TFS_ERROR : TFS_SUCCESS;
}

int MockDataServerInstance::get_file_info(Message* msg)
{
  bool bret = msg != NULL;
  if (bret)
  {
    FileInfoMessage* message = dynamic_cast<FileInfoMessage*>(msg);
    RespFileInfoMessage* rmsg = new RespFileInfoMessage();
    rmsg->set_file_info(&gfile_info);
    message->reply_message(rmsg);
  }
  return !bret ? TFS_ERROR : TFS_SUCCESS;
}

int MockDataServerInstance::post_message_to_server(Message* message, const VUINT64& ds_list)
{
  VUINT64 result;
  VUINT64::const_iterator iter = ds_list.begin();
  for (; iter != ds_list.end(); ++iter)
  {
    if ((*iter) == information_.id_)
    {
      continue;
    }
    result.push_back((*iter));
  }
  if (result.empty())
  {
    return 0;
  }
  if (async_post_message_to_servers(message, result, this) == TFS_SUCCESS)
  {
    return 1;
  }
  return -1;
}

int MockDataServerInstance::send_message_to_slave(Message* message, const VUINT64& ds_list)
{
  VUINT64::const_iterator iter = ds_list.begin();
  for (; iter  != ds_list.end(); ++iter)
  {
    if ((*iter) == information_.id_)
    {
      continue;
    }
    int iret = send_message_to_server((*iter), message, NULL);
    if (iret != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "send message to server to dataserver(%s) fail.", tbsys::CNetUtil::addrToString((*iter)).c_str());
      return iret;
    }
  }
  return TFS_SUCCESS;
}

int MockDataServerInstance::command_done(Message* send_message, bool status, const string& error)
{
  return DefaultAsyncCallback::command_done(send_message, status, error);
}

int MockDataServerInstance::commit_to_nameserver(std::map<uint32_t, BlockEntry>::iterator iter, uint32_t block_id, uint32_t lease_id, int32_t status, common::UnlinkFlag flag)
{
  if (iter == blocks_.end())
  {
    return TFS_ERROR;
  }
  BlockInfo info;
  memcpy(&info, &iter->second.info_, sizeof(info));
  ++info.version_;
  BlockWriteCompleteMessage rmsg;
  rmsg.set_block(&info);
  rmsg.set_server_id(information_.id_);
  rmsg.set_lease_id(lease_id);
  rmsg.set_success(WRITE_COMPLETE_STATUS_YES);
  rmsg.set_unlink_flag(flag);

  Message* result = NULL;
  int iret = send_message_to_server(ns_ip_port_, &rmsg, &result);
  if (iret != TFS_SUCCESS || result == NULL)
  {
    return iret;
  }

  if (result->getPCode() != STATUS_MESSAGE)
  {
    iret = TFS_ERROR;
  }
  else
  {
    StatusMessage* message = dynamic_cast<StatusMessage*>(result);
    if (STATUS_MESSAGE_OK == message->get_status())
    {
      iter->second.info_.version_++;
      iret = TFS_SUCCESS;
    }
    else
    {
      iret = TFS_ERROR;
    }
  }
  tbsys::gDelete(result);
  return TFS_SUCCESS;
}

KeepaliveTimerTask::KeepaliveTimerTask(MockDataServerInstance& instance)
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

