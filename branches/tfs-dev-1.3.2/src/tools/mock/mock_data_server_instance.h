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
#include <stdio.h>
#include <pthread.h>
#include <vector>
#include <string>
#include <queue>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <tbsys.h>
#include <tbnet.h>
#include <Timer.h>
#include "message/message.h"
#include "message/message_factory.h"
#include "message/tfs_packet_streamer.h"
#include "message/async_client.h"

namespace tfs
{
namespace mock
{

struct BlockEntry
{
  BlockEntry()
  : file_id_factory_(0)
  {
    memset(&info_, 0, sizeof(info_));
  }
  common::BlockInfo info_;
  std::map<int64_t, common::FileInfo> files_;
  int64_t file_id_factory_;
};

class MockDataServerInstance:
    public tbnet::IServerAdapter,
    public message::DefaultAsyncCallback,
    public tbnet::IPacketQueueHandler

{
public:
  MockDataServerInstance();
  virtual ~MockDataServerInstance();

  tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Connection* conn, tbnet::Packet* packet);
  bool handlePacketQueue(tbnet::Packet *packet, void *args);
  int command_done(message::Message* msg, bool status, const string& error);
  int initialize(int32_t port, int64_t capacity, const std::string& work_index, const std::string& conf);
  int wait_for_shut_down();
  bool destroy();

  int keepalive();

private:

  int write(message::Message* msg);
  int read(message::Message* msg);
  int readv2(message::Message* msg);
  int close(message::Message* msg);
  int create_file_number(message::Message* msg);
  int new_block(message::Message* msg);
  int get_file_info(message::Message* msg);
  int post_message_to_server(message::Message* msg, const common::VUINT64& ds_list);
  int send_message_to_slave(message::Message* msg, const common::VUINT64& ds_list);
  int commit_to_nameserver(std::map<uint32_t, BlockEntry>::iterator, uint32_t block_id, uint32_t lease_id, int32_t status, common::UnlinkFlag flag = common::UNLINK_FLAG_NO);

private:
  tbnet::Transport transport_;
  message::MessageFactory msg_factory_;
  message::TfsPacketStreamer streamer_;
  tbnet::PacketQueueThread main_work_queue_;

  tbutil::TimerPtr timer_;

  std::map<uint32_t, BlockEntry> blocks_;
  common::RWLock blocks_mutex_;
  common::DataServerStatInfo information_;
  common::RWLock infor_mutex_;

  uint64_t ns_ip_port_;

  common::HasBlockFlag need_send_block_to_ns_;
};

class KeepaliveTimerTask: public tbutil::TimerTask
{
public:
  KeepaliveTimerTask(MockDataServerInstance& instance);
  virtual ~KeepaliveTimerTask();

  void runTimerTask();
private:
  MockDataServerInstance& instance_;
};
typedef tbutil::Handle<KeepaliveTimerTask> KeepaliveTimerTaskPtr;
}
}

