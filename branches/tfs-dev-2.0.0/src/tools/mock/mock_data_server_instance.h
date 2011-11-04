/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: mock_data_server_instance.h 418 2011-06-03 07:20:32Z duanfei@taobao.com $
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

#include "common/lock.h"
#include "common/internal.h"
#include "common/base_service.h"

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
  int64_t file_id_factory_;
};

class MockDataService: public common::BaseService
{
public:
  MockDataService();
  virtual ~MockDataService();

  /** application parse args*/
  virtual int parse_common_line_args(int argc, char* argv[]);

  /** get listen port*/
  virtual int get_listen_port() const ;

  int32_t get_ns_port() const;

  virtual const char* get_log_file_path();

  /** initialize application data*/
  virtual int initialize(int argc, char* argv[]);

  /** destroy application data*/
  virtual int destroy_service() {return common::TFS_SUCCESS;}

  /** create the packet streamer, this is used to create packet according to packet code */
  virtual tbnet::IPacketStreamer* create_packet_streamer()
  {
    return new common::BasePacketStreamer();
  }

  /** destroy the packet streamer*/
  virtual void destroy_packet_streamer(tbnet::IPacketStreamer* streamer)
  {
    tbsys::gDelete(streamer);
  }

  /** create the packet streamer, this is used to create packet*/
  virtual common::BasePacketFactory* create_packet_factory()
  {
    return new common::BasePacketFactory();
  }

  /** destroy packet factory*/
  virtual void destroy_packet_factory(common::BasePacketFactory* factory)
  {
    tbsys::gDelete(factory);
  }

  /** handle packet*/
  virtual bool handlePacketQueue(tbnet::Packet *packet, void *args);

  int callback(common::NewClient* client);

  int keepalive();
private:

  int write(common::BasePacket* msg);
  int read(common::BasePacket* msg);
  int readv2(common::BasePacket* msg);
  int close(common::BasePacket* msg);
  int create_file_number(common::BasePacket* msg);
  int new_block(common::BasePacket* msg);
  int get_file_info(common::BasePacket* msg);
  int post_message_to_server(common::BasePacket* msg, const common::VUINT64& ds_list);
  int send_message_to_slave(common::BasePacket* msg, const common::VUINT64& ds_list);
  int commit_to_nameserver(std::map<uint32_t, BlockEntry>::iterator, uint32_t block_id, uint32_t lease_id, int32_t status, common::UnlinkFlag flag = common::UNLINK_FLAG_NO);

private:
  tbnet::PacketQueueThread main_work_queue_;

  std::map<uint32_t, BlockEntry> blocks_;
  common::RWLock blocks_mutex_;
  common::DataServerStatInfo information_;
  common::RWLock infor_mutex_;

  uint64_t ns_ip_port_;
  common::HasBlockFlag need_send_block_to_ns_;
  int32_t MAX_WRITE_FILE_SIZE;

  std::string log_file_path_;
  std::string server_index_;
};

class KeepaliveTimerTask: public tbutil::TimerTask
{
public:
  KeepaliveTimerTask(MockDataService& instance);
  virtual ~KeepaliveTimerTask();

  void runTimerTask();
private:
  MockDataService& instance_;
};
typedef tbutil::Handle<KeepaliveTimerTask> KeepaliveTimerTaskPtr;
}
}

