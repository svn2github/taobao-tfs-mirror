#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <list>
#include <ext/hash_set>

#include "message/server_status_message.h"
#include "tools/nameserver/common.h"
#include "message/message_factory.h"
#include "common/internal.h"
#include "common/client_manager.h"
#include "common/func.h"
#include "common/version.h"
#include "requester/ns_requester.h"
#include "common.h"

using namespace std;
using namespace tfs::tools;
using namespace tfs::message;
using namespace tfs::common;

static bool g_interrupt = false;

static void sign_handler(int32_t sig)
{
  switch (sig)
  {
    case SIGINT:
    case SIGTERM:
      g_interrupt = true;
      fprintf(stderr, "stop, interrupt\n");
      break;
    default:
      break;
  }
}

void version(const char* app_name)
{
  fprintf(stderr, "%s %s\n", app_name, Version::get_build_description());
  exit(0);
}

int fetch_blocks(const uint64_t ns_id, list<BlockBase>& block_list, const int32_t num)
{
  ShowServerInformationMessage msg;
  SSMScanParameter& param = msg.get_param();
  param.type_ = SSM_TYPE_BLOCK;
  param.child_type_ = SSM_CHILD_BLOCK_TYPE_INFO | SSM_CHILD_BLOCK_TYPE_SERVER;

  param.should_actual_count_ = (num << 16);
  param.end_flag_ = SSM_SCAN_CUTOVER_FLAG_YES;

  block_list.clear();
  MessageFactory packet_factory;
  BasePacketStreamer packet_streamer(&packet_factory);
  int ret = NewClientManager::get_instance().initialize(&packet_factory, &packet_streamer);
  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "NewClientManager initialize fail, ret: %d", ret);
    return ret;
  }
  while ((!((param.end_flag_ >> 4) & SSM_SCAN_END_FLAG_YES)) && !g_interrupt)
  {
    param.data_.clear();
    tbnet::Packet*ret_msg = NULL;
    NewClient* client = NewClientManager::get_instance().create_client();
    ret = send_msg_to_server(ns_id, client, &msg, ret_msg);
    if (TFS_SUCCESS != ret || ret_msg == NULL)
    {
      TBSYS_LOG(ERROR, "get block info error, ret: %d", ret);
      NewClientManager::get_instance().destroy_client(client);
      return TFS_ERROR;
    }

    if(ret_msg->getPCode() != SHOW_SERVER_INFORMATION_MESSAGE)
    {
      if (ret_msg->getPCode() == STATUS_MESSAGE)
      {
        StatusMessage* msg = dynamic_cast<StatusMessage*>(ret_msg);
        TBSYS_LOG(ERROR, "get invalid message type: error: %s", msg->get_error());
      }
      TBSYS_LOG(ERROR, "get invalid message type, pcode: %d", ret_msg->getPCode());
      NewClientManager::get_instance().destroy_client(client);
      return TFS_ERROR;
    }
    ShowServerInformationMessage* message = dynamic_cast<ShowServerInformationMessage*>(ret_msg);
    SSMScanParameter& ret_param = message->get_param();

    int32_t data_len = ret_param.data_.getDataLen();
    int32_t offset = 0;
    while ((data_len > offset) && !g_interrupt)
    {
      BlockBase block;
      if (TFS_SUCCESS == block.deserialize(ret_param.data_, data_len, offset, param.child_type_))
      {
        block_list.push_front(block);
      }
    }
    param.start_next_position_ = (ret_param.start_next_position_ << 16) & 0xffff0000;
    param.end_flag_ = ret_param.end_flag_;
    if (param.end_flag_ & SSM_SCAN_CUTOVER_FLAG_NO)
    {
      param.addition_param1_ = ret_param.addition_param2_;
    }
    NewClientManager::get_instance().destroy_client(client);
  }
  NewClientManager::get_instance().destroy();
  return ret;
}


void arrange_block_order(list<BlockBase>& block_list, const int ds_size, const double rato, vector<uint64_t>& order_list)
{
  int ds_size_limit = static_cast<int>(ds_size * rato);
  __gnu_cxx ::hash_set<uint64_t> server_set(ds_size * 2);// busy ds set

  int last_size = -1;
  int slice_count = 0;
  bool busy = false;
  list<BlockBase>::iterator it = block_list.begin();
  while (!block_list.empty() && !g_interrupt)
  {
    if (it == block_list.end())
    {
      it = block_list.begin();
      if (last_size != (int)(block_list.size()))
      {
        last_size = static_cast<int>(block_list.size());
      }
      else
      {
        // can not find any block to compact in idle dataservers, start next round loop
        printf("scan over block list but can't arrage any block more, although used servers count only: %zd( < %d ), left block count: %zd\n",
            server_set.size(), ds_size_limit, block_list.size());
        server_set.clear();
        ++slice_count;
        ds_size_limit = (int)(0.95 * ds_size_limit);// relax
        if (0 == ds_size_limit)
        {
          ds_size_limit = 1;
        }
      }
    }

    busy = false;
    // check all ds of the block busy or not
    for (uint32_t i = 0; i < it->server_list_.size(); ++i)
    {
      ServerInfo& server_info = it->server_list_[i];
      if (server_set.find(server_info.server_id_) != server_set.end())
      {
        busy = true;
        break;
      }
    }
    if ( !busy )// not busy, arrage the block to compact this time
    {
      order_list.push_back(it->info_.block_id_);
      server_set.insert(it->server_list_.begin(), it->server_list_.end());
      block_list.erase(it++);
      if ((int)server_set.size() >= ds_size_limit)// reach to used rato limit, then arrage next batch block
      {
        server_set.clear();
        ++slice_count;
      }
    }
    else
    {
      ++it;
    }
  }
  printf("order_list size: %zd, slice count: %d, ds count limit: %d per slice\n", order_list.size(), slice_count, ds_size_limit);
}

void helper()
{
  std::string options=
    "gen_compact_blk_order -s ns_addr -o block_list [-r percent] [-c num]\n"
    "Options:\n"
    "-s                 ns ip:port\n"
    "-o                 re-arranged blocks id output file path\n"
    "-r                 percent for compact filling rato of all dataservers, default 80\n"
    "-c                 block number fetch from ns once, default 1024\n"
    "-h                 show this message\n";
  fprintf(stderr,"Usage:\n%s", options.c_str());
}

int main(int argc, char* argv[])
{
  int ret = tfs::common::TFS_SUCCESS;
  int num = 1024;
  string ns_ip_addr;
  string file_name;
  int percent = 80;
  int i;
  while((i = getopt(argc, argv, "s:o:r:c:v")) != EOF)
  {
    switch (i)
    {
      case 's':
        ns_ip_addr = optarg;
        break;
      case 'o':
        file_name = optarg;
        break;
      case 'r':
        percent = atoi(optarg);
        break;
      case 'c':
        num = atoi(optarg);
        break;
      case 'v':
        version(argv[0]);
        break;
      default:
        helper();
        return 0;
    }
  }

  if (ns_ip_addr.empty() || file_name.empty() || num <= 0
    || percent > 100 || percent <= 0)
  {
    helper();
    return -1;
  }
  uint64_t ns_id = Func::get_host_ip(ns_ip_addr.c_str());

  signal(SIGINT, sign_handler);
  signal(SIGTERM, sign_handler);


  // fetch all block info and ds list
  list<BlockBase> block_list;
  ret = fetch_blocks(ns_id, block_list, num);
  if (tfs::common::TFS_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "fetch blocks fail, ret:%d", ret);
    return ret;
  }
  printf("block count: %zd\n", block_list.size());

  tfs::common::VUINT64 ds_list;
  ret = tfs::requester::NsRequester::get_ds_list(ns_id, ds_list);
  if (tfs::common::TFS_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "get all ds list fail, ret:%d", ret);
    return ret;
  }
  double rato = 1.0 * percent / 100;
  vector<uint64_t> order_list;
  arrange_block_order(block_list, ds_list.size(), rato, order_list);

  FILE* fp = fopen(file_name.c_str(), "w+");
  if (NULL == fp)
  {
    printf("open file %s, failed\n", file_name.c_str());
    return tfs::common::TFS_ERROR;
  }

  vector<uint64_t>::iterator it = order_list.begin();
  for(; it != order_list.end(); ++it)
  {
    fprintf(fp, "%lu\n", *it);
  }
  fclose(fp);
  return ret;
}
