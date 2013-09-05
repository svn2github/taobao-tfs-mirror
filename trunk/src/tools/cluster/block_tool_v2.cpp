/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: blocktool.cpp 445 2011-06-08 09:27:48Z nayan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   chuyu <chuyu@taobao.com>
 *      - modify 2010-03-20
 *
 */
#include <stdio.h>
#include <limits.h>
#include <stdlib.h>
#include <vector>
#include <string>
#include <bitset>

#include "common/config_item.h"
#include "common/func.h"
#include "common/version.h"
#include "common/new_client.h"
#include "common/client_manager.h"
#include "common/base_packet_factory.h"
#include "common/base_packet_streamer.h"
#include "message/block_info_message.h"
#include "clientv2/fsname.h"
#include "clientv2/tfs_client_api_v2.h"
#include "message/block_info_message_v2.h"
#include "tools/util/tool_util.h"
#include "message/family_info_message.h"

using namespace std;
using namespace tfs::clientv2;
using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::tools;

static const int32_t MAX_BITS_SIZE = 81920;

static FILE *g_fp = NULL;

int list_block(const uint64_t ds_id, VUINT64& block_vec);
int check_blocks_copy(const uint64_t ns_id, const uint64_t ds_id, vector<BlockMeta>& blocks_meta, vector<uint64_t>& result_blocks);
int check_family(const int64_t family_id, const uint64_t ns_id, const uint64_t ds_id, const uint64_t block_id, bool& is_safe);
void print_block(const vector<uint64_t>& result_blocks);


static void usage(const char* name)
{
  fprintf(stderr, "Usage: %s -s ns_ip -d ds_ip [-h], get all block which have only one copy and lost it will cause block lost finally in cluster\n", name);
  fprintf(stderr, "       -s nameserver ip port\n");
  fprintf(stderr, "       -d dataserver ip port\n");
  fprintf(stderr, "       -f output file name\n");
  fprintf(stderr, "       -v show version\n");
  fprintf(stderr, "       -h help\n");
  exit(EXIT_TFS_ERROR);
}

static void version(const char* app_name)
{
  fprintf(stderr, "%s %s\n", app_name, Version::get_build_description());
  exit(0);
}

int main(int argc, char* argv[])
{
  int32_t i, ret = TFS_SUCCESS;
  std::string ns_ip = "", ds_ip = "";
  string filename="";
  // analyze arguments
  while ((i = getopt(argc, argv, "s:d:f:hv")) != EOF)
  {
    switch (i)
    {
    case 's':
      ns_ip = optarg;
      break;
    case 'd':
      ds_ip = optarg;
      break;
    case 'f':
      filename = optarg;
      break;
    case 'v':
      version(argv[0]);
      break;
    case 'h':
    default:
      usage(argv[0]);
    }
  }

  if ((ns_ip.empty())
      || (ns_ip.compare(" ") == 0)
      || (ds_ip.empty())
      || (ds_ip.compare(" ") == 0)
      || (filename.empty())
      || (filename.compare(" ") == 0))
  {
    usage(argv[0]);
  }

  TBSYS_LOGGER.setLogLevel("debug");
  TfsClientV2* tfs_client = TfsClientV2::Instance();
  ret = tfs_client->initialize(ns_ip.c_str());

  if (TFS_SUCCESS == ret)
  {
    if((g_fp = fopen(filename.c_str(), "wb+")) == NULL)
    {
      TBSYS_LOG(ERROR, "open fail output file: %s, error: %s\n", filename.c_str(), strerror(errno));
      ret = EXIT_OPEN_FILE_ERROR;
    }
    else
    {
      VUINT64 block_vec;
      uint64_t ds_id = Func::get_host_ip(ds_ip.c_str());
      ret = list_block(ds_id, block_vec);
      if (TFS_SUCCESS == ret)
      {
        vector<BlockMeta> blocks_meta;
        ret = ToolUtil::get_all_blocks_meta(tfs_client->get_server_id(), block_vec, blocks_meta, true);// need to get check block besides data block
        if (TFS_SUCCESS == ret)
        {
          vector<uint64_t> result_blocks;
          ret = check_blocks_copy(tfs_client->get_server_id(), ds_id, blocks_meta, result_blocks);
          if (TFS_SUCCESS == ret)
          {
            print_block(result_blocks);
          }
          else
          {
            TBSYS_LOG(ERROR, "check blocks copy fail, ds: %s",  tbsys::CNetUtil::addrToString(ds_id).c_str());
          }
        }
      }
      fclose(g_fp);
    }
    tfs_client->destroy();
  }
  else
  {
    TBSYS_LOG(ERROR, "TfsClientV2 initialize fail , ret: %d\n", ret);
    ret = EXIT_TFS_ERROR;
  }

  return ret;
}

int list_block(const uint64_t ds_id, VUINT64& block_vec)
{
  int ret = TFS_ERROR;
  ListBlockMessage req_lb_msg;
  req_lb_msg.set_block_type(LB_BLOCK);

  NewClient* client = NewClientManager::get_instance().create_client();
  if (NULL != client)
  {
    tbnet::Packet* ret_msg= NULL;
    ret = send_msg_to_server(ds_id, client, &req_lb_msg, ret_msg);
    if (TFS_SUCCESS == ret)
    {
      if (ret_msg->getPCode() == RESP_LIST_BLOCK_MESSAGE)
      {
        RespListBlockMessage* resp_lb_msg = dynamic_cast<RespListBlockMessage*> (ret_msg);
        block_vec = *(resp_lb_msg->get_blocks());
      }
      else
      {
        ret = EXIT_TFS_ERROR;
        TBSYS_LOG(ERROR, "get block list reply msg fail, pcode: %d, ds: %s", ret_msg->getPCode(), tbsys::CNetUtil::addrToString(ds_id).c_str());
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "list block message fail, ds: %s", tbsys::CNetUtil::addrToString(ds_id).c_str());
    }

    NewClientManager::get_instance().destroy_client(client);
  }
  return ret;
}

int check_family(const int64_t family_id, const uint64_t ns_id, const uint64_t ds_id, const uint64_t block_id, bool& is_safe)
{
  int ret = TFS_SUCCESS;
  pair<uint64_t,uint64_t>* family_members = NULL;
  int32_t family_aid_info = 0;

  // get family info
  GetFamilyInfoMessage gfi_message;
  gfi_message.set_family_id(family_id);
  gfi_message.set_mode(T_READ);
  tbnet::Packet* rsp = NULL;
  NewClient* client = NewClientManager::get_instance().create_client();
  ret = send_msg_to_server(ns_id, client, &gfi_message, rsp);
  if (TFS_SUCCESS == ret)
  {
    if (rsp->getPCode() == RSP_GET_FAMILY_INFO_MESSAGE)
    {
      GetFamilyInfoResponseMessage* gfi_resp = dynamic_cast<GetFamilyInfoResponseMessage*>(rsp);
      family_members = gfi_resp->get_members();
      family_aid_info = gfi_resp->get_family_aid_info();
    }
    else if (rsp->getPCode() == STATUS_MESSAGE)
    {
      StatusMessage* sm = dynamic_cast<StatusMessage*>(rsp);
      ret = sm->get_status();
      TBSYS_LOG(WARN, "get family info fail, blockid: %"PRI64_PREFIX"u, familyid: %"PRI64_PREFIX"d, error msg: %s, ret: %d",
        block_id, family_id, sm->get_error(), ret);
    }
    else
    {
      ret = EXIT_UNKNOWN_MSGTYPE;
      TBSYS_LOG(WARN, "get family info fail, blockid: %"PRI64_PREFIX"u, familyid: %"PRI64_PREFIX"d, unknown msg, pcode: %d",
        block_id, family_id, rsp->getPCode());
    }
  }
  else
  {
    TBSYS_LOG(WARN, "send GetFamilyInfoMessage fail, blockid: %"PRI64_PREFIX"u, familyid: %"PRI64_PREFIX"d, ret: %d",
            block_id, family_id, ret);
  }

  // check whether family break down after close down the ds forcedly
  if (TFS_SUCCESS == ret)
  {
    int data_num = GET_DATA_MEMBER_NUM(family_aid_info);
    int member_num = data_num + GET_CHECK_MEMBER_NUM(family_aid_info);
    int32_t index = 0;
    bool is_include = false;
    for (int i = 0; i < member_num; ++i)
    {
      pair<uint64_t, uint64_t>* item = &(family_members[i]);
      if (item->second != INVALID_SERVER_ID)
      {
        ++index;
        if (item->second == ds_id)
        {
          is_include = true;
        }
      }
    }
    if ( !is_include )
    {
      ret = EXIT_FAMILY_MEMBER_INFO_ERROR;
      TBSYS_LOG(WARN, "ns family member info error, blockid: %"PRI64_PREFIX"u, familyid: %"PRI64_PREFIX"d", block_id, family_id);
    }
    else
    {
      if (index > data_num)// family can reinstate stil if ds close down
      {
        is_safe = true;
        TBSYS_LOG(DEBUG, "ds: %s close down, family can restore by itself, blockid: %"PRI64_PREFIX"u, familyid: %"PRI64_PREFIX"d",
                  tbsys::CNetUtil::addrToString(ds_id).c_str(), block_id, family_id);
      }
      else if (index == data_num)// family just can't reinstate if ds close down
      {
        is_safe =  false;
        TBSYS_LOG(WARN, "if ds: %s close down, family will break down, blockid: %"PRI64_PREFIX"u, familyid: %"PRI64_PREFIX"d",
          tbsys::CNetUtil::addrToString(ds_id).c_str(), block_id, family_id);
      }
      else if ( !IS_VERFIFY_BLOCK(block_id) )// data block should't lost when family already can't reinstate, waiting for dissolving
      {
        is_safe = false;
        TBSYS_LOG(WARN, "family already broken down, but now data block(blockid: %"PRI64_PREFIX"u) has only one copy in ds: %s, familyid: %"PRI64_PREFIX"d",
          block_id, tbsys::CNetUtil::addrToString(ds_id).c_str(), family_id);
      }
      else// check block is useless when family can't reinstate
      {
        is_safe = true;
        TBSYS_LOG(WARN, "family already broken down, check block(blockid: %"PRI64_PREFIX"u) is useless already in ds: %s, familyid: %"PRI64_PREFIX"d",
          block_id, tbsys::CNetUtil::addrToString(ds_id).c_str(), family_id);
      }
    }
  }
  NewClientManager::get_instance().destroy_client(client);

  return ret;
}

int check_blocks_copy(const uint64_t ns_id, const uint64_t ds_id, vector<BlockMeta>& blocks_meta, vector<uint64_t>& result_blocks)
{
  int ret = TFS_SUCCESS;
  vector<BlockMeta>::iterator iter = blocks_meta.begin();
  for (; iter != blocks_meta.end(); iter++)
  {
    if (iter->size_ == 0) // 至少应该存在本ds的copy
    {// 一般不发生，但是某一短暂可能发生, 只能重新再做检查
      ret = EXIT_TFS_ERROR;
      fprintf(g_fp, "%"PRI64_PREFIX"u\n", iter->block_id_);// unkown block status
      TBSYS_LOG(ERROR, "block no exist or ds list is empty in nameserver, blockid: %"PRI64_PREFIX"u.\n", iter->block_id_);
      break;
    }

    if (iter->family_info_.family_id_ == INVALID_FAMILY_ID )// can't be check block
    {
      if ((iter->size_ == 1) && (iter->ds_[0] == ds_id))// dangerous block
      {
        result_blocks.push_back(iter->block_id_);
        TBSYS_LOG(WARN, "data block only left one copy in ds: %s, blockid: %"PRI64_PREFIX"u.\n", tbsys::CNetUtil::addrToString(ds_id).c_str(), iter->block_id_);
      }
    }
    else
    {
      // check data block or check block safety
      bool is_safe = false;
      ret = check_family(iter->family_info_.family_id_, ns_id, ds_id, iter->block_id_, is_safe);
      if (TFS_SUCCESS == ret)
      {
        if ( !is_safe )// dangerous block
        {
          result_blocks.push_back(iter->block_id_);
        }
      }
      else
      {
        fprintf(g_fp, "%"PRI64_PREFIX"u\n", iter->block_id_);
        TBSYS_LOG(ERROR, "check family fail, familyid: %"PRI64_PREFIX"d, blockid:%"PRI64_PREFIX"u.", iter->family_info_.family_id_, iter->block_id_);
        break;
      }
    }
  }

  return ret;
}

void print_block(const vector<uint64_t>& result_blocks)
{
  vector<uint64_t>::const_iterator vit = result_blocks.begin();
  for (; vit != result_blocks.end(); vit++)
  {
    fprintf(g_fp, "%"PRI64_PREFIX"u\n", (*vit));
  }
}
