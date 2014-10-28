/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 */
#include <stdio.h>
#include "common/internal.h"
#include "common/parameter.h"
#include "common/client_manager.h"
#include "common/status_message.h"
#include "common/version.h"
#include "dataserver/block_manager.h"
#include "tools/util/tool_util.h"
#include "tools/util/ds_lib.h"
#include "common/config_item.h"
#include "message/block_info_message_v2.h"
#include "nameserver/ns_define.h"
#include "clientv2/tfs_client_impl_v2.h"

using namespace tfs::dataserver;
using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::tools;
using namespace tfs::clientv2;
using namespace std;

// if parameter: ds_id != INVALID_SERVER_ID, check from current cluster, need exclude repliates in this ds;
// if parameter: ds_id == INVALID_SERVER_ID, check from slave cluster
int get_and_check_all_blocks_copy(const uint64_t ns_id, const VUINT64& blocks, VUINT64& no_copy_blocks, const uint64_t ds_id = INVALID_SERVER_ID)
{
  no_copy_blocks.clear();
  vector<BlockMeta> blocks_meta;
  int ret = ToolUtil::get_all_blocks_meta(ns_id, blocks, blocks_meta, true);
  if (TFS_SUCCESS == ret)
  {
    VUINT64 lost_in_family;
    for (uint32_t block_index = 0; block_index < blocks_meta.size(); ++block_index)
    {
      // blk_meta.result_ just be used in trunk(stable2.7)
      BlockMeta blk_meta = blocks_meta.at(block_index);
      uint64_t block_id = blk_meta.block_id_;
      FamilyInfoExt family_info = blk_meta.family_info_;
      if (INVALID_FAMILY_ID == family_info.family_id_)
      {
        // if family can't reinstall, family_id here is also invalid
        if (INVALID_BLOCK_ID == block_id || IS_VERFIFY_BLOCK(block_id))
          continue;
        // else family data block regard as normal block

        if (0 == blk_meta.size_)
        {
          // maybe not exist the block
          no_copy_blocks.push_back(block_id);
        }
        else if (INVALID_SERVER_ID != ds_id && 1 == blk_meta.size_ && ds_id == blk_meta.ds_[0])
        {
          // only exist one copy and be positioned in this ds
          no_copy_blocks.push_back(block_id);
        }
      }
      else // block is marshalling
      {
        int data_num = GET_DATA_MEMBER_NUM(family_info.family_aid_info_);
        int member_num = data_num + GET_CHECK_MEMBER_NUM(family_info.family_aid_info_);
        int32_t alive_num = 0;
        lost_in_family.clear();
        for (int i = 0; i < member_num; ++i)
        {
          // pair is <blockid, ds_id>
          pair<uint64_t, uint64_t>& item = family_info.members_[i];
          if (INVALID_SERVER_ID != item.second && item.second != ds_id) // exclude current ds
          {
            ++alive_num;
          }
          else
          {
            // keep all lost(and will lost) data blockid
            if (!IS_VERFIFY_BLOCK(item.first) && INVALID_BLOCK_ID != item.first)
            {
              lost_in_family.push_back(item.first);
            }
          }
        }
        if (alive_num < data_num) // can't reinstate
        {
          no_copy_blocks.insert(no_copy_blocks.end(), lost_in_family.begin(), lost_in_family.end());
        }
      }
    }
  }
  else
  {
    // this error need script-calller care
    TBSYS_LOG(ERROR, "get blockis ds_list fail from ns: %s, ret:%d",
        tbsys::CNetUtil::addrToString(ns_id).c_str(), ret);
  }
  return ret;
}


int check_all_block_in_disk(const char* ns_addr, const char* ns_slave_addr, const uint64_t ds_id,
    const VUINT64& blocks, VUINT64& need_sync_block_list, VUINT64& lost_block_list)
{
  int ret = TFS_SUCCESS;
  uint64_t ns_id = Func::get_host_ip(ns_addr);
  uint64_t slave_ns_id = Func::get_host_ip(ns_slave_addr);
  need_sync_block_list.clear();
  lost_block_list.clear();

  // check current cluster's block in blocks array
  ret = get_and_check_all_blocks_copy(ns_id, blocks, need_sync_block_list, ds_id);
  if (TFS_SUCCESS == ret && need_sync_block_list.size() > 0)
  {
    // check slave cluster's block in need_sync_block_list array
    VUINT64 raw_lost_blocks;
    ret = get_and_check_all_blocks_copy(slave_ns_id, need_sync_block_list, raw_lost_blocks);

    // raw_lost_blocks need unique, then remove all blocks which not in need_sync_block_list
    set<uint64_t> need_sync_block_set, lost_block_set;
    need_sync_block_set.insert(need_sync_block_list.begin(), need_sync_block_list.end()); // to sort
    lost_block_set.insert(raw_lost_blocks.begin(), raw_lost_blocks.end()); // to sort & unique

    lost_block_list.resize(raw_lost_blocks.size());
    VUINT64::iterator it = set_intersection(raw_lost_blocks.begin(), raw_lost_blocks.end(),
        need_sync_block_list.begin(), need_sync_block_list.end(), lost_block_list.begin());
    lost_block_list.resize(it - lost_block_list.begin());// set real size
  }
  return ret;
}


void print_result(const VUINT64& need_sync_block_list, const VUINT64& lost_block_list)
{
  if (lost_block_list.size() > 0)
  {
    fprintf(stdout, "BOTH LOST BLOCK COUNT: %zd\n", lost_block_list.size());
    for (uint32_t i = 0; i < lost_block_list.size(); i++)
    {
      fprintf(stdout, "%"PRI64_PREFIX"u\n", lost_block_list.at(i));
    }
  }

  int32_t real_need_sync_block_size = need_sync_block_list.size() - lost_block_list.size();
  if (real_need_sync_block_size > 0)
  {
    fprintf(stdout, "NEED SYNC BLOCK COUNT: %d\n", real_need_sync_block_size);
    set<uint64_t> lost_block_set;
    lost_block_set.insert(lost_block_list.begin(), lost_block_list.end());
    for (uint32_t i = 0; i < need_sync_block_list.size(); i++)
    {
      uint64_t block_id = need_sync_block_list.at(i);
      if (lost_block_set.find(block_id) == lost_block_set.end())
      {
        fprintf(stdout, "%"PRI64_PREFIX"u\n", block_id);// can sync from slave cluster
      }
    }
  }
}

int main(int argc,char* argv[])
{
  char* conf_file = NULL;
  char* ns_slave_addr = NULL;
  int32_t help_info = 0;
  int32_t i;
  string server_index;
  bool set_log_level = false;

  while ((i = getopt(argc, argv, "f:s:i:nvh")) != EOF)
  {
    switch (i)
    {
      case 'f':
        conf_file = optarg;
        break;
      case 's':
        ns_slave_addr = optarg;
        break;
      case 'i':
        server_index = optarg;
        break;
      case 'n':
        set_log_level = true;
        break;
      case 'v':
        fprintf(stderr, "recover disk data to cluster tool, version: %s\n",
            Version::get_build_description());
        return 0;
      case 'h':
      default:
        help_info = 1;
        break;
    }
  }

  if (NULL == conf_file || NULL == ns_slave_addr || 0 == server_index.size() || help_info)
  {
    fprintf(stderr, "\nUsage: %s -f ds_conf -i index -s ns_slave_addr [-h] [-v]\n", argv[0]);
    fprintf(stderr, "  -f dataserver configure file\n");
    fprintf(stderr, "  -s slave cluster addr\n");
    fprintf(stderr, "  -i server_index  dataserver index number\n");
    fprintf(stderr, "  -n set log level to DEBUG, default ERROR\n");
    fprintf(stderr, "  -v show version info\n");
    fprintf(stderr, "  -h help info\n");
    fprintf(stderr, "\n");
    return -1;
  }

  if (set_log_level)
  {
    TBSYS_LOGGER.setLogLevel("DEBUG");
  }
  else
  {
    TBSYS_LOGGER.setLogLevel("ERROR");
  }

  // load config
  int32_t ret = 0;
  TBSYS_CONFIG.load(conf_file);
  if (TFS_SUCCESS != (ret = SYSPARAM_FILESYSPARAM.initialize(server_index)))
  {
    TBSYS_LOG(ERROR, "SysParam::loadFileSystemParam failed: %s", conf_file);
    return ret;
  }

  const char* ns_ip = TBSYS_CONFIG.getString(CONF_SN_DATASERVER, CONF_IP_ADDR);
  if (NULL == ns_ip)
  {
    TBSYS_LOG(ERROR, "Can not find %s in [%s]", CONF_IP_ADDR, CONF_SN_DATASERVER);
    return EXIT_SYSTEM_PARAMETER_ERROR;
  }
  int32_t ns_port = TBSYS_CONFIG.getInt(CONF_SN_DATASERVER, CONF_PORT);
  if (ns_port < 1024 || ns_port > 65535)
  {
    TBSYS_LOG(ERROR, "Can not find %s in [%s] or ns port is invalid", CONF_PORT, CONF_SN_DATASERVER);
    return EXIT_SYSTEM_PARAMETER_ERROR;
  }
  char ns_addr[30] = {0};
  sprintf(ns_addr, "%s:%d", ns_ip, ns_port);

  const char* ds_ip = TBSYS_CONFIG.getString(CONF_SN_PUBLIC, CONF_IP_ADDR);
  if (NULL == ds_ip)
  {
    TBSYS_LOG(ERROR, "Can not find %s in [%s]", CONF_IP_ADDR, CONF_SN_PUBLIC);
    return EXIT_SYSTEM_PARAMETER_ERROR;
  }
  int32_t base_ds_port = TBSYS_CONFIG.getInt(CONF_SN_PUBLIC, CONF_PORT);
  if (base_ds_port < 1024 || base_ds_port > 65535)
  {
    TBSYS_LOG(ERROR, "Can not find %s in [%s] or ds port is invalid", CONF_PORT, CONF_SN_PUBLIC);
    return EXIT_SYSTEM_PARAMETER_ERROR;
  }
  int32_t ds_port = SYSPARAM_DATASERVER.get_real_ds_port(base_ds_port, server_index);
  char ds_addr[30] = {0};
  sprintf(ds_addr, "%s:%d", ds_ip, ds_port);
  uint64_t ds_id = Func::get_host_ip(ds_addr);
  if (ds_id == INVALID_SERVER_ID)
  {
    TBSYS_LOG(ERROR, "get_host_ip from ds_addr: %s invalid", ds_addr);
    return EXIT_SYSTEM_PARAMETER_ERROR;
  }

  // init TfsClient
  ret = TfsClientImplV2::Instance()->initialize(ns_addr);
  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "init TfsClientImplV2 client fail, ret: %d", ret);
    return ret;
  }

  //init block manager
  string super_block_path = string(SYSPARAM_FILESYSPARAM.mount_name_) + SUPERBLOCK_NAME;
  BlockManager block_manager(super_block_path);
  ret = block_manager.bootstrap(SYSPARAM_FILESYSPARAM);
  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "load all blocks failed, ret: %d", ret);
    fprintf(stdout, "load all blocks failed\n");
    return ret;
  }
  else
  {
    fprintf(stdout, "load all blocks success\n");
  }

  // get all blocks from ds disk
  VUINT64 blocks;
  block_manager.get_all_block_ids(blocks);//肯定返回成功

  // check block copy
  VUINT64 need_sync_block_list, lost_block_list;
  ret = check_all_block_in_disk(ns_addr, ns_slave_addr, ds_id, blocks, need_sync_block_list, lost_block_list);

  // print result
  if (TFS_SUCCESS == ret)
  {
    fprintf(stdout, "check all block finish\n");
    print_result(need_sync_block_list, lost_block_list);
  }
  else
  {
    TBSYS_LOG(ERROR, "check blocks fail finally, ret: %d", ret);
  }

  TfsClientImplV2::Instance()->destroy();
  return 0;
}

