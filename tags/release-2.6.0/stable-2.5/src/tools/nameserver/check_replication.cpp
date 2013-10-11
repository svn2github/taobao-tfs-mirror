/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *   linqing <linqing.zyd@taobao.com>
 *      - initial release
 */

#include <stdio.h>
#include <vector>
#include <algorithm>
#include "common/internal.h"
#include "common/error_msg.h"
#include "common/version.h"
#include "common/func.h"
#include "common/base_packet_streamer.h"
#include "common/base_packet.h"
#include "common/client_manager.h"
#include "message/message_factory.h"
#include "tools/util/tool_util.h"
#include "tools/util/ds_lib.h"
#include "clientv2/tfs_client_impl_v2.h"
#include "dataserver/ds_define.h"
#include "dataserver/dataservice.h"
#include "dataserver/data_helper.h"

using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::dataserver;
using namespace tfs::tools;
using namespace std;

void usage(const char* app_name)
{
  char *options =
    "-s           nameserver ip:port\n"
    "-b           block id\n"
    "-g           group mask, optional\n"
    "-l           log level\n"
    "-v           version information\n"
    "-h           help\n";
  fprintf(stderr, "%s usage:\n%s", app_name, options);
  exit(-1);
}

void version()
{
  fprintf(stderr, Version::get_build_description());
  exit(0);
}

struct FileInfoCompare
{
  bool operator () (const FileInfoV2& left, const FileInfoV2& right)
  {
    return left.id_ < right.id_;
  }
};

bool compare_block_index(const IndexDataV2& left_index, const IndexDataV2& right_index)
{
  bool same =
    left_index.header_.info_.block_id_ == right_index.header_.info_.block_id_ &&
    left_index.header_.info_.family_id_ == right_index.header_.info_.family_id_ &&
    left_index.header_.info_.version_ == right_index.header_.info_.version_ &&
    left_index.header_.info_.size_ == right_index.header_.info_.size_ &&
    left_index.header_.info_.file_count_ == right_index.header_.info_.file_count_ &&
    left_index.header_.info_.del_size_ == right_index.header_.info_.del_size_ &&
    left_index.header_.info_.del_file_count_ == right_index.header_.info_.del_file_count_ &&
    left_index.header_.info_.update_size_ == right_index.header_.info_.update_size_ &&
    left_index.header_.info_.update_file_count_ == right_index.header_.info_.update_file_count_ &&
    left_index.header_.marshalling_offset_ == right_index.header_.marshalling_offset_ &&
    left_index.header_.used_offset_  == right_index.header_.used_offset_ &&
    left_index.header_.seq_no_ == right_index.header_.seq_no_ &&
    left_index.header_.used_file_info_bucket_size_ == right_index.header_.used_file_info_bucket_size_;

  uint64_t block_id = left_index.header_.info_.block_id_;
  if (same)
  {
    same = left_index.finfos_.size() == right_index.finfos_.size();
    if (same)
    {
      set<FileInfoV2, FileInfoCompare> right_infos;
      vector<FileInfoV2>::const_iterator iter = right_index.finfos_.begin();
      for ( ; iter != right_index.finfos_.end(); iter++)
      {
        right_infos.insert(*iter);
      }

      iter = left_index.finfos_.begin();
      for (; iter != left_index.finfos_.end() && same; iter++)
      {
        set<FileInfoV2, FileInfoCompare>::iterator sit = right_infos.find(*iter);
        if (sit == right_infos.end())  // a file not in right
        {
          TBSYS_LOG(WARN, "blockid %"PRI64_PREFIX"u fileid %"PRI64_PREFIX"u not in right",
              block_id, iter->id_);
          same = false;
          break;
        }
        else
        {
          const FileInfoV2& left = *iter;
          const FileInfoV2& right = *sit;
          same = left.id_ == right.id_ &&
            left.offset_ == right.offset_ &&
            left.size_ == right.size_ &&
            left.crc_ == right.crc_ &&
            left.status_ == right.status_ &&
            left.create_time_ == right.create_time_ &&
            left.modify_time_ == right.modify_time_;

          if (!same)
          {
            TBSYS_LOG(WARN, "blockid %"PRI64_PREFIX"u fileid %"PRI64_PREFIX"u file info not same",
                block_id, left.id_);
            break;
          }
          right_infos.erase(sit);
        }
      }

      if (right_infos.size() > 0)
      {
        same = false;
        set<FileInfoV2, FileInfoCompare>::iterator sit = right_infos.begin();
        for ( ; sit != right_infos.end(); sit++)
        {
          TBSYS_LOG(WARN, "blockid %"PRI64_PREFIX"u fileid %"PRI64_PREFIX"u not in left",
              block_id, sit->id_);
        }
      }
    }
    else
    {
      TBSYS_LOG(WARN, "block %"PRI64_PREFIX"u index item size not consistency", block_id);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "block %"PRI64_PREFIX"u index header not consistency", block_id);
  }

  return same;
}


bool compare_block_data(const uint64_t block_id,
    const FileInfoV2& finfo, const uint64_t left_ds, const uint64_t right_ds, DataHelper& helper)
{
  bool same = false;
  char* left_buf = new (std::nothrow) char[finfo.size_];
  assert(NULL != left_buf);
  char* right_buf = new (std::nothrow) char[finfo.size_];
  assert(NULL != right_buf);
  int ret = helper.read_file(left_ds,
      block_id, block_id, finfo.id_, left_buf, finfo.size_, 0, READ_DATA_OPTION_FLAG_FORCE);
  if (TFS_SUCCESS == ret)
  {
    ret = helper.read_file(right_ds,
      block_id, block_id, finfo.id_, right_buf, finfo.size_, 0, READ_DATA_OPTION_FLAG_FORCE);
  }

  if (TFS_SUCCESS == ret)
  {
    uint32_t left_crc = Func::crc(0, left_buf, finfo.size_);
    uint32_t right_crc = Func::crc(0, right_buf, finfo.size_);
    same = (finfo.crc_ == left_crc) && (left_crc == right_crc);
    if (!same)
    {
      TBSYS_LOG(WARN, "blockid %"PRI64_PREFIX"u fileid %"PRI64_PREFIX"u file data not same. "
          "finfo crc: %u, left crc: %u, right crc: %u",
          block_id, finfo.id_, finfo.crc_, left_crc, right_crc);
    }
  }

  return same;
}

bool compare_block_replicas(const uint64_t block_id,
    const uint64_t left_ds, const uint64_t right_ds)
{
  bool same = false;

  DataService service;
  DataHelper helper(service);

  IndexDataV2 left_index;
  IndexDataV2 right_index;

  int ret = helper.read_index(left_ds, block_id, block_id, left_index);
  if (TFS_SUCCESS == ret)
  {
    ret = helper.read_index(right_ds, block_id, block_id, right_index);
  }

  if (TFS_SUCCESS == ret)
  {
    same = compare_block_index(left_index, right_index);
    if (same)
    {
      vector<FileInfoV2>::iterator iter = left_index.finfos_.begin();
      for ( ; iter != left_index.finfos_.end() && same; iter++)
      {
        same = compare_block_data(block_id, *iter, left_ds, right_ds, helper);
      }
    }
  }

  return same;
}

int main(int argc, char* argv[])
{
  string ns_addr;
  string log_level = "info";
  string group_mask = "255.255.255.255"; // default mask
  uint64_t block_id = 0;
  int flag = 0;

  while ((flag = getopt(argc, argv, "s:g:l:b:hv")) != EOF)
  {
    switch (flag)
    {
      case 's':
        ns_addr = optarg;
        break;
      case 'g':
        group_mask = optarg;
        break;
      case 'b':
        block_id = strtoull(optarg, NULL, 10);
        break;
      case 'l':
        log_level = optarg;
        break;
      case 'v':
        version();
        break;
      case 'h':
      default:
        usage(argv[0]);
        break;
    }
  }

  if ((ns_addr.length() == 0) || (block_id == 0))
  {
    usage(argv[0]);
  }

  MessageFactory* factory = new (std::nothrow) MessageFactory();
  assert(NULL != factory);
  BasePacketStreamer* streamer = new (std::nothrow) BasePacketStreamer(factory);
  assert(NULL != streamer);

  TBSYS_LOGGER.setLogLevel(log_level.c_str());
  int ret = NewClientManager::get_instance().initialize(factory, streamer);

  // get block replica info
  VUINT64 replicas;
  if (TFS_SUCCESS == ret)
  {
    ret = ToolUtil::get_block_ds_list_v2(Func::get_host_ip(ns_addr.c_str()),
      block_id, replicas, T_READ);
  }

  if (TFS_SUCCESS == ret)
  {
    if (replicas.size() == 0)
    {
      printf("%"PRI64_PREFIX"u %s\n", block_id, "NONE");
    }
    else if (replicas.size() == 1)
    {
      printf("%"PRI64_PREFIX"u %s\n", block_id, "ONE");
    }
    else
    {
      bool location_safe = true;
      set<uint32_t> lans;
      VUINT64::iterator iter = replicas.begin();
      for ( ; iter != replicas.end(); iter++)
      {
        TBSYS_LOG(INFO, "block %"PRI64_PREFIX"u : %s",
            block_id, tbsys::CNetUtil::addrToString(*iter).c_str());
        uint32_t lan = Func::get_lan(*iter, Func::get_addr(group_mask.c_str()));
        set<uint32_t>::iterator sit = lans.find(lan);
        if (sit != lans.end())
        {
          TBSYS_LOG(WARN, "block %"PRI64_PREFIX"u replicas are not location safe.", block_id);
          location_safe = false;
          break;
        }
      }

      // randomly choose two replicas to do compare for simplicity
      random_shuffle(replicas.begin(), replicas.end());
      bool same = compare_block_replicas(block_id, replicas[0], replicas[1]);
      printf("%"PRI64_PREFIX"u %s\n", block_id, same ? "SAME": "DIFF");
    }
  }

  NewClientManager::get_instance().destroy();

  tbsys::gDelete(streamer);
  tbsys::gDelete(factory);

  return ret;
}
