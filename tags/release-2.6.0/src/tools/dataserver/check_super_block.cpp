/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: clear_file_system.cpp 380 2011-05-30 08:04:16Z daoan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   chuyu <chuyu@taobao.com>
 *      - modify 2010-03-20
 *   linqing <linqing.zyd@taobao.com>
 *      - modify 2013-01-10
 *
 */
#include "tbsys.h"
#include <stdio.h>
#include <vector>
#include <algorithm>
#include <sstream>
#include <map>
#include "common/parameter.h"
#include "dataserver/block_manager.h"
#include "common/version.h"
#include "common/internal.h"

using namespace tfs::dataserver;
using namespace tfs::common;
using namespace std;

const int32_t default_dirty_expire = 30;
const char* os_dirty_expire_path = "/proc/sys/vm/dirty_expire_centisecs";

void dump_super_block(const SuperBlockInfo& info);
bool check_super_block(BlockManager& block_manager);
bool check_data(BlockManager& block_manager, int32_t dirty_expire);
bool check_block(BlockManager& block_manager, const uint64_t block_id);
bool check_file(BlockManager& block_manager, const uint64_t block_id, const FileInfoV2& finfo);
int32_t get_os_dirty_expire();

void dump_super_block(const SuperBlockInfo& info)
{
  printf("%-25s%s\n", "mount_tag:", info.mount_tag_);
  printf("%-25s%s\n", "mount_path:", info.mount_point_);
  printf("%-25s%"PRI64_PREFIX"d\n", "mount_space:", info.mount_point_use_space_);
  printf("%-25s%d\n", "mount fs type:", info.mount_fs_type_);
  printf("%-25s%d\n", "sb reserve offset:", info.superblock_reserve_offset_);
  printf("%-25s%d\n", "index reserve offset:", info.block_index_offset_);
  printf("%-25s%d\n", "max block element:", info.max_block_index_element_count_);
  printf("%-25s%d\n", "total main block count:", info.total_main_block_count_);
  printf("%-25s%d\n", "used main block count:", info.used_main_block_count_);
  printf("%-25s%d\n", "max main block size:", info.max_main_block_size_);
  printf("%-25s%d\n", "max extend block size:", info.max_extend_block_size_);
  printf("%-25s%d\n", "max hash bucket count:", info.max_hash_bucket_count_);
  printf("%-25s%d\n", "hash bucket count:", info.hash_bucket_count_);
  printf("%-25s%lf\n", "max use block ratio:", info.max_use_block_ratio_);
  printf("%-25s%lf\n", "max use hash ratio:", info.max_use_hash_bucket_ratio_);
}

bool check_super_block(BlockManager& block_manager)
{
  bool valid = true;
  SuperBlockInfo* info = NULL;
  int ret = block_manager.get_super_block_manager().get_super_block_info(info);
  if (TFS_SUCCESS == ret)
  {
    // dump_super_block(*info);
    vector<BlockIndex> index_vecs;
    index_vecs.push_back(BlockIndex()); // to make the index start at 1
    for (int i = 1; i <= info->max_block_index_element_count_; i++)
    {
      BlockIndex index;
      block_manager.get_super_block_manager().get_block_index(index, i);
      index_vecs.push_back(index);
    }

    std::map<uint64_t, uint64_t> extend_used_map;

    for (int i = 1; i <= info->total_main_block_count_; i++)
    {
      BlockIndex current = index_vecs[i];
      if (0 == current.logic_block_id_)
      {
        continue;
      }
      if (0 != current.physical_block_id_)  // allocated block
      {
        uint64_t curr_logic_block_id = current.logic_block_id_;
        while (0 != current.next_index_)
        {
          current = index_vecs[current.next_index_];
          if (current.physical_block_id_ == 0)
          {
            valid = false;
            TBSYS_LOG(WARN, "logic_block %"PRI64_PREFIX"u ==> extend block %d no allocated.",
                curr_logic_block_id, current.physical_block_id_);
            continue;
          }

          if (current.logic_block_id_ != curr_logic_block_id)
          {
            valid = false;
            TBSYS_LOG(WARN, "logic_block %"PRI64_PREFIX"u ==> extend block %d belongs to"
                "another logic block %"PRI64_PREFIX"u.",
                curr_logic_block_id, current.physical_block_id_, current.logic_block_id_);
            continue;
          }

          BlockIndex& alloc = index_vecs[current.physical_file_name_id_];
          if (alloc.physical_block_id_ == 0)
          {
            valid = false;
            TBSYS_LOG(WARN, "logic_block %"PRI64_PREFIX"u ==> alloc block %d not allocated, extend block: %d",
                curr_logic_block_id, current.physical_file_name_id_, current.physical_block_id_);
            continue;
          }

          if (alloc.split_flag_ != BLOCK_SPLIT_FLAG_YES)
          {
            valid = false;
            TBSYS_LOG(WARN, "logic_block %"PRI64_PREFIX"u ==> alloc block %d not splited, extend block: %d",
                curr_logic_block_id, current.physical_file_name_id_, current.physical_block_id_);
            continue;
          }

          uint64_t extend_key = (((uint64_t)alloc.physical_file_name_id_) << 32) + current.index_;
          std::map<uint64_t, uint64_t>::iterator iter = extend_used_map.find(extend_key);
          if (iter != extend_used_map.end())
          {
            valid = false;
            TBSYS_LOG(WARN, "logic_block %"PRI64_PREFIX"u ==> alloc block %d index %d is linked to "
                "another logic block %"PRI64_PREFIX"u",
                curr_logic_block_id, alloc.physical_file_name_id_, current.index_, iter->second);
          }
          else
          {
            extend_used_map.insert(make_pair(extend_key, curr_logic_block_id));
          }
        }
      }
    }
  }

  return valid;
}

bool check_data(BlockManager& block_manager, int32_t dirty_expire)
{
  bool valid = true;
  std::map<uint64_t, ThroughputV2> binfos;
  int ret = block_manager.get_all_block_statistic_visit_info(binfos, false);
  if (TFS_SUCCESS != ret)
  {
    valid = false;
    TBSYS_LOG(WARN, "get all block info fail.");
  }
  else
  {
    // find lastest update time
    int64_t latest = 0;
    std::map<uint64_t, ThroughputV2> ::const_iterator iter = binfos.begin();
    for ( ; iter != binfos.end(); iter++)
    {
      if (iter->second.last_update_time_ > latest)
      {
        latest = iter->second.last_update_time_;
      }
    }

    // check possible dirty data, TODO: optimize total files need to check
    for (iter = binfos.begin() ; iter != binfos.end() && valid; iter++)
    {
      if (iter->second.last_update_time_ + dirty_expire >= latest)
      {
        valid = check_block(block_manager, iter->first);
      }
    }
  }

  return valid;
}

bool check_block(BlockManager& block_manager, const uint64_t block_id)
{
  bool valid = true;
  IndexHeaderV2 header;
  std::vector<FileInfoV2> finfos;
  int ret = block_manager.traverse(header, finfos, block_id, block_id);
  if (TFS_SUCCESS != ret)
  {
    valid = false;
    TBSYS_LOG(WARN, "check block fail. blockid: %"PRI64_PREFIX"u, ret: %d",
        block_id, ret);
  }
  else
  {
    std::vector<FileInfoV2>::iterator iter = finfos.begin();
    for ( ; iter != finfos.end() && valid; iter++)
    {
      valid = check_file(block_manager, block_id, *iter);
    }
  }
  TBSYS_LOG(DEBUG, "check block %"PRI64_PREFIX"u, ret: %d", block_id, ret);
  return valid;
}

bool check_file(BlockManager& block_manager, const uint64_t block_id, const FileInfoV2& finfo)
{
  int ret = TFS_SUCCESS;
  char data[MAX_READ_SIZE];
  int32_t offset = finfo.offset_ + sizeof(FileInfoInDiskExt);
  int32_t end = finfo.offset_ + finfo.size_;
  int32_t length = 0;
  uint32_t data_crc = 0;
  while (offset < end)
  {
    length = std::min(MAX_READ_SIZE, end - offset);
    ret = block_manager.pread(data, length, offset, block_id);
    ret = (ret < 0) ? ret : TFS_SUCCESS;
    if (TFS_SUCCESS == ret)
    {
      data_crc = Func::crc(data_crc, data, length);
      offset += length;
    }
  }

  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "check file fail. blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, ret: %d",
      block_id, finfo.id_, ret);
  }
  else
  {
    if (data_crc != finfo.crc_)
    {
      TBSYS_LOG(WARN, "crc error. blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, "
        "data_crc: %u, index_crc: %u",
        block_id, finfo.id_, data_crc, finfo.crc_);
    }
  }

  return TFS_SUCCESS == ret && data_crc == finfo.crc_;
}

int32_t get_os_dirty_expire()
{
  int32_t dirty_expire = default_dirty_expire;
  char line[128];
  FILE* fp = fopen(os_dirty_expire_path, "r");
  if (fp != NULL && fgets(line, 128, fp) != NULL)
  {
    dirty_expire = atoi(line) / 100;
    TBSYS_LOG(INFO, "dirty expire: %d", dirty_expire);
  }
  if (fp != NULL)
  {
    fclose(fp);
  }
  return dirty_expire;
}

int main(int argc, char* argv[])
{
  char* conf_file = NULL;
  int help_info = 0;
  int i;
  bool only_super = false;
  std::string server_index;
  std::stringstream log_name;
  std::string log_level;

  while ((i = getopt(argc, argv, "f:i:l:svh")) != EOF)
  {
    switch (i)
    {
    case 'f':
      conf_file = optarg;
      break;
    case 'i':
      server_index = optarg;
      break;
    case 'l':
      log_level = optarg;
      break;
    case 's':
      only_super = true;
      break;
    case 'v':
      fprintf(stderr, "create tfs file system tool, version: %s\n", Version::get_build_description());
      return 0;
    case 'h':
    default:
      help_info = 1;
      break;
    }
  }

  if ((conf_file == NULL) || (server_index.size() == 0) || help_info)
  {
    fprintf(stderr, "\nUsage: %s -f conf_file -i server_index -l log_level -h -v\n", argv[0]);
    fprintf(stderr, "  -f configure file\n");
    fprintf(stderr, "  -i server_index  dataserver index number\n");
    fprintf(stderr, "  -l log_level debug | info | warn | error\n");
    fprintf(stderr, "  -v show version info\n");
    fprintf(stderr, "  -h help info\n");
    fprintf(stderr, "\n");
    return -1;
  }

  log_name << argv[0] << server_index << ".log";
  TBSYS_LOGGER.setLogLevel(log_level.c_str());
  TBSYS_LOGGER.setFileName(log_name.str().c_str());
  TBSYS_LOGGER.rotateLog(log_name.str().c_str());

  int ret = 0;
  if (EXIT_SUCCESS != TBSYS_CONFIG.load(conf_file))
  {
    cerr << "load config error conf_file is " << conf_file;
    return TFS_ERROR;
  }
  if ((ret = SYSPARAM_DATASERVER.initialize(conf_file, server_index)) != TFS_SUCCESS)
  {
    cerr << "SysParam::load file system param failed:" << conf_file << endl;
    return ret;
  }

  TBSYS_LOG(INFO, "check dataserver %s integrity start", server_index.c_str());

  string super_block_path = string(SYSPARAM_FILESYSPARAM.mount_name_) + SUPERBLOCK_NAME;
  BlockManager block_manager(super_block_path);
  ret = block_manager.bootstrap(SYSPARAM_FILESYSPARAM);
  if (TFS_SUCCESS == ret)
  {
    bool valid = check_super_block(block_manager);
    if (valid)
    {
      if (!only_super)
      {
        valid = check_data(block_manager, get_os_dirty_expire() * 2);
        if (!valid)
        {
          TBSYS_LOG(WARN, "data invalid");
        }
      }
    }
    else
    {
      TBSYS_LOG(WARN, "super block invalid");
    }
  }

  TBSYS_LOG(INFO, "check dataserver %s integrity end", server_index.c_str());

  return 0;
}
