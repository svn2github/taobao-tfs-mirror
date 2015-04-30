/*
 * (C) 2007-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id
 *
 * Authors:
 *   duafnei@taobao.com
 *      - initial release
 *
 */
#include <string>
#include <tbsys.h>
#include <tbtimeutil.h>
#include <Memory.hpp>
#include <gtest/gtest.h>
#include "ds_define.h"
#include "common/error_msg.h"
#include "common/internal.h"
#include "block_manager.h"
#include "super_block_manager.h"
#include "physical_block_manager.h"
#include "logic_blockv2.h"
#include "block_manager.h"

using namespace tfs::common;
using namespace tfs::dataserver;

int main(int argc, char* argv[])
{
  if (argc < 2)
  {
    printf("parameter is invalid, mybe 2\n");
    return -1;
  }
  std::string mount_path(argv[1]);
  FileSystemParameter parameter;
  parameter.mount_name_ =  argv[1];
  parameter.max_mount_size_ = 43383972;
  parameter.base_fs_type_   = EXT3_FULL;
  parameter.super_block_reserve_offset_ = 1024;
  parameter.avg_segment_size_ = 40 * 1024;
  parameter.main_block_size_  = 72 * 1024 * 1024;
  parameter.extend_block_size_= 2  * 1024 * 1024;
  parameter.block_type_ratio_ = 0.95;
  parameter.file_system_version_ = 2;
  parameter.hash_slot_ratio_ = 0.8;
  BlockManager block_manager_(mount_path + "super_block");
  //EXPECT_EQ(TFS_SUCCESS, block_manager_.format(parameter));
  int32_t MAX_LOGIC_BLOCK_COUNT = 312;
  EXPECT_EQ(TFS_SUCCESS, block_manager_.bootstrap(parameter));
  int32_t count = MAX_LOGIC_BLOCK_COUNT;
  while (count-- > 0)
  {
    uint64_t logic_block_id = random() % 0xfffffff + 1024;
    if (!block_manager_.exist(logic_block_id))
      EXPECT_EQ(TFS_SUCCESS, block_manager_.new_block(logic_block_id, false));
  }

  std::vector<BlockInfoV2> blocks;
  EXPECT_EQ(TFS_SUCCESS, block_manager_.get_all_block_info(blocks));

  for (int32_t i = 0; i < 100; i++)
  {
      const int32_t MAX_DATA_SIZE = random() % 2048  + 30 * 1024;
      char data[MAX_DATA_SIZE];
      for (int32_t i = 0; i < MAX_DATA_SIZE; ++i)
        data[i] = random() % 256;
      DataFile dfile(0xfff, "./tmp");
      FileInfoInDiskExt dinfo;
      memset(&dinfo, 0, sizeof(dinfo));
      int32_t ret = dfile.pwrite(dinfo, data, MAX_DATA_SIZE, 0);
      EXPECT_EQ(MAX_DATA_SIZE, ret);
      uint64_t fileid = 0;

      BlockInfoV2 binfo = blocks[random() % blocks.size()];

      const int32_t total_len = sizeof(FileInfoInDiskExt) + MAX_DATA_SIZE;

      EXPECT_EQ(total_len, block_manager_.write(fileid, dfile, binfo.block_id_, binfo.block_id_));
  }

  blocks.clear();
  EXPECT_EQ(TFS_SUCCESS, block_manager_.get_all_block_info(blocks));
  std::vector<BlockInfoV2>::const_iterator iter = blocks.begin();
  for (; iter != blocks.end();++iter)
  {
    EXPECT_EQ(TFS_SUCCESS, block_manager_.del_block((*iter).block_id_));
  }
  return 0;
}
