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
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   chuyu <chuyu@taobao.com>
 *      - modify 2010-03-20
 *
 */
#include "dataserver/blockfile_manager.h"
#include "common/parameter.h"
#include "dataserver/version.h"
#include "dataserver/superblock.h"
#include <stdio.h>

using namespace tfs::common;
using namespace tfs::dataserver;
int main(int argc, char* argv[])
{
  char* conf_file = NULL;
  int32_t help_info = 0;
  int32_t i;
  string server_index;

  while ((i = getopt(argc, argv, "f:vh")) != EOF) 
  {
    switch (i) 
    {
      case 'f':
        conf_file = optarg; 
        break;
      case 'i':
        server_index = optarg;
        break;
      case 'v':
        fprintf(stderr, "modify tfs file system super block tool, version: %s\n", Version::get_build_description()); 
        return 0;
      case 'h':
      default:
        help_info = 1;
        break;
    }
  }

  if (NULL == conf_file || 0 == server_index.size() || help_info)
  {
    fprintf(stderr, "\nUsage: %s -f conf_file -i server_index -h -v\n", argv[0]);
    fprintf(stderr, "  -f configure file\n");
    fprintf(stderr, "  -i server_index  dataserver index number\n");
    fprintf(stderr, "  -v show version info\n");
    fprintf(stderr, "  -h help info\n");
    fprintf(stderr, "\n");
    return -1;
  }

  int32_t ret = 0;
  if ((ret = SysParam::instance().load_data_server(conf_file, server_index)) != TFS_SUCCESS) 
  {
    cerr << "SysParam::loadFileSystemParam failed:" << conf_file << endl;
    return ret;
  }

  cout << "mount name: " << SysParam::instance().filesystem_param().mount_name_
    << " max mount size: " << SysParam::instance().filesystem_param().max_mount_size_
    << " base fs type: " << SysParam::instance().filesystem_param().base_fs_type_
    << " superblock reserve offset: " << SysParam::instance().filesystem_param().super_block_reserve_offset_
    << " main block size: " << SysParam::instance().filesystem_param().main_block_size_
    << " extend block size: " << SysParam::instance().filesystem_param().extend_block_size_
    << " block ratio: " << SysParam::instance().filesystem_param().block_type_ratio_
    << " file system version: " << SysParam::instance().filesystem_param().file_system_version_
    << " avg inner file size: " << SysParam::instance().filesystem_param().avg_segment_size_
    << " hash slot ratio: " << SysParam::instance().filesystem_param().hash_slot_ratio_
    << endl;

  SuperBlock super_block;
  SuperBlockImpl* super_block_impl_ = new SuperBlockImpl(SysParam::instance().filesystem_param().mount_name_
      , SysParam::instance().filesystem_param().super_block_reserve_offset_);
  ret = super_block_impl_->read_super_blk(super_block);
  if (ret)
  {    
    TBSYS_LOG(ERROR, "read super block error. ret: %d, desc: %s\n", ret, strerror(errno));
    return ret;
  }
  super_block.mmap_option_.first_mmap_size_ = 122880;
  ret = super_block_impl_->write_super_blk(super_block);
  if (ret)
  {    
    TBSYS_LOG(ERROR, "write super block error. ret: %d, desc: %s\n", ret, strerror(errno));
    return ret;
  }
  return 0;
}
