/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: dataserver_define.h 643 2011-08-02 07:38:33Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 *
 */
#include <Time.h>
#include "ds_define.h"
#include "common/error_msg.h"

namespace tfs
{
  namespace dataserver
  {
    int SuperBlockInfo::dump(std::stringstream& stream) const
    {
      stream << "version: " << version_ <<  ",mount tag: " << mount_tag_ << ",mount point: " << mount_point_
        << ",mount_time: " <<tbutil::Time::seconds(mount_time_).toDateTime() << ",mout_fs_type: " << mount_fs_type_
        << ",superblock_reserve_offset: " << superblock_reserve_offset_ << ",block_index_offset: " << block_index_offset_
        << ",max_block_index_element_count: " << max_block_index_element_count_ << ",total_main_block_count: " << total_main_block_count_
        << ",used_main_block_count: " << used_main_block_count_ << ",used_extend_block_count: " << used_extend_block_count_
        << ",max_main_block_size: " << max_main_block_size_ << ",max_extend_block_size: " << max_extend_block_size_
        << ",hash_bucket_count: " << hash_bucket_count_ << ",max_mmap_size: " <<mmap_option_.max_mmap_size_
        << ",first_mmap_size: " << mmap_option_.first_mmap_size_ << ",per_mmap_size: " << mmap_option_.per_mmap_size_
        << ",max_use_block_ratio: " << max_use_block_ratio_ << ",max_use_hash_bucket_ratio: " <<max_use_hash_bucket_ratio_ <<std::endl;
      return common::TFS_SUCCESS;
    }
  }/** end namespace dataserver **/
}/** end namespace tfs **/

