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
 *
 */
#ifndef TFS_DATASERVER_SUPERBLOCK_H_
#define TFS_DATASERVER_SUPERBLOCK_H_

#include <time.h>
#include <string.h>
#include <iostream>
#include "dataserver_define.h"
#include "common/interval.h"
#include "common/error_msg.h"

namespace tfs
{
  namespace dataserver
  {
    struct SuperBlock
    {
        char mount_tag_[MAX_DEV_TAG_LEN];
        int32_t time_;
        char mount_point_[common::MAX_DEV_NAME_LEN]; //name of mount point
        int64_t mount_point_use_space_; //the max space of the mount point
        BaseFsType base_fs_type_; //ext4, ext3...

        int32_t superblock_reserve_offset_; //super block start offset. not used
        int32_t bitmap_start_offset_; //bitmap start offset

        int32_t avg_segment_size_; //avg file size in block file

        float block_type_ratio_; //block type ration
        int32_t main_block_count_; //total count of main block
        int32_t main_block_size_; //per size of main block

        int32_t extend_block_count_; //total count of ext block
        int32_t extend_block_size_; //per size of ext block

        int32_t used_block_count_; //used main block count
        int32_t used_extend_block_count_; //used ext block count

        float hash_slot_ratio_;
        int32_t hash_slot_size_;
        MMapOption mmap_option_;
        int32_t version_; //version

        SuperBlock()
        {
          memset(mount_tag_, 0, sizeof(mount_tag_));
          time_ = time(NULL);

          memset(mount_point_, 0, sizeof(mount_point_));
          mount_point_use_space_ = 0;
          base_fs_type_ = NO_INIT;
          superblock_reserve_offset_ = 0;
          bitmap_start_offset_ = 0;
          avg_segment_size_ = 0;
          block_type_ratio_ = 0; //block type ration
          main_block_count_ = 0;
          main_block_size_ = 0;
          extend_block_count_ = 0;
          extend_block_size_ = 0;
          used_block_count_ = 0;
          used_extend_block_count_ = 0;
          hash_slot_ratio_ = 0;
          hash_slot_size_ = 0;
          memset(&mmap_option_, 0, sizeof(MMapOption));
          version_ = 0;
        }

        SuperBlock(const SuperBlock& r)
        {
          memcpy(mount_tag_, DEV_TAG, sizeof(mount_tag_));
          time_ = time(NULL);

          memcpy(mount_point_, r.mount_point_, sizeof(mount_point_));
          mount_point_use_space_ = r.mount_point_use_space_;
          base_fs_type_ = r.base_fs_type_;
          superblock_reserve_offset_ = r.superblock_reserve_offset_;
          bitmap_start_offset_ = r.bitmap_start_offset_;
          avg_segment_size_ = r.avg_segment_size_;
          block_type_ratio_ = r.block_type_ratio_;
          main_block_count_ = r.main_block_count_;
          main_block_size_ = r.main_block_size_;
          extend_block_count_ = r.extend_block_count_;
          extend_block_size_ = r.extend_block_size_;
          used_block_count_ = r.used_block_count_;
          used_extend_block_count_ = r.used_extend_block_count_;
          hash_slot_ratio_ = r.hash_slot_ratio_;
          hash_slot_size_ = r.hash_slot_size_;
          mmap_option_ = r.mmap_option_;
          version_ = r.version_;
        }

        SuperBlock& operator=(const SuperBlock& r)
        {
          if (this == &r)
          {
            return *this;
          }
          memcpy(mount_tag_, r.mount_tag_, sizeof(mount_tag_));
          time_ = r.time_;
          memcpy(mount_point_, r.mount_point_, sizeof(mount_point_));
          mount_point_use_space_ = r.mount_point_use_space_;
          base_fs_type_ = r.base_fs_type_;
          superblock_reserve_offset_ = r.superblock_reserve_offset_;
          bitmap_start_offset_ = r.bitmap_start_offset_;
          avg_segment_size_ = r.avg_segment_size_;
          block_type_ratio_ = r.block_type_ratio_;
          main_block_count_ = r.main_block_count_;
          main_block_size_ = r.main_block_size_;
          extend_block_count_ = r.extend_block_count_;
          extend_block_size_ = r.extend_block_size_;
          used_block_count_ = r.used_block_count_;
          used_extend_block_count_ = r.used_extend_block_count_;
          hash_slot_ratio_ = r.hash_slot_ratio_;
          hash_slot_size_ = r.hash_slot_size_;
          mmap_option_ = r.mmap_option_;
          version_ = r.version_;
          return *this;
        }

        void display()
        {
          std::cout << "tag " << mount_tag_ << std::endl;
          std::cout << "mount time " << time_ << std::endl;
          std::cout << "mount desc " << mount_point_ << std::endl;
          std::cout << "max use space " << mount_point_use_space_ << std::endl;
          std::cout << "base filesystem " << base_fs_type_ << std::endl;
          std::cout << "superblock reserve " << superblock_reserve_offset_ << std::endl;
          std::cout << "bitmap start offset " << bitmap_start_offset_ << std::endl;
          std::cout << "avg inner file size " << avg_segment_size_ << std::endl;
          std::cout << "block type ratio " << block_type_ratio_ << std::endl;
          std::cout << "main block count " << main_block_count_ << std::endl;
          std::cout << "main block size " << main_block_size_ << std::endl;
          std::cout << "extend block count " << extend_block_count_ << std::endl;
          std::cout << "extend block size " << extend_block_size_ << std::endl;
          std::cout << "used main block count " << used_block_count_ << std::endl;
          std::cout << "used extend block count " << used_extend_block_count_ << std::endl;
          std::cout << "hash slot ratio " << hash_slot_ratio_ << std::endl;
          std::cout << "hash slot size " << hash_slot_size_ << std::endl;
          std::cout << "first mmap size " << mmap_option_.first_mmap_size_ << std::endl;
          std::cout << "mmap size step " << mmap_option_.per_mmap_size_ << std::endl;
          std::cout << "max mmap size " << mmap_option_.max_mmap_size_ << std::endl;
          std::cout << "version " << version_ << std::endl;
        }
    };

  }
}
#endif //TFS_DATASERVER_SUPERBLOCK_H_
