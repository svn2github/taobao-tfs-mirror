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
#include <Memory.hpp>
#include <gtest/gtest.h>
#include "ds_define.h"
#include "common/error_msg.h"
#include "block_manager.h"
#include "super_block_manager.h"
#include "physical_block_manager.h"

using namespace tfs::common;
namespace tfs
{
  namespace dataserver
  {
    class TestIndexHandle: public ::testing::Test
    {
      public:
        TestIndexHandle():
          block_manager_("./super_block"),
          index_handle_("./index.data"),
          verify_index_handle_("./index.verify.data")
        {

        }

        virtual ~TestIndexHandle()
        {

        }

        virtual void SetUp()
        {
          SuperBlockInfo info;
          memset(&info, 0, sizeof(info));
          info.max_block_index_element_count_ = 1024;
          info.total_main_block_count_        = 512;
          info.max_main_block_size_ = 72 * 1024 * 1024;
          info.max_extend_block_size_ = 4 * 1024 * 1024;
          info.hash_bucket_count_   = 1024;
          sprintf(info.mount_point_, "%s", ".");
          const int32_t pagesize = getpagesize();
          const int32_t avg_segment_size    = 40 * 1024;
          const int32_t avg_file_count = info.max_main_block_size_ / avg_segment_size;
          const int32_t mmap_size = sizeof(IndexHeaderV2) + (info.hash_bucket_count_ + 1) * sizeof(FileInfoV2);
          const int32_t count     = mmap_size / pagesize;
          const int32_t remainder = mmap_size % pagesize;
          const int32_t max_mmap_size = sizeof(IndexHeaderV2) + (avg_file_count + 1) * sizeof(FileInfoV2);
          const int32_t max_count     = max_mmap_size / pagesize;
          const int32_t max_remainder = max_mmap_size % pagesize;
          info.mmap_option_.first_mmap_size_=  remainder ? (count + 1) * pagesize : count * pagesize;
          info.mmap_option_.per_mmap_size_  =  pagesize;
          if (max_remainder)
            info.mmap_option_.max_mmap_size_ = (max_count + 1) * pagesize * INDEXFILE_SAFE_MULT;
          else
            info.mmap_option_.max_mmap_size_ = max_count * pagesize * INDEXFILE_SAFE_MULT;
          EXPECT_EQ(TFS_SUCCESS, block_manager_.get_super_block_manager().format(info));

          const uint64_t LOGIC_BLOCK_ID = 0xfff;
          const uint64_t VERIFY_LOGIC_BLOCK_ID = 0xffff;
          const int64_t  FAMILY_ID = 0xfffff;
          const int32_t  INDEX_NUM = 8;
          EXPECT_EQ(TFS_SUCCESS, index_handle_.create(LOGIC_BLOCK_ID, info.hash_bucket_count_, info.mmap_option_));
          EXPECT_EQ(TFS_SUCCESS, verify_index_handle_.create(VERIFY_LOGIC_BLOCK_ID, FAMILY_ID, INDEX_NUM));
        }

        virtual void TearDown()
        {
          ::unlink("./super_block");
          ::unlink("./index.data");
          ::unlink("./index.verify.data");
        }
      protected:
        BlockManager block_manager_;
        IndexHandle index_handle_;
        VerifyIndexHandle verify_index_handle_;
    };

    TEST_F(TestIndexHandle, get_slot_v1)
    {
      SuperBlockInfo* super_info = NULL;
      SuperBlockManager& super_block_manager = block_manager_.get_super_block_manager();
      EXPECT_EQ(TFS_SUCCESS, super_block_manager.get_super_block_info(super_info));

      char* data = new char[super_info->mmap_option_.first_mmap_size_];
      memset(data, 0, super_info->mmap_option_.first_mmap_size_);
      IndexHeaderV2* header = reinterpret_cast<IndexHeaderV2*>(data);
      FileInfoV2* finfos    = reinterpret_cast<FileInfoV2*>(data + sizeof(IndexHeaderV2));
      header->info_.block_id_ = 0xfff;
      header->info_.version_  = 1;
      header->seq_no_   = 0;
      header->info_.family_id_  = INVALID_FAMILY_ID;
      header->info_.last_update_time_ = time(NULL);
      header->file_info_bucket_size_ =  super_info->hash_bucket_count_;

      uint16_t slot = 0;
      uint64_t fileid = 0;
      FileInfoV2* current = NULL, *prev = NULL, *tmp = NULL;
      for (uint32_t i = 0; i < (uint32_t)header->file_info_bucket_size_; ++i)
      {
        current = NULL, prev = NULL, tmp = NULL, slot = 0, fileid = 0;
        EXPECT_EQ(TFS_SUCCESS, index_handle_.get_slot_(slot, fileid, current, prev, finfos, header, GET_SLOT_TYPE_GEN));
        EXPECT_EQ((i + 1) , fileid);
        EXPECT_TRUE(NULL != current);
        EXPECT_TRUE(NULL == prev);
        EXPECT_EQ(TFS_SUCCESS, index_handle_.get_slot_(slot, fileid, tmp, prev, finfos, header, GET_SLOT_TYPE_INSERT));
        EXPECT_EQ(current, tmp);
        tmp = NULL;
        EXPECT_EQ(EXIT_META_NOT_FOUND_ERROR, index_handle_.get_slot_(slot, fileid, tmp, prev, finfos, header, GET_SLOT_TYPE_QUERY));
        current->id_ = fileid;
        tmp = NULL;
        EXPECT_EQ(TFS_SUCCESS, index_handle_.get_slot_(slot, fileid, tmp, prev, finfos, header, GET_SLOT_TYPE_QUERY));
        EXPECT_EQ(current->id_, tmp->id_);
        tmp = NULL;
        EXPECT_EQ(TFS_SUCCESS, index_handle_.get_slot_(slot, fileid, tmp, prev, finfos, header, GET_SLOT_TYPE_QUERY));
        EXPECT_EQ(current->id_, tmp->id_);
      }
      current = NULL, prev = NULL, tmp = NULL, slot = 0, fileid = 0xFFFFFFFFFFF;
      EXPECT_EQ(EXIT_META_NOT_FOUND_ERROR, index_handle_.get_slot_(slot, fileid, current, prev, finfos, header, GET_SLOT_TYPE_QUERY));

      header->seq_no_   = 222227777;
      header->used_file_info_bucket_size_ = 0;
      memset((data + sizeof(IndexHeaderV2)), 0,  super_info->mmap_option_.first_mmap_size_ - sizeof(IndexHeaderV2));

      current = NULL, prev = NULL, tmp = NULL, slot = 0, fileid = 0;
      for (uint32_t j = 0; j < (uint32_t)header->file_info_bucket_size_; ++j)
      {
        current = NULL, prev = NULL, tmp = NULL, slot = 0, fileid = 0;
        EXPECT_EQ(TFS_SUCCESS, index_handle_.get_slot_(slot, fileid, current, prev, finfos, header, GET_SLOT_TYPE_GEN));
        EXPECT_TRUE(NULL != current);
        EXPECT_EQ(TFS_SUCCESS, index_handle_.get_slot_(slot, fileid, tmp, prev, finfos, header, GET_SLOT_TYPE_INSERT));
        EXPECT_EQ(current, tmp);
        tmp = NULL;
        EXPECT_EQ(EXIT_META_NOT_FOUND_ERROR, index_handle_.get_slot_(slot, fileid, tmp, prev, finfos, header, GET_SLOT_TYPE_QUERY));
        current->id_ = fileid;
        tmp = NULL;
        EXPECT_EQ(TFS_SUCCESS, index_handle_.get_slot_(slot, fileid, tmp, prev, finfos, header, GET_SLOT_TYPE_QUERY));
        EXPECT_EQ(current->id_, tmp->id_);
        tmp = NULL;
        EXPECT_EQ(TFS_SUCCESS, index_handle_.get_slot_(slot, fileid, tmp, prev, finfos, header, GET_SLOT_TYPE_QUERY));
        EXPECT_EQ(current->id_, tmp->id_);
      }

      current = NULL, prev = NULL, tmp = NULL, slot = 0, fileid = 0xFFFFFFFFFFF;
      EXPECT_EQ(EXIT_META_NOT_FOUND_ERROR, index_handle_.get_slot_(slot, fileid, current, prev, finfos, header, GET_SLOT_TYPE_QUERY));
      tbsys::gDeleteA(data);
    }

    TEST_F(TestIndexHandle, insert_file_info)
    {
      SuperBlockInfo* super_info = NULL;
      SuperBlockManager& super_block_manager = block_manager_.get_super_block_manager();
      EXPECT_EQ(TFS_SUCCESS, super_block_manager.get_super_block_info(super_info));

      char* data = new char[super_info->mmap_option_.first_mmap_size_];
      memset(data, 0, super_info->mmap_option_.first_mmap_size_);
      IndexHeaderV2* header = reinterpret_cast<IndexHeaderV2*>(data);
      header->info_.block_id_ = 0xfff;
      header->info_.version_  = 1;
      header->seq_no_   = 0;
      header->info_.family_id_  = INVALID_FAMILY_ID;
      header->info_.last_update_time_ = time(NULL);
      header->file_info_bucket_size_ =  super_info->hash_bucket_count_;

      uint16_t slot = 0;
      uint64_t fileid = 0;
      FileInfoV2* current = NULL, *prev = NULL, *tmp = NULL;
      for (uint32_t i = 0; i < (uint32_t)header->file_info_bucket_size_; ++i)
      {
        current = NULL, prev = NULL, tmp = NULL, slot = 0, fileid = 0;
        EXPECT_EQ(TFS_SUCCESS, index_handle_.get_slot_(slot, fileid, current, prev, data, super_info->mmap_option_.first_mmap_size_, GET_SLOT_TYPE_GEN));
        EXPECT_EQ((i + 1) , fileid);
        EXPECT_TRUE(NULL != current);
        EXPECT_TRUE(NULL == prev);

        FileInfoV2 info;
        memset(&info, 0, sizeof(info));
        info.id_ = fileid;
        info.offset_ = random() % 0xFFFFFFF;
        info.size_ = random() % 0xFFFF;
        info.crc_  = random() % 0xFFFFFFFF;
        info.modify_time_ = time(NULL);
        info.create_time_ = time(NULL);

        EXPECT_EQ(TFS_SUCCESS, index_handle_.insert_file_info_(info,data, super_info->mmap_option_.first_mmap_size_, false));
        current = NULL, prev = NULL, tmp = NULL, slot = 0;

        EXPECT_EQ(TFS_SUCCESS, index_handle_.get_slot_(slot, fileid, current, prev, data, super_info->mmap_option_.first_mmap_size_, GET_SLOT_TYPE_QUERY));
        EXPECT_EQ(info.id_, current->id_);
        EXPECT_EQ(info.offset_, current->offset_);
        EXPECT_EQ(info.size_,current->size_);
        EXPECT_EQ(info.crc_, current->crc_);
        EXPECT_EQ(info.modify_time_, current->modify_time_);
        EXPECT_EQ(info.create_time_, current->create_time_);
      }
    }

    TEST_F(TestIndexHandle, remmap)
    {
      srandom(time(NULL));
      const double threshold = 0.20;
      IndexHeaderV2* header  = index_handle_.get_index_header_();
      EXPECT_EQ(0, header->used_file_info_bucket_size_);
      EXPECT_TRUE(header->file_info_bucket_size_ > 0);
      const int32_t FILE_INFO_BUCKET_SIZE = header->file_info_bucket_size_;

      EXPECT_EQ(TFS_SUCCESS, index_handle_.remmap_(threshold, 65535));
      header  = index_handle_.get_index_header_();
      EXPECT_EQ(FILE_INFO_BUCKET_SIZE, header->file_info_bucket_size_);

      header->seq_no_ = random() % 0xFFFFFFFF;

      int32_t i = 0;
      std::vector<FileInfoV2> files;
      int32_t MAX_INSERT_ITEM_SIZE = (int32_t)(header->file_info_bucket_size_ * 0.1);
      for (i = 0; i < MAX_INSERT_ITEM_SIZE; i++)
      {
        FileInfoV2 info;
        memset(&info, 0, sizeof(info));
        info.id_ = i + 1;
        info.offset_ = random() % 0xFFFFFFF;
        info.size_ = random() % 0xFFFF;
        info.crc_  = random() % 0xFFFFFFFF;
        info.modify_time_ = time(NULL);
        info.create_time_ = time(NULL);
        files.push_back(info);
        EXPECT_EQ(TFS_SUCCESS, index_handle_.write_file_info(info, threshold, 65535, 1, false));
      }

      EXPECT_EQ(FILE_INFO_BUCKET_SIZE, header->file_info_bucket_size_);
      EXPECT_EQ(MAX_INSERT_ITEM_SIZE, header->used_file_info_bucket_size_);

      MAX_INSERT_ITEM_SIZE = (int32_t)(header->file_info_bucket_size_ * (threshold));
      for (i = 0; i < MAX_INSERT_ITEM_SIZE; i++)
      {
        FileInfoV2 info;
        memset(&info, 0, sizeof(info));
        info.id_ =  (i + 1 +MAX_INSERT_ITEM_SIZE);
        info.offset_ = random() % 0xFFFFFFF;
        info.size_ = random() % 0xFFFF;
        info.crc_  = random() % 0xFFFFFFFF;
        info.modify_time_ = time(NULL);
        info.create_time_ = time(NULL);
        files.push_back(info);
        EXPECT_EQ(TFS_SUCCESS, index_handle_.write_file_info(info, threshold, 65535, 1, false));
      }
      header  = index_handle_.get_index_header_();
      EXPECT_TRUE(header->file_info_bucket_size_ >= FILE_INFO_BUCKET_SIZE);
      EXPECT_EQ((int32_t)(files.size()), header->used_file_info_bucket_size_);

    }

    TEST_F(TestIndexHandle, write_read_file_info)
    {
      srandom(time(NULL));
      const int32_t MAX_HASH_BUCKET_COUNT = 65535;
      const double threshold = 0.20;
      const int32_t LOGIC_BLOCK_ID = 0xfff;
      IndexHeaderV2* header  = index_handle_.get_index_header_();
      EXPECT_EQ(0, header->used_file_info_bucket_size_);
      EXPECT_TRUE(header->file_info_bucket_size_ > 0);
      const int32_t FILE_INFO_BUCKET_SIZE = header->file_info_bucket_size_;

      EXPECT_EQ(TFS_SUCCESS, index_handle_.remmap_(threshold, MAX_HASH_BUCKET_COUNT));
      header  = index_handle_.get_index_header_();
      EXPECT_EQ(FILE_INFO_BUCKET_SIZE, header->file_info_bucket_size_);

      header->seq_no_ = random() % 0xFFFFFFFF;

      FileInfoV2 tmp;
      tmp.id_ = 0xfffff;
      EXPECT_TRUE(TFS_SUCCESS != index_handle_.read_file_info(tmp, threshold, MAX_HASH_BUCKET_COUNT, LOGIC_BLOCK_ID));

      int32_t i = 0;
      std::vector<FileInfoV2> files;
      int32_t MAX_INSERT_ITEM_SIZE = (int32_t)(header->file_info_bucket_size_ * 0.1);
      for (i = 0; i < MAX_INSERT_ITEM_SIZE; i++)
      {
        FileInfoV2 info;
        memset(&info, 0, sizeof(info));
        info.id_ = i + 1;
        info.offset_ = random() % 0xFFFFFFF;
        info.size_ = random() % 0xFFFF;
        info.crc_  = random() % 0xFFFFFFFF;
        info.modify_time_ = time(NULL);
        info.create_time_ = time(NULL);
        EXPECT_EQ(TFS_SUCCESS, index_handle_.write_file_info(info, threshold, MAX_HASH_BUCKET_COUNT, LOGIC_BLOCK_ID, false));
        files.push_back(info);
      }

      EXPECT_TRUE(!files.empty());

      std::vector<FileInfoV2>::const_iterator iter = files.begin();
      for (; iter != files.end(); ++iter)
      {
        memset(&tmp, 0, sizeof(tmp));
        tmp.id_ = (*iter).id_;
        EXPECT_EQ(TFS_SUCCESS, index_handle_.read_file_info(tmp, threshold, MAX_HASH_BUCKET_COUNT, LOGIC_BLOCK_ID));
      }
      IndexHeaderV2 output_header;
      std::vector<FileInfoV2> tfiles;
      EXPECT_EQ(TFS_SUCCESS, index_handle_.traverse(output_header, tfiles, LOGIC_BLOCK_ID));
      EXPECT_EQ(files.size(), tfiles.size());
    }

    TEST_F(TestIndexHandle, verify_write_read_infos)
    {
      uint64_t LOGIC_BLOCK_ID = 0xffff;
      const int64_t  FAMILY_ID = 0xfffff;
      const double threshold = 0.20;
      const int32_t MAX_HASH_BUCKET_COUNT = 65535;
      IndexHeaderV2 header;
      for (int32_t j = 0; j < 8; j++)
      {
        memset(&header, 0, sizeof(header));
        header.info_.block_id_ = LOGIC_BLOCK_ID + j;
        header.info_.version_  = 1;
        header.seq_no_   = 0;
        header.info_.family_id_ = FAMILY_ID;
        header.info_.last_update_time_ = time(NULL);
        header.throughput_.last_statistics_time_ = header.info_.last_update_time_;
        header.used_file_info_bucket_size_ = random() % 1024 + 1024;
        header.file_info_bucket_size_ = random() % 2048+ 2048;
        std::vector<FileInfoV2> files, rfiles;
        for (int32_t i = 1; i <= header.used_file_info_bucket_size_; i++)
        {
          FileInfoV2 info;
          memset(&info, 0, sizeof(info));
          info.id_ = i;
          info.offset_ = random() % 0xFFFFFFF;
          info.size_ = random() % 0xFFFF;
          info.crc_  = random() % 0xFFFFFFFF;
          info.modify_time_ = time(NULL);
          info.create_time_ = time(NULL);
          files.push_back(info);
        }

        IndexHeaderV2 output_header;
        EXPECT_EQ(TFS_SUCCESS, verify_index_handle_.write_file_infos(header, files, threshold, MAX_HASH_BUCKET_COUNT, header.info_.block_id_));
        EXPECT_TRUE(TFS_SUCCESS != verify_index_handle_.traverse(output_header, rfiles, LOGIC_BLOCK_ID + 0xfff));
        EXPECT_EQ(0U, rfiles.size());
        EXPECT_EQ(TFS_SUCCESS, verify_index_handle_.traverse(output_header,rfiles, header.info_.block_id_));
        EXPECT_EQ(files.size(), rfiles.size());
        IndexHeaderV2* pheader = verify_index_handle_.get_index_header_();
        EXPECT_TRUE(NULL != pheader);
        EXPECT_EQ(j + 1, pheader->index_num_);
        VerifyIndexHandle::InnerIndex* inner = verify_index_handle_.get_inner_index_(header.info_.block_id_);
        EXPECT_TRUE(NULL != inner);
        EXPECT_EQ(header.info_.block_id_, inner->logic_block_id_);
        TBSYS_LOG(INFO, "inner index offset : %d", inner->offset_);
      }
    }
  }
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
