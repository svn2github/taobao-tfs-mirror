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

using namespace tfs::common;
namespace tfs
{
  namespace dataserver
  {
    class TestLogicBlock: public ::testing::Test
    {
      public:
        TestLogicBlock():
          block_manager_("./super_block"),
          LOGIC_BLOCK_ID(0xfff),
          VERIFY_LOGIC_BLOCK_ID(0xffff000000000000),
          MAX_BLOCK_SIZE(72 * 1024 * 1024),
          MAX_EXT_BLOCK_SIZE( 4 * 1024 * 1024),
          MAX_PHYSICAL_BLOCK_NUM(10),
          MAX_ALLOC_PHYSICAL_BLOCK_NUM(5),
          FAMILY_ID(0xfffff),
          logic_block_(&block_manager_, LOGIC_BLOCK_ID, "./test.logic.block.index.data"),
          verify_logic_block_(&block_manager_, VERIFY_LOGIC_BLOCK_ID,".test.verify.logic.block.index.data")
        {

        }

        virtual ~TestLogicBlock()
        {

        }

        virtual void SetUp()
        {
          SuperBlockInfo info;
          memset(&info, 0, sizeof(info));
          info.max_block_index_element_count_ = 1024;
          info.total_main_block_count_        = 512;
          info.max_main_block_size_ = MAX_BLOCK_SIZE;
          info.max_extend_block_size_ = MAX_EXT_BLOCK_SIZE;
          info.hash_bucket_count_   = 1024;
          info.max_use_hash_bucket_ratio_ = 0.8;
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

          SuperBlockManager& super_manager = block_manager_.get_super_block_manager();
          PhysicalBlockManager& manager = block_manager_.get_physical_block_manager();
          int32_t i = 0;
          int32_t start = BLOCK_RESERVER_LENGTH;
          int32_t end   = info.max_main_block_size_;
          for (i = 1; i <= MAX_PHYSICAL_BLOCK_NUM; ++i)
          {
            std::stringstream str;
            str << "./" << i;
            BlockIndex index;
            memset(&index, 0, sizeof(index));
            int32_t physical_block_id = INVALID_PHYSICAL_BLOCK_ID;
            int32_t ret = super_manager.get_legal_physical_block_id(physical_block_id);
            index.logic_block_id_ = LOGIC_BLOCK_ID;
            index.physical_file_name_id_  = index.physical_block_id_ = physical_block_id;
            EXPECT_TRUE(index.physical_block_id_ > 0);
            EXPECT_EQ(TFS_SUCCESS, ret);
            FileOperation file_op(str.str(), O_RDWR | O_CREAT);
            char data[1024];
            memset(data, 0, 1024);
            ret = file_op.write(data, 1024);
            EXPECT_TRUE(ret > 0);
            file_op.close();
            ret = super_manager.update_block_index(index, index.physical_block_id_);
            EXPECT_EQ(TFS_SUCCESS, ret);
            ret = manager.insert(index, index.physical_block_id_, str.str().c_str(), start, end);
          }

          start = 0;
          for (i = MAX_PHYSICAL_BLOCK_NUM; i <= MAX_PHYSICAL_BLOCK_NUM + MAX_ALLOC_PHYSICAL_BLOCK_NUM; i++)
          {
            std::stringstream str;
            str << "./" << i;
            BlockIndex index;
            memset(&index, 0, sizeof(index));
            int32_t physical_block_id = INVALID_PHYSICAL_BLOCK_ID;
            int32_t ret = super_manager.get_legal_physical_block_id(physical_block_id);
            index.physical_file_name_id_  = index.physical_block_id_ = physical_block_id;
            index.split_flag_ = BLOCK_SPLIT_FLAG_YES;
            EXPECT_TRUE(index.physical_block_id_ > 0);
            EXPECT_EQ(TFS_SUCCESS, ret);
            FileOperation file_op(str.str(), O_RDWR | O_CREAT);
            char data[1024];
            memset(data, 0, 1024);
            ret = file_op.write(data, 1024);
            EXPECT_TRUE(ret > 0);
            file_op.close();
            ret = manager.insert(index, index.physical_block_id_, str.str().c_str(), start, end);
          }

          EXPECT_EQ(TFS_SUCCESS, logic_block_.create_index(info.hash_bucket_count_, info.mmap_option_));
          EXPECT_EQ(TFS_SUCCESS, verify_logic_block_.create_index(VERIFY_LOGIC_BLOCK_ID, FAMILY_ID, 7));
        }

        virtual void TearDown()
        {
          ::unlink("./super_block");
          ::unlink("./test.logic.block.index.data");
          ::unlink("./test.verify.logic.block.index.data");
          for (int32_t i = 1; i <= MAX_PHYSICAL_BLOCK_NUM + MAX_ALLOC_PHYSICAL_BLOCK_NUM; ++i)
          {
            std::stringstream str;
            str << "./" << i;
            ::unlink(str.str().c_str());
          }
        }
      protected:
        BlockManager block_manager_;
        const uint64_t LOGIC_BLOCK_ID;
        const uint64_t VERIFY_LOGIC_BLOCK_ID;
        const int32_t  MAX_BLOCK_SIZE;
        const int32_t  MAX_EXT_BLOCK_SIZE;
        const int32_t MAX_PHYSICAL_BLOCK_NUM;
        const int32_t MAX_ALLOC_PHYSICAL_BLOCK_NUM;
        const int64_t FAMILY_ID;
        LogicBlock logic_block_;
        VerifyLogicBlock verify_logic_block_;
    };

    TEST_F(TestLogicBlock, extend_block_)
    {
      const int32_t MAIN_PHYSICAL_BLOCK_ID = 1;
      PhysicalBlockManager& manager = block_manager_.get_physical_block_manager();
      BasePhysicalBlock* physical_block = manager.get(MAIN_PHYSICAL_BLOCK_ID);
      EXPECT_TRUE(NULL != physical_block);
      EXPECT_EQ(TFS_SUCCESS, logic_block_.add_physical_block(dynamic_cast<PhysicalBlock*>(physical_block)));
      int32_t avail_offset = 0;
      EXPECT_EQ(TFS_SUCCESS, logic_block_.get_avail_offset(avail_offset));
      EXPECT_EQ(MAX_BLOCK_SIZE - BLOCK_RESERVER_LENGTH, avail_offset);

      int32_t request_size = 1 * 1024 * 1024;
      int32_t request_offset = 1 * 1024 * 1024;
      EXPECT_EQ(TFS_SUCCESS, logic_block_.extend_block_(request_size, request_offset, false));
      avail_offset = 0;
      EXPECT_EQ(TFS_SUCCESS, logic_block_.get_avail_offset(avail_offset));
      EXPECT_EQ(MAX_BLOCK_SIZE - BLOCK_RESERVER_LENGTH, avail_offset);
      std::vector<int32_t> blocks;
      EXPECT_EQ(TFS_SUCCESS, logic_block_.get_all_physical_blocks(blocks));
      EXPECT_EQ(1U, blocks.size());

      request_size = 1 * 1024 * 1024;
      request_offset = MAX_BLOCK_SIZE - BLOCK_RESERVER_LENGTH + 128;
      EXPECT_EQ(TFS_SUCCESS, logic_block_.extend_block_(request_size, request_offset, false));
      avail_offset = 0;
      EXPECT_EQ(TFS_SUCCESS, logic_block_.get_avail_offset(avail_offset));
      EXPECT_EQ(MAX_BLOCK_SIZE - BLOCK_RESERVER_LENGTH + MAX_EXT_BLOCK_SIZE, avail_offset);
      blocks.clear();
      EXPECT_EQ(TFS_SUCCESS, logic_block_.get_all_physical_blocks(blocks));
      EXPECT_EQ(2U, blocks.size());
    }

    TEST_F(TestLogicBlock, write_)
    {
      const int32_t MAIN_PHYSICAL_BLOCK_ID = 1;
      PhysicalBlockManager& manager = block_manager_.get_physical_block_manager();
      BasePhysicalBlock* physical_block = manager.get(MAIN_PHYSICAL_BLOCK_ID);
      EXPECT_TRUE(NULL != physical_block);
      EXPECT_EQ(TFS_SUCCESS, logic_block_.add_physical_block(dynamic_cast<PhysicalBlock*>(physical_block)));
      int32_t avail_offset = 0;
      EXPECT_EQ(TFS_SUCCESS, logic_block_.get_avail_offset(avail_offset));
      EXPECT_EQ(MAX_BLOCK_SIZE - BLOCK_RESERVER_LENGTH, avail_offset);

      const int32_t MAX_DATA_SIZE = 1 * 1024 * 1024;
      int32_t request_size = MAX_DATA_SIZE;
      int32_t request_offset = MAX_BLOCK_SIZE - BLOCK_RESERVER_LENGTH + 128;
      EXPECT_EQ(TFS_SUCCESS, logic_block_.extend_block_(request_size, request_offset, false));
      avail_offset = 0;
      EXPECT_EQ(TFS_SUCCESS, logic_block_.get_avail_offset(avail_offset));
      EXPECT_EQ(MAX_BLOCK_SIZE - BLOCK_RESERVER_LENGTH + MAX_EXT_BLOCK_SIZE, avail_offset);
      std::vector<int32_t> blocks;
      EXPECT_EQ(TFS_SUCCESS, logic_block_.get_all_physical_blocks(blocks));
      EXPECT_EQ(2U, blocks.size());

      char data[MAX_DATA_SIZE];
      for (int32_t i = 0; i < MAX_DATA_SIZE; ++i)
        data[i] = random() % 256;
      DataFile dfile(0xfff, "./tmp");
      FileInfoInDiskExt dinfo;
      memset(&dinfo, 0, sizeof(dinfo));
      int32_t ret = dfile.pwrite(dinfo, data, MAX_DATA_SIZE, 0);
      EXPECT_EQ(MAX_DATA_SIZE, ret);

      FileInfoV2 new_finfo, old_finfo;

      ret = logic_block_.write_(new_finfo, dfile, old_finfo, false);
      EXPECT_EQ(TFS_SUCCESS, ret);
    }

    TEST_F(TestLogicBlock, write)
    {
      const int32_t MAIN_PHYSICAL_BLOCK_ID = 1;
      PhysicalBlockManager& manager = block_manager_.get_physical_block_manager();
      BasePhysicalBlock* physical_block = manager.get(MAIN_PHYSICAL_BLOCK_ID);
      EXPECT_TRUE(NULL != physical_block);
      EXPECT_EQ(TFS_SUCCESS, logic_block_.add_physical_block(dynamic_cast<PhysicalBlock*>(physical_block)));
      int32_t avail_offset = 0;
      EXPECT_EQ(TFS_SUCCESS, logic_block_.get_avail_offset(avail_offset));
      EXPECT_EQ(MAX_BLOCK_SIZE - BLOCK_RESERVER_LENGTH, avail_offset);

      int32_t MAX_DATA_SIZE = 1 * 1024 * 1024;
      int32_t request_size = MAX_DATA_SIZE;
      int32_t request_offset = MAX_BLOCK_SIZE - BLOCK_RESERVER_LENGTH + 128;
      EXPECT_EQ(TFS_SUCCESS, logic_block_.extend_block_(request_size, request_offset, false));
      avail_offset = 0;
      EXPECT_EQ(TFS_SUCCESS, logic_block_.get_avail_offset(avail_offset));
      EXPECT_EQ(MAX_BLOCK_SIZE - BLOCK_RESERVER_LENGTH + MAX_EXT_BLOCK_SIZE, avail_offset);
      std::vector<int32_t> blocks;
      EXPECT_EQ(TFS_SUCCESS, logic_block_.get_all_physical_blocks(blocks));
      EXPECT_EQ(2U, blocks.size());

      char data[MAX_DATA_SIZE + sizeof(FileInfoInDiskExt)];
      for (int32_t i = 0; i < MAX_DATA_SIZE; ++i)
        data[i] = random() % 256;
      DataFile dfile(0xfff, "./tmp");
      FileInfoInDiskExt dinfo;
      memset(&dinfo, 0, sizeof(dinfo));
      int32_t ret = dfile.pwrite(dinfo, data, MAX_DATA_SIZE, 0);
      EXPECT_EQ(MAX_DATA_SIZE, ret);

      uint64_t fileid = INVALID_FILE_ID;
      ret = logic_block_.write(fileid, dfile, LOGIC_BLOCK_ID, false);
      EXPECT_EQ(dfile.length(), ret);

      MAX_DATA_SIZE = MAX_DATA_SIZE + sizeof(FileInfoInDiskExt);

      EXPECT_TRUE(fileid > 0);
      memset(data, 0, MAX_DATA_SIZE );
      ret = logic_block_.read(data, MAX_DATA_SIZE, 0, fileid, READ_DATA_OPTION_FLAG_FORCE, LOGIC_BLOCK_ID);
      EXPECT_EQ(MAX_DATA_SIZE, ret);
      uint32_t crc = 0;
      crc = Func::crc(0, data, MAX_DATA_SIZE);
      EXPECT_EQ(crc, dfile.crc());
    }

    TEST_F(TestLogicBlock, unlink)
    {
      const int32_t MAIN_PHYSICAL_BLOCK_ID = 1;
      PhysicalBlockManager& manager = block_manager_.get_physical_block_manager();
      BasePhysicalBlock* physical_block = manager.get(MAIN_PHYSICAL_BLOCK_ID);
      EXPECT_TRUE(NULL != physical_block);
      EXPECT_EQ(TFS_SUCCESS, logic_block_.add_physical_block(dynamic_cast<PhysicalBlock*>(physical_block)));
      int32_t avail_offset = 0;
      EXPECT_EQ(TFS_SUCCESS, logic_block_.get_avail_offset(avail_offset));
      EXPECT_EQ(MAX_BLOCK_SIZE - BLOCK_RESERVER_LENGTH, avail_offset);

      int32_t MAX_DATA_SIZE = 1024;
      char data[MAX_DATA_SIZE + sizeof(FileInfoInDiskExt)];
      for (int32_t i = 0; i < MAX_DATA_SIZE; ++i)
        data[i] = random() % 256;
      DataFile dfile(0xfff, "./tmp");
      FileInfoInDiskExt dinfo;
      memset(&dinfo, 0, sizeof(dinfo));
      int32_t ret = dfile.pwrite(dinfo, data, MAX_DATA_SIZE, 0);
      EXPECT_EQ(MAX_DATA_SIZE, ret);
      FileInfoV2 finfo;
      memset(&finfo, 0, sizeof(finfo));
      finfo.id_ = 0xfff;
      EXPECT_EQ(EXIT_META_NOT_FOUND_ERROR, logic_block_.stat(finfo,FORCE_STAT, LOGIC_BLOCK_ID));

      uint64_t fileid = INVALID_FILE_ID;
      ret = logic_block_.write(fileid, dfile, LOGIC_BLOCK_ID, false);
      EXPECT_EQ(dfile.length(), ret);

      MAX_DATA_SIZE = MAX_DATA_SIZE + sizeof(FileInfoInDiskExt);

      memset(&finfo, 0, sizeof(finfo));
      finfo.id_ = fileid;
      EXPECT_EQ(TFS_SUCCESS, logic_block_.stat(finfo, FORCE_STAT, LOGIC_BLOCK_ID));

      EXPECT_TRUE(fileid > 0);
      memset(data, 0, MAX_DATA_SIZE );
      ret = logic_block_.read(data, MAX_DATA_SIZE, 0, fileid, READ_DATA_OPTION_FLAG_FORCE, LOGIC_BLOCK_ID);
      EXPECT_EQ(MAX_DATA_SIZE, ret);
      uint32_t crc = 0;
      crc = Func::crc(0, data, MAX_DATA_SIZE);
      EXPECT_EQ(crc, dfile.crc());

      int64_t size = 0;
      int32_t action = DELETE;
      EXPECT_EQ(TFS_SUCCESS, logic_block_.unlink(size, fileid, action, LOGIC_BLOCK_ID));
      EXPECT_EQ(size, (int64_t)(MAX_DATA_SIZE - sizeof(FileInfoInDiskExt)));
      EXPECT_EQ(TFS_SUCCESS, logic_block_.stat(finfo, NORMAL_STAT, LOGIC_BLOCK_ID));
      EXPECT_EQ(FILE_STATUS_DELETE, finfo.status_);
    }

    TEST_F(TestLogicBlock, scan_file)
    {
      const int32_t MAIN_PHYSICAL_BLOCK_ID = 1;
      PhysicalBlockManager& manager = block_manager_.get_physical_block_manager();
      BasePhysicalBlock* physical_block = manager.get(MAIN_PHYSICAL_BLOCK_ID);
      EXPECT_TRUE(NULL != physical_block);
      EXPECT_EQ(TFS_SUCCESS, logic_block_.add_physical_block(dynamic_cast<PhysicalBlock*>(physical_block)));
      int32_t avail_offset = 0;
      EXPECT_EQ(TFS_SUCCESS, logic_block_.get_avail_offset(avail_offset));
      EXPECT_EQ(MAX_BLOCK_SIZE - BLOCK_RESERVER_LENGTH, avail_offset);

      std::map<uint64_t, FileInfoV2> files;
      const int32_t MAX_FILE_COUNT =  random() % 10240 + 10240;
      for (int32_t j = 0; j < MAX_FILE_COUNT; j++)
      {
        int32_t MAX_DATA_SIZE = random() % 512 + 4096;
        char* data = new char[MAX_DATA_SIZE + sizeof(FileInfoInDiskExt)];
        for (int32_t i = 0; i < MAX_DATA_SIZE; ++i)
          data[i] = random() % 10+ 100;
        DataFile dfile(0xfff, "./tmp");
        FileInfoInDiskExt dinfo;
        memset(&dinfo, 0, sizeof(dinfo));
        dfile.pwrite(dinfo, data, MAX_DATA_SIZE, 0);

        uint64_t fileid = INVALID_FILE_ID;
        logic_block_.write(fileid, dfile, LOGIC_BLOCK_ID, false);

        MAX_DATA_SIZE = MAX_DATA_SIZE + sizeof(FileInfoInDiskExt);

        FileInfoV2 finfo;
        finfo.id_ = fileid;
        logic_block_.stat(finfo, NORMAL_STAT, LOGIC_BLOCK_ID);
        files[finfo.id_] = finfo;

        tbsys::gDeleteA(data);
      }


      LogicBlock::Iterator iter(&logic_block_);


      int64_t start = tbsys::CTimeUtil::getTime();
      int32_t mem_offset = 0;
      const char* data = NULL;
      FileInfoV2* finfov2  = NULL;
      while (TFS_SUCCESS == iter.next(finfov2))
      {
        data = iter.get_data(mem_offset,finfov2->size_);
        if (NULL != data)
        {
          uint32_t crc = Func::crc(0, data, finfov2->size_);
          EXPECT_EQ(crc, finfov2->crc_);
          FileInfoV2 old_info = files[finfov2->id_];
          EXPECT_EQ(old_info.crc_, finfov2->crc_);
          EXPECT_EQ(old_info.size_, finfov2->size_);
          EXPECT_EQ(old_info.offset_, finfov2->offset_);
          EXPECT_EQ(old_info.modify_time_, finfov2->modify_time_);
          EXPECT_EQ(old_info.create_time_, finfov2->create_time_);
        }
      }
      int64_t end = tbsys::CTimeUtil::getTime();
      int32_t used_offset = 0;
      EXPECT_EQ(TFS_SUCCESS, logic_block_.get_used_offset(used_offset));
      TBSYS_LOG(INFO, "SCAN FILE CONSUME: %ld us, BLOCK LENGTH: %d", end - start, used_offset);
    }

    TEST_F(TestLogicBlock, choose_physic_block)
    {
      PhysicalBlockManager& manager = block_manager_.get_physical_block_manager();
      for (int32_t i = 1; i <= 5 ; i++)
      {
        BasePhysicalBlock* physical_block = manager.get(i);
        EXPECT_TRUE(NULL != physical_block);
        EXPECT_EQ(TFS_SUCCESS, logic_block_.add_physical_block(dynamic_cast<PhysicalBlock*>(physical_block)));
      }

      int32_t length = 10, offset = 0, inner_offset = 0;
      PhysicalBlock* physical_block = NULL;
      EXPECT_EQ(TFS_SUCCESS, logic_block_.choose_physic_block(physical_block, length, inner_offset, offset));
      EXPECT_EQ((MAX_BLOCK_SIZE - BLOCK_RESERVER_LENGTH), length);
      EXPECT_EQ(0, offset);
      EXPECT_EQ(0, inner_offset);

      length = 200 * 1024;
      offset = MAX_BLOCK_SIZE - BLOCK_RESERVER_LENGTH  - 1024;
      inner_offset = 0;
      EXPECT_EQ(TFS_SUCCESS, logic_block_.choose_physic_block(physical_block, length, inner_offset, offset));
      EXPECT_EQ(1024, length);
      EXPECT_EQ(MAX_BLOCK_SIZE - BLOCK_RESERVER_LENGTH  - 1024, inner_offset);

      PhysicalBlock* physical_blockv2 = NULL;
      length = 200 * 1024;
      offset = MAX_BLOCK_SIZE - BLOCK_RESERVER_LENGTH + 1024;
      inner_offset = 0;
      EXPECT_EQ(TFS_SUCCESS, logic_block_.choose_physic_block(physical_blockv2, length, inner_offset, offset));
      EXPECT_EQ(MAX_BLOCK_SIZE - BLOCK_RESERVER_LENGTH  - 1024, length);
      EXPECT_EQ(1024, inner_offset);
      EXPECT_TRUE(physical_block != physical_blockv2);
      EXPECT_TRUE(physical_block->id() != physical_blockv2->id());

      std::vector<int32_t> blocks;
      EXPECT_EQ(TFS_SUCCESS, logic_block_.get_all_physical_blocks(blocks));
      std::vector<int32_t>::const_iterator iter = blocks.begin();
      for (int32_t index = 1; iter != blocks.end(); ++iter,++index)
      {
        EXPECT_EQ(index, (*iter));
      }
    }

    TEST_F(TestLogicBlock, verify_unlink)
    {
      /*const int32_t MAIN_PHYSICAL_BLOCK_ID =2 ;
      PhysicalBlockManager& manager = block_manager_.get_physical_block_manager();
      BasePhysicalBlock* physical_block = manager.get(MAIN_PHYSICAL_BLOCK_ID);
      EXPECT_TRUE(NULL != physical_block);
      EXPECT_EQ(TFS_SUCCESS, verify_logic_block_.add_physical_block(dynamic_cast<PhysicalBlock*>(physical_block)));
      int32_t avail_offset = 0;
      EXPECT_EQ(TFS_SUCCESS, verify_logic_block_.get_avail_offset(avail_offset));
      EXPECT_EQ(MAX_BLOCK_SIZE - BLOCK_RESERVER_LENGTH, avail_offset);

      int32_t MAX_DATA_SIZE = 1024;
      char data[MAX_DATA_SIZE + sizeof(FileInfoInDiskExt)];
      for (int32_t i = 0; i < MAX_DATA_SIZE; ++i)
        data[i] = random() % 256;
      DataFile dfile(0xfff, "./tmp");
      FileInfoInDiskExt dinfo;
      memset(&dinfo, 0, sizeof(dinfo));
      int32_t ret = dfile.pwrite(dinfo, data, MAX_DATA_SIZE, 0);
      EXPECT_EQ(MAX_DATA_SIZE, ret);
      FileInfoV2 finfo;
      memset(&finfo, 0, sizeof(finfo));
      finfo.id_ = 0xfff;
      EXPECT_EQ(EXIT_META_NOT_FOUND_ERROR, verify_logic_block_.stat(finfo, LOGIC_BLOCK_ID));

      uint64_t fileid = INVALID_FILE_ID;
      ret = verify_logic_block_.write(fileid, dfile, LOGIC_BLOCK_ID);
      EXPECT_EQ(dfile.length(), ret);

      MAX_DATA_SIZE = MAX_DATA_SIZE + sizeof(FileInfoInDiskExt);

      memset(&finfo, 0, sizeof(finfo));
      finfo.id_ = fileid;
      EXPECT_EQ(TFS_SUCCESS, verify_logic_block_.stat(finfo, LOGIC_BLOCK_ID));

      EXPECT_TRUE(fileid > 0);
      memset(data, 0, MAX_DATA_SIZE );
      ret = verify_logic_block_.read(data, MAX_DATA_SIZE, 0, fileid, READ_DATA_OPTION_FLAG_FORCE, LOGIC_BLOCK_ID);
      EXPECT_EQ(MAX_DATA_SIZE, ret);
      uint32_t crc = 0;
      crc = Func::crc(0, data, MAX_DATA_SIZE);
      EXPECT_EQ(crc, dfile.crc());

      int64_t size = 0;
      int32_t action = DELETE;
      EXPECT_EQ(TFS_SUCCESS, verify_logic_block_.unlink(size, fileid, action, LOGIC_BLOCK_ID));
      EXPECT_EQ(size, (int64_t)(MAX_DATA_SIZE - sizeof(FileInfoInDiskExt)));
      EXPECT_EQ(TFS_SUCCESS, verify_logic_block_.stat(finfo, LOGIC_BLOCK_ID));
      EXPECT_EQ(FILE_STATUS_DELETE, finfo.status_);*/
    }
  }
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
