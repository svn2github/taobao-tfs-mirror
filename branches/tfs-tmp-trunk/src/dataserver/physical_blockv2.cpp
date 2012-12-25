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
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */

#include "common/func.h"
#include "common/error_msg.h"
#include "physical_blockv2.h"

using namespace tfs::common;

namespace tfs
{
  namespace dataserver
  {
    const int32_t BasePhysicalBlock::MAX_WARN_CONSUME_TIME_US = 1000000000;
    const int8_t AllocPhysicalBlock::STORE_ALLOC_BIT_MAP_SIZE = sizeof(uint64_t);
    const int8_t AllocPhysicalBlock::ALLOC_BIT_MAP_SIZE   = 32;//只使用32位
    const uint64_t BIT_MAP_MASK[AllocPhysicalBlock::ALLOC_BIT_MAP_SIZE] =
    {
      0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80,
      0x100, 0x200, 0x400, 0x800, 0x1000, 0x2000, 0x4000, 0x8000,
      0x10000, 0x20000, 0x40000, 0x80000, 0x100000, 0x200000, 0x400000, 0x800000,
      0x1000000, 0x2000000, 0x4000000, 0x8000000, 0x10000000, 0x20000000, 0x40000000, 0x80000000
    };

    BasePhysicalBlock::BasePhysicalBlock(const int32_t physical_block_id, const int32_t start, const int32_t end):
      physical_block_id_(physical_block_id),
      start_(start),
      end_(end)
    {

    }

    BasePhysicalBlock::~BasePhysicalBlock()
    {

    }

    BasePhysicalBlock::BasePhysicalBlock(const int32_t physical_block_id):
      physical_block_id_(physical_block_id),
      start_(0),
      end_(0)
    {

    }

    AllocPhysicalBlock::AllocPhysicalBlock(const int32_t physical_block_id, const std::string& path,
        const int32_t start, const int32_t end):
      BasePhysicalBlock(physical_block_id, start, end),
      file_op_(path)
    {

    }

    AllocPhysicalBlock::~AllocPhysicalBlock()
    {

    }

    int AllocPhysicalBlock::alloc(int8_t& index, int32_t& start, int32_t& end, const int32_t max_ext_block_size)
    {
      index = -1, start = 0, end = 0;
      int32_t ret =  CHECK_EXT_BLOCK_SIZE(max_ext_block_size) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        int8_t retry_times = 0;
        do
        {
          ret = (!(alloc_bit_map_ & BIT_MAP_MASK[retry_times])) ? TFS_SUCCESS : EXIT_BIT_MAP_OUT_OF_RANGE;
          if (TFS_SUCCESS == ret)
          {
            index = retry_times + 1;
            start = start_ + retry_times * max_ext_block_size;
            end   = start  + max_ext_block_size;
            alloc_bit_map_ |= (1 << retry_times);
            ret = file_op_.pwrite(reinterpret_cast<char*>(&alloc_bit_map_), STORE_ALLOC_BIT_MAP_SIZE, 0);
            ret = ret >= STORE_ALLOC_BIT_MAP_SIZE ? TFS_SUCCESS : EXIT_WRITE_ALLOC_BIT_MAP_ERROR;
            if (TFS_SUCCESS == ret)
              ret = file_op_.fsync();
          }
        }
        while (TFS_SUCCESS != ret && retry_times++ < ALLOC_BIT_MAP_SIZE);
      }
      return ret;
    }

    int AllocPhysicalBlock::free(const int8_t index)
    {
      int32_t ret = (index > 0 && index <= ALLOC_BIT_MAP_SIZE) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        if (alloc_bit_map_ & BIT_MAP_MASK[index])
        {
          alloc_bit_map_ &= ~(1 << (index - 1));
          ret = file_op_.pwrite(reinterpret_cast<char*>(&alloc_bit_map_), STORE_ALLOC_BIT_MAP_SIZE, 0);
          ret = ret >= STORE_ALLOC_BIT_MAP_SIZE ? TFS_SUCCESS : EXIT_WRITE_ALLOC_BIT_MAP_ERROR;
          if (TFS_SUCCESS == ret)
          {
            ret = file_op_.fsync();
          }
        }
      }
      return ret;
    }

    bool AllocPhysicalBlock::empty() const
    {
      bool ret = true;
      for (int8_t index = 0; index < ALLOC_BIT_MAP_SIZE && ret; ++index)
      {
        ret = (0 == (alloc_bit_map_ & BIT_MAP_MASK[index]));
      }
      return ret;
    }

    bool AllocPhysicalBlock::full() const
    {
      int8_t count = 0;
      for (int8_t index = 0; index < ALLOC_BIT_MAP_SIZE; ++index)
      {
        if (alloc_bit_map_ & BIT_MAP_MASK[index])
          ++count;
      }
      return count == ALLOC_BIT_MAP_SIZE;
    }

    int AllocPhysicalBlock::load_alloc_bit_map_()
    {
      int32_t ret = file_op_.pread(reinterpret_cast<char*>(&alloc_bit_map_), STORE_ALLOC_BIT_MAP_SIZE, 0);
      if (ret >= 0)
      {
        ret = (ret >= STORE_ALLOC_BIT_MAP_SIZE) ? TFS_SUCCESS : EXIT_READ_ALLOC_BIT_MAP_ERROR;
      }
      return ret;
    }

    PhysicalBlock::PhysicalBlock(const int32_t physical_block_id, const std::string& path,
        const int32_t start, const int32_t end):
      BasePhysicalBlock(physical_block_id, start, end),
      file_op_(path)
    {

    }

    PhysicalBlock::~PhysicalBlock()
    {

    }

   //返回值: < 0: 错误码， >= 0: 写入数据长度
    int PhysicalBlock::pwrite(const char* buf, const int32_t nbytes, const int32_t offset)
    {
      int32_t ret = (NULL != buf && nbytes > 0 && offset >= 0) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = (offset + nbytes > end_) ? EXIT_PHYSIC_BLOCK_OFFSET_ERROR : TFS_SUCCESS;
      }
      if (TFS_SUCCESS == ret)
      {
        int64_t begin = Func::get_monotonic_time_us();
        ret = file_op_.pwrite(buf, nbytes, start_ + offset);
        int64_t consume = Func::get_monotonic_time_us() - begin;
        if (consume >= MAX_WARN_CONSUME_TIME_US)
        {
          TBSYS_LOG(WARN, "write data warning, physical block id : %d, offfset: %d, length: %d, consume: %"PRI64_PREFIX"d",
            physical_block_id_, offset, nbytes, consume);
        }
      }
      return ret;
    }

    //返回值: < 0: 错误码， >= 0: 读出数据长度
    int PhysicalBlock::pread(char* buf, const int32_t nbytes, const int32_t offset)
    {
      int32_t ret = (NULL != buf && nbytes > 0 && offset >= 0) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = (offset + nbytes > end_) ? EXIT_PHYSIC_BLOCK_OFFSET_ERROR : TFS_SUCCESS;
      }
      if (TFS_SUCCESS == ret)
      {
        int64_t begin = Func::get_monotonic_time_us();
        ret = file_op_.pread(buf, nbytes, start_ + offset);
        int64_t consume = Func::get_monotonic_time_us() - begin;
        if (consume >= MAX_WARN_CONSUME_TIME_US)
        {
          TBSYS_LOG(WARN, "read data warning, physical block id : %d, offfset: %d, length: %d, consume: %"PRI64_PREFIX"d",
            physical_block_id_, offset, nbytes, consume);
        }
      }
      return ret;
    }
  }/** end namespace dataserver**/
}/** end namespace tfs **/