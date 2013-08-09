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
#include "common/error_msg.h"

#include "logic_blockv2.h"
#include "data_handlev2.h"
#include "physical_blockv2.h"

using namespace tfs::common;

namespace tfs
{
  namespace dataserver
  {
    DataHandle::DataHandle(BaseLogicBlock& logic_block):
      logic_block_(logic_block)
    {

    }

    DataHandle::~DataHandle()
    {

    }

    int DataHandle::pwrite(const char* buf, const int32_t nbytes, const int32_t offset)
    {
      int32_t ret = (NULL != buf && nbytes > 0 && offset >= 0) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        PhysicalBlock* physical_block = NULL, *last_physical_block = NULL;
        int32_t inner_offset = 0, length = nbytes, current_offset = offset;
        int32_t inner_length = 0, mem_offset = 0,  total_write_length = 0;
        while ((TFS_SUCCESS == ret) && (current_offset < (offset + nbytes)))
        {
          physical_block = NULL;
          inner_length = length;
          ret = logic_block_.choose_physic_block(physical_block, inner_length, inner_offset, current_offset);
          if (TFS_SUCCESS == ret)
          {
            length = std::min(length, inner_length);
            ret = physical_block->pwrite((buf + mem_offset), length, inner_offset);
            ret = ret >= 0 ? TFS_SUCCESS : ret;
            if (TFS_SUCCESS == ret)
            {
              if (NULL != last_physical_block && last_physical_block != physical_block)
              {
                ret = last_physical_block->fdatasync();
              }
              last_physical_block = physical_block;
            }
            if (TFS_SUCCESS == ret)
            {
              current_offset += length;
              mem_offset     += length;
              total_write_length += length;
              length         =  nbytes - total_write_length;
            }
          }
        }
        if (TFS_SUCCESS == ret)
        {
          assert(NULL != last_physical_block);
          ret = last_physical_block->fdatasync();
        }
      }
      return TFS_SUCCESS == ret ? nbytes : ret;
    }

    int DataHandle::pread(char* buf, const int32_t nbytes, const int32_t offset)
    {
      int32_t ret = (NULL != buf && nbytes > 0 && offset >= 0) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        PhysicalBlock* physical_block = NULL;
        int32_t inner_offset = 0, length = nbytes, current_offset = offset;
        int32_t inner_length = 0, mem_offset = 0,  total_read_length = 0;
        while ((TFS_SUCCESS == ret) && (current_offset < (offset + nbytes)))
        {
          inner_length  = length;
          ret = logic_block_.choose_physic_block(physical_block, inner_length, inner_offset, current_offset);
          if (TFS_SUCCESS == ret)
          {
            length = std::min(length, inner_length);
            ret = physical_block->pread((buf + mem_offset), length, inner_offset);
            ret = ret >= 0 ? TFS_SUCCESS : ret;
            if (TFS_SUCCESS == ret)
            {
              current_offset += length;
              mem_offset     += length;
              total_read_length += length;
              length         =  nbytes - total_read_length;
            }
          }
        }
      }
      return TFS_SUCCESS == ret ? nbytes : ret;
    }
  }/** end namespace dataserver **/
}/** end namespace tfs **/
