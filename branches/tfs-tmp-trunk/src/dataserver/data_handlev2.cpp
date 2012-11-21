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
      int32_t ret = (NULL != buf && nbytes >= 0 && offset >= 0) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret && nbytes > 0)
      {
        PhysicalBlock* physical_block = NULL;
        int32_t inner_offset = 0, length = nbytes, current_offset = offset, mem_offset = 0;
        while ((TFS_SUCCESS == ret) && (current_offset < (offset + nbytes)))
        {
          ret = logic_block_.choose_physic_block(physical_block, length, inner_offset, current_offset);
          if (TFS_SUCCESS == ret)
          {
            ret = physical_block->pwrite((buf + mem_offset), length, inner_offset);
            ret = ret >= 0 ? TFS_SUCCESS : -ret;
            if (TFS_SUCCESS == ret)
            {
              current_offset += length;
              mem_offset     += length;
              length         = (offset + nbytes) - current_offset;
            }
          }
        }
      }
      return TFS_SUCCESS == ret ? nbytes : ret;
    }

    int DataHandle::pread(char* buf, const int32_t nbytes, const int32_t offset)
    {
      int32_t ret = (NULL != buf && nbytes >= 0 && offset >= 0) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret && nbytes > 0)
      {
        PhysicalBlock* physical_block = NULL;
        int32_t inner_offset = 0, length = nbytes, current_offset = offset, mem_offset = 0;
        while ((TFS_SUCCESS == ret) && (current_offset < (offset + nbytes)))
        {
          ret = logic_block_.choose_physic_block(physical_block, length, inner_offset, current_offset);
          if (TFS_SUCCESS == ret)
          {
            ret = physical_block->pread((buf + mem_offset), length, inner_offset);
            ret = ret >= 0 ? TFS_SUCCESS : ret;
            if (TFS_SUCCESS == ret)
            {
              current_offset += length;
              mem_offset     += length;
              length         = (offset + nbytes) - current_offset;
            }
          }
        }
      }
      return TFS_SUCCESS == ret ? nbytes : ret;
    }
  }/** end namespace dataserver **/
}/** end namespace tfs **/
