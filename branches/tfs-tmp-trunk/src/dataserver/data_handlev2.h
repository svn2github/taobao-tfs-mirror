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

#ifndef TFS_DATASERVER_DATA_HANDLE_H_
#define TFS_DATASERVER_DATA_HANDLE_H_

#include "common/internal.h"

namespace tfs
{
  namespace dataserver
  {
    class LogicBlock;
    class DataHandle
    {
      public:
        explicit DataHandle(LogicBlock& logic_block);
        virtual ~DataHandle();
        int pwrite(const char* buf, const int32_t nbytes, const int32_t offset);
        int pread(char* buf, const int32_t nbytes, const int32_t offset);
      private:
        DISALLOW_COPY_AND_ASSIGN(DataHandle);
        LogicBlock& logic_block_;
    };
  }/** end namespace dataserver **/
}/** end namespace tfs **/
#endif /*TFS_DATASERVER_DATA_HANDLE_H_*/
