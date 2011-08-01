/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: block_id_factory.cpp 2014 2011-01-06 07:41:45Z duanfei $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */
#include "common/error_msg.h"
#include "common/directory_op.h"
#include "block_id_factory.h"

namespace tfs
{
  namespace nameserver
  {
    const uint16_t BlockIdFactory::BLOCK_START_NUMBER = 100;
    const uint16_t BlockIdFactory::SKIP_BLOCK_NUMBER  = 100;
    const uint32_t BlockIdFactory::INVALID_BLOCK_ID = 0;
    BlockIdFactory::BlockIdFactory():
      global_id_(BLOCK_START_NUMBER),
      count_(0),
      fd_(-1)
    {

    }

    BlockIdFactory::~BlockIdFactory()
    {

    }

    int BlockIdFactory::initialize(const std::string& path)
    {
      int32_t iret = path.empty() ? common::EXIT_GENERAL_ERROR : common::TFS_SUCCESS;
      if (common::TFS_SUCCESS == iret)
      {
        if (!common::DirectoryOp::create_full_path(path.c_str()))
        {
          TBSYS_LOG(ERROR, "create directory: %s fail...", path.c_str());
          iret = common::EXIT_GENERAL_ERROR;
        }
        if (common::TFS_SUCCESS == iret)
        {
          std::string fs_path = path + "/ns.meta";
          fd_ = ::open(fs_path.c_str(), O_RDWR | O_CREAT, 0600);
          if (fd_ < 0)
          {
            TBSYS_LOG(ERROR, "open file %s failed, errors: %s", fs_path.c_str(), strerror(errno));
            iret = common::EXIT_GENERAL_ERROR;
          }
        }
        if (common::TFS_SUCCESS == iret)
        {
          char data[common::INT_SIZE];
          int32_t length = ::read(fd_, data, common::INT_SIZE);
          if (length == common::INT_SIZE)//read successful
          {
            int64_t pos = 0;
            iret = common::Serialization::get_int32(data, common::INT_SIZE, pos, reinterpret_cast<int32_t*>(&global_id_));
            if (common::TFS_SUCCESS != iret)
            {
              TBSYS_LOG(ERROR, "serialize global block id error, iret: %d", iret);
            }
            else
            {
              if (global_id_ < BLOCK_START_NUMBER)
                global_id_ = BLOCK_START_NUMBER;
            }
            if (common::TFS_SUCCESS == iret)
            {
              global_id_ += SKIP_BLOCK_NUMBER;
            }
          }
        }
      }
      return iret;
    }

    int BlockIdFactory::destroy()
    {
      int32_t iret = common::TFS_SUCCESS; 
      if (fd_ > 0)
      {
        iret = update(global_id_);
        ::close(fd_);
      }
      return iret;
    }

    uint32_t BlockIdFactory::generation(const uint32_t id)
    {
      bool update_flag = false;
      uint32_t ret_id = INVALID_BLOCK_ID;
      {
        tbutil::Mutex::Lock lock(mutex_);
        ++count_;
        if (id == 0)
          ret_id = ++global_id_;
        else
          ret_id = id;
        if (count_ >= SKIP_BLOCK_NUMBER)
        {
          update_flag = true;
          count_ = 0;
        }
      }
      if (update_flag)
      {
        int32_t iret = update(ret_id);
        if (common::TFS_SUCCESS != iret)
        {
          TBSYS_LOG(ERROR, "update global block id failed, id: %u, iret: %d", ret_id, iret);
          ret_id = INVALID_BLOCK_ID;
        }
      }
      return ret_id;
    }

    int BlockIdFactory::update(const uint32_t id) const
    {
      assert(fd_ != -1);
      char data[common::INT_SIZE];
      int64_t pos = 0;
      int32_t iret = common::Serialization::set_int32(data, common::INT_SIZE, pos, id);
      if (common::TFS_SUCCESS == iret)
      {
        int32_t offset = 0;
        int32_t length = 0;
        int32_t count  = 0;
        do
        {
          ++count;
          length = ::write(fd_, (data + offset), (common::INT_SIZE - offset));
          if (length > 0)
          {
            offset += length;
          }
        }
        while (count < 3 && offset < common::INT_SIZE);
        iret = common::INT_SIZE == offset ? common::TFS_SUCCESS : common::TFS_ERROR;
      }
      return iret;
    }
  }/** nameserver **/
}/** tfs **/
