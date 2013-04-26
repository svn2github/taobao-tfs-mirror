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
    const uint64_t BlockIdFactory::MAX_BLOCK_ID = 0xFFFFFFFFFFFF -1;
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
      int32_t ret = path.empty() ? common::EXIT_GENERAL_ERROR : common::TFS_SUCCESS;
      if (common::TFS_SUCCESS == ret)
      {
        if (!common::DirectoryOp::create_full_path(path.c_str()))
        {
          TBSYS_LOG(ERROR, "create directory: %s errors : %s", path.c_str(), strerror(errno));
          ret = common::EXIT_GENERAL_ERROR;
        }
        if (common::TFS_SUCCESS == ret)
        {
          std::string fs_path = path + "/ns.meta";
          fd_ = ::open(fs_path.c_str(), O_RDWR | O_CREAT, 0600);
          if (fd_ < 0)
          {
            TBSYS_LOG(ERROR, "open file %s failed, errors: %s", fs_path.c_str(), strerror(errno));
            ret = common::EXIT_GENERAL_ERROR;
          }
        }
        if (common::TFS_SUCCESS == ret)
        {
          char data[common::INT64_SIZE];
          int32_t length = ::read(fd_, data, common::INT64_SIZE);
          if (length == common::INT64_SIZE)//read successful
          {
            int64_t pos = 0;
            ret = common::Serialization::get_int64(data, common::INT64_SIZE, pos, reinterpret_cast<int64_t*>(&global_id_));
            if (common::TFS_SUCCESS != ret)
            {
              TBSYS_LOG(ERROR, "serialize global block id error, ret: %d", ret);
            }
            else
            {
              if (global_id_ < BLOCK_START_NUMBER)
                global_id_ = BLOCK_START_NUMBER;
            }
            if (common::TFS_SUCCESS == ret)
            {
              global_id_ += SKIP_BLOCK_NUMBER;
            }
          }
        }
      }
      return ret;
    }

    int BlockIdFactory::destroy()
    {
      int32_t ret = common::TFS_SUCCESS;
      if (fd_ > 0)
      {
        ret = flush_(global_id_);
        ::close(fd_);
      }
      return ret;
    }

    uint64_t BlockIdFactory::generation(const bool verify)
    {
      mutex_.lock();
      ++count_;
      uint64_t id = ++global_id_;
      assert(id <= MAX_BLOCK_ID);
      bool flush_flag = false;
      if (count_ >= SKIP_BLOCK_NUMBER)
      {
        flush_flag = true;
        count_ = 0;
      }
      mutex_.unlock();
      int32_t ret = common::TFS_SUCCESS;
      if (flush_flag)
      {
        ret = flush_(id);
        if (common::TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "update global block id failed, id: %"PRI64_PREFIX"u, ret: %d", id, ret);
        }
      }
      if (common::TFS_SUCCESS == ret)
      {
        if (verify)
          id |= 0xFF00000000000000;
      }
      return id;
    }

    int BlockIdFactory::update(const uint64_t id)
    {
      bool flush_flag = false;
      uint64_t tmp_id = IS_VERFIFY_BLOCK(id) ? id & 0x7FFFFFFFFFFFFFFF : id;
      int32_t ret = (common::INVALID_BLOCK_ID == id) ? common::EXIT_PARAMETER_ERROR : common::TFS_SUCCESS;
      if (common::TFS_SUCCESS == ret)
      {
        tbutil::Mutex::Lock lock(mutex_);
        ++count_;
        global_id_ = std::max(global_id_, tmp_id);
        if (count_ >= SKIP_BLOCK_NUMBER)
        {
          flush_flag = true;
          count_ = 0;
        }
      }
      if (common::TFS_SUCCESS == ret && flush_flag)
      {
        ret = flush_(tmp_id);
        if (common::TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "flush global block id failed, id: %"PRI64_PREFIX"u, ret: %d", tmp_id, ret);
        }
      }
      return ret;
    }

    uint64_t BlockIdFactory::skip(const int32_t num)
    {
      mutex_.lock();
      global_id_ += num;
      uint64_t id = global_id_;
      mutex_.unlock();
      int32_t ret = update(id);
      if (common::TFS_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "update global block id failed, id: %"PRI64_PREFIX"u, ret: %d", id, ret);
      }
      return id;
    }

    int BlockIdFactory::flush_(const uint64_t id) const
    {
      assert(fd_ != -1);
      char data[common::INT64_SIZE];
      int64_t pos = 0;
      int32_t ret = common::Serialization::set_int64(data, common::INT64_SIZE, pos, id);
      if (common::TFS_SUCCESS == ret)
      {
        int32_t offset = 0;
        int32_t length = 0;
        int32_t count  = 0;
        ::lseek(fd_, 0, SEEK_SET);
        do
        {
          ++count;
          length = ::write(fd_, (data + offset), (common::INT64_SIZE - offset));
          if (length > 0)
          {
            offset += length;
          }
        }
        while (count < 3 && offset < common::INT64_SIZE);
        ret = common::INT64_SIZE == offset ? common::TFS_SUCCESS : common::TFS_ERROR;
        if (common::TFS_SUCCESS == ret)
          fsync(fd_);
      }
      return ret;
    }
  }/** nameserver **/
}/** tfs **/
