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
 *   qushan<qushan@taobao.com> 
 *      - modify 2009-03-27
 *   duanfei <duanfei@taobao.com> 
 *      - modify 2010-04-23
 *
 */
#include <tbsys.h>
#include <Memory.hpp>
#include "oplog.h"
#include "common/error_msg.h"
#include "common/directory_op.h"
#include "common/parameter.h"
#include "block_collect.h"
#include "server_collect.h"
#include "data_container.h"

using namespace tfs::common;

namespace tfs
{
  namespace nameserver
  {
    OpLog::OpLog(const std::string& logname, int maxLogSlotsSize) :
      MAX_LOG_SLOTS_SIZE(maxLogSlotsSize), MAX_LOG_BUFFER_SIZE(maxLogSlotsSize * MAX_LOG_SIZE), path_(logname), seqno_(
          0), last_flush_time_(0), slots_offset_(0), fd_(-1), buffer_(new char[maxLogSlotsSize * MAX_LOG_SIZE + 1])
    {

    }

    OpLog::~OpLog()
    {
      tbsys::gDeleteA( buffer_);
      if (fd_ > 0)
        ::close( fd_);
    }

    int OpLog::initialize()
    {
      if (path_.empty())
        return EXIT_GENERAL_ERROR;
      if (!DirectoryOp::create_full_path(path_.c_str()))
      {
        TBSYS_LOG(ERROR, "create directory(%s) fail...", path_.c_str());
        return EXIT_GENERAL_ERROR;
      }
      std::string headPath = path_ + "/rotateheader.dat";
      fd_ = open(headPath.c_str(), O_RDWR | O_CREAT, 0600);
      if (fd_ < 0)
      {
        TBSYS_LOG(ERROR, "open file(%s) fail(%s)", headPath.c_str(), strerror(errno));
        return EXIT_GENERAL_ERROR;
      }
      int iret = read(fd_, &oplog_rotate_header_, sizeof(oplog_rotate_header_));
      if (iret != sizeof(oplog_rotate_header_))
      {
        oplog_rotate_header_.rotate_seqno_ = 0x01;
        oplog_rotate_header_.rotate_offset_ = 0x00;
      }
      return TFS_SUCCESS;
    }

    int OpLog::update_oplog_rotate_header(const OpLogRotateHeader& head)
    {
      tbutil::Mutex::Lock lock(mutex_);
      std::string headPath = path_ + "/rotateheader.dat";
      memcpy(&oplog_rotate_header_, &head, sizeof(head));
      if (fd_ < 0)
      {
        fd_ = open(headPath.c_str(), O_RDWR | O_CREAT, 0600);
        if (fd_ < 0)
        {
          TBSYS_LOG(ERROR, "open file(%s) fail(%s)", headPath.c_str(), strerror(errno));
          return EXIT_GENERAL_ERROR;
        }
      }

      lseek(fd_, 0, SEEK_SET);
      int iret = ::write(fd_, &oplog_rotate_header_, sizeof(oplog_rotate_header_));
      if (iret != sizeof(oplog_rotate_header_))
      {
        TBSYS_LOG(ERROR, "wirte data fail: file(%s), erros(%s)...", headPath.c_str(), strerror(errno));
        ::close( fd_);
        fd_ = -1;
        fd_ = open(headPath.c_str(), O_RDWR | O_CREAT, 0600);
        if (fd_ < 0)
        {
          TBSYS_LOG(ERROR, "open file(%s) fail(%s)", headPath.c_str(), strerror(errno));
          return EXIT_GENERAL_ERROR;
        }
        lseek(fd_, 0, SEEK_SET);
        iret = ::write(fd_, &oplog_rotate_header_, sizeof(oplog_rotate_header_));
        if (iret != sizeof(oplog_rotate_header_))
        {
          TBSYS_LOG(ERROR, "wirte data fail: file(%s), erros(%s)...", headPath.c_str(), strerror(errno));
          return EXIT_GENERAL_ERROR;
        }
      }
      return TFS_SUCCESS;
    }

    bool OpLog::finish(time_t now, bool force/* = false*/) const
    {
      if (!force)
      {
        if ((slots_offset_ < MAX_LOG_BUFFER_SIZE) && (MAX_LOG_SLOTS_SIZE != 0))
        {
          return false;
        }
      }
      if (now - last_flush_time_ < (time_t)(SYSPARAM_NAMESERVER.heart_interval_ * 4))
        return false;
      return true;
    }

    int OpLog::write(int32_t cmd, const BlockInfo* const blk, const VUINT64& dsList)
    {
      //check
      const uint32_t ds_size = dsList.size();
      const int32_t ds_buffer_length = ds_size * INT64_SIZE;
      const int32_t size = sizeof(OpLogHeader) + BLOCKINFO_SIZE + 0x01 + ds_buffer_length;
      const int32_t dope_offset = slots_offset_ + size;
      if (dope_offset > MAX_LOG_BUFFER_SIZE)
      {
        //TBSYS_LOG(DEBUG, "(slots_offset_ + size)(%d) > MAX_LOG_BUFFER_SIZE(%d)", dope_offset, MAX_LOG_BUFFER_SIZE);
        return EXIT_SLOTS_OFFSET_SIZE_ERROR;
      }
      //TBSYS_LOG(DEBUG, "size(%d), dope_offset(%d), MAX_LOG_BUFFER_SIZE(%d)", size, dope_offset, MAX_LOG_BUFFER_SIZE);
      char* const buffer = buffer_ + slots_offset_;
      OpLogHeader *header = (OpLogHeader*) buffer;
      header->time_ = time(NULL);
      header->seqno_ = ++seqno_;
      header->length_ = BLOCKINFO_SIZE + 0x01 + ds_buffer_length;
      header->cmd_ = cmd;
      memcpy(header->data_, (char*) blk, BLOCKINFO_SIZE);

      int32_t offset = sizeof(OpLogHeader) + BLOCKINFO_SIZE;
      buffer[offset] = ds_size;
      offset += 0x01;

      for (size_t i = 0; i < ds_size; ++i)
      {
        memcpy((buffer + offset), &(dsList[i]), INT64_SIZE);
        offset += INT64_SIZE;
      }

      slots_offset_ += size;
      return TFS_SUCCESS;
    }
  }//end namespace nameserver
}//end namespace tfs
