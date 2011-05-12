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

using namespace tfs::common;

namespace tfs
{
  namespace nameserver
  {
    void BlockOpLog::dump(void) const
    {
      /*std::string dsstr = OpLogSyncManager::printDsList(ds_list);
      TBSYS_LOG(DEBUG, "cmd(%s), id(%u) version(%u) file_count(%u) size(%u) delfile_count(%u) del_size(%u) seqno(%u), ds_size(%u), dataserver(%s)",
          cmd == OPLOG_INSERT ? "insert" : cmd == OPLOG_REMOVE ? "remove" : cmd == OPLOG_RELEASE_RELATION ? "release" : "update",
          block_info.id, block_info.version, block_info.file_count, block_info.size, block_info.delfile_count, block_info.del_size,
          block_info.seqno, ds_list.size(), dsstr.c_str());*/
    }

    int BlockOpLog::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
    {
      if ((buf == NULL)
          || (pos + get_serialize_size() > buf_len))
      {
        return -1;
      }
      buf[pos] = cmd_;
      pos++;
      memcpy((buf+pos), &info_, sizeof(info_));
      pos += sizeof(info_);
      buf[pos] = blocks_.size();
      pos++;
      for (uint32_t j = 0; j < blocks_.size(); ++j)
      {
        memcpy((buf+pos), &blocks_[j], sizeof(uint32_t));
        pos += sizeof(uint32_t);
      }
      buf[pos] = servers_.size();
      pos++;
      for (uint32_t i = 0; i < servers_.size(); ++i)
      {
        memcpy((buf+pos), &servers_[i], INT64_SIZE);
        pos += INT64_SIZE;
      }
      return 0;
    }

    int BlockOpLog::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
    {
      if ((buf == NULL)
          || (pos + get_serialize_size() > data_len))
      {
        return -1;
      }
      cmd_ = buf[pos];
      pos++;
      memcpy(&info_, (buf+pos), sizeof(info_));
      pos += sizeof(info_);
      int32_t size = buf[pos];
      pos++;
      uint32_t block = 0;
      for (int32_t j = 0; j < size; ++j)
      {
        memcpy(&block, (buf + pos), sizeof(uint32_t));
        blocks_.push_back(block);
        pos += sizeof(uint32_t);
      }
      size = buf[pos];
      pos++;
      uint64_t server= 0;
      for (int32_t i = 0; i < size; ++i)
      {
        memcpy(&server, (buf+pos), INT64_SIZE);
        servers_.push_back(server);
        pos += INT64_SIZE;
      }
      return 0;
    }

    int64_t BlockOpLog::get_serialize_size(void) const
    {
      return 1 + sizeof(info_) + 1 + blocks_.size() * sizeof(uint32_t) + 1 +  servers_.size() * sizeof(uint64_t);
    }

    OpLog::OpLog(const std::string& logname, int maxLogSlotsSize) :
      MAX_LOG_SLOTS_SIZE(maxLogSlotsSize), MAX_LOG_BUFFER_SIZE(maxLogSlotsSize * MAX_LOG_SIZE), path_(logname), seqno_(
          0), last_flush_time_(0), slots_offset_(0), fd_(-1), buffer_(new char[maxLogSlotsSize * MAX_LOG_SIZE + 1])
    {
      memset(buffer_, 0, maxLogSlotsSize * MAX_LOG_SIZE + 1); 
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

    int OpLog::write(uint8_t type, const char* const data, const int32_t length)
    {
      if ((NULL == data)
          || (length <= 0)
          || (length > OpLog::MAX_LOG_SIZE))
      {
        return -1;
      }
      const int32_t size = sizeof(OpLogHeader) + length;
      const int32_t dope_offset = slots_offset_ + size;
      if (dope_offset > MAX_LOG_BUFFER_SIZE)
      {
        TBSYS_LOG(DEBUG, "(slots_offset_ + size)(%d) > MAX_LOG_BUFFER_SIZE(%d)",
            dope_offset, MAX_LOG_BUFFER_SIZE);
        return EXIT_SLOTS_OFFSET_SIZE_ERROR;
      }

      char* const buffer = buffer_ + slots_offset_;
      OpLogHeader* header = (OpLogHeader*)buffer;
      header->crc_  = 0;
      header->crc_  = Func::crc(header->crc_, (char*)data, length);
      header->time_ = time(NULL);
      header->length_ = length;
      header->seqno_  = ++seqno_;
      header->type_ = type;
      memcpy(header->data_, data, length);
      slots_offset_ += size;
      return TFS_SUCCESS;
    }
  }//end namespace nameserver
}//end namespace tfs
