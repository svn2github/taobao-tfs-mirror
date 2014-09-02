/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: sync_file_base.cpp 2312 2013-06-13 08:46:08Z duanfei $
 *
 * Authors:
 *   chuyu <chuyu@taobao.com>
 *      - initial release
 *
 */
#include "clientv2/fsname.h"
#include "clientv2/tfs_client_impl_v2.h"
#include "sync_file_base_v2.h"
#include "common/error_msg.h"

using namespace std;
using namespace tfs::clientv2;
using namespace tfs::common;

int rename_file(const char* file_path)
{
  int ret = tfs::common::TFS_SUCCESS;
  if (access(file_path, F_OK) == 0)
  {
    char old_file_path[256];
    snprintf(old_file_path, 256, "%s.%s", file_path, tfs::common::Func::time_to_str(time(NULL), 1).c_str());
    ret = rename(file_path, old_file_path);
  }
  return ret;
}

SyncFileBase::SyncFileBase(string src_ns_addr, string dest_ns_addr) : src_ns_addr_(src_ns_addr), dest_ns_addr_(dest_ns_addr)
{
}

SyncFileBase::~SyncFileBase()
{
}


int SyncFileBase::cmp_and_sync_file(const uint64_t block_id, const FileInfoV2& source_file_info, const FileInfoV2& dest_file_info, const int32_t timestamp, SyncResult& result, string& file_name, const bool force, const bool unlink)
{
  int ret = TFS_SUCCESS;
  FSName fsname(block_id, source_file_info.id_);
  file_name = string(fsname.get_name());

  TfsFileStat source_buf, dest_buf;
  memset(&source_buf, 0, sizeof(source_buf));
  memset(&dest_buf, 0, sizeof(dest_buf));
  get_filestat_from_file_info(source_file_info, source_buf);
  get_filestat_from_file_info(dest_file_info, dest_buf);
  ret = sync_file(file_name, source_buf, dest_buf, timestamp, force, unlink, result);
  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(INFO, "sync file (%s) failed, blockid: %"PRI64_PREFIX"u fileid: %"PRI64_PREFIX"d, ret: %d", file_name.c_str(), block_id, source_file_info.id_, ret);
  }
  return ret;
}

int SyncFileBase::cmp_and_sync_file(const string& file_name, const int32_t timestamp, SyncResult& result, const bool force, const bool unlink)
{
  int ret = TFS_SUCCESS;
  TfsFileStat source_buf, dest_buf;
  memset(&source_buf, 0, sizeof(source_buf));
  memset(&dest_buf, 0, sizeof(dest_buf));

  ret = get_filestat_from_file_name(file_name, src_ns_addr_, source_buf);
  if (TFS_SUCCESS == ret)
  {
    ret = get_filestat_from_file_name(file_name, dest_ns_addr_, dest_buf);
    if (EXIT_BLOCK_NOT_FOUND == ret || EXIT_NO_DATASERVER == ret || EXIT_META_NOT_FOUND_ERROR == ret)
    {
      dest_buf.file_id_ = 0;//目标集群中文件不存在
      ret = TFS_SUCCESS;
    }
    else
    {
      TBSYS_LOG(WARN, "get dest file stat info (%s) failed unknown, ret: %d", file_name.c_str(), ret);
    }

    if (TFS_SUCCESS == ret)
    {
      ret = sync_file(file_name, source_buf, dest_buf, timestamp, force, unlink, result);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "sync file (%s) failed, ret: %d", file_name.c_str(), ret);
      }
    }
    else
    {
      result = SYNC_FAILED;
    }
  }
  else//EXIT_BLOCK_NOT_FOUND, EXIT_NO_DATASERVER, EXIT_META_NOT_FOUND_ERROR etc
  {
    //sync_by_file没有删除冗余文件的设置,不用管目标比源上多的文件
    result = SYNC_FAILED;
    TBSYS_LOG(WARN, "get source file stat info (%s) failed, ret: %d", file_name.c_str(), ret);
  }
  return ret;
}

int SyncFileBase::sync_file(const string& file_name, const TfsFileStat& source_buf, const TfsFileStat& dest_buf, const int32_t timestamp, const bool force, const bool unlink, SyncResult& result)
{
  int ret = TFS_SUCCESS;
  TBSYS_LOG(DEBUG, "file(%s), fileid: %"PRI64_PREFIX"u: flag--(%d -> %d), crc--(%u -> %u), size--(%"PRI64_PREFIX"d -> %"PRI64_PREFIX"d),"
    "source modify time: %s -> dest modify time: %s", file_name.c_str(), source_buf.file_id_, source_buf.flag_, dest_buf.flag_, source_buf.crc_, dest_buf.crc_,
    source_buf.size_, dest_buf.size_, Func::time_to_str(source_buf.modify_time_).c_str(), Func::time_to_str(dest_buf.modify_time_).c_str());

  if (0 == dest_buf.file_id_)// 1. dest file not exists, rewrite file
  {
    if (0 == (source_buf.flag_ & FILE_STATUS_DELETE) || unlink)
    {
      ret = copy_file(file_name, source_buf.flag_);
      result = (TFS_SUCCESS == ret) ? SYNC_SUCCESS : SYNC_FAILED;
      TBSYS_LOG(DEBUG, "dest filename: %s is not exist , copy file %s !!!", file_name.c_str(), TFS_SUCCESS == ret ? "success" : "fail");
    }
    else
    {
      TBSYS_LOG(DEBUG, "ignore filename: %s althought it is not exist in dest, its status is 'DELETE' in source cluster, unlink is set 'false'", file_name.c_str());
    }
  }
  else if (dest_buf.modify_time_ > timestamp)//2. dest file exists and is new file, just skip.
  {
    TBSYS_LOG(WARN, "dest filename: %s has been modifyed, do nothing %s > %s",
        file_name.c_str(), Func::time_to_str(dest_buf.modify_time_).c_str(), Func::time_to_str(timestamp).c_str());
  }
  else if ((dest_buf.size_ != source_buf.size_) || (dest_buf.crc_ != source_buf.crc_)) //3. dest file exist and crc conflict, rewrite file
  {
    if ( force && (0 == (source_buf.flag_ & FILE_STATUS_DELETE) || unlink) )
    {
      ret = copy_file(file_name, source_buf.flag_);
      result = (TFS_SUCCESS == ret) ? SYNC_SUCCESS : SYNC_FAILED;
      TBSYS_LOG(WARN, "file info size or crc conflict!! filename: %s, source size: %"PRI64_PREFIX"d -> dest size: %"PRI64_PREFIX"d, source crc: %u -> dest crc: %u, force:%d, copy file %s", file_name.c_str(), source_buf.size_, dest_buf.size_, source_buf.crc_, dest_buf.crc_, force, TFS_SUCCESS == ret ? "success" : "fail");
    }
  }
  else if (source_buf.flag_ != dest_buf.flag_)//4. dest file data has not update, keeep status agreed with src status
  { // 不需要判断 source_buf.modify_time_ >= dest_buf.modify_time_but in diff stat
    ret = unlink_file(file_name, source_buf.flag_);
    result = (TFS_SUCCESS == ret) ? SYNC_SUCCESS : SYNC_FAILED;
    TBSYS_LOG(WARN, "file info flag conflict!! filname: %s: source flag:%d -> dest flag:%d, unlink file %s", file_name.c_str(), source_buf.flag_, dest_buf.flag_, TFS_SUCCESS == ret ? "success" : "fail");
  }
  else
  {
     TBSYS_LOG(DEBUG, "filename: %s source and dest file crc and status both are consistent, do nothing.", file_name.c_str());
  }
  return ret;
}


void SyncFileBase::get_filestat_from_file_info(const FileInfoV2& file_info, TfsFileStat& buf)
{
  buf.file_id_ = file_info.id_;
  buf.offset_ = file_info.offset_;
  buf.size_ = file_info.size_ - FILEINFO_EXT_SIZE;//通过blockid从ds批量拉取fileinfo会比数据实际大小多这4个字节
  buf.modify_time_ = file_info.modify_time_;
  buf.create_time_ = file_info.create_time_;
  buf.flag_ = file_info.status_;
  buf.crc_ = file_info.crc_;
}

int SyncFileBase::get_filestat_from_file_name(const string& file_name, const string& ns_addr, TfsFileStat& buf)
{
  int ret = TFS_SUCCESS;
  ret = TfsClientImplV2::Instance()->stat_file(&buf, file_name.c_str(), NULL, FORCE_STAT, ns_addr.c_str());
  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "stat file fail from ns:%s, filename:%s, ret:%d.", ns_addr.c_str(), file_name.c_str(), ret);
  }
  return ret;
}


int SyncFileBase::unlink_file(const string& file_name, const int32_t status)
{
  int ret = TFS_SUCCESS;
  int32_t override_action = 0;
  int64_t file_size;//unlink可以获取操作成功后文件的size大小，这里没用
  SET_OVERRIDE_FLAG(override_action, status);//将源集群的文件状态原样同步到目标机器
  ret = TfsClientImplV2::Instance()->unlink(file_size, file_name.c_str(), NULL, static_cast<TfsUnlinkType>(override_action), dest_ns_addr_.c_str());
  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "unlink dest tfsfile fail, filename: %s, src status:%d, ret:%d", file_name.c_str(), status, ret);
  }

  return ret;
}

int SyncFileBase::copy_file(const string& file_name, int32_t status)
{
  int ret = TFS_SUCCESS;
  char data[MAX_READ_DATA_SIZE];
  int32_t rlen = 0;
  int32_t wlen = 0;
  int source_fd = -1, dest_fd = -1;
  source_fd = TfsClientImplV2::Instance()->open(file_name.c_str(), NULL, src_ns_addr_.c_str(), T_READ | T_FORCE);
  if (source_fd < 0)
  {
    ret = source_fd;
    TBSYS_LOG(ERROR, "open source tfsfile fail when copy file, filename: %s, ret:%d", file_name.c_str(), ret);
  }
  if (TFS_SUCCESS == ret)
  {
    dest_fd = TfsClientImplV2::Instance()->open(file_name.c_str(), NULL, dest_ns_addr_.c_str(), T_WRITE | T_NEWBLK);
    if (dest_fd < 0)
    {
      ret = dest_fd;
      TBSYS_LOG(ERROR, "open dest tfsfile fail when copy file, filename: %s, ret:%d", file_name.c_str(), ret);
    }
  }

  if (TFS_SUCCESS == ret)
  {
    if ((ret = TfsClientImplV2::Instance()->set_option_flag(dest_fd, TFS_FILE_NO_SYNC_LOG)) != TFS_SUCCESS)//不必继续同步到目标集群的辅助集群上
    {
      TBSYS_LOG(ERROR, "set option flag failed. ret: %d", ret);
    }
    else
    {
      for (;;)
      {
        rlen = TfsClientImplV2::Instance()->read(source_fd, data, MAX_READ_DATA_SIZE);
        if (rlen < 0)
        {
          ret = rlen;
          TBSYS_LOG(ERROR, "read tfsfile fail, filename: %s, ret: %d", file_name.c_str(), ret);
          break;
        }
        if (rlen == 0)
        {
          break;
        }

        wlen = TfsClientImplV2::Instance()->write(dest_fd, data, rlen);
        if (wlen != rlen)
        {
          ret = wlen;
          TBSYS_LOG(ERROR, "write tfsfile fail, filename: %s, datalen: %d, ret: %d", file_name.c_str(), rlen, ret);
          break;
        }

        if (rlen < MAX_READ_DATA_SIZE)
        {
          break;
        }
      }
    }
  }

  if (source_fd > 0)
  {
    TfsClientImplV2::Instance()->close(source_fd);
  }
  if (dest_fd > 0)
  {
    if ( TFS_SUCCESS != (ret = TfsClientImplV2::Instance()->close(dest_fd, NULL, 0, status)) )//statue=DELETE时ds close要能更新blockinfo的delete相关字段
    {
      TBSYS_LOG(ERROR, "close dest tfsfile fail, filename: %s, ret:%d", file_name.c_str(), ret);
    }
  }
  return ret;
}

