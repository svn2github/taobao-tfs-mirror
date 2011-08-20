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
 *      - initial release
 *
 */
#include "tfs_meta_manager.h"
#include "fsname.h"

using namespace tfs::client;
using namespace tfs::common;
using namespace std;

int TfsMetaManager::initialize(const char* ns_ip)
{
  int ret = TFS_SUCCESS;
  if (ns_ip == NULL)
  {
    TBSYS_LOG(WARN, "invalid ns_ip. ns_ip is null");
  }
  else
  {
    tfs_client_ = TfsClient::Instance();
    if ((ret = tfs_client_->initialize(ns_ip, DEFAULT_BLOCK_CACHE_TIME, 1000, false)) != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "initialize tfs client failed, ns_ip: %s", ns_ip);
    }
  }
  return ret;
}
int64_t TfsMetaManager::read_data(const uint32_t block_id, const uint64_t file_id, 
    void* buffer, const int32_t pos, const int64_t length)
{

  FSName fsname;
  fsname.set_block_id(block_id);
  fsname.set_file_id(file_id);

  int64_t ret_length = -1;
  int fd = TfsMetaManager::tfs_client_->open(fsname.get_name(), NULL, T_READ);
  if (fd < 0)
  {
    TBSYS_LOG(ERROR, "open read file error, file_name: %s, fd: %d", fsname.get_name(), fd);
  }
  else
  {
    ret_length = tfs_client_->pread(fd, buffer, length, pos);
    tfs_client_->close(fd);
  }

  return ret_length;
}

int64_t TfsMetaManager::write_data(void* buffer, const int64_t pos, const int64_t length, 
    common::FragMeta& frag_meta)
{
  //TODO get tfs_file from file_pool

  int ret = TFS_SUCCESS;
  uint32_t block_id = 0;
  uint64_t file_id = 0;
  int64_t cur_offset = pos;
  int64_t cur_length, left_length = length;
  int fd = tfs_client_->open(NULL, NULL, T_WRITE);
  if (fd < 0)
  {
    TBSYS_LOG(ERROR, "open write file error, fd: %d", fd);
  }
  else
  {
    do
    {
      cur_length = min(left_length, MAX_WRITE_DATA_IO);
      int64_t write_length = tfs_client_->pwrite(fd, buffer, cur_length, cur_offset);
      if (write_length != cur_length)
      {
        TBSYS_LOG(ERROR, "tfs write data error, ret: %"PRI64_PREFIX"d", write_length);
        left_length -= write_length;
        ret = TFS_ERROR;
        break;
      }
      left_length -= write_length;
    }
    while(left_length > 0);
    tfs_client_->close(fd);
  }

  frag_meta.block_id_ = block_id;
  frag_meta.file_id_ = file_id;
  frag_meta.offset_ = cur_offset;
  frag_meta.size_ = cur_length;

  return (length - left_length);
}
