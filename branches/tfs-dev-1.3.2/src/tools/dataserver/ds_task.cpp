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
 *   jihe
 *      - initial release
 *   chuyu <chuyu@taobao.com>
 *      - modify 2010-03-20
 *   zhuhui <zhuhui_a.pt@taobao.com>
 *      - modify 2010-04-23
 *
 */
#include <stdio.h>
#include <stdlib.h>
#include "ds_task.h"
#include "ds_lib.h"

using namespace tfs::common;

DsTask::DsTask()
{
  ds_lib_ = new DsLib();
}

DsTask::~DsTask()
{
  if (ds_lib_ != NULL)
  {
    delete ds_lib_;
    ds_lib_ = NULL;
  }
}

int DsTask::init(const uint64_t server_id)
{
  server_id_ = server_id;
  return TFS_SUCCESS;
}

int DsTask::init(const uint64_t server_id, const uint32_t block_id)
{
  server_id_ = server_id;
  block_id_ = block_id;
  return TFS_SUCCESS;
}

int DsTask::init(const uint64_t server_id, const int32_t num_row)
{
  server_id_ = server_id;
  num_row_ = num_row;
  return TFS_SUCCESS;
}

int DsTask::init_list_block_type(const uint64_t server_id, const int32_t type)
{
  server_id_ = server_id;
  list_block_type_ = type;
  return TFS_SUCCESS;
}

int DsTask::init(const uint64_t server_id, const uint32_t block_id, const uint64_t new_file_id)
{
  server_id_ = server_id;
  block_id_ = block_id;
  new_file_id_ = new_file_id;
  return TFS_SUCCESS;
}

int DsTask::init(const uint64_t server_id, const uint32_t block_id, const uint64_t file_id, const int32_t mode)
{
  server_id_ = server_id;
  block_id_ = block_id;
  old_file_id_ = file_id;
  mode_ = mode;
  return TFS_SUCCESS;
}

int DsTask::init(const uint64_t server_id, const uint32_t block_id, const uint64_t file_id, const char *local_file)
{
  server_id_ = server_id;
  block_id_ = block_id;
  new_file_id_ = file_id;
  strcpy(local_file_, local_file);
  return TFS_SUCCESS;
}

int DsTask::init(const uint64_t server_id, const uint32_t block_id, const uint64_t old_file_id,
    const uint64_t new_file_id)
{
  server_id_ = server_id;
  block_id_ = block_id;
  old_file_id_ = old_file_id;
  new_file_id_ = new_file_id;
  return TFS_SUCCESS;
}

int DsTask::init(const uint64_t server_id, const uint32_t block_id, const uint64_t file_id, const int32_t unlink_type,
    const int32_t option_flag, int32_t is_master)
{
  server_id_ = server_id;
  block_id_ = block_id;
  new_file_id_ = file_id;
  unlink_type_ = unlink_type;
  option_flag_ = option_flag;
  is_master_ = is_master;
  return TFS_SUCCESS;
}

int DsTask::get_server_status()
{
  return ds_lib_->get_server_status(this);
}

int DsTask::get_ping_status()
{
  return ds_lib_->get_ping_status(this);
}

int DsTask::new_block()
{
  return ds_lib_->new_block(this);
}

int DsTask::remove_block()
{
  return ds_lib_->remove_block(this);
}

int DsTask::get_block_info()
{
  return ds_lib_->get_block_info(this);
}

int DsTask::reset_block_version()
{
  return ds_lib_->reset_block_version(this);
}

int DsTask::rename_file()
{
  return ds_lib_->rename_file(this);
}

int DsTask::list_block()
{
  return ds_lib_->list_block(this);
}

int DsTask::create_file_id()
{
  return ds_lib_->create_file_id(this);
}

int DsTask::read_file_data()
{
  return ds_lib_->read_file_data(this);
}

int DsTask::write_file_data()
{
  return ds_lib_->write_file_data(this);
}

int DsTask::unlink_file()
{
  return ds_lib_->unlink_file(this);
}

int DsTask::read_file_info()
{
  return ds_lib_->read_file_info(this);
}

int DsTask::list_file()
{
  return ds_lib_->list_file(this);
}

int DsTask::list_bitmap()
{
  return ds_lib_->list_bitmap(this);
}

uint64_t DsTask::get_server_id()
{
  return this->server_id_;
}
uint32_t DsTask::get_block_id()
{
  return this->block_id_;
}
uint64_t DsTask::get_old_file_id()
{
  return this->old_file_id_;
}

uint64_t DsTask::get_new_file_id()
{
  return this->new_file_id_;
}

char* DsTask::get_local_file()
{
  return this->local_file_;
}

int DsTask::get_num_row()
{
  return this->num_row_;
}

int DsTask::get_list_block_type()
{
  return this->list_block_type_;
}

int DsTask::get_unlink_type()
{
  return this->unlink_type_;
}

int DsTask::get_option_flag()
{
  return this->option_flag_;
}

int DsTask::get_is_master()
{
  return this->is_master_;
}

int DsTask::get_mode()
{
  return this->mode_;
}

int DsTask::init(const uint64_t server_id, const uint32_t block_id, const uint64_t file_id, const uint32_t crc,
    const int32_t error_flag, const uint64_t failed_servers[], const int32_t failed_num)
{
  server_id_ = server_id;
  block_id_ = block_id;
  new_file_id_ = file_id;
  crc_ = crc;
  option_flag_ = error_flag;

  failed_servers_.clear();
  int32_t i = 0;
  for (i = 0; i < failed_num; i++)
  {
    failed_servers_.push_back(failed_servers[i]);
  }

  return TFS_SUCCESS;
}

uint32_t DsTask::get_crc()
{
  return crc_;
}

int DsTask::get_error_flag()
{
  return option_flag_;
}

void DsTask::get_failed_servers(VUINT64& failed_server_ids)
{
  failed_server_ids.clear();
  VUINT64::iterator it = failed_servers_.begin();
  for (; it != failed_servers_.end(); it++)
  {
    failed_server_ids.push_back(*it);
  }
}

int DsTask::send_crc_error()
{
  return ds_lib_->send_crc_error(this);
}
