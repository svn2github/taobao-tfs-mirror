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
#ifndef TFS_TOOL_LIB_H_
#define TFS_TOOL_LIB_H_

#include "common/internal.h"
#include "client/tfs_file.h"
#include "client/fsname.h"
#include "message/message.h"
#include "message/client_pool.h"
#include "message/message_factory.h"
#include "ds_task.h"

class DsLib
{
public:
  DsLib()
  {
  }

  ~DsLib()
  {
  }

  int get_server_status(DsTask* ds_task);
  int get_ping_status(DsTask* ds_task);
  int new_block(DsTask* ds_task);
  int remove_block(DsTask* ds_task);
  int list_block(DsTask* list_block_task);
  int get_block_info(DsTask* list_block_task);
  int reset_block_version(DsTask* list_block_task);
  int create_file_id(DsTask* ds_task);
  int list_file(DsTask* ds_task);
  int read_file_data(DsTask* ds_task);
  int write_file_data(DsTask* ds_task);
  int unlink_file(DsTask* ds_task);
  int rename_file(DsTask* ds_task);
  int read_file_info(DsTask* ds_task);
  int list_bitmap(DsTask* ds_task);
  int send_crc_error(DsTask* ds_task);

private:
  int send_message_to_ds(const uint64_t server_id, const tfs::message::Message *ds_msg, std::string &err_msg, tfs::message::Message** ret_msg);
  int write_data(const uint64_t server_id, const uint32_t block_id, const char* data, const int32_t length,
      const int32_t offset, const uint64_t file_id, const uint64_t file_num);
  int create_file_num(const uint64_t server_id, const uint32_t block_id, const uint64_t file_id, uint64_t& new_file_id,
      int64_t& file_num);
  int close_data(const uint64_t server_id, const uint32_t block_id, const uint32_t crc, const uint64_t file_id,
      const uint64_t file_num);
};

#endif
