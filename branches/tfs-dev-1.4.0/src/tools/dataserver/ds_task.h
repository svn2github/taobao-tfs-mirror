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
#ifndef TFS_DSTOOL_TASK_H_
#define TFS_DSTOOL_TASK_H_
#include "client/tfs_file.h"
#include "common/define.h"

class DsLib;

class DsTask
{
public:
  DsTask();
  ~DsTask();
  int init(const uint64_t server_id);
  int init(const uint64_t server_id, const int32_t row_num);
  int init(const uint64_t server_id, const uint32_t block_id);
  int init(const uint64_t server_id, const uint32_t block_id, const uint64_t file_id);
  int init(const uint64_t server_id, const uint32_t block_id, const uint64_t old_file_id, const uint64_t new_file_id);
  int init(const uint64_t server_id, const uint32_t block_id, const uint64_t old_file_id, const int32_t mode);
  int init(const uint64_t server_id, const uint32_t block_id, const uint64_t old_file_id, const char *localfile);
  int init(const uint64_t server_id, const uint32_t block_id, const uint64_t file_id, const int32_t unlink_type,
      const int32_t option_flag, const int32_t is_master);
  int init(const uint64_t server_id, const uint32_t block_id, const uint64_t file_id, const uint32_t crc,
      const int32_t error_flag, const uint64_t failed_servers[], const int32_t failed_num);
  int init_list_block_type(const uint64_t server_id, const int32_t type);
  int get_server_status();
  int get_ping_status();
  int new_block();
  int remove_block();
  int list_block();
  int get_block_info();
  int reset_block_version();
  int create_file_id();
  int read_file_data();
  int write_file_data();
  int unlink_file();
  int read_file_info();
  int list_file();
  int rename_file();
  int send_crc_error();
  int list_bitmap();

  uint64_t get_server_id();
  uint32_t get_block_id();
  uint64_t get_old_file_id();
  uint64_t get_new_file_id();
  uint32_t get_crc();
  char* get_local_file();
  int32_t get_num_row();
  int32_t get_list_block_type();
  int32_t get_unlink_type();
  int32_t get_option_flag();
  int32_t get_is_master();
  int32_t get_mode();
  int32_t get_error_flag();
  void get_failed_servers(tfs::common::VUINT64& failed_server_ids);

private:
  uint64_t server_id_;
  uint32_t block_id_;
  uint64_t old_file_id_;
  uint64_t new_file_id_;
  char local_file_[256];
  int32_t num_row_;
  //data member, used in list block
  int32_t list_block_type_;
  //data member, used in unlink file
  int32_t unlink_type_;
  int32_t option_flag_;
  //true: 0; false: 1.
  int32_t is_master_;
  //data member, used in read file info
  int32_t mode_;
  uint32_t crc_;
  tfs::common::VUINT64 failed_servers_;

  DsLib* ds_lib_;
};

#endif
