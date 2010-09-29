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
#include<stdio.h>
#include<vector>
#include "client/tfs_file.h"
#include "client/fsname.h"
#include "dataserver/bit_map.h"
#include "ds_lib.h"

using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::dataserver;

int DsLib::get_server_status(DsTask* ds_task)
{
  uint64_t server_id = ds_task->get_server_id();
  int32_t num_row = ds_task->get_num_row();
  int ret_status = TFS_ERROR;

  GetServerStatusMessage req_gss_msg;

  req_gss_msg.set_status_type(GSS_MAX_VISIT_COUNT);
  req_gss_msg.set_return_row(num_row);

  std::string err_msg;
  Message* ret_msg = NULL;
  ret_status = send_message_to_ds(server_id, &req_gss_msg, err_msg, &ret_msg);
  if (ret_status == TFS_SUCCESS)
  {
    if ((ret_msg == NULL) || (ret_msg->get_message_type() != CARRY_BLOCK_MESSAGE))
    {
      fprintf(stderr, "Can't get response message from dataserver.\n");
      ret_status = TFS_ERROR;
    }
    else
    {
      CarryBlockMessage* req_cb_msg = dynamic_cast<CarryBlockMessage*> (ret_msg);
      VUINT32 most_visited_blocks;
      VUINT32 visited_count;
      most_visited_blocks = *(req_cb_msg->get_expire_blocks());
      visited_count = *(req_cb_msg->get_new_blocks());
      printf("Block ID\t\tVisted Count\n");
      int32_t block_size = most_visited_blocks.size();
      int32_t i = 0;
      for (; (i < num_row) && (i < block_size); ++i)
      {
        printf("%u\t\t\t%u\n", most_visited_blocks[i], visited_count[i]);
      }
      ret_status = TFS_SUCCESS;
    }
  }
  else
  {
    fprintf(stderr, "Get server status message send failure.\n");
    ret_status = TFS_ERROR;
  }
  delete ret_msg;
  return ret_status;
}

int DsLib::get_ping_status(DsTask* ds_task)
{
  uint64_t server_id = ds_task->get_server_id();
  int ret_status = TFS_ERROR;

  StatusMessage s_msg;
  std::string err_msg;
  Message* ret_msg = NULL;
  ret_status = this->send_message_to_ds(server_id, &s_msg, err_msg, &ret_msg);
  if (ret_status == TFS_SUCCESS)
  {
    if (ret_msg == NULL)
    {
      fprintf(stderr, "Can't get response message from dataserver.\n");
      ret_status = TFS_ERROR;
    }
    else
    {
      printf("ping dataserver success.\n");
      ret_status = TFS_SUCCESS;
    }
  }
  else
  {
    fprintf(stderr, "Ping message send failed.\n");
    ret_status = TFS_ERROR;
  }
  delete ret_msg;
  return ret_status;
}

int DsLib::new_block(DsTask* ds_task)
{
  uint64_t server_id = ds_task->get_server_id();
  uint32_t block_id = ds_task->get_block_id();

  NewBlockMessage req_nb_msg;
  req_nb_msg.add_new_id(block_id);
  std::string err_msg;
  Message* ret_msg = NULL;
  int ret_status = TFS_ERROR;
  ret_status = this->send_message_to_ds(server_id, &req_nb_msg, err_msg, &ret_msg);

  if (ret_status == TFS_SUCCESS)
  {
    if ((ret_msg->get_message_type() == STATUS_MESSAGE) && ((dynamic_cast<StatusMessage*> (ret_msg))->get_status()
        == STATUS_MESSAGE_OK))
    {
      printf("New block success\n");
      ret_status = TFS_SUCCESS;
    }
    else
    {
      fprintf(stderr, "New block fail\n");
      ret_status = TFS_ERROR;
    }
  }
  else
  {
    fprintf(stderr, "New block message send fail\n");
    ret_status = TFS_ERROR;
  }
  delete ret_msg;
  return ret_status;
}

int DsLib::remove_block(DsTask* ds_task)
{
  uint64_t server_id = ds_task->get_server_id();
  uint32_t block_id = ds_task->get_block_id();

  RemoveBlockMessage req_rb_msg;
  req_rb_msg.add_remove_id(block_id);
  std::string err_msg;
  Message* ret_msg = NULL;
  int ret_status = TFS_ERROR;
  ret_status = this->send_message_to_ds(server_id, &req_rb_msg, err_msg, &ret_msg);

  if (ret_status == TFS_SUCCESS)
  {
    if ((ret_msg->get_message_type() == STATUS_MESSAGE) && ((dynamic_cast<StatusMessage*> (ret_msg))->get_status()
        == STATUS_MESSAGE_OK))
    {
      printf("Remove block success\n");
      ret_status = TFS_SUCCESS;
    }
    else
    {
      fprintf(stderr, "Remove block fail\n");
      ret_status = TFS_ERROR;
    }
  }
  else
  {
    fprintf(stderr, "Remove block message send fail\n");
    ret_status = TFS_ERROR;
  }
  delete ret_msg;
  return ret_status;
}

void print_block_id(VUINT32* list_blocks)
{
  int32_t size = 0;
  printf("Logic Block Nums :%d\n", static_cast<int> (list_blocks->size()));
  vector<uint32_t>::iterator vit = list_blocks->begin();
  for (; vit != list_blocks->end(); vit++)
  {
    size++;
    printf("%d ", *vit);
    if (size % 20 == 0)
    {
      printf("\n");
      size = 0;
    }
  }
  printf("\nLogic Block Nums :%d\n", static_cast<int>(list_blocks->size()));
}

void print_block_info(map<uint32_t, BlockInfo*>* block_infos)
{
  int64_t total_file_count = 0;
  int64_t total_size = 0;
  int64_t total_delfile_count = 0;
  int64_t total_del_size = 0;
  printf("BLOCK_ID   VERSION    FILECOUNT  SIZE       DEL_FILE   DEL_SIZE   SEQ_NO\n");
  printf("---------- ---------- ---------- ---------- ---------- ---------- ----------\n");
  map<uint32_t, BlockInfo*>::iterator it = block_infos->begin();
  for (; it != block_infos->end(); it++)
  {
    BlockInfo* block_info = it->second;
    printf("%-10u %10u %10u %10u %10u %10u %10u\n", block_info->block_id_, block_info->version_,
        block_info->file_count_, block_info->size_, block_info->del_file_count_, block_info->del_size_,
        block_info->seq_no_);

    total_file_count += block_info->file_count_;
    total_size += block_info->size_;
    total_delfile_count += block_info->del_file_count_;
    total_del_size += block_info->del_size_;
  }
printf("TOTAL:     %10d %10" PRI64_PREFIX "u %10s %10" PRI64_PREFIX "u %10s\n\n", static_cast<int>(block_infos->size()),
    total_file_count, Func::format_size(total_size).c_str(), total_delfile_count,
    Func::format_size(total_del_size).c_str());
}

void print_block_pair(map<uint32_t, vector<uint32_t> >* logic_phy_pairs)
{
  vector<uint32_t>::iterator vit;
  printf("BLOCK_ID   PHYSICAL_MAIN_ID PHYSICAL_EXT_ID_LIST\n");
  printf("---------- -----------------------------------------------------------------------------------------\n");
  map<uint32_t, vector<uint32_t> >::iterator it = logic_phy_pairs->begin();
  for (; it != logic_phy_pairs->end(); it++)
  {
    vector < uint32_t >* v_phy_list = &(it->second);
    printf("%-10u ", it->first);
    vit = v_phy_list->begin();
    for (; vit != v_phy_list->end(); vit++)
    {
      printf("%-10u ", *vit);
    }
    printf("\n");
  }
  printf("ALL OK\n\n");
}

int DsLib::list_block(DsTask* ds_task)
{
  uint64_t server_id = ds_task->get_server_id();
  int32_t type = ds_task->get_list_block_type();

  int ret_status = TFS_ERROR;
  ListBlockMessage req_lb_msg;
  int32_t xtype = type;
  if (type & 2)
  {
    xtype |= LB_PAIRS;
  }
  if (type & 4)
  {
    xtype |= LB_INFOS;
  }
  req_lb_msg.set_block_type(xtype);

  std::string err_msg;
  Message* ret_msg = NULL;

  map < uint32_t, vector<uint32_t> >* logic_phy_pairs = NULL;
  map<uint32_t, BlockInfo*>* block_infos = NULL;
  VUINT32* list_blocks = NULL;

  ret_status = this->send_message_to_ds(server_id, &req_lb_msg, err_msg, &ret_msg);
  if ((ret_status == TFS_ERROR) || (ret_msg->get_message_type() != RESP_LIST_BLOCK_MESSAGE))
  {
    return ret_status;
  }
  else
  {
    printf("get message type: %d\n", ret_msg->get_message_type());
    RespListBlockMessage* resp_lb_msg = dynamic_cast<RespListBlockMessage*> (ret_msg);

    list_blocks = const_cast<VUINT32*> (resp_lb_msg->get_blocks());
    logic_phy_pairs = const_cast< map < uint32_t, vector<uint32_t> >* > (resp_lb_msg->get_pairs());
    block_infos = const_cast<map<uint32_t, BlockInfo*>*> (resp_lb_msg->get_infos());

    ret_status = TFS_SUCCESS;
  }

  if (type & 1)
  {
    print_block_id(list_blocks);
  }
  if (type & 2)
  {
    print_block_pair(logic_phy_pairs);
  }
  if (type & 4)
  {
    print_block_info(block_infos);
  }

  if(ret_msg != NULL)
  {
    delete ret_msg;
  }
  return ret_status;
}

int DsLib::get_block_info(DsTask* ds_task)
{
  uint64_t server_id = ds_task->get_server_id();
  uint32_t block_id = ds_task->get_block_id();

  GetBlockInfoMessage req_gbi_msg;
  req_gbi_msg.set_block_id(block_id);

  std::string err_msg;
  Message* ret_msg = NULL;
  int ret_status = TFS_ERROR;

  ret_status = this->send_message_to_ds(server_id, &req_gbi_msg, err_msg, &ret_msg);
  if (ret_status == TFS_SUCCESS)
  {
    if (ret_msg->get_message_type() == UPDATE_BLOCK_INFO_MESSAGE)
    {
      UpdateBlockInfoMessage *req_ubi_msg = dynamic_cast<UpdateBlockInfoMessage*> (ret_msg);
      SdbmStat *db_stat = req_ubi_msg->get_db_stat();
      if (block_id != 0)
      {
        BlockInfo* block_info = req_ubi_msg->get_block();
        printf("ID:            %u\n", block_info->block_id_);
        printf("VERSION:       %u\n", block_info->version_);
        printf("FILE_COUNT:    %d\n", block_info->file_count_);
        printf("SIZE:          %d\n", block_info->size_);
        printf("DELFILE_COUNT: %d\n", block_info->del_file_count_);
        printf("DEL_SIZE:      %d\n", block_info->del_size_);
        printf("SEQNO:         %d\n", block_info->seq_no_);
        printf("VISITCOUNT:    %d\n", req_ubi_msg->get_repair());
        int32_t value = req_ubi_msg->get_server_id();
        printf("INFO_LOADED:   %d%s\n", value, (value == 1 ? " (ERR)" : ""));
      }
      else if (db_stat)
      {
        printf("CACHE_HIT:     %d%%\n", 100 * (db_stat->fetch_count_ - db_stat->miss_fetch_count_)
            / (db_stat->fetch_count_ + 1));
        printf("FETCH_COUNT:   %d\n", db_stat->fetch_count_);
        printf("MISFETCH_COUNT:%d\n", db_stat->miss_fetch_count_);
        printf("STORE_COUNT:   %d\n", db_stat->store_count_);
        printf("DELETE_COUNT:  %d\n", db_stat->delete_count_);
        printf("OVERFLOW:      %d\n", db_stat->overflow_count_);
        printf("ITEM_COUNT:    %d\n", db_stat->item_count_);
      }
    }
    else if (ret_msg->get_message_type() == STATUS_MESSAGE)
    {
      StatusMessage* s_msg = dynamic_cast<StatusMessage*> (ret_msg);
      if (s_msg->get_error() != NULL)
      {
        printf("%s\n", s_msg->get_error());
      }
    }
  }
  else
  {
    fprintf(stderr, "send message to Data Server failure\n");
    ret_status = TFS_ERROR;
  }
  delete ret_msg;
  return ret_status;
}

int DsLib::reset_block_version(DsTask* ds_task)
{
  uint64_t server_id = ds_task->get_server_id();
  uint32_t block_id = ds_task->get_block_id();

  ResetBlockVersionMessage req_rbv_msg;
  req_rbv_msg.set_block_id(block_id);

  std::string err_msg;
  Message* ret_msg = NULL;

  int ret_status = TFS_ERROR;
  ret_status = this->send_message_to_ds(server_id, &req_rbv_msg, err_msg, &ret_msg);
  if (ret_status == TFS_SUCCESS)
  {
    if (ret_msg->get_message_type() == STATUS_MESSAGE)
    {
      if ((dynamic_cast<StatusMessage*> (ret_msg))->get_status() == STATUS_MESSAGE_OK)
        printf("Reset block version success\n");
      ret_status = TFS_SUCCESS;
    }
    else
    {
      fprintf(stderr, "Reset block version fail\n");
      ret_status = TFS_ERROR;
    }
  }
  else
  {
    fprintf(stderr, "send message to Data Server failure\n");
    ret_status = TFS_ERROR;
  }
  delete ret_msg;
  return ret_status;
}

int DsLib::create_file_id(DsTask* ds_task)
{
  uint64_t server_id = ds_task->get_server_id();
  uint32_t block_id = ds_task->get_block_id();
  uint64_t new_file_id = ds_task->get_new_file_id();

  CreateFilenameMessage req_cf_msg;
  req_cf_msg.set_block_id(block_id);
  req_cf_msg.set_file_id(new_file_id);

  std::string err_msg;
  Message* ret_msg = NULL;
  int ret_status = TFS_ERROR;

  ret_status = this->send_message_to_ds(server_id, &req_cf_msg, err_msg, &ret_msg);
  if (ret_status == TFS_SUCCESS)
  {
    if (ret_msg->get_message_type() == RESP_CREATE_FILENAME_MESSAGE)
    {
      printf("create file id succeed\n");
      ret_status = TFS_SUCCESS;
    }
    else if (ret_msg->get_message_type() == STATUS_MESSAGE)
    {
      fprintf(stderr, "return error status\n");
      ret_status = TFS_ERROR;
    }
  }
  else
  {
    printf("send message to Data Server failure\n");
    ret_status = TFS_ERROR;
  }
  delete ret_msg;
  return ret_status;
}

int DsLib::read_file_data(DsTask* ds_task)
{
  uint64_t server_id = ds_task->get_server_id();
  uint32_t block_id = ds_task->get_block_id();
  uint64_t file_id = ds_task->get_new_file_id();
  char local_file[256] = { '\0' };
  strcpy(local_file, ds_task->get_local_file());

  int fd = open(local_file, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU);
  if (fd == -1)
  {
    fprintf(stderr, "Open %s fail\n", local_file);
    return TFS_ERROR;
  }

  int32_t read_len = MAX_READ_SIZE;
  static int32_t offset;
  offset = 0;

  static ReadDataMessage rd_message;
  rd_message.set_block_id(block_id);
  rd_message.set_file_id(file_id);
  rd_message.set_length(read_len);
  rd_message.set_offset(offset);

  std::string err_msg;
  Message* ret_msg = NULL;
  int ret_status = TFS_ERROR;
  while (this->send_message_to_ds(server_id, &rd_message, err_msg, &ret_msg) == TFS_SUCCESS)
  {
    ret_status = TFS_SUCCESS;
    RespReadDataMessage *resp_rd_msg = (RespReadDataMessage *) ret_msg;
    int32_t len_tmp = resp_rd_msg->get_length();

    if (len_tmp < 0)
    {
      fprintf(stderr, "read file(id: %lu) data error. ret: %d\n", file_id, len_tmp);
      ret_status = TFS_ERROR;
      delete ret_msg;
      break;
    }

    if (len_tmp == 0)
    {
      delete ret_msg;
      break;
    }

    ssize_t write_len = write(fd, resp_rd_msg->get_data(), len_tmp);

    if (-1 == write_len)
    {
      fprintf(stderr, "write local file fail :%s\n", strerror(errno));
      delete ret_msg;
      break;
    }
    if (len_tmp < MAX_READ_SIZE)
    {
      delete ret_msg;
      break;
    }
    offset += write_len;
    rd_message.set_block_id(block_id);
    rd_message.set_file_id(file_id);
    rd_message.set_length(read_len);
    rd_message.set_offset(offset);
    delete ret_msg;
  }

  if (ret_status == TFS_SUCCESS)
  {
    printf("read file successful, block: %u, file: %lu\n", block_id, file_id);
  }
  close(fd);
  return ret_status;
}

int DsLib::write_file_data(DsTask* ds_task)
{
  uint64_t server_id = ds_task->get_server_id();
  uint32_t block_id = ds_task->get_block_id();
  uint64_t file_id = ds_task->get_new_file_id();
  char local_file[256] = { '\0' };
  strcpy(local_file, ds_task->get_local_file());

  uint64_t new_file_id = 0;
  int64_t file_num = 0;
  uint32_t crc = 0;

  int ret = this->create_file_num(server_id, block_id, file_id, new_file_id, file_num);
  if (ret == TFS_ERROR)
  {
    fprintf(stderr, "Create file num fail \n");
  }
  int fd = open(local_file, O_RDONLY);
  if (fd == -1)
  {
    fprintf(stderr, "Open local file : %s failed\n", local_file);
    return TFS_ERROR;
  }

  char data[MAX_READ_SIZE];
  int32_t read_len;
  int32_t offset = 0;
  ret = TFS_SUCCESS;
  while ((read_len = read(fd, data, MAX_READ_SIZE)) > 0)
  {

    if (this->write_data(server_id, block_id, data, read_len, offset, file_id, file_num) != read_len)
    {
      ret = TFS_ERROR;
      break;
    }
    offset += read_len;
    crc = Func::crc(crc, data, read_len);
  }

  if (ret == TFS_SUCCESS)
  {
    ret = this->close_data(server_id, block_id, crc, file_id, file_num);
    if (ret)
    {
      printf("Write local file to tfs success\n\n");
    }
  }
  else
  {
    fprintf(stderr, "write local file to tfs failed\n\n");
  }
  return TFS_SUCCESS;
}

int DsLib::unlink_file(DsTask* ds_task)
{
  uint64_t server_id = ds_task->get_server_id();
  uint32_t block_id = ds_task->get_block_id();
  uint64_t file_id = ds_task->get_new_file_id();
  int32_t unlink_type = ds_task->get_unlink_type();
  int32_t option_flag = ds_task->get_option_flag();
  int32_t is_master = ds_task->get_is_master();

  UnlinkFileMessage req_uf_msg;
  req_uf_msg.set_block_id(block_id);
  req_uf_msg.set_file_id(file_id);
  req_uf_msg.set_unlink_type(unlink_type);
  req_uf_msg.set_option_flag(option_flag);
  if (is_master == 0)
  {
    req_uf_msg.set_server();
  }

  std::string err_msg;
  Message* ret_msg = NULL;
  int ret_status = TFS_ERROR;
  ret_status = this->send_message_to_ds(server_id, &req_uf_msg, err_msg, &ret_msg);
  if ((ret_status == TFS_SUCCESS) && (ret_msg != NULL))
  {
    StatusMessage* s_msg = dynamic_cast<StatusMessage*> (ret_msg);
    if (s_msg->get_status() == STATUS_MESSAGE_ERROR)
    {
      ret_status = TFS_SUCCESS;
      fprintf(stderr, "Unlink file fail!\n");
    }
    else if (s_msg->get_status() == STATUS_MESSAGE_OK)
      printf("unlink file success\n");
  }
  else
  {
    fprintf(stderr, "Unlink file fail!\n");
  }
  delete ret_msg;
  return ret_status;

}

int DsLib::read_file_info(DsTask* ds_task)
{
  uint64_t server_id = ds_task->get_server_id();
  uint32_t block_id = ds_task->get_block_id();
  uint64_t file_id = ds_task->get_old_file_id();
  int32_t mode = ds_task->get_mode();

  FileInfoMessage req_fi_msg;
  req_fi_msg.set_block_id(block_id);
  req_fi_msg.set_file_id(file_id);
  req_fi_msg.set_mode(mode);

  std::string err_msg;
  Message* ret_msg = NULL;
  int ret_status = TFS_ERROR;
  ret_status = this->send_message_to_ds(server_id, &req_fi_msg, err_msg, &ret_msg);

  if ((ret_status == TFS_SUCCESS) && (ret_msg != NULL))
  {
    if (ret_msg->get_message_type() == RESP_FILE_INFO_MESSAGE)
    {
      RespFileInfoMessage* resp_fi_msg = dynamic_cast<RespFileInfoMessage*> (ret_msg);
      if (resp_fi_msg->get_file_info() != NULL)
      {
        FileInfo file_info;
        memcpy(&file_info, resp_fi_msg->get_file_info(), FILEINFO_SIZE);
        if (file_info.id_ == file_id)
        {
          ret_status = TFS_SUCCESS;
          tfs::client::FSName fsname;
          fsname.set_block_id(block_id);
          fsname.set_file_id(file_id);
          printf("  FILE_NAME:     %s\n", fsname.get_name());
          printf("  BLOCK_ID:      %u\n", fsname.get_block_id());
          printf("  FILE_ID:       %" PRI64_PREFIX "u\n", file_info.id_);
          printf("  OFFSET:        %d\n", file_info.offset_);
          printf("  SIZE:          %d\n", file_info.size_);
          printf("  MODIFIED_TIME: %s\n", Func::time_to_str(file_info.modify_time_).c_str());
          printf("  CREATE_TIME:   %s\n", Func::time_to_str(file_info.create_time_).c_str());
          printf("  STATUS:        %d\n", file_info.flag_);
          printf("  CRC:           %u\n", file_info.crc_);
        }
      }
    }
    else if (ret_msg->get_message_type() == STATUS_MESSAGE)
    {
      printf("Read file info error:%s", (dynamic_cast<StatusMessage*> (ret_msg))->get_error());
      ret_status = TFS_ERROR;
    }
    else
    {
      printf("message type is error.");
      ret_status = TFS_ERROR;
    }
  }
  else
  {
    fprintf(stderr, "Read file info fail\n");
    ret_status = TFS_ERROR;
  }

  delete ret_msg;
  return ret_status;

}

int DsLib::list_file(DsTask* ds_task)
{
  uint64_t server_id = ds_task->get_server_id();
  uint32_t block_id = ds_task->get_block_id();

  GetServerStatusMessage req_gss_msg;
  req_gss_msg.set_status_type(GSS_BLOCK_FILE_INFO);
  req_gss_msg.set_return_row(block_id);

  std::string err_msg;
  Message* ret_msg = NULL;
  int ret_status = TFS_ERROR;
  ret_status = this->send_message_to_ds(server_id, &req_gss_msg, err_msg, &ret_msg);
  FILE_INFO_LIST file_list;

  //if the information of file can be accessed.
  if ((ret_status == TFS_SUCCESS) && (ret_msg != NULL))
  {
    if (ret_msg->get_message_type() == BLOCK_FILE_INFO_MESSAGE)
    {

      FILE_INFO_LIST* file_info_list = (dynamic_cast<BlockFileInfoMessage*> (ret_msg))->get_fileinfo_list();
      int32_t i = 0;
      int32_t list_size = file_info_list->size();
      for (i = 0; i < list_size; i++)
      {
        FileInfo *file_info = new FileInfo();
        memcpy(file_info, file_info_list->at(i), sizeof(FileInfo));
        file_list.push_back(file_info);
      }
      //output file information
      printf("FileList Size = %d\n", list_size);
      printf(
          "FILE_NAME          FILE_ID             OFFSET        SIZE        USIZE       M_TIME               C_TIME              FLAG CRC\n");
      printf(
          "---------- ---------- ---------- ---------- ----------  ---------- ---------- ---------- ---------- ---------- ---------- ----------\n");

      for (i = 0; i < list_size; i++)
      {
        FileInfo* file_info = file_list[i];
        tfs::client::FSName fsname;
        fsname.set_block_id(block_id);
        fsname.set_file_id(file_info->id_);
        printf("%s %20lu %10u %10u %10u %s %s %02d %10u\n", fsname.get_name(), file_info->id_, file_info->offset_,
            file_info->size_, file_info->usize_, Func::time_to_str(file_info->modify_time_).c_str(), Func::time_to_str(
                file_info->create_time_).c_str(), file_info->flag_, file_info->crc_);

        delete file_info;
      }
      printf(
          "---------- ---------- ---------- ---------- ----------  ---------- ---------- ---------- ---------- ---------- ---------- ----------\n");
      printf(
          "FILE_NAME          FILE_ID             OFFSET        SIZE        USIZE       M_TIME               C_TIME              FLAG CRC\n");

      printf("Total : %d files\n", static_cast<int> (file_list.size()));
      file_list.clear();
    }
    else if (ret_msg->get_message_type() == STATUS_MESSAGE)
    {
      printf("%s", (dynamic_cast<StatusMessage*> (ret_msg))->get_error());
    }
  }
  else
  {
    fprintf(stderr, "Get File list in Block failure\n");
    fprintf(stderr, "%s\n", const_cast<char*> (err_msg.c_str()));
  }

  delete ret_msg;
  return ret_status;

}

int DsLib::rename_file(DsTask* ds_task)
{
  uint64_t server_id = ds_task->get_server_id();
  uint32_t block_id = ds_task->get_block_id();
  uint64_t old_file_id = ds_task->get_old_file_id();
  uint64_t new_file_id = ds_task->get_new_file_id();

  int ret_status = TFS_ERROR;
  RenameFileMessage req_rf_msg;

  req_rf_msg.set_block_id(block_id);
  req_rf_msg.set_file_id(old_file_id);
  req_rf_msg.set_new_file_id(new_file_id);
  std::string err_msg;
  Message* ret_msg = NULL;
  ret_status = this->send_message_to_ds(server_id, &req_rf_msg, err_msg, &ret_msg);
  if (ret_status == TFS_SUCCESS) 
  {
    printf("Rename file succeed\n");
  }
  else
  {
    fprintf(stderr, "Rename file failure\n");
    printf("%s\n", const_cast<char*> (err_msg.c_str()));
  }
  return ret_status;
}

int DsLib::send_message_to_ds(const uint64_t server_id, const Message* ds_msg, std::string &err_msg,
    Message** ret_msg)
{
  Client* client = CLIENT_POOL.get_client(server_id);
  if (client->connect() != TFS_SUCCESS)
  {
    CLIENT_POOL.release_client(client);
    return TFS_ERROR;
  }
  int ret_status = TFS_ERROR;
  Message* message = client->call(const_cast<Message*>(ds_msg));
  if (message != NULL)
  {
    if (ret_msg == NULL)
    {
      if (message->get_message_type() == STATUS_MESSAGE)
      {
        StatusMessage* s_msg = dynamic_cast<StatusMessage*> (message);
        if (STATUS_MESSAGE_OK == s_msg->get_status())
        {
          ret_status = TFS_SUCCESS;
        }
        if (s_msg->get_error() != NULL)
        {
          err_msg = s_msg->get_error();
        }
      }
      delete message;
    }
    else
    {
      (*ret_msg) = message;
      ret_status = TFS_SUCCESS;
    }
  }
  client->disconnect();
  CLIENT_POOL.release_client(client);
  return (ret_status);
}

int DsLib::create_file_num(const uint64_t server_ip, const uint32_t block_id, const uint64_t file_id,
    uint64_t& new_file_id, int64_t& file_num)
{
  Client* client = CLIENT_POOL.get_client(server_ip);
  if (client->connect() != TFS_SUCCESS)
  {
    CLIENT_POOL.release_client(client);
    return TFS_ERROR;
  }

  int ret = TFS_ERROR;
  CreateFilenameMessage req_cf_msg;

  req_cf_msg.set_block_id(block_id);
  req_cf_msg.set_file_id(file_id);
  Message *message = client->call(&req_cf_msg);
  if (message != NULL)
  {
    if (message->get_message_type() == RESP_CREATE_FILENAME_MESSAGE)
    {
      RespCreateFilenameMessage* resp_cf_msg = dynamic_cast<RespCreateFilenameMessage*> (message);
      new_file_id = resp_cf_msg->get_file_id();
      file_num = resp_cf_msg->get_file_number();
      ret = TFS_SUCCESS;
    }
    else if (message->get_message_type() == STATUS_MESSAGE)
    {
      StatusMessage *s_msg = (StatusMessage*) message;
      fprintf(stderr, "Createfilename message,Return error status: %s, (%d), %s\n", tbsys::CNetUtil::addrToString(
          client->get_mip()).c_str(), s_msg->get_status(), s_msg->get_error());
    }
    else
    {
      fprintf(stderr, "Createfilename message is error: %d\n", message->get_message_type());
    }
    delete message;
  }
  else
  {
    fprintf(stderr, "Createfilename message send failed.\n");
  }

  CLIENT_POOL.release_client(client);
  return (ret);
}

int DsLib::write_data(const uint64_t server_ip, const uint32_t block_id, const char* data, const int32_t length,
    const int32_t offset, const uint64_t file_id, const uint64_t file_num)
{
  Client* client = CLIENT_POOL.get_client(server_ip);
  if (client->connect() != TFS_SUCCESS)
  {
    CLIENT_POOL.release_client(client);
    return TFS_ERROR;
  }

  VUINT64 ds_list;
  ds_list.clear();
  ds_list.push_back(server_ip);

  int ret = TFS_ERROR;
  WriteDataMessage req_wd_msg;
  req_wd_msg.set_file_number(file_num);
  req_wd_msg.set_block_id(block_id);
  req_wd_msg.set_file_id(file_id);
  req_wd_msg.set_offset(offset);
  req_wd_msg.set_length(length);
  req_wd_msg.set_ds_list(ds_list);
  req_wd_msg.set_data(const_cast<char*>(data));

  Message* message = client->call(&req_wd_msg);
  if (message != NULL)
  {
    if (message->get_message_type() == STATUS_MESSAGE)
    {
      StatusMessage* s_msg = (StatusMessage*) message;
      if (s_msg->get_status() == STATUS_MESSAGE_OK)
      {
        ret = length;
      }
      else
      {
        fprintf(stderr, "Write data failed:%s\n", s_msg->get_error());
        ret = -1;
      }
    }
    delete message;
  }
  else
  {
    fprintf(stderr, "Write data message send failed\n");
    ret = -1;
  }

  CLIENT_POOL.release_client(client);
  return (ret);
}

int DsLib::close_data(const uint64_t server_ip, const uint32_t block_id, const uint32_t crc, const uint64_t file_id,
    const uint64_t file_num)
{
  Client* client = CLIENT_POOL.get_client(server_ip);
  if (client->connect() != TFS_SUCCESS)
  {
    CLIENT_POOL.release_client(client);
    return TFS_ERROR;
  }

  VUINT64 ds_list;
  ds_list.clear();
  ds_list.push_back(server_ip);

  int ret = TFS_ERROR;
  CloseFileMessage req_cf_msg;
  req_cf_msg.set_file_number(file_num);
  req_cf_msg.set_block_id(block_id);
  req_cf_msg.set_file_id(file_id);
  req_cf_msg.set_ds_list(ds_list);
  req_cf_msg.set_crc(crc);

  Message* message = client->call(&req_cf_msg);
  if (message != NULL)
  {
    if (message->get_message_type() == STATUS_MESSAGE)
    {
      StatusMessage* s_msg = dynamic_cast<StatusMessage*> (message);
      if (s_msg->get_status() == STATUS_MESSAGE_OK)
      {
        ret = TFS_SUCCESS;
      }
      else
      {
        fprintf(stderr, "Close file %s\n", s_msg->get_error());
      }
    }
    delete message;
  }
  else
  {
    fprintf(stderr, "Close file message ,response is null\n");
  }
  if (ret)
  {
    fprintf(stderr, "Close file message send failed\n");
  }

  CLIENT_POOL.release_client(client);
  return (ret);
}

int DsLib::send_crc_error(DsTask* ds_task)
{
  uint64_t server_id = ds_task->get_server_id();
  uint32_t block_id = ds_task->get_block_id();
  uint64_t file_id = ds_task->get_new_file_id();
  uint32_t crc = ds_task->get_crc();
  int error_flag = ds_task->get_error_flag();
  VUINT64 failed_servers;
  ds_task->get_failed_servers(failed_servers);

  CrcErrorMessage crc_message;
  crc_message.set_block_id(block_id);
  crc_message.set_file_id(file_id);
  crc_message.set_error_flag(static_cast<CheckDsBlockType> (error_flag));
  crc_message.set_crc(crc);
  VUINT64::iterator it = failed_servers.begin();
  for (; it != failed_servers.end(); it++)
  {
    crc_message.add_fail_server(*it);
  }

  std::string err_msg;
  Message* ret_msg = NULL;
  int ret_status = TFS_ERROR;
  ret_status = this->send_message_to_ds(server_id, &crc_message, err_msg, &ret_msg);
  if ((ret_status == TFS_SUCCESS) && (ret_msg != NULL))
  {
    StatusMessage *s_msg = dynamic_cast<StatusMessage*> (ret_msg);
    if (s_msg->get_status() == STATUS_MESSAGE_ERROR)
    {
      ret_status = TFS_SUCCESS;
      fprintf(stderr, "send crc error fail!\n");
    }
    else if (s_msg->get_status() == STATUS_MESSAGE_OK)
    {
      printf("send crc error success\n");
      printf("labeled file %lu on block %u as crc error(crc: %u)\n", file_id, block_id, crc);
      printf("related ds servers: ");
      VUINT64::iterator it = failed_servers.begin();
      for (; it != failed_servers.end(); it++)
      {
        printf("%ld ", *it);
      }
      printf("\n");
    }
  }
  else
  {
    fprintf(stderr, "send crc error fail!\n");
  }
  delete ret_msg;
  return ret_status;

}
void print_bitmap(const int32_t map_len, const int32_t used_len, const char* data)
{
  printf("LsBitMap. MapLen : %u,UsedLen : %u\n", map_len, used_len);

  printf("Used Block ID:\n");

  uint32_t item_count = map_len * 8;
  BitMap bit_map(item_count);
  bit_map.copy(static_cast<uint32_t>(map_len), data);
  int32_t num = 0;
  int32_t i = 1;
  for (i = 1; i < map_len * 8; i++)
  {
    if (bit_map.test(i))
    {
      num++;
      printf("%8d ", i - 1);
      if (num % 10 == 0)
      {
        printf("\n");
      }
    }
  }

  printf("\nBitMap UsedLen : %d\n", num);
  return;
}
int DsLib::list_bitmap(DsTask* ds_task)
{
  uint64_t server_id = ds_task->get_server_id();
  int32_t type = ds_task->get_list_block_type();
  if (type >= 1)
  {
    printf("usage: list_bitmap ip type\n type 0: normal bitmap\n type 1: error bitmap\n");
    return TFS_ERROR;
  }
  printf("server ip: %s,type: %d\n", tbsys::CNetUtil::addrToString(server_id).c_str(), type);

  ListBitMapMessage req_lbm_msg;
  int ret_status = TFS_ERROR;
  req_lbm_msg.set_bitmap_type(type);

  std::string err_msg;
  Message* ret_msg = NULL;

  ret_status = send_message_to_ds(server_id, &req_lbm_msg, err_msg, &ret_msg);
  if (ret_status == TFS_ERROR)
  {
    return ret_status;
  }

  char* bit_data = NULL;
  int32_t map_len = 0, used_len = 0;
  if ((ret_msg != NULL) && (ret_msg->get_message_type() == RESP_LIST_BITMAP_MESSAGE))
  {
      RespListBitMapMessage* resp_lbm_msg = dynamic_cast<RespListBitMapMessage*> (ret_msg);

      map_len = resp_lbm_msg->get_length();
      used_len = resp_lbm_msg->get_use_count();
      bit_data = resp_lbm_msg->get_data();
      ret_status = TFS_SUCCESS;
  }
  else
  {
    printf("get message type: %d\n", ret_msg->get_message_type());
    printf("get response message from dataserver failed.\n");
    return TFS_ERROR;
  }

  print_bitmap(map_len, used_len, bit_data);

  if(ret_msg != NULL)
  {
    delete ret_msg;
  }
  return ret_status;
}
