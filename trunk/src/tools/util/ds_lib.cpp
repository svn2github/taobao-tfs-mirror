/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: ds_lib.cpp 413 2011-06-03 00:52:46Z daoan@taobao.com $
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

#include "common/client_manager.h"
#include "common/new_client.h"
#include "common/status_message.h"
#include "message/server_status_message.h"
#include "message/block_info_message.h"
#include "message/create_filename_message.h"
#include "message/unlink_file_message.h"
#include "message/read_data_message.h"
#include "message/file_info_message.h"
#include "message/rename_file_message.h"
#include "message/write_data_message.h"
#include "message/close_file_message.h"
#include "message/crc_error_message.h"
#include "message/block_info_message_v2.h"
#include "message/read_file_message_v2.h"
// #include "dataserver/bit_map.h"
// #include "new_client/tfs_file.h"
// #include "clientv2/tfs_file.h"
#include "clientv2/fsname.h"

#include "message/write_file_message_v2.h"
#include "message/close_file_message_v2.h"
#include "message/unlink_file_message_v2.h"
#include "message/get_dataserver_all_blocks_header.h"

#include "ds_lib.h"

using namespace tfs::common;
using namespace tfs::message;
// using namespace tfs::dataserver;
using namespace std;

namespace tfs
{
  namespace tools
  {
    int DsLib::get_server_status(DsTask& ds_task)
    {
      uint64_t server_id = ds_task.server_id_;
      int32_t num_row = ds_task.num_row_;
      int ret_status = TFS_ERROR;

      GetServerStatusMessage req_gss_msg;

      req_gss_msg.set_status_type(GSS_MAX_VISIT_COUNT);
      req_gss_msg.set_return_row(num_row);

      NewClient* client = NewClientManager::get_instance().create_client();
      tbnet::Packet* ret_msg = NULL;
      if (TFS_SUCCESS == send_msg_to_server(server_id, client, &req_gss_msg, ret_msg))
      {
        if (ret_msg->getPCode() != CARRY_BLOCK_MESSAGE)
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
      NewClientManager::get_instance().destroy_client(client);
      return ret_status;
    }

    int DsLib::get_ping_status(DsTask& ds_task)
    {
      uint64_t server_id = ds_task.server_id_;
      int ret_status = TFS_ERROR;

      StatusMessage s_msg;
      if (TFS_SUCCESS == send_msg_to_server(server_id, &s_msg, ret_status))
      {
        printf("ping dataserver success.\n");
        ret_status = TFS_SUCCESS;
      }
      else
      {
        fprintf(stderr, "Ping message send failed.\n");
        ret_status = TFS_ERROR;
      }
      return ret_status;
    }

    int DsLib::new_block(DsTask& ds_task)
    {
      uint64_t server_id = ds_task.server_id_;
      uint32_t block_id = ds_task.block_id_;

      NewBlockMessage req_nb_msg;
      req_nb_msg.add_new_id(block_id);
      int ret_status = TFS_ERROR;

      int32_t status_value = 0;
      ret_status = send_msg_to_server(server_id, &req_nb_msg, status_value);

      if (ret_status == TFS_SUCCESS)
      {
        if (STATUS_MESSAGE_OK == status_value)
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
      return ret_status;
    }

    int DsLib::remove_block(DsTask& ds_task)
    {
      uint64_t server_id = ds_task.server_id_;
      uint64_t block_id = ds_task.block_id_;

      RemoveBlockMessageV2 req_rb_msg;
      req_rb_msg.set_block_id(block_id);
      req_rb_msg.set_tmp_flag(false);

      int ret_status = TFS_ERROR;
      int32_t status_value = 0;
      ret_status = send_msg_to_server(server_id, &req_rb_msg, status_value);
      if (ret_status == TFS_SUCCESS)
      {
        if (STATUS_MESSAGE_OK == status_value)
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
      return ret_status;
    }

    void print_block_id(VUINT64* list_blocks)
    {
      int32_t size = 0;
      printf("Logic Block Nums :%d\n", static_cast<int> (list_blocks->size()));
      vector<uint64_t>::iterator vit = list_blocks->begin();
      for (; vit != list_blocks->end(); vit++)
      {
        size++;
        printf("%"PRI64_PREFIX"u ", *vit);
        if (size % 20 == 0)
        {
          printf("\n");
          size = 0;
        }
      }
      printf("\nLogic Block Nums :%d\n", static_cast<int>(list_blocks->size()));
    }

    void print_block_info(vector<BlockInfoV2>* block_infos)
    {
      int64_t total_file_count = 0;
      int64_t total_size = 0;
      int64_t total_delfile_count = 0;
      int64_t total_del_size = 0;
      printf("FAMILY_ID BLOCK_ID   VERSION    FILECOUNT  SIZE       DEL_FILE   DEL_SIZE\n");
      printf("---------- ---------- ---------- ---------- ---------- ---------- ----------\n");
      vector<BlockInfoV2>::iterator it = block_infos->begin();
      for (; it != block_infos->end(); it++)
      {
        BlockInfoV2* block_info = &(*it);
        printf("%10"PRI64_PREFIX"d %10"PRI64_PREFIX"u %10u %10u %10u %10u %10u\n", block_info->family_id_, block_info->block_id_, block_info->version_,
               block_info->file_count_, block_info->size_, block_info->del_file_count_, block_info->del_size_);

        total_file_count += block_info->file_count_;
        total_size += block_info->size_;
        total_delfile_count += block_info->del_file_count_;
        total_del_size += block_info->del_size_;
      }
      printf("TOTAL:     %10d %10" PRI64_PREFIX "u %10s %10" PRI64_PREFIX "u %10s\n\n", static_cast<int>(block_infos->size()),
             total_file_count, Func::format_size(total_size).c_str(), total_delfile_count,
             Func::format_size(total_del_size).c_str());
    }

    void print_block_pair(map<uint64_t, vector<int32_t> >* logic_phy_pairs)
    {
      vector<int32_t>::iterator vit;
      printf("BLOCK_ID   PHYSICAL_MAIN_ID PHYSICAL_EXT_ID_LIST\n");
      printf("---------- -----------------------------------------------------------------------------------------\n");
      map<uint64_t, vector<int32_t> >::iterator it = logic_phy_pairs->begin();
      for (; it != logic_phy_pairs->end(); it++)
      {
        vector < int32_t >* v_phy_list = &(it->second);
        printf("%-10"PRI64_PREFIX"u ", it->first);
        vit = v_phy_list->begin();
        for (; vit != v_phy_list->end(); vit++)
        {
          printf("%-10d ", *vit);
        }
        printf("\n");
      }
      printf("ALL OK\n\n");
    }

    int DsLib::list_block(DsTask& ds_task)
    {
      uint64_t server_id = ds_task.server_id_;
      int32_t type = ds_task.list_block_type_;

      int ret_status = TFS_ERROR;
      ListBlockMessage req_lb_msg;
      int32_t xtype = type;

      if (xtype & 1)
      {
        xtype |= LB_BLOCK;
      }
      if (type & 2)
      {
        xtype |= LB_PAIRS;
      }
      if (type & 4)
      {
        xtype |= LB_INFOS;
      }
      req_lb_msg.set_block_type(xtype);

      map < uint64_t, vector<int32_t> >* logic_phy_pairs = NULL;
      vector<BlockInfoV2>* block_infos = NULL;
      VUINT64* list_blocks = NULL;

      NewClient* client = NewClientManager::get_instance().create_client();
      tbnet::Packet* ret_msg = NULL;
      ret_status = send_msg_to_server(server_id, client, &req_lb_msg, ret_msg);

      if (TFS_SUCCESS == ret_status && (RESP_LIST_BLOCK_MESSAGE == ret_msg->getPCode()))
      {
        printf("get message type: %d\n", ret_msg->getPCode());
        RespListBlockMessage* resp_lb_msg = dynamic_cast<RespListBlockMessage*> (ret_msg);

        list_blocks = const_cast<VUINT64*> (resp_lb_msg->get_blocks());
        logic_phy_pairs = const_cast< map < uint64_t, vector<int32_t> >* > (resp_lb_msg->get_pairs());
        block_infos = const_cast< vector<BlockInfoV2>*> (resp_lb_msg->get_infos());
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

        ret_status = TFS_SUCCESS;
      }
      NewClientManager::get_instance().destroy_client(client);

      return ret_status;
    }

    int DsLib::get_block_info(DsTask& ds_task)
    {
      uint64_t server_id = ds_task.server_id_;
      uint64_t block_id = ds_task.block_id_;

      GetBlockInfoMessageV2 req_gbi_msg;
      req_gbi_msg.set_block_id(block_id);

      int ret_status = TFS_ERROR;
      NewClient* client = NewClientManager::get_instance().create_client();
      tbnet::Packet* ret_msg = NULL;
      if(TFS_SUCCESS == send_msg_to_server(server_id, client, &req_gbi_msg, ret_msg))
      {
        if (UPDATE_BLOCK_INFO_MESSAGE_V2 == ret_msg->getPCode())
        {
          UpdateBlockInfoMessageV2 *req_ubi_msg = dynamic_cast<UpdateBlockInfoMessageV2*> (ret_msg);
          if (block_id != 0)
          {
            const BlockInfoV2& block_info = req_ubi_msg->get_block_info();
            printf("ID:            %"PRI64_PREFIX"u\n", block_info.block_id_);
            printf("FAMILY_ID:     %"PRI64_PREFIX"u\n", block_info.family_id_);
            printf("VERSION:       %u\n", block_info.version_);
            printf("FILE_COUNT:    %d\n", block_info.file_count_);
            printf("SIZE:          %d\n", block_info.size_);
            printf("DELFILE_COUNT: %d\n", block_info.del_file_count_);
            printf("DEL_SIZE:      %d\n", block_info.del_size_);
            printf("UPDATE_COUNT:  %d\n", block_info.update_file_count_);
            printf("UPDATE_SIZE:   %d\n", block_info.update_size_);
            int32_t value = req_ubi_msg->get_server_id();
            printf("INFO_LOADED:   %d%s\n", value, (value == 1 ? " (ERR)" : ""));
          }
          /*
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
          */
        }
        else if (STATUS_MESSAGE == ret_msg->getPCode())
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
      NewClientManager::get_instance().destroy_client(client);
      return ret_status;
    }

    int DsLib::reset_block_version(DsTask& ds_task)
    {
      uint64_t server_id = ds_task.server_id_;
      uint32_t block_id = ds_task.block_id_;

      ResetBlockVersionMessage req_rbv_msg;
      req_rbv_msg.set_block_id(block_id);

      int ret_status = TFS_ERROR;

      int32_t status_value = 0;
      ret_status = send_msg_to_server(server_id, &req_rbv_msg, status_value);
      if (ret_status == TFS_SUCCESS)
      {
        if (STATUS_MESSAGE_OK == status_value)
        {
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
      return ret_status;
    }

    int DsLib::create_file_id(DsTask& ds_task)
    {
      uint64_t server_id = ds_task.server_id_;
      uint32_t block_id = ds_task.block_id_;
      uint64_t new_file_id = ds_task.new_file_id_;

      CreateFilenameMessage req_cf_msg;
      req_cf_msg.set_block_id(block_id);
      req_cf_msg.set_file_id(new_file_id);

      int ret_status = TFS_ERROR;
      NewClient* client = NewClientManager::get_instance().create_client();
      tbnet::Packet* ret_msg = NULL;
      if (TFS_SUCCESS == send_msg_to_server(server_id, client, &req_cf_msg, ret_msg))
      {
        if (ret_msg->getPCode() == RESP_CREATE_FILENAME_MESSAGE)
        {
          printf("create file id succeed\n");
          ret_status = TFS_SUCCESS;
        }
        else if (ret_msg->getPCode() == STATUS_MESSAGE)
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
      NewClientManager::get_instance().destroy_client(client);
      return ret_status;
    }

    int DsLib::verify_file_data(DsTask& ds_task)
    {
      uint64_t server_id = ds_task.server_id_;
      uint64_t block_id = ds_task.block_id_;
      uint64_t attach_block_id = ds_task.attach_block_id_;
      uint64_t file_id = ds_task.new_file_id_;

      int32_t read_len = MAX_READ_SIZE;
      int32_t offset = FILEINFO_EXT_SIZE;
      uint32_t crc = 0;

      FileInfoV2 file_info;

      ReadFileMessageV2 rd_message;
      rd_message.set_block_id(block_id);
      rd_message.set_attach_block_id(attach_block_id);
      rd_message.set_file_id(file_id);
      rd_message.set_length(read_len);
      rd_message.set_offset(offset);
      rd_message.set_flag(READ_DATA_OPTION_WITH_FINFO);

      int ret = TFS_SUCCESS;
      do
      {
        int length = 0;
        char* data = NULL;
        NewClient* client = NewClientManager::get_instance().create_client();
        tbnet::Packet* ret_msg = NULL;
        ret = send_msg_to_server(server_id, client, &rd_message, ret_msg);
        if (TFS_SUCCESS == ret)
        {
          if (READ_FILE_RESP_MESSAGE_V2 == ret_msg->getPCode())
          {
            ReadFileRespMessageV2 *resp_rd_msg = dynamic_cast<ReadFileRespMessageV2*>(ret_msg);
            length = resp_rd_msg->get_length();
            file_info = resp_rd_msg->get_file_info();
            data = resp_rd_msg->get_data();
          }
        }

        if (TFS_SUCCESS == ret)
        {
          crc = Func::crc(crc, data, length);
          offset += length;
          rd_message.set_length(read_len);
          rd_message.set_offset(offset);
        }

        // read error, or reach to the end
        if ((TFS_SUCCESS != ret) || (length < read_len))
        {
          break;
        }

        NewClientManager::get_instance().destroy_client(client);
      } while ((TFS_SUCCESS == ret) && (offset < file_info.size_ - FILEINFO_EXT_SIZE));

      ret = (crc == file_info.crc_) ? TFS_SUCCESS : EXIT_CHECK_CRC_ERROR;
      if (TFS_SUCCESS != ret)
      {
        printf("fileinfo crc: %u, filedata crc: %u\n", file_info.crc_, crc);
      }

      return ret;
    }

    int DsLib::read_file_data(DsTask& ds_task)
    {
      uint64_t server_id = ds_task.server_id_;
      uint64_t block_id = ds_task.block_id_;
      uint64_t attach_block_id = ds_task.attach_block_id_;
      uint64_t file_id = ds_task.new_file_id_;

      int fd = open(ds_task.local_file_, O_WRONLY | O_CREAT | O_TRUNC, 0644);
      if (fd == -1)
      {
        fprintf(stderr, "Open %s fail\n", ds_task.local_file_);
        return TFS_ERROR;
      }

      int32_t read_len = MAX_READ_SIZE;
      static int32_t offset = 4;

      ReadFileMessageV2 rd_message;
      rd_message.set_block_id(block_id);
      rd_message.set_attach_block_id(attach_block_id);
      rd_message.set_file_id(file_id);
      rd_message.set_length(read_len);
      rd_message.set_offset(offset);

      int ret_status = TFS_SUCCESS;
      while (TFS_SUCCESS == ret_status)
      {
        NewClient* client = NewClientManager::get_instance().create_client();
        tbnet::Packet* ret_msg = NULL;
        ret_status = send_msg_to_server(server_id, client, &rd_message, ret_msg);
        if (TFS_SUCCESS == ret_status && READ_FILE_RESP_MESSAGE_V2 != ret_msg->getPCode())
        {
          fprintf(stderr, "unexpected packet packet id :%d", ret_msg->getPCode());
          ret_status = TFS_ERROR;
        }
        ReadFileRespMessageV2 *resp_rd_msg = NULL;
        int32_t len_tmp = 0;
        if (TFS_SUCCESS == ret_status)
        {
          resp_rd_msg = (ReadFileRespMessageV2 *) ret_msg;
          len_tmp = resp_rd_msg->get_length();

          if (len_tmp < 0)
          {
            fprintf(stderr, "read file(id: %" PRI64_PREFIX "u) data error. ret: %d\n", file_id, len_tmp);
            ret_status = TFS_ERROR;
          }
        }
        if (TFS_SUCCESS == ret_status)
        {
          if (len_tmp == 0)
          {
            break;
          }
        }
        ssize_t write_len = 0;
        if (TFS_SUCCESS == ret_status)
        {
          write_len = write(fd, resp_rd_msg->get_data(), len_tmp);

          if (-1 == write_len)
          {
            fprintf(stderr, "write local file fail :%s\n", strerror(errno));
            ret_status = TFS_ERROR;
          }
        }
        if (TFS_SUCCESS == ret_status)
        {
          if (len_tmp < MAX_READ_SIZE)
          {
            break;
          }
        }
        if (TFS_SUCCESS == ret_status)
        {
          offset += write_len;
          rd_message.set_block_id(block_id);
          rd_message.set_file_id(file_id);
          rd_message.set_length(read_len);
          rd_message.set_offset(offset);
        }
        NewClientManager::get_instance().destroy_client(client);
      }

      if (ret_status == TFS_SUCCESS)
      {
        printf("read file successful, block: %"PRI64_PREFIX"u, %"PRI64_PREFIX"u, file: %" PRI64_PREFIX "u\n", block_id, attach_block_id, file_id);
      }
      close(fd);
      return ret_status;
    }

    int DsLib::write_file_data(DsTask& ds_task)
    {
      uint64_t server_id = ds_task.server_id_;
      uint32_t block_id = ds_task.block_id_;
      uint64_t file_id = ds_task.new_file_id_;

      uint64_t new_file_id = 0;
      int64_t file_num = 0;
      uint32_t crc = 0;

      int ret = create_file_num(server_id, block_id, file_id, new_file_id, file_num);
      if (ret == TFS_ERROR)
      {
        fprintf(stderr, "Create file num fail \n");
      }
      int fd = open(ds_task.local_file_, O_RDONLY);
      if (fd == -1)
      {
        fprintf(stderr, "Open local file : %s failed\n", ds_task.local_file_);
        return TFS_ERROR;
      }

      char data[MAX_READ_SIZE];
      int32_t read_len;
      int32_t offset = 0;
      ret = TFS_SUCCESS;
      while ((read_len = read(fd, data, MAX_READ_SIZE)) > 0)
      {
        if (write_data(server_id, block_id, data, read_len, offset, file_id, file_num) != read_len)
        {
          ret = TFS_ERROR;
          break;
        }
        offset += read_len;
        crc = Func::crc(crc, data, read_len);
      }
      close(fd);

      if (TFS_SUCCESS == ret)
      {
        ret = close_data(server_id, block_id, crc, file_id, file_num);
        if (TFS_SUCCESS == ret)
        {
          printf("Write local file to tfs success\n\n");
        }
      }
      else
      {
        fprintf(stderr, "write local file to tfs failed\n\n");
      }
      return ret;
    }

    int DsLib::write_file_data_v2(DsTask& ds_task)
    {
      int ret = TFS_SUCCESS;
      uint64_t server_id = ds_task.server_id_;
      uint64_t block_id = ds_task.block_id_;
      uint64_t file_id = ds_task.new_file_id_;
      uint64_t lease_id = INVALID_LEASE_ID;
      uint32_t crc = 0;

      int fd = open(ds_task.local_file_, O_RDONLY);
      if (fd == -1)
      {
        fprintf(stderr, "Open local file : %s failed\n", ds_task.local_file_);
        ret = EXIT_OPEN_FILE_ERROR;
      }
      else
      {
        char data[MAX_READ_SIZE];
        int32_t read_len = 0;
        int32_t offset = 0;
        int ret = TFS_SUCCESS;
        while ((read_len = read(fd, data, MAX_READ_SIZE)) > 0)
        {
          if (write_data_v2(server_id, block_id, data, read_len, offset, file_id, lease_id) != read_len)
          {
            ret = EXIT_WRITE_FILE_ERROR;
            break;
          }
          offset += read_len;
          crc = Func::crc(crc, data, read_len);
        }
        close(fd);

        if (TFS_SUCCESS == ret)
        {
          ret = close_data_v2(server_id, block_id, crc, file_id, lease_id);
          if (TFS_SUCCESS == ret)
          {
            printf("Write local file to tfs success\n\n");
          }
          else
          {
            fprintf(stderr, "ds close file failed\n\n");
          }
        }
        else
        {
          fprintf(stderr, "write local file to tfs failed\n\n");
        }
      }
      return ret;
    }

    int DsLib::unlink_file(DsTask& ds_task)
    {
      uint64_t server_id = ds_task.server_id_;
      uint32_t block_id = ds_task.block_id_;
      uint64_t file_id = ds_task.new_file_id_;
      int32_t unlink_type = ds_task.unlink_type_;
      int32_t option_flag = ds_task.option_flag_;
      int32_t is_master = ds_task.is_master_;

      UnlinkFileMessage req_uf_msg;
      req_uf_msg.set_block_id(block_id);
      req_uf_msg.set_file_id(file_id);
      req_uf_msg.set_unlink_type(unlink_type);
      req_uf_msg.set_option_flag(option_flag);
      if (is_master == 0)
      {
        req_uf_msg.set_server();
      }

      int ret_status = TFS_ERROR;
      int32_t status_value = 0;
      ret_status = send_msg_to_server(server_id, &req_uf_msg, status_value);
      if ((ret_status == TFS_SUCCESS) )
      {
        if (STATUS_MESSAGE_OK == status_value)
        {
          printf("unlink file success\n");
        }
        else
        {
          ret_status = TFS_SUCCESS;
          fprintf(stderr, "Unlink file fail!\n");
        }
      }
      else
      {
        fprintf(stderr, "Unlink file fail!\n");
      }
      return ret_status;

    }


    int DsLib::unlink_file_v2(DsTask& ds_task, uint64_t& lease_id, bool prepare)
    {
      int ret = TFS_SUCCESS;
      tbnet::Packet* resp_msg = NULL;
      NewClient* client = NewClientManager::get_instance().create_client();
      if (NULL == client)
      {
        ret = EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
        fprintf(stderr, "create new client fail.\n\n");
      }
      else
      {
        VUINT64 ds_list;
        ds_list.push_back(ds_task.server_id_);

        UnlinkFileMessageV2 msg;
        msg.set_block_id(ds_task.block_id_);
        msg.set_attach_block_id(ds_task.block_id_);
        msg.set_file_id(ds_task.new_file_id_);
        msg.set_lease_id(lease_id);
        msg.set_master_id(ds_task.server_id_);
        msg.set_ds(ds_list);
        msg.set_action(ds_task.unlink_type_);
        msg.set_version(-1);//版本号为负数则不进行版本检查
        //msg.set_flag(file_.opt_flag_);
        msg.set_prepare_flag(prepare);
        ret = send_msg_to_server(ds_task.server_id_, client, &msg, resp_msg);
        if(TFS_SUCCESS == ret)
        {
          if (STATUS_MESSAGE != resp_msg->getPCode())
          {
            ret = EXIT_UNKNOWN_MSGTYPE;
          }
          else
          {
            StatusMessage* smsg = dynamic_cast<StatusMessage*>(resp_msg);
            ret = smsg->get_status();
            if (TFS_SUCCESS != ret)
            {
              fprintf(stderr, "unlink file fail. blockid: %"PRI64_PREFIX"u, "
                  "fileid: %"PRI64_PREFIX"u, server: %s, prepare: %s, error msg: %s, ret: %d\n\n",
                  ds_task.block_id_, ds_task.new_file_id_,
                  tbsys::CNetUtil::addrToString(ds_task.server_id_).c_str(), prepare ? "true" : "false",
                  smsg->get_error(), ret);
            }
            else
            {
              if (prepare)
              {
                lease_id = strtoll(smsg->get_error(), NULL, 10);
              }
              printf("unlink file success, prepare:%s\n", prepare ? "true" : "false");
            }
          }
        }
        else
        {
          fprintf(stderr, "unlink file fail. blockid: %"PRI64_PREFIX"u, "
              "fileid: %"PRI64_PREFIX"u, server: %s, prepare: %s, ret: %d\n\n",
              ds_task.block_id_, ds_task.new_file_id_,
              tbsys::CNetUtil::addrToString(ds_task.server_id_).c_str(), prepare ? "true" : "false", ret);
        }
        NewClientManager::get_instance().destroy_client(client);
      }
      return ret;
    }

    int DsLib::read_file_info(DsTask& ds_task)
    {
      uint64_t server_id = ds_task.server_id_;
      int32_t cluster_id = ds_task.cluster_id_;
      uint32_t block_id = ds_task.block_id_;
      uint64_t file_id = ds_task.new_file_id_;
      int32_t mode = ds_task.mode_;

      FileInfoMessage req_fi_msg;
      req_fi_msg.set_block_id(block_id);
      req_fi_msg.set_file_id(file_id);
      req_fi_msg.set_mode(mode);

      int ret_status = TFS_ERROR;
      NewClient* client = NewClientManager::get_instance().create_client();
      tbnet::Packet* ret_msg = NULL;
      ret_status = send_msg_to_server(server_id, client, &req_fi_msg, ret_msg);
      if ((ret_status == TFS_SUCCESS))
      {
        if (RESP_FILE_INFO_MESSAGE == ret_msg->getPCode())
        {
          RespFileInfoMessage* resp_fi_msg = dynamic_cast<RespFileInfoMessage*> (ret_msg);
          if (resp_fi_msg->get_file_info() != NULL)
          {
            FileInfo file_info;
            memcpy(&file_info, resp_fi_msg->get_file_info(), FILEINFO_SIZE);
            if (file_info.id_ == file_id)
            {
              ret_status = TFS_SUCCESS;
              tfs::clientv2::FSName fsname(block_id, file_id, cluster_id);
              printf("  FILE_NAME:     %s\n", fsname.get_name());
              printf("  BLOCK_ID:      %"PRI64_PREFIX"u\n", fsname.get_block_id());
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
        else if (STATUS_MESSAGE == ret_msg->getPCode())
        {
          printf("Read file info error:%s\n", (dynamic_cast<StatusMessage*> (ret_msg))->get_error());
          ret_status = TFS_ERROR;
        }
        else
        {
          printf("message type is error.\n");
          ret_status = TFS_ERROR;
        }
      }
      else
      {
        fprintf(stderr, "Read file info fail\n");
        ret_status = TFS_ERROR;
      }

      NewClientManager::get_instance().destroy_client(client);
      return ret_status;

    }

    int DsLib::list_file(DsTask& ds_task)
    {
      uint64_t server_id = ds_task.server_id_;
      int32_t cluster_id = ds_task.cluster_id_;
      uint64_t block_id = ds_task.block_id_;
      uint64_t attach_block_id = ds_task.attach_block_id_;
      int32_t mode = ds_task.mode_;

      GetServerStatusMessage req_gss_msg;
      req_gss_msg.set_status_type(GSS_BLOCK_FILE_INFO_V2);
      req_gss_msg.set_return_row(block_id);
      req_gss_msg.set_from_row(attach_block_id);

      int ret_status = TFS_ERROR;
      NewClient* client = NewClientManager::get_instance().create_client();
      tbnet::Packet* ret_msg = NULL;
      ret_status = send_msg_to_server(server_id, client, &req_gss_msg, ret_msg);

      //if the information of file can be accessed.
      if ((ret_status == TFS_SUCCESS))
      {
        if (BLOCK_FILE_INFO_MESSAGE_V2 == ret_msg->getPCode())
        {
          FILE_INFO_LIST_V2* file_info_list = (dynamic_cast<BlockFileInfoMessageV2*> (ret_msg))->get_fileinfo_list();
          int32_t i = 0;
          int32_t list_size = file_info_list->size();

          //output file information
          printf("FileList Size = %d\n", list_size);
          if (mode != 0)
          {
            printf(
                "FILE_NAME                  FILE_ID           OFFSET       SIZE     M_TIME               C_TIME      FLAG       CRC\n");
            printf(
                "---------- ---------- ---------- ---------- ----------  ---------- ---------- ---------- ---------- ---------- ----------\n");

            for (i = 0; i < list_size; i++)
            {
              FileInfoV2& file_info = file_info_list->at(i);
              tfs::clientv2::FSName fsname(block_id, file_info.id_, cluster_id);
              print_file_info_v2(fsname.get_name(), file_info);
            }
            printf(
                "---------- ---------- ---------- ---------- ----------  ---------- ---------- ---------- ---------- ---------- ---------- ----------\n");
            printf(
                "FILE_NAME                  FILE_ID           OFFSET       SIZE        USIZE    M_TIME               C_TIME      FLAG       CRC\n");

          }
          else
          {
            // just print file
            for (i = 0; i < list_size; i++)
            {
              FileInfoV2& file_info = file_info_list->at(i);
              tfs::clientv2::FSName fsname(block_id, file_info.id_, cluster_id);
              fprintf(stdout, "\n%s", fsname.get_name());
            }
            fprintf(stdout, "\n");
          }
          printf("Total : %d files\n", list_size);
        }
        else if (STATUS_MESSAGE == ret_msg->getPCode())
        {
          printf("%s", (dynamic_cast<StatusMessage*> (ret_msg))->get_error());
        }
      }
      else
      {
        fprintf(stderr, "Get File list in Block failure\n");
      }

      NewClientManager::get_instance().destroy_client(client);
      return ret_status;

    }

    int DsLib::check_file_info(DsTask& ds_task)
    {
      GetServerStatusMessage req_gss_msg;
      req_gss_msg.set_status_type(GSS_BLOCK_FILE_INFO_V2);
      req_gss_msg.set_return_row(ds_task.block_id_);
      req_gss_msg.set_from_row(ds_task.attach_block_id_);

      tbnet::Packet* rsp = NULL;
      NewClient* client = NewClientManager::get_instance().create_client();
      int ret = send_msg_to_server(ds_task.server_id_, client, &req_gss_msg, rsp);

      if (rsp != NULL)
      {
        if (rsp->getPCode() == BLOCK_FILE_INFO_MESSAGE_V2)
        {
          BlockFileInfoMessageV2* req_bfi_msg = reinterpret_cast<BlockFileInfoMessageV2*>(rsp);
          FILE_INFO_LIST_V2* file_list = req_bfi_msg->get_fileinfo_list();
          bool found = false;
          for (int32_t i = 0; i < static_cast<int32_t> (file_list->size()); ++i)
          {
            if (file_list->at(i).id_ == ds_task.new_file_id_)
            {
              printf("file found in server: %s\n", tbsys::CNetUtil::addrToString(ds_task.server_id_).c_str());
              tfs::clientv2::FSName fsname(ds_task.block_id_, ds_task.new_file_id_, ds_task.cluster_id_);
              print_file_info_v2(fsname.get_name(), file_list->at(i));
              found = true;
              break;
            }
          }
          if (!found)
          {
            printf("file not found in server: %s\n", tbsys::CNetUtil::addrToString(ds_task.server_id_).c_str());
          }
        }
        else if (rsp->getPCode() == STATUS_MESSAGE)
        {
          printf("get file info fail, error: %s\n", dynamic_cast<StatusMessage*>(rsp)->get_error());
        }
      }
      else
      {
        printf("get NULL respose message, ret: %d\n", ret);
      }

      NewClientManager::get_instance().destroy_client(client);
      return ret;
    }

    int DsLib::rename_file(DsTask& ds_task)
    {
      uint64_t server_id = ds_task.server_id_;
      uint32_t block_id = ds_task.block_id_;
      uint64_t old_file_id = ds_task.old_file_id_;
      uint64_t new_file_id = ds_task.new_file_id_;

      int ret_status = TFS_ERROR;
      RenameFileMessage req_rf_msg;

      req_rf_msg.set_block_id(block_id);
      req_rf_msg.set_file_id(old_file_id);
      req_rf_msg.set_new_file_id(new_file_id);
      int32_t status_value = 0;
      ret_status = send_msg_to_server(server_id, &req_rf_msg, status_value);
      if (ret_status == TFS_SUCCESS)
      {
        printf("Rename file succeed\n");
      }
      else
      {
        fprintf(stderr, "Rename file failure\n");
      }
      return ret_status;
    }

    int DsLib::create_file_num(const uint64_t server_ip, const uint32_t block_id, const uint64_t file_id,
                               uint64_t& new_file_id, int64_t& file_num)
    {

      int ret = TFS_ERROR;
      CreateFilenameMessage req_cf_msg;
      req_cf_msg.set_block_id(block_id);
      req_cf_msg.set_file_id(file_id);

      NewClient* client = NewClientManager::get_instance().create_client();
      tbnet::Packet* ret_msg = NULL;
      ret = send_msg_to_server(server_ip, client, &req_cf_msg, ret_msg);

      if (TFS_SUCCESS == ret)
      {
        if (RESP_CREATE_FILENAME_MESSAGE == ret_msg->getPCode())
        {
          RespCreateFilenameMessage* resp_cf_msg = dynamic_cast<RespCreateFilenameMessage*> (ret_msg);
          new_file_id = resp_cf_msg->get_file_id();
          file_num = resp_cf_msg->get_file_number();
          ret = TFS_SUCCESS;
        }
        else if (STATUS_MESSAGE == ret_msg->getPCode())
        {
          StatusMessage *s_msg = (StatusMessage*) ret_msg;
          fprintf(stderr, "Createfilename message,Return error status: %s, (%d), %s\n", tbsys::CNetUtil::addrToString(
                    server_ip).c_str(), s_msg->get_status(), s_msg->get_error());
        }
        else
        {
          fprintf(stderr, "Createfilename message is error: %d\n", ret_msg->getPCode());
        }
      }
      else
      {
        fprintf(stderr, "Createfilename message send failed.\n");
      }
      NewClientManager::get_instance().destroy_client(client);

      return (ret);
    }

    void DsLib::print_file_info(const char* name, FileInfo& file_info)
    {
      printf("%s %20" PRI64_PREFIX "u %10u %10u %10u %s %s %02d %10u\n", name, file_info.id_, file_info.offset_,
             file_info.size_, file_info.usize_, Func::time_to_str(file_info.modify_time_).c_str(), Func::time_to_str(
               file_info.create_time_).c_str(), file_info.flag_, file_info.crc_);
    }

    void DsLib::print_file_info_v2(const char* name, FileInfoV2& file_info)
    {
      printf("%s %20"PRI64_PREFIX"u %10u %10u %s %s %02d %10u\n", name, file_info.id_, file_info.offset_,
             file_info.size_ - FILEINFO_EXT_SIZE,
             Func::time_to_str(file_info.modify_time_).c_str(),
             Func::time_to_str(file_info.create_time_).c_str(), file_info.status_, file_info.crc_);
    }

    int DsLib::write_data(const uint64_t server_ip, const uint32_t block_id, const char* data, const int32_t length,
                          const int32_t offset, const uint64_t file_id, const uint64_t file_num)
    {

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

      NewClient* client = NewClientManager::get_instance().create_client();
      tbnet::Packet* ret_msg = NULL;
      ret = send_msg_to_server(server_ip, client, &req_wd_msg, ret_msg);
      if (TFS_SUCCESS == ret)
      {
        if (STATUS_MESSAGE == ret_msg->getPCode())
        {
          StatusMessage* s_msg = (StatusMessage*)ret_msg;
          if (STATUS_MESSAGE_OK == s_msg->get_status())
          {
            ret = length;
          }
          else
          {
            fprintf(stderr, "Write data failed:%s\n", s_msg->get_error());
            ret = -1;
          }
        }
      }
      else
      {
        fprintf(stderr, "Write data message send failed\n");
        ret = -1;
      }
      NewClientManager::get_instance().destroy_client(client);

      return (ret);
    }

    int DsLib::write_data_v2(const uint64_t server_ip, const uint64_t block_id, const char* data, const int32_t length,
                              const int32_t offset, uint64_t& file_id, uint64_t& lease_id)
    {
      int ret = TFS_SUCCESS;
      VUINT64 ds_list;
      ds_list.push_back(server_ip);
      WriteFileMessageV2 msg;
      msg.set_block_id(block_id);
      msg.set_attach_block_id(block_id);
      msg.set_file_id(file_id);
      msg.set_offset(offset);
      msg.set_length(length);
      msg.set_lease_id(lease_id);
      msg.set_master_id(server_ip);
      msg.set_version(-1);//版本号为负数则不进行版本检查
      msg.set_ds(ds_list);
      msg.set_data(data);
      //默认family_info都是无效
      NewClient* client = NewClientManager::get_instance().create_client();
      if (NULL == client)
      {
        ret = EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
        fprintf(stderr, "create new client fail.\n");
      }
      else
      {
        tbnet::Packet* resp_msg = NULL;
        ret = send_msg_to_server(server_ip, client, &msg, resp_msg);
        if (TFS_SUCCESS == ret)
        {
          if (WRITE_FILE_RESP_MESSAGE_V2 != resp_msg->getPCode())
          {
            if (STATUS_MESSAGE != resp_msg->getPCode())
            {
              ret = EXIT_UNKNOWN_MSGTYPE;
            }
            else
            {
              StatusMessage* smsg = dynamic_cast<StatusMessage*>(resp_msg);
              ret = TFS_ERROR;//回复状态消息算失败
              fprintf(stderr, "write file data fail. blockid: %"PRI64_PREFIX"u, "
                  "fileid: %"PRI64_PREFIX"u, server: %s, error msg: %s, status: %d\n",
                  block_id, file_id, tbsys::CNetUtil::addrToString(server_ip).c_str(), smsg->get_error(), smsg->get_status());
            }
          }
          else
          {
            WriteFileRespMessageV2* response = dynamic_cast<WriteFileRespMessageV2*>(resp_msg);
            lease_id = response->get_lease_id();
            file_id = response->get_file_id();
            //printf("write file data. fileid: %"PRI64_PREFIX"u, leaseid: %"PRI64_PREFIX"u\n", file_id, lease_id);
          }
        }
        else
        {
          fprintf(stderr, "write data message send failed. blockid: %"PRI64_PREFIX"u, "
              "fileid: %"PRI64_PREFIX"u, server: %s, ret: %d\n",
              block_id, file_id, tbsys::CNetUtil::addrToString(server_ip).c_str(), ret);
        }
        NewClientManager::get_instance().destroy_client(client);
      }
      if(TFS_SUCCESS == ret)
      {
        return length;
      }
      else
      {
        return ret;
      }
    }

    int DsLib::close_data(const uint64_t server_ip, const uint32_t block_id, const uint32_t crc, const uint64_t file_id,
        const uint64_t file_num)
    {
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

      NewClient* client = NewClientManager::get_instance().create_client();

      tbnet::Packet* ret_msg = NULL;
      ret = send_msg_to_server(server_ip, client, &req_cf_msg, ret_msg);
      if (TFS_SUCCESS == ret)
      {
        if (STATUS_MESSAGE == ret_msg->getPCode())
        {
          StatusMessage* s_msg = dynamic_cast<StatusMessage*> (ret_msg);
          if (s_msg->get_status() == STATUS_MESSAGE_OK)
          {
            ret = TFS_SUCCESS;
          }
          else
          {
            fprintf(stderr, "Close file %s, ret_status: %d\n", s_msg->get_error(), s_msg->get_status());
          }
        }
      }
      else
      {
        fprintf(stderr, "Close file message ,response is null\n");
      }
      if (ret)
      {
        fprintf(stderr, "Close file message send failed\n");
      }
      NewClientManager::get_instance().destroy_client(client);

      return (ret);
    }

    int DsLib::close_data_v2(const uint64_t server_ip, const uint64_t block_id, const uint32_t crc, const uint64_t file_id,
        uint64_t lease_id)
    {
      int ret = TFS_SUCCESS;
      VUINT64 ds_list;
      ds_list.clear();
      ds_list.push_back(server_ip);

      CloseFileMessageV2 msg;
      msg.set_block_id(block_id);
      msg.set_attach_block_id(block_id);
      msg.set_file_id(file_id);
      msg.set_lease_id(lease_id);
      msg.set_master_id(server_ip);
      msg.set_ds(ds_list);
      msg.set_crc(crc);
      //msg.set_status(status);默认是-1
      //msg.set_version(version);//close不需要版本号

      NewClient* client = NewClientManager::get_instance().create_client();
      if (NULL == client)
      {
        ret = EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
        fprintf(stderr, "create new client fail.\n");
      }
      else
      {
        tbnet::Packet* resp_msg = NULL;
        ret = send_msg_to_server(server_ip, client, &msg, resp_msg);
        if(TFS_SUCCESS == ret)
        {
          if (STATUS_MESSAGE != resp_msg->getPCode())
          {
            ret = EXIT_UNKNOWN_MSGTYPE;
          }
          else
          {
            StatusMessage* smsg = dynamic_cast<StatusMessage*>(resp_msg);
            ret = smsg->get_status();
            if (TFS_SUCCESS != ret)
            {
              fprintf(stderr, "close file data fail. blockid: %"PRI64_PREFIX"u, "
                  "fileid: %"PRI64_PREFIX"u, server: %s, error msg: %s, ret: %d\n",
                  block_id, file_id, tbsys::CNetUtil::addrToString(server_ip).c_str(), smsg->get_error(), ret);
            }
          }
        }
        else
        {
          fprintf(stderr, "Close file data fail. blockid: %"PRI64_PREFIX"u, "
              "fileid: %"PRI64_PREFIX"u, server: %s, ret: %d\n",
               block_id, file_id, tbsys::CNetUtil::addrToString(server_ip).c_str(), ret);
        }
        NewClientManager::get_instance().destroy_client(client);
      }
      return ret;
    }

    int DsLib::send_crc_error(DsTask& ds_task)
    {
      uint64_t server_id = ds_task.server_id_;
      uint32_t block_id = ds_task.block_id_;
      uint64_t file_id = ds_task.new_file_id_;
      uint32_t crc = ds_task.crc_;
      int error_flag = ds_task.option_flag_;
      VUINT64 failed_servers = ds_task.failed_servers_;

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
      int ret_status = TFS_ERROR;
      if(TFS_SUCCESS == send_msg_to_server(server_id, &crc_message, ret_status))
      {

        if (STATUS_MESSAGE_ERROR == ret_status)
        {
          ret_status = TFS_SUCCESS;
          fprintf(stderr, "send crc error fail!\n");
        }
        else if (STATUS_MESSAGE_OK == ret_status)
        {
          printf("send crc error success\n");
          printf("labeled file %" PRI64_PREFIX "u on block %u as crc error(crc: %u)\n", file_id, block_id, crc);
          printf("related ds servers: ");
          VUINT64::iterator it = failed_servers.begin();
          for (; it != failed_servers.end(); it++)
          {
            printf("%" PRI64_PREFIX "u", *it);
          }
          printf("\n");
        }
      }
      else
      {
        fprintf(stderr, "send crc error fail!\n");
      }
      return ret_status;
    }

    int DsLib::get_blocks_index_header(common::DsTask& ds_task, vector<common::IndexHeaderV2>& blocks_header)
    {
      uint64_t server_id = ds_task.server_id_;
      int32_t ret = TFS_SUCCESS;
      NewClient* client = NewClientManager::get_instance().create_client();
      if (NULL != client)
      {
        GetAllBlocksHeaderMessage message;
        tbnet::Packet* rmsg = NULL;
        ret = send_msg_to_server(server_id, client, &message, rmsg);
        if (common::TFS_SUCCESS == ret)
        {
          if (GET_ALL_BLOCKS_HEADER_RESP_MESSAGE == rmsg->getPCode())
          {
            GetAllBlocksHeaderRespMessage* gabh_rsp = dynamic_cast<GetAllBlocksHeaderRespMessage*>(rmsg);
            blocks_header = gabh_rsp->get_all_blocks_header();
          }
          else if (STATUS_MESSAGE == rmsg->getPCode())
          {
            StatusMessage* smsg = dynamic_cast<StatusMessage*>(rmsg);
            ret = smsg->get_status();
            fprintf(stderr, "get all blocks index header data fail, server: %s, error msg: %s, ret: %d\n",
                 tbsys::CNetUtil::addrToString(server_id).c_str(), smsg->get_error(), ret);
          }
          else
          {
            ret = EXIT_UNKNOWN_MSGTYPE;
            fprintf(stderr, "get all blocks index header data fail, server: %s, unknown msg, pcode: %d\n",
                 tbsys::CNetUtil::addrToString(server_id).c_str(), rmsg->getPCode());
          }
        }
        NewClientManager::get_instance().destroy_client(client);
      }
      else
      {
        ret = EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
        fprintf(stderr, "NewClientManager create client fail\n");
      }
      return ret;
    }
    /*
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
    int DsLib::list_bitmap(DsTask& ds_task)
    {
      uint64_t server_id = ds_task.server_id_;
      int32_t type = ds_task.list_block_type_;
      if (type > 1)
      {
        printf("usage: list_bitmap ip type\n type 0: normal bitmap\n type 1: error bitmap\n");
        return TFS_ERROR;
      }
      printf("server ip: %s,type: %d\n", tbsys::CNetUtil::addrToString(server_id).c_str(), type);

      ListBitMapMessage req_lbm_msg;
      int ret_status = TFS_ERROR;
      req_lbm_msg.set_bitmap_type(type);

      NewClient* client = NewClientManager::get_instance().create_client();
      tbnet::Packet* ret_msg = NULL;
      ret_status = send_msg_to_server(server_id, client, &req_lbm_msg, ret_msg);

      if (ret_status == TFS_ERROR)
      {
        NewClientManager::get_instance().destroy_client(client);
        return ret_status;
      }

      char* bit_data = NULL;
      int32_t map_len = 0, used_len = 0;
      if (RESP_LIST_BITMAP_MESSAGE == ret_msg->getPCode())
      {
        RespListBitMapMessage* resp_lbm_msg = dynamic_cast<RespListBitMapMessage*> (ret_msg);

        map_len = resp_lbm_msg->get_length();
        used_len = resp_lbm_msg->get_use_count();
        bit_data = resp_lbm_msg->get_data();
        ret_status = TFS_SUCCESS;
      }
      else
      {
        printf("get message type: %d\n", ret_msg->getPCode());
        printf("get response message from dataserver failed.\n");
        NewClientManager::get_instance().destroy_client(client);
        return TFS_ERROR;
      }

      print_bitmap(map_len, used_len, bit_data);
      NewClientManager::get_instance().destroy_client(client);
      return ret_status;
    }
  */
  }
}
