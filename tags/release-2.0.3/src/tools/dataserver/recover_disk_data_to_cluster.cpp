/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: recover_disk_data_to_cluster.cpp 553 2011-06-24 08:47:47Z duanfei@taobao.com $
 *
 * Authors:
 *   mingyan <mingyan.zc@taobao.com>
 *      - initial release
 *
 */
#include <stdio.h>
#include "common/internal.h"
#include "common/parameter.h"
#include "common/client_manager.h"
#include "common/status_message.h"
#include "message/client_cmd_message.h"
#include "new_client/tfs_client_impl.h"
#include "new_client/fsname.h"
#include "dataserver/version.h"
#include "dataserver/blockfile_manager.h"
#include "tools/util/tool_util.h"

using namespace tfs::dataserver;
using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::client;
using namespace tfs::tools;
using namespace std;

TfsClientImpl* g_tfs_client = NULL;

int remove_block(const uint32_t block_id)
{
  int ret = block_id > 0 ? TFS_SUCCESS : TFS_ERROR;
  if (TFS_SUCCESS == ret)
  {
    ClientCmdMessage req_cc_msg;
    req_cc_msg.set_cmd(CLIENT_CMD_EXPBLK);
    req_cc_msg.set_value3(block_id);

    send_msg_to_server(g_tfs_client->get_server_id(), &req_cc_msg, ret);
  }
  return ret;
}

int copy_file(LogicBlock* logic_block, const uint32_t block_id, const uint64_t file_id)
{
  int32_t ret = (logic_block != NULL && block_id > 0 ) ? TFS_SUCCESS : TFS_ERROR;
  if (TFS_SUCCESS == ret)
  {
    FSName fsname(block_id, file_id, g_tfs_client->get_cluster_id());
    FileInfo finfo;

    int dest_fd = g_tfs_client->open(fsname.get_name(), NULL, NULL, T_WRITE|T_NEWBLK);
    if (dest_fd <= 0)
    {
      TBSYS_LOG(ERROR, "open dest tfsfile fail. ret: %d", dest_fd);
      ret = TFS_ERROR;
    }
    else
    {
      char data[MAX_READ_SIZE];
      uint32_t crc  = 0;
      int32_t offset = 0;
      int32_t write_length = 0;
      int32_t length = MAX_READ_SIZE;
      int64_t total_length = 0;

      ret = logic_block->read_file(file_id, data, length, offset, READ_DATA_OPTION_FLAG_NORMAL); //read first data & fileinfo
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "read file fail. blockid: %u, fileid: %"PRI64_PREFIX"u, offset: %d, ret: %d",
            block_id, file_id, offset, ret);
      }
      else
      {
        if (length < FILEINFO_SIZE)
        {
          ret = EXIT_READ_FILE_SIZE_ERROR;
          TBSYS_LOG(ERROR,
              "read file fail. blockid: %u, fileid: %"PRI64_PREFIX"u, read len: %d < sizeof(FileInfo):%d, ret: %d",
              block_id, file_id, length, FILEINFO_SIZE, ret);
        }
        else
        {
          memcpy(&finfo, data, FILEINFO_SIZE);
          write_length = g_tfs_client->write(dest_fd, (data + FILEINFO_SIZE), (length - FILEINFO_SIZE));
          if (write_length != (length - FILEINFO_SIZE))
          {
            ret = EXIT_WRITE_FILE_ERROR;
            TBSYS_LOG(ERROR,
                    "write dest tfsfile fail. blockid: %u, fileid: %"PRI64_PREFIX"u, write len: %d <> %d, file size: %d",
                    block_id, file_id, write_length, (length - FILEINFO_SIZE), finfo.size_);
          }
          else
          {
            total_length = length - FILEINFO_SIZE;
            finfo.size_  -= FILEINFO_SIZE;
            offset += length;
            crc = Func::crc(crc, (data + FILEINFO_SIZE), (length - FILEINFO_SIZE));
          }
        }
      }

      if (TFS_SUCCESS == ret)
      {
        while (total_length < finfo.size_
              && TFS_SUCCESS == ret)
        {
          length = ((finfo.size_ - total_length) > MAX_READ_SIZE) ? MAX_READ_SIZE : finfo.size_ - total_length;
          ret = logic_block->read_file(file_id, data, length, offset, READ_DATA_OPTION_FLAG_NORMAL);//read first data & fileinfo
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "read file fail. blockid: %u, fileid: %"PRI64_PREFIX"u, offset: %d, ret: %d",
                block_id, file_id, offset, ret);
          }
          else
          {
            write_length = g_tfs_client->write(dest_fd, data, length);
            if (write_length != length )
            {
              ret = EXIT_WRITE_FILE_ERROR;
              TBSYS_LOG(ERROR,
                  "write dest tfsfile fail. blockid: %u, fileid: %"PRI64_PREFIX"u, write len: %d <> %d, file size: %d",
                  block_id, file_id, write_length, length, finfo.size_);
            }
            else
            {
              total_length += length;
              offset += length;
              crc = Func::crc(crc, data, length);
            }
          }
        }
      }

      //write successful & check file size & check crc
      if (TFS_SUCCESS == ret)
      {
        ret = total_length == finfo.size_ ? TFS_SUCCESS : EXIT_SYNC_FILE_ERROR; // check file size
        if (TFS_SUCCESS != ret)
        {
         TBSYS_LOG(ERROR, "file size error. %s, blockid: %u, fileid :%" PRI64_PREFIX "u, crc: %u <> %u, size: %d <> %d",
               fsname.get_name(), block_id, file_id, crc, finfo.crc_, total_length, finfo.size_);
        }
        else
        {
          ret = crc != finfo.crc_ ? EXIT_CHECK_CRC_ERROR : TFS_SUCCESS; // check crc
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "crc error. %s, blockid: %u, fileid :%" PRI64_PREFIX "u, crc: %u <> %u, size: %d <> %d",
                    fsname.get_name(), block_id, file_id, crc, finfo.crc_, total_length, finfo.size_);
          }
        }
      }

      // close destination tfsfile anyway
      int32_t iret = g_tfs_client->close(dest_fd);
      if (TFS_SUCCESS != iret)
      {
        TBSYS_LOG(ERROR, "close dest tfsfile fail, but write data %s . blockid: %u, fileid :%" PRI64_PREFIX "u",
                 TFS_SUCCESS == ret ? "successful": "fail",block_id, file_id);
      }

      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "copy file fail. blockid: %d, fileid: %"PRI64_PREFIX"u, ret: %d", block_id, file_id, ret);
      }
      else
      {
        TBSYS_LOG(INFO, "copy file success. blockid: %d, fileid: %"PRI64_PREFIX"u", block_id, file_id);
      }
    }
  }
  return ret;
}

int main(int argc,char* argv[])
{
  char* conf_file = NULL;
  int32_t help_info = 0;
  int32_t i;
  string server_index;
  bool set_log_level = false;

  while ((i = getopt(argc, argv, "f:i:nvh")) != EOF)
  {
    switch (i)
    {
      case 'f':
        conf_file = optarg;
        break;
      case 'i':
        server_index = optarg;
        break;
      case 'n':
        set_log_level = true;
        break;
      case 'v':
        fprintf(stderr, "recover disk data to cluster tool, version: %s\n", Version::get_build_description());
        return 0;
      case 'h':
      default:
        help_info = 1;
        break;
    }
  }

  if (NULL == conf_file || 0 == server_index.size() || help_info)
  {
    fprintf(stderr, "\nUsage: %s -f conf_file -i index -h -v\n", argv[0]);
    fprintf(stderr, "  -f configure file\n");
    fprintf(stderr, "  -i server_index  dataserver index number\n");
    fprintf(stderr, "  -n set log level to DEBUG\n");
    fprintf(stderr, "  -v show version info\n");
    fprintf(stderr, "  -h help info\n");
    fprintf(stderr, "\n");
    return -1;
  }

  if (set_log_level)
  {
    TBSYS_LOGGER.setLogLevel("DEBUG");
  }
  else
  {
    TBSYS_LOGGER.setLogLevel("ERROR");
  }

  // load config
  int32_t ret = 0;
  TBSYS_CONFIG.load(conf_file);
  if (TFS_SUCCESS != (ret = SYSPARAM_FILESYSPARAM.initialize(server_index)))
  {
    fprintf(stderr, "SysParam::loadFileSystemParam failed: %s\n", conf_file);
    return ret;
  }
  const char* ns_ip = TBSYS_CONFIG.getString(CONF_SN_DATASERVER, CONF_IP_ADDR);
  if (NULL == ns_ip)
  {
    fprintf(stderr, "Can not find %s in [%s]", CONF_IP_ADDR, CONF_SN_DATASERVER);
    return EXIT_SYSTEM_PARAMETER_ERROR;
  }
  int32_t ns_port = TBSYS_CONFIG.getInt(CONF_SN_DATASERVER, CONF_PORT);
  if (0 == ns_port)
  {
    fprintf(stderr, "Can not find %s in [%s]", CONF_PORT, CONF_SN_DATASERVER);
    return EXIT_SYSTEM_PARAMETER_ERROR;
  }
  char ns_addr[30] = {0};
  sprintf(ns_addr, "%s:%d", ns_ip, ns_port);

  cout << "mount name: " << SYSPARAM_FILESYSPARAM.mount_name_
    << " max mount size: " << SYSPARAM_FILESYSPARAM.max_mount_size_
    << " base fs type: " << SYSPARAM_FILESYSPARAM.base_fs_type_
    << " superblock reserve offset: " << SYSPARAM_FILESYSPARAM.super_block_reserve_offset_
    << " main block size: " << SYSPARAM_FILESYSPARAM.main_block_size_
    << " extend block size: " << SYSPARAM_FILESYSPARAM.extend_block_size_
    << " block ratio: " << SYSPARAM_FILESYSPARAM.block_type_ratio_
    << " file system version: " << SYSPARAM_FILESYSPARAM.file_system_version_
    << " hash slot ratio: " << SYSPARAM_FILESYSPARAM.hash_slot_ratio_
    << endl;

  // load super block & load block file, create logic block map associate stuff
  ret = BlockFileManager::get_instance()->bootstrap(SYSPARAM_FILESYSPARAM);
  if (TFS_SUCCESS != ret)
  {
    fprintf(stderr, "blockfile manager boot fail. ret: %d\n",ret);
    return ret;
  }

  // query super block
  SuperBlock super_block;
  ret = BlockFileManager::get_instance()->query_super_block(super_block);
  if (TFS_SUCCESS != ret)
  {
    fprintf(stderr, "query superblock fail. ret: %d\n",ret);
    return ret;
  }

  // display super block
  super_block.display();

  // init TfsClient
  g_tfs_client = TfsClientImpl::Instance();
  ret = g_tfs_client->initialize(ns_addr, DEFAULT_BLOCK_CACHE_TIME, 1000, false);
  if (TFS_SUCCESS != ret)
  {
    fprintf(stderr, "init tfs client fail, ret: %d\n", ret);
    return ret;
  }

  // for each LogicBlock, query if it exists in the cluster, if not exists or no ds holds it, need recover
  std::list<LogicBlock*> logic_block_list;
  std::list<LogicBlock*> compact_logic_block_list;
  ret = BlockFileManager::get_instance()->get_all_logic_block(logic_block_list, C_MAIN_BLOCK);
  ret = BlockFileManager::get_instance()->get_all_logic_block(compact_logic_block_list, C_COMPACT_BLOCK);
  std::list<LogicBlock*>::iterator iter = logic_block_list.begin();
  LogicBlock* logic_block = NULL;
  BlockInfo* block_info_local = NULL;
  uint32_t block_id = 0;
  VUINT64 ds_list;
  FILE_INFO_LIST file_info_list;
  uint64_t ns_id = g_tfs_client->get_server_id();
  VUINT32 success_block_list;
  VUINT32 no_need_recover_block_list;
  map<uint32_t, uint64_t> fail_block_file_list;
  fprintf(stderr, "this disk has logic block num: %u\n", (unsigned int)logic_block_list.size());
  for (; iter != logic_block_list.end(); iter++)
  {
    logic_block = (*iter);
    block_info_local = logic_block->get_block_info();
    block_id = block_info_local->block_id_;
    // get ds_list from ns, check if block already exists in cluster
    ds_list.clear();
    ret = ToolUtil::get_block_ds_list(ns_id, block_id, ds_list, T_READ);
    if (TFS_SUCCESS == ret)
    {
      if (ds_list.size() <= 0)
      {
        fprintf(stderr, "unknown error, get block %u ds list success but got empty ds list.\n", block_id);
        fail_block_file_list.insert(pair<uint32_t, uint64_t>(block_id, 0));
      }
      else
        no_need_recover_block_list.push_back(block_id);
    }
    else
    {
      // recover this block
      // ds list empty, need remove block first
      if (EXIT_NO_DATASERVER == ret)
      {
        TBSYS_LOG(DEBUG, "no any ds hold block %u in the cluster, need remove first then recover it\n", block_id);
        ret = remove_block(block_id);
        if (TFS_SUCCESS != ret)
        {
          fprintf(stderr, "remove block %u failed, ret: %d\n", block_id, ret);
          fail_block_file_list.insert(pair<uint32_t, uint64_t>(block_id, 0));
          continue;
        }
      }
      else if (EXIT_BLOCK_NOT_FOUND == ret)
      {
        TBSYS_LOG(DEBUG, "block %u not exists in the cluster, need recover\n", block_id);
      }

      // get local file info list
      ret = logic_block->get_file_infos(file_info_list);
      if (TFS_SUCCESS == ret)
      {
        bool all_success = true;
        // for each file in the block, copyfile
        fprintf(stderr, "block: %u has file num: %u\n", block_id, (unsigned int)file_info_list.size());
        for (uint32_t i = 0; i < file_info_list.size(); i++)
        {
          // skip deleted file
          if ((file_info_list.at(i).flag_ & (FI_DELETED | FI_INVALID)) != 0)
            continue;
          uint64_t file_id = file_info_list.at(i).id_;
          ret = copy_file(logic_block, block_id, file_id);
          if (TFS_SUCCESS == ret)
          {
            fprintf(stderr, "recover block_id: %u, file_id: %"PRI64_PREFIX"u successful!\n", block_id, file_id);
          }
          else
          {
            fprintf(stderr, "recover block_id: %u, file_id: %"PRI64_PREFIX"u failed!\n", block_id, file_id);
            all_success = false;
            fail_block_file_list.insert(pair<uint32_t, uint64_t>(block_id, file_id));
          }
        }
        if (all_success)
          success_block_list.push_back(block_id);
      }
      else
      {
        fprintf(stderr, "block %u get local file infos failed.\n", block_id);
        fail_block_file_list.insert(pair<uint32_t, uint64_t>(block_id, 0));
      }
    }
  }
  g_tfs_client->destroy();

  // print out result
  fprintf(stderr, "********************** recover process finished. succeeded block: **********************\n");
  for (uint32_t i = 0; i < success_block_list.size(); i++)
  {
    fprintf(stderr, "%u ", success_block_list.at(i));
  }
  fprintf(stderr, "\n************************************* sum: %u *******************************************\n",
          static_cast<uint32_t>(success_block_list.size()));
  if (fail_block_file_list.size() > 0)
  {
    fprintf(stderr, "*********************************** failed block-file: ************************************\n");
    map<uint32_t, uint64_t>::iterator mit = fail_block_file_list.begin();
    for (; mit != fail_block_file_list.end(); mit++)
    {
      if (mit->second != 0)
        fprintf(stderr, "block_id: %u, file_id: %"PRI64_PREFIX"u\n", mit->first, mit->second);
      else
        fprintf(stderr, "block_id: %u\n", mit->first);
    }
    fprintf(stderr, "*************************************** sum: %u ********************************************\n",
            static_cast<uint32_t>(fail_block_file_list.size()));
  }
  fprintf(stderr, "********************************* no need to recover block: *************************************\n");
  for (uint32_t i = 0; i < no_need_recover_block_list.size(); i++)
  {
    fprintf(stderr, "%u ", no_need_recover_block_list.at(i));
  }
  fprintf(stderr, "\n************************************* sum: %u ******************************************\n",
          static_cast<uint32_t>(no_need_recover_block_list.size()));
  return 0;
}

