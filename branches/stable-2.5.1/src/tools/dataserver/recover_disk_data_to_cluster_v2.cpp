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
#include "message/block_info_message.h"
#include "clientv2/tfs_client_impl_v2.h"
#include "clientv2/fsname.h"
#include "common/version.h"
#include "dataserver/block_manager.h"
#include "tools/util/tool_util.h"
#include "tools/util/ds_lib.h"
#include "common/config_item.h"
#include "message/block_info_message_v2.h"
#include "nameserver/ns_define.h"

using namespace tfs::dataserver;
using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::clientv2;
using namespace tfs::tools;
using namespace std;

static const int32_t MAX_READ_DATA_SIZE = 1024;

int copy_file(BlockManager& block_manager, const uint64_t block_id, const FileInfoV2& file_info)
{
  int32_t ret = block_id != INVALID_BLOCK_ID && file_info.size_ > FILEINFO_EXT_SIZE && file_info.size_ <= MAX_SINGLE_FILE_SIZE ? TFS_SUCCESS : TFS_ERROR;
  if (TFS_SUCCESS == ret)
  {
    uint64_t file_id = file_info.id_;
    FSName fsname(block_id, file_id);// 已知ns_addr时cluster_id不需要

    // firstly try to read disk file data
    int32_t offset = FILEINFO_EXT_SIZE;// skip FileInfoInDiskExt
    int32_t length = file_info.size_ - FILEINFO_EXT_SIZE;
    char* data = new (nothrow) char[length];
    if (NULL == data)
    {
      ret = EXIT_NO_MEMORY;
      TBSYS_LOG(ERROR, "new buffer failed, size: %d, blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u", length, block_id, file_id);
    }
    else
    {
      ret = block_manager.read(data, length, offset, file_id, READ_DATA_OPTION_FLAG_FORCE, block_id, block_id);
      ret = (ret < 0) ? ret: TFS_SUCCESS;// 如果磁盘有问题，读数据时就会检查出并程序core掉
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "read file fail. blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, offset: %d, ret: %d",
          block_id, file_id, offset, ret);
      }
      else
      {
        ret = length == (file_info.size_ - FILEINFO_EXT_SIZE) ? TFS_SUCCESS : EXIT_CHECK_CRC_ERROR; // check file size
        if (TFS_SUCCESS != ret)
        {
         TBSYS_LOG(ERROR, "file size error. %s, blockid: %"PRI64_PREFIX"u, fileid :%" PRI64_PREFIX "u, read size: %d <> %d",
               fsname.get_name(), block_id, file_id, length, file_info.size_ - FILEINFO_EXT_SIZE);
        }
        else
        {
          uint32_t crc = Func::crc(0, data, length);
          ret = crc == file_info.crc_ ? TFS_SUCCESS : EXIT_CHECK_CRC_ERROR; // check crc
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "crc error. %s, blockid: %"PRI64_PREFIX"u, fileid :%" PRI64_PREFIX "u, crc: %u <> %u, size: %d <> %d",
                    fsname.get_name(), block_id, file_id, crc, file_info.crc_, length, file_info.size_ - FILEINFO_EXT_SIZE);
          }
        }
      }

      // read file data success, then write to cluster
      if (TFS_SUCCESS == ret)
      {
        int dest_fd = TfsClientImplV2::Instance()->open(fsname.get_name(), NULL, NULL, T_WRITE|T_NEWBLK);//初始化为输入时的ns_addr
        if (dest_fd <= 0)
        {
          ret = dest_fd;
          TBSYS_LOG(ERROR, "open dest tfsfile fail. ret: %d", ret);
        }
        else if ((ret = TfsClientImplV2::Instance()->set_option_flag(dest_fd, TFS_FILE_NO_SYNC_LOG)) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "set option flag failed. ret: %d", ret);
        }
        else
        {
          int32_t write_length = TfsClientImplV2::Instance()->write(dest_fd, data, length);
          if (write_length != length)
          {
            ret = write_length;
            TBSYS_LOG(ERROR, "write dest file fail. blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, ret: %d", block_id, file_id, ret);
          }
        }

         // close destination tfsfile if check success
        if (dest_fd > 0)
        {
          ret = TfsClientImplV2::Instance()->close(dest_fd, NULL, 0, file_info.status_);//如隐藏文件状态附带过去
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "close dest tfsfile fail, blockid: %"PRI64_PREFIX"u, fileid :%" PRI64_PREFIX "u, ret: %d", block_id, file_id, ret);
          }
        }
      }

      tbsys::gDeleteA(data);
    }
    TBSYS_LOG(DEBUG, "copy file %s, blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, ret: %d", TFS_SUCCESS == ret ? "success" : "failed", block_id, file_id, ret);
  }
  return ret;
}

int rm_no_replicate_block_from_ns(const char* ns_addr, const uint64_t block_id)
{
  int ret = TFS_SUCCESS;
  ClientCmdMessage req_cc_msg;
  req_cc_msg.set_cmd(CLIENT_CMD_EXPBLK);
  req_cc_msg.set_value3(block_id);
  req_cc_msg.set_value4(tfs::nameserver::HANDLE_DELETE_BLOCK_FLAG_ONLY_RELATION);

  NewClient* client = NewClientManager::get_instance().create_client();
  tbnet::Packet* rsp = NULL;
  if((ret = send_msg_to_server(Func::get_host_ip(ns_addr), client, &req_cc_msg, rsp)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "send remove block from ns command failed. block_id: %"PRI64_PREFIX"u, ret: %d", block_id, ret);
  }
  else
  {
    assert(NULL != rsp);
    if (STATUS_MESSAGE == rsp->getPCode())
    {
      StatusMessage* sm = dynamic_cast<StatusMessage*>(rsp);
      if (STATUS_MESSAGE_OK == sm->get_status())
      {
        TBSYS_LOG(INFO, "remove block from ns success, ns_addr: %s, blockid: %"PRI64_PREFIX"u", ns_addr, block_id);
      }
      else
      {
        TBSYS_LOG(ERROR, "remove block from ns fail, ns_addr: %s, blockid: %"PRI64_PREFIX"u, ret: %d",
            ns_addr, block_id, sm->get_status());
        ret = sm->get_status();
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "remove block from ns: %s fail, blockid: %"PRI64_PREFIX"u, unkonw msg type", ns_addr, block_id);
      ret = EXIT_UNKNOWN_MSGTYPE;
    }
  }
  NewClientManager::get_instance().destroy_client(client);
  return ret;
}

int copy_file_from_slave_cluster(const char* src_ns_addr, const char* dest_ns_addr, const uint64_t block_id, const FileInfoV2& file_info)
{
  int ret = TFS_SUCCESS;
  char data[MAX_READ_DATA_SIZE];
  int32_t rlen = 0;
  int32_t wlen = 0;
  uint32_t crc = 0;

  uint64_t file_id = file_info.id_;
  FSName fsname(block_id, file_id);//已知ns_addr时cluster_id不需要
  string file_name = fsname.get_name();

  int source_fd = TfsClientImplV2::Instance()->open(file_name.c_str(), NULL, src_ns_addr, T_READ);
  int dest_fd = -1;
  if (source_fd < 0)
  {
    ret = source_fd;
    TBSYS_LOG(ERROR, "open source tfsfile fail when copy file, filename: %s, ret: %d", file_name.c_str(), ret);
  }
  else
  {
    dest_fd = TfsClientImplV2::Instance()->open(file_name.c_str(), NULL, dest_ns_addr, T_WRITE | T_NEWBLK);
    if (dest_fd < 0)
    {
      ret = dest_fd;
      TBSYS_LOG(ERROR, "open dest tfsfile fail when copy file, filename: %s, ret: %d", file_name.c_str(), ret);
    }
  }
  if (TFS_SUCCESS == ret)
  {
    if ((ret = TfsClientImplV2::Instance()->set_option_flag(dest_fd, TFS_FILE_NO_SYNC_LOG)) != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "set dest_fd option flag failed. ret: %d", ret);
    }
    else if ((ret = TfsClientImplV2::Instance()->set_option_flag(source_fd, READ_DATA_OPTION_FLAG_FORCE)) != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "set source_fd option flag failed. ret: %d", ret);
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

        crc = Func::crc(crc, data, rlen);
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
    if (crc != file_info.crc_)
    {
      ret = EXIT_CHECK_CRC_ERROR;
      TBSYS_LOG(ERROR, "read file data real crc conflict with file info crc, filename: %s", file_name.c_str());
    }
    else
    {
      ret = TfsClientImplV2::Instance()->close(dest_fd, NULL, 0, file_info.status_);//隐藏文件状态附带过去
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "close dest tfsfile fail, filename: %s, ret: %d", file_name.c_str(), ret);
      }
    }
  }
  return ret;
}

int recover_block_from_disk_data(const char* ns_addr, const char* ns_slave_addr, BlockManager& block_manager,
    VUINT64& tmp_fail_block_list, VUINT64& no_need_recover_block_list, VUINT64& success_block_list,
    multimap<uint64_t, uint64_t>& fail_block_file_list, VUINT64& fail_block_list)
{
  int ret = TFS_SUCCESS;
  VUINT64 blocks;
  vector<FileInfoV2> finfos;
  multimap<uint64_t, uint64_t> tmp_fail_block_file_list;
  uint64_t ns_id = Func::get_host_ip(ns_addr);

  //get all blocks from ds disk
  block_manager.get_all_block_ids(blocks);//肯定返回成功

  vector<BlockMeta> blocks_meta;
  ret = ToolUtil::get_all_blocks_meta(ns_id, blocks, blocks_meta, false);// no need get check block
  TBSYS_LOG(DEBUG , "all logic blocks count: %zd, data block count: %zd", blocks.size(), blocks_meta.size());
  if (TFS_SUCCESS == ret)
  {
    int32_t bret = TFS_SUCCESS;
    for (uint32_t block_index = 0; block_index < blocks_meta.size(); ++block_index)
    {
      BlockMeta blk_meta = blocks_meta.at(block_index);
      uint64_t block_id = blk_meta.block_id_;
      if (blk_meta.size_ > 0 || INVALID_FAMILY_ID != blk_meta.family_info_.family_id_)
      {
        TBSYS_LOG(DEBUG , "blockid: %"PRI64_PREFIX"u  no need recover, ds_size:%d, family_id: %"PRI64_PREFIX"u",
            block_id, blk_meta.size_, blk_meta.family_info_.family_id_);
        no_need_recover_block_list.push_back(block_id);//只要还有副本或者副本丢失但有编组(不考虑退化读恢复)都不用本工具恢复
      }
      else
      {
        bret = rm_no_replicate_block_from_ns(ns_addr, block_id);//now T_NEWBLK will can not remove empty ds_list's block
        if (TFS_SUCCESS != bret)
        {
          fail_block_list.push_back(block_id);
          TBSYS_LOG(WARN , "remove block %"PRI64_PREFIX"u from ns: %s failed, ret: %d", block_id, ns_addr, bret);
        }
        else
        {
          IndexHeaderV2 header;
          finfos.clear();
          bret = block_manager.traverse(header, finfos, block_id, block_id);
          if (TFS_SUCCESS != bret)
          {
            TBSYS_LOG(WARN , "block %"PRI64_PREFIX"u get local file infos failed, ret: %d", block_id, bret);
            tmp_fail_block_list.push_back(block_id);//只有本磁盘读取block的文件index错误，才需要整个block尝试从辅集群恢复
          }
          else
          {
            bool all_success = true;
            int32_t copy_file_succ_count = 0;
            tmp_fail_block_file_list.clear();
            for (uint32_t file_index = 0; file_index < finfos.size(); ++file_index)
            {
              // skip deleted file
              if ((finfos.at(file_index).status_ & FI_DELETED) != 0)
                continue;

              uint64_t file_id = finfos.at(file_index).id_;
              bret = copy_file(block_manager, block_id, finfos.at(file_index));
              if (TFS_SUCCESS == bret)
              {
                TBSYS_LOG(DEBUG, "recover block_id: %"PRI64_PREFIX"u, file_id: %"PRI64_PREFIX"u successful!", block_id, file_id);
              }
              else
              {// 如果磁盘中该文件数据已经损坏(如crc出错)，则从对等集群(ns_slave_addr)拷贝数据复制
                bret = copy_file_from_slave_cluster(ns_slave_addr, ns_addr, block_id, finfos.at(file_index));
                if (TFS_SUCCESS == bret)
                {
                  TBSYS_LOG(DEBUG, "recover block_id: %"PRI64_PREFIX"u, file_id: %"PRI64_PREFIX"u successful from slave cluster!", block_id, file_id);
                }
                else
                {
                  TBSYS_LOG(WARN, "recover block_id: %"PRI64_PREFIX"u, file_id: %"PRI64_PREFIX"u failed from slave cluster, ret: %d!", block_id, file_id, bret);
                  all_success = false;
                  tmp_fail_block_file_list.insert(pair<uint64_t, uint64_t>(block_id, file_id));
                }
              }
              if (TFS_SUCCESS == bret)
              {
                ++copy_file_succ_count;
              }
            }

            if (all_success)
            {
              success_block_list.push_back(block_id);
              if (0 == copy_file_succ_count)
              {
                TBSYS_LOG(DEBUG, "recover block_id: %"PRI64_PREFIX"u need to do nothing,"
                    " because the count of files who need to recover is ZERO except DELETE status files!", block_id);
              }
            }
            else if (0 == copy_file_succ_count)// all file(exclude DELETE) fail
            {
              TBSYS_LOG(WARN, "recover block_id: %"PRI64_PREFIX"u's files failed, copy_file_succ_count is Zero !", block_id);
              fail_block_list.push_back(block_id);// for print fail block to out log file at end
            }
            else
            {
              fail_block_file_list.insert(tmp_fail_block_file_list.begin(), tmp_fail_block_file_list.end());
            }
          }
        }
      }
    }//end for blocks loop
  }
  else
  {
    TBSYS_LOG(WARN, "get blockis ds_list error, ret:%d", ret);
  }
  TBSYS_LOG(INFO, "success_block_list size: %zd, tmp_fail_block_list size: %zd, fail_block_list size: %zd",
      success_block_list.size(), tmp_fail_block_list.size(), fail_block_list.size());
  return ret;
}

void recover_block_from_slave_cluster(const char* ns_addr, const char* ns_slave_addr,
    const VUINT64& tmp_fail_block_list, VUINT64& success_block_list,
    VUINT64& fail_block_list, multimap<uint64_t, uint64_t>& fail_block_file_list)
{
  int ret = TFS_SUCCESS;
  VUINT64::const_iterator vit = tmp_fail_block_list.begin();
  multimap<uint64_t, uint64_t> tmp_fail_block_file_list;
  vector<FileInfoV2> finfos;
  for (; vit != tmp_fail_block_list.end(); vit++)
  {
    uint64_t block_id = (*vit);
    finfos.clear();
    ret = ToolUtil::read_file_infos_v2(Func::get_host_ip(ns_slave_addr), block_id, finfos);
    if (ret == TFS_SUCCESS)
    {
      bool all_success = true;
      int32_t copy_file_succ_count = 0;
      tmp_fail_block_file_list.clear();
      vector<FileInfoV2>::const_iterator v_file_iter = finfos.begin();
      for (; v_file_iter != finfos.end(); v_file_iter++)
      {
        if ((v_file_iter->status_ & FI_DELETED) != 0)
          continue;

        uint64_t file_id = v_file_iter->id_;
        ret = copy_file_from_slave_cluster(ns_slave_addr, ns_addr, block_id, (*v_file_iter));
        if (TFS_SUCCESS == ret)
        {
          ++copy_file_succ_count;
          TBSYS_LOG(DEBUG, "recover block_id: %"PRI64_PREFIX"u, file_id: %"PRI64_PREFIX"u successful from slave cluster!", block_id, file_id);
        }
        else
        {
          TBSYS_LOG(WARN, "recover block_id: %"PRI64_PREFIX"u, file_id: %"PRI64_PREFIX"u failed from slave cluster, ret: %d", block_id, file_id, ret);
          all_success = false;
          tmp_fail_block_file_list.insert(pair<uint64_t, uint64_t>(block_id, file_id));
        }
      }

      if (all_success)
      {
        success_block_list.push_back(block_id);
      }
      else if (0 == copy_file_succ_count)
      {
        fail_block_list.push_back(block_id);
      }
      else
      {
        fail_block_file_list.insert(tmp_fail_block_file_list.begin(), tmp_fail_block_file_list.end());
      }
    }
    else
    {
      fail_block_list.push_back(block_id);
    }
  }
}

int main(int argc,char* argv[])
{
  char* conf_file = NULL;
  char* ns_slave_addr = NULL;
  int32_t help_info = 0;
  int32_t i;
  string server_index;
  string log_file = "recover_disk_data.log";
  bool set_log_level = false;

  while ((i = getopt(argc, argv, "f:s:i:nvh")) != EOF)
  {
    switch (i)
    {
      case 'f':
        conf_file = optarg;
        break;
      case 's':
        ns_slave_addr = optarg;
        break;
      case 'i':
        server_index = optarg;
        break;
      case 'n':
        set_log_level = true;
        break;
      case 'v':
      case 'h':
      default:
        help_info = 1;
        break;
    }
  }

  if (NULL == conf_file || NULL == ns_slave_addr || 0 == server_index.size() || help_info)
  {
    fprintf(stderr, "version: %s\n", Version::get_build_description());
    fprintf(stderr, "Usage: %s -f ds_conf -i index -s ns_slave_addr [-h] [-v]\n", argv[0]);
    fprintf(stderr, "  -f dataserver configure file\n");
    fprintf(stderr, "  -s slave cluster addr\n");
    fprintf(stderr, "  -i server_index  dataserver index number\n");
    fprintf(stderr, "  -n set log level to DEBUG, default ERROR\n");
    fprintf(stderr, "  -v show version info\n");
    fprintf(stderr, "  -h help info\n");
    fprintf(stderr, "\n");
    return -1;
  }

  TBSYS_LOGGER.setFileName(log_file.c_str(), true);
  TBSYS_LOGGER.rotateLog(log_file.c_str());
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
    TBSYS_LOG(ERROR, "SysParam::loadFileSystemParam failed: %s", conf_file);
    return ret;
  }
  const char* ns_ip = TBSYS_CONFIG.getString(CONF_SN_DATASERVER, CONF_IP_ADDR);
  if (NULL == ns_ip)
  {
    TBSYS_LOG(ERROR, "Can not find %s in [%s]", CONF_IP_ADDR, CONF_SN_DATASERVER);
    return EXIT_SYSTEM_PARAMETER_ERROR;
  }
  int32_t ns_port = TBSYS_CONFIG.getInt(CONF_SN_DATASERVER, CONF_PORT);
  if (ns_port < 1024 || ns_port > 65535)
  {
    TBSYS_LOG(ERROR, "Can not find %s in [%s] or ns port is invalid", CONF_PORT, CONF_SN_DATASERVER);
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

  //init block manager
  string super_block_path = string(SYSPARAM_FILESYSPARAM.mount_name_) + SUPERBLOCK_NAME;
  BlockManager block_manager(super_block_path);
  ret = block_manager.bootstrap(SYSPARAM_FILESYSPARAM);
  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "load all blocks failed, ret: %d", ret);
    return ret;
  }

  SuperBlockInfo* info = NULL;
  ret = block_manager.get_super_block_manager().get_super_block_info(info);
  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "get super block info failed, ret: %d", ret);
    return ret;
  }
  else
  {
    cout << "Dump super block info:" << endl;
    stringstream strstream;
    info->dump(strstream);
    cout << strstream.str() << endl;
  }

  // init TfsClient
  ret = TfsClientImplV2::Instance()->initialize(ns_addr);
  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "init TfsClientImplV2 client fail, ret: %d", ret);
    return ret;
  }

  VUINT64 tmp_fail_block_list, fail_block_list, no_need_recover_block_list, success_block_list;
  multimap<uint64_t, uint64_t> fail_block_file_list;

  // recover from dist firstly
  ret = recover_block_from_disk_data(ns_addr, ns_slave_addr, block_manager, tmp_fail_block_list,
      no_need_recover_block_list, success_block_list, fail_block_file_list, fail_block_list);
  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "recover_block_from_disk_data fail, ret: %d", ret);
    return ret;
  }

  // copy the read finfos failed block:tmp_fail_block_list from disk by slave cluster
  if (tmp_fail_block_list.size() > 0)
  {
    recover_block_from_slave_cluster(ns_addr, ns_slave_addr, tmp_fail_block_list,
        success_block_list, fail_block_list, fail_block_file_list);
  }

  TfsClientImplV2::Instance()->destroy();

  // print result
  fprintf(stdout, "********************** succeeded block sum: %u **********************\n",
      static_cast<uint32_t>(success_block_list.size()));
  for (uint32_t i = 0; i < success_block_list.size(); i++)
  {
    fprintf(stdout, "%"PRI64_PREFIX"u\n", success_block_list.at(i));
  }

  fprintf(stdout, "********************** failed block sum: %u **********************\n",
      static_cast<uint32_t>(fail_block_list.size()));
  for (uint32_t i = 0; i < fail_block_list.size(); i++)
  {
    fprintf(stdout, "%"PRI64_PREFIX"u\n", fail_block_list.at(i));
  }

  fprintf(stdout, "*********************************** failed block-file sum : %u ************************************\n",
          static_cast<uint32_t>(fail_block_file_list.size()));
  multimap<uint64_t, uint64_t>::iterator mit = fail_block_file_list.begin();
  for (; mit != fail_block_file_list.end(); mit++)
  {
      FSName fsname(mit->first, mit->second);
      fprintf(stdout, "block_id: %"PRI64_PREFIX"u, file_id: %"PRI64_PREFIX"u, file_name: %s\n", mit->first, mit->second, fsname.get_name());
  }

  fprintf(stdout, "********************************* no need to recover block sum: %u *************************************\n",
          static_cast<uint32_t>(no_need_recover_block_list.size()));
  for (uint32_t i = 0; i < no_need_recover_block_list.size(); i++)
  {
    fprintf(stdout, "%"PRI64_PREFIX"u\n", no_need_recover_block_list.at(i));
  }
  fprintf(stdout, "********************** recover process finished **********************\n");

  return 0;
}

