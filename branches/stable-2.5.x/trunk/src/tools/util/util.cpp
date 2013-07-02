/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * * Version: $Id: util.cpp 413 2013-04-18 16:52:46Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei@taobao.com
 *      - initial release
 */
#include<stdio.h>
#include<vector>

#include "common/client_manager.h"
#include "common/new_client.h"
#include "common/status_message.h"
#include "common/internal.h"
#include "common/directory_op.h"
#include "common/new_client.h"
#include "common/client_manager.h"
#include "common/status_message.h"
#include "message/server_status_message.h"
#include "message/block_info_message.h"
#include "message/block_info_message_v2.h"
#include "new_client/tfs_client_impl.h"
#include "new_client/fsname.h"
#include "util.h"
#include "tool_util.h"

using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::client;
using namespace std;

namespace tfs
{
  namespace tools
  {
    int Util::read_file_infos(const std::string& ns_addr, const uint64_t block, std::multiset<std::string>& files, const int32_t version)
    {
      int32_t ret = (INVALID_BLOCK_ID == block) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        std::set<FileInfo, CompareFileInfoByFileId> file_sets;
        ret = read_file_infos(ns_addr, block, file_sets, version);
        if (TFS_SUCCESS == ret)
        {
          std::set<FileInfo, CompareFileInfoByFileId>::const_iterator iter = file_sets.begin();
          for (; iter != file_sets.end(); ++iter)
          {
            FSName fsname(block, (*iter).id_,  TfsClientImpl::Instance()->get_cluster_id(ns_addr.c_str()));
            files.insert(fsname.get_name());
          }
        }
      }
      return ret;
    }

    int Util::read_file_infos(const std::string& ns_addr, const uint64_t block, std::set<FileInfo, CompareFileInfoByFileId>& files, const int32_t version)
    {
      int32_t ret = (INVALID_BLOCK_ID == block) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        std::vector<uint64_t> servers;
        ToolUtil::get_block_ds_list(Func::get_host_ip(ns_addr.c_str()), block, servers);
        ret = servers.empty() ? EXIT_DATASERVER_NOT_FOUND : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          int32_t index = random() % servers.size();
          uint64_t server = servers[index];
          GetServerStatusMessage req_msg;
          req_msg.set_status_type(GSS_BLOCK_FILE_INFO);
          req_msg.set_return_row(block);
          req_msg.set_from_row(block);
          tbnet::Packet* ret_msg= NULL;
          NewClient* client = NewClientManager::get_instance().create_client();
          ret = send_msg_to_server(server, client, &req_msg, ret_msg, 5000);
          if (TFS_SUCCESS == ret)
          {
            if (0 == version)
              ret = ret_msg->getPCode() == BLOCK_FILE_INFO_MESSAGE ? TFS_SUCCESS : EXIT_UNKNOWN_MSGTYPE;
            else if (1 == version)
              ret = ret_msg->getPCode() == BLOCK_FILE_INFO_MESSAGE_V2 ? TFS_SUCCESS : EXIT_UNKNOWN_MSGTYPE;
            else
              ret = EXIT_UNKNOWN_MSGTYPE;
            if (TFS_SUCCESS != ret)
            {
              ret = ret_msg->getPCode() == STATUS_MESSAGE ? EXIT_FILE_INFO_ERROR : EXIT_UNKNOWN_MSGTYPE;
              if (EXIT_FILE_INFO_ERROR == ret)
              {
                StatusMessage* st = dynamic_cast<StatusMessage*>(ret_msg);
                TBSYS_LOG(WARN, "read file infos error: %s", st->get_error());
              }
            }
          }
          if (TFS_SUCCESS == ret)
          {
            if (0 == version)
            {
              BlockFileInfoMessage* bfm = dynamic_cast<BlockFileInfoMessage*>(ret_msg);
              assert(NULL != bfm);
              std::vector<FileInfo>::iterator iter = bfm->get_fileinfo_list().begin();
              for (; iter != bfm->get_fileinfo_list().end(); ++iter)
                files.insert((*iter));
            }
            else if (1 == version)
            {
              BlockFileInfoMessageV2* bfm = dynamic_cast<BlockFileInfoMessageV2*>(ret_msg);
              assert(NULL != bfm);
              std::vector<FileInfoV2>::iterator iter = bfm->get_fileinfo_list()->begin();
              for (; iter != bfm->get_fileinfo_list()->end(); ++iter)
              {
                FileInfo info;
                info.id_ = (*iter).id_;
                info.offset_ = (*iter).offset_;
                info.size_= (*iter).size_;
                info.modify_time_= (*iter).modify_time_;
                info.create_time_= (*iter).create_time_;
                info.flag_ = (*iter).status_;
                info.crc_  = (*iter).crc_;
                files.insert(info);
              }
            }
          }
          NewClientManager::get_instance().destroy_client(client);
        }
      }
      return ret;
    }

    int Util::read_file_info(const std::string& ns_addr, const std::string& filename, FileInfo& info)
    {
      int32_t fd = TfsClientImpl::Instance()->open(filename.c_str(), NULL, ns_addr.c_str(), T_READ);
      int32_t ret = fd < 0 ? fd : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        TfsFileStat stat;
        ret = TfsClientImpl::Instance()->fstat(fd, &stat, FORCE_STAT);
        if (TFS_SUCCESS == ret)
        {
          info.id_ = stat.file_id_;
          info.offset_ = stat.offset_;
          info.size_ = stat.size_;
          info.usize_ = stat.usize_;
          info.modify_time_ = stat.modify_time_;
          info.create_time_ = stat.create_time_;
          info.flag_ = stat.flag_;
          info.crc_ = stat.crc_;
          ret = (stat.flag_ == 1 || stat.flag_ == 4 || stat.flag_ == 5) ? META_FLAG_ABNORMAL : TFS_SUCCESS;
        }
        TfsClientImpl::Instance()->close(fd);
      }
      return ret;
    }

    int Util::read_file_real_crc(const std::string& ns_addr, const std::string& filename, uint32_t& crc)
    {
      crc = 0;
      int32_t fd = TfsClientImpl::Instance()->open(filename.c_str(), NULL, ns_addr.c_str(), T_READ);
      int32_t ret = fd < 0 ? fd : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        int32_t total = 0;
        char data[MAX_READ_SIZE]={'\0'};
        TfsFileStat stat;
        while (true)
        {
          int32_t rlen = TfsClientImpl::Instance()->readv2(fd, data, MAX_READ_SIZE, &stat);
          ret = rlen < 0 ? rlen : TFS_SUCCESS;
          if (TFS_SUCCESS == ret)
          {
            if (0 == rlen)
              break;
            total += rlen;
            crc = Func::crc(crc, data, rlen);
            //Func::hex_dump(data, 10, true, TBSYS_LOG_LEVEL_INFO);
            //TBSYS_LOG(INFO, "FILENAME : %s, READ LENGTH: %d, crc: %u", filename.c_str(), rlen, crc);
          }
        }
        TfsClientImpl::Instance()->close(fd);
      }
      return ret;
    }
  }
}
