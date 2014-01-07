/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *  linqing <linqing.zyd@taobao.com>
 *      - initial release
 *
 */

#include "common/func.h"
#include "common/client_manager.h"
#include "common/base_packet.h"
#include "common/status_message.h"
#include "message/message_factory.h"
#include "clientv2/tfs_client_impl_v2.h"
#include "clientv2/fsname.h"
#include "ns_requester.h"

using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::clientv2;
using namespace std;

namespace tfs
{
  namespace requester
  {
    int NsRequester::get_block_replicas(const uint64_t ns_id, const uint64_t block_id, VUINT64& replicas)
    {
      int ret = INVALID_SERVER_ID != ns_id && INVALID_BLOCK_ID != block_id ?
        TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        replicas.clear();
        BlockMeta meta;
        ret = get_block_replicas_ex(ns_id, block_id, F_FAMILY_INFO_NONE, meta);
        if (TFS_SUCCESS == ret)
        {
          for (int32_t index = 0; index < meta.size_; ++index)
            replicas.push_back(meta.ds_[index]);
        }
      }
      return ret;
    }

    int NsRequester::get_block_replicas(const uint64_t ns_id,
            const uint64_t block_id, common::BlockMeta& meta)
    {
      int ret = INVALID_SERVER_ID != ns_id && INVALID_BLOCK_ID != block_id ?
        TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = get_block_replicas_ex(ns_id, block_id, F_FAMILY_INFO, meta);
      }
      return ret;
    }

    int NsRequester::get_cluster_id(const uint64_t ns_id, int32_t& cluster_id)
    {
      std::string value;
      int ret = get_ns_param(ns_id, "cluster_index", value);
      if (TFS_SUCCESS == ret)
      {
        cluster_id = atoi(value.c_str()) - '0';
      }
      return ret;
    }

    int NsRequester::get_group_count(const uint64_t ns_id, int32_t& group_count)
    {
      std::string value;
      int ret = get_ns_param(ns_id, "group_count", value);
      if (TFS_SUCCESS == ret)
      {
        group_count = atoi(value.c_str());
      }
      return ret;
    }

    int NsRequester::get_group_seq(const uint64_t ns_id, int32_t& group_seq)
    {
      std::string value;
      int ret = get_ns_param(ns_id, "group_seq", value);
      if (TFS_SUCCESS == ret)
      {
        group_seq = atoi(value.c_str());
      }
      return ret;
    }

    int NsRequester::get_ns_param(const uint64_t ns_id, const std::string& key, std::string& value)
    {
      int ret = (INVALID_SERVER_ID != ns_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      int32_t key_index = -1;

      // find key's index in param table
      if (TFS_SUCCESS == ret)
      {
        ret = EXIT_PARAMETER_ERROR;
        int32_t count = sizeof(dynamic_parameter_str) / sizeof(dynamic_parameter_str[0]);
        for (int i = 0; i < count; i++)
        {
          if (!strcmp(key.c_str(), dynamic_parameter_str[i]))
          {
            key_index = i + 1;
            ret = TFS_SUCCESS;
            break;
          }
        }
      }

      if (TFS_SUCCESS == ret)
      {
        ClientCmdMessage msg;
        msg.set_cmd(CLIENT_CMD_SET_PARAM);
        msg.set_value3(key_index);
        msg.set_value4(0);

        tbnet::Packet* resp_msg = NULL;
        NewClient* client = NewClientManager::get_instance().create_client();
        ret = (NULL != client) ? TFS_SUCCESS : EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = send_msg_to_server(ns_id, client, &msg, resp_msg);
        }

        if (TFS_SUCCESS == ret)
        {
          if (STATUS_MESSAGE == resp_msg->getPCode())
          {
            StatusMessage* smsg = dynamic_cast<StatusMessage*>(resp_msg);
            ret = smsg->get_status();
            if (STATUS_MESSAGE_OK == ret)
            {
              value = smsg->get_error();
            }
          }
          else
          {
            ret = EXIT_UNKNOWN_MSGTYPE;
          }
        }

        NewClientManager::get_instance().destroy_client(client);
      }

      return ret;
    }

    ServerStat::ServerStat():
      id_(0), use_capacity_(0), total_capacity_(0), current_load_(0), block_count_(0),
      last_update_time_(0), startup_time_(0), current_time_(0)
    {
      memset(&total_tp_, 0, sizeof(total_tp_));
      memset(&last_tp_, 0, sizeof(last_tp_));
    }

    ServerStat::~ServerStat()
    {
    }

    int ServerStat::deserialize(tbnet::DataBuffer& input, const int32_t length, int32_t& offset)
    {
      if (input.getDataLen() <= 0 || offset >= length)
      {
        return TFS_ERROR;
      }
      int32_t len = input.getDataLen();
      id_ = input.readInt64();
      use_capacity_ = input.readInt64();
      total_capacity_ = input.readInt64();
      current_load_ = input.readInt32();
      block_count_  = input.readInt32();
      last_update_time_ = input.readInt64();
      startup_time_ = input.readInt64();
      total_tp_.write_byte_ = input.readInt64();
      total_tp_.read_byte_ = input.readInt64();
      total_tp_.write_file_count_ = input.readInt64();
      total_tp_.read_file_count_ = input.readInt64();
      total_tp_.unlink_file_count_ = input.readInt64();
      total_tp_.fail_write_byte_ = input.readInt64();
      total_tp_.fail_read_byte_ = input.readInt64();
      total_tp_.fail_write_file_count_ = input.readInt64();
      total_tp_.fail_read_file_count_ = input.readInt64();
      total_tp_.fail_unlink_file_count_ = input.readInt64();
      current_time_ = input.readInt64();
      status_ = (DataServerLiveStatus)input.readInt32();
      offset += (len - input.getDataLen());

      return TFS_SUCCESS;
    }

    int NsRequester::get_ds_list(const uint64_t ns_id, common::VUINT64& ds_list)
    {
      ds_list.clear();
      int ret = TFS_SUCCESS;
      ShowServerInformationMessage msg;
      SSMScanParameter& param = msg.get_param();
      param.type_ = SSM_TYPE_SERVER;
      param.child_type_ = SSM_CHILD_SERVER_TYPE_INFO;
      param.start_next_position_ = 0x0;
      param.should_actual_count_= (100 << 16);  // get 100 ds every turn
      param.end_flag_ = SSM_SCAN_CUTOVER_FLAG_YES;

      while (TFS_SUCCESS == ret && !((param.end_flag_ >> 4) & SSM_SCAN_END_FLAG_YES))
      {
        param.data_.clear();
        tbnet::Packet* ret_msg = NULL;
        NewClient* client = NewClientManager::get_instance().create_client();
        ret = send_msg_to_server(ns_id, client, &msg, ret_msg);
        if (TFS_SUCCESS == ret)
        {
          if (ret_msg->getPCode() != SHOW_SERVER_INFORMATION_MESSAGE)
          {
            if (ret_msg->getPCode() == STATUS_MESSAGE)
            {
              ret = dynamic_cast<StatusMessage*>(ret_msg)->get_status();
            }
            else
            {
              ret = EXIT_UNKNOWN_MSGTYPE;
            }
          }
        }

        if (TFS_SUCCESS == ret)
        {
          ShowServerInformationMessage* message = dynamic_cast<ShowServerInformationMessage*>(ret_msg);
          SSMScanParameter& ret_param = message->get_param();

          int32_t data_len = ret_param.data_.getDataLen();
          int32_t offset = 0;
          while (data_len > offset)
          {
            ServerStat server;
            if (TFS_SUCCESS == server.deserialize(ret_param.data_, data_len, offset))
            {
              ds_list.push_back(server.id_);
              std::string ip_port = Func::addr_to_str(server.id_, true);
            }
          }
          param.addition_param1_ = ret_param.addition_param1_;
          param.addition_param2_ = ret_param.addition_param2_;
          param.end_flag_ = ret_param.end_flag_;
        }
        NewClientManager::get_instance().destroy_client(client);
      }

      return ret;
    }

    int NsRequester::read_file_infos(const std::string& ns_addr, const uint64_t block, std::multiset<std::string>& files, const int32_t version)
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
            FSName fsname(block, (*iter).id_,  TfsClientImplV2::Instance()->get_cluster_id());
            files.insert(fsname.get_name());
          }
        }
      }
      return ret;
    }

    int NsRequester::read_file_infos(const std::string& ns_addr, const uint64_t block, std::set<FileInfo, CompareFileInfoByFileId>& files, const int32_t version)
    {
      int32_t ret = (INVALID_BLOCK_ID == block) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        std::vector<uint64_t> servers;
        get_block_replicas(Func::get_host_ip(ns_addr.c_str()), block, servers);
        ret = servers.empty() ? EXIT_DATASERVER_NOT_FOUND : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          int32_t index = random() % servers.size();
          uint64_t server = servers[index];
          GetServerStatusMessage req_msg;
          if (0 == version)
          {
            req_msg.set_status_type(GSS_BLOCK_FILE_INFO);
          }
          else if (1 == version)
          {
            req_msg.set_status_type(GSS_BLOCK_FILE_INFO_V2);
          }
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

    int NsRequester::read_file_info(const std::string& ns_addr, const std::string& filename, FileInfo& info)
    {
      int32_t fd = TfsClientImplV2::Instance()->open(filename.c_str(), NULL, ns_addr.c_str(), T_READ);
      int32_t ret = fd < 0 ? fd : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        TfsFileStat stat;
        ret = TfsClientImplV2::Instance()->set_option_flag(fd, FORCE_STAT);
        if (TFS_SUCCESS == ret)
        {
          ret = TfsClientImplV2::Instance()->fstat(fd, &stat);
        }
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
        TfsClientImplV2::Instance()->close(fd);
      }
      return ret;
    }

    int NsRequester::read_file_real_crc(const std::string& ns_addr, const std::string& filename, uint32_t& crc)
    {
      crc = 0;
      int32_t fd = TfsClientImplV2::Instance()->open(filename.c_str(), NULL, ns_addr.c_str(), T_READ);
      int32_t ret = fd < 0 ? fd : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        int32_t total = 0;
        char data[MAX_READ_SIZE]={'\0'};
        TfsFileStat stat;
        while (true)
        {
          int32_t rlen = TfsClientImplV2::Instance()->readv2(fd, data, MAX_READ_SIZE, &stat);
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
          else
          {
            break;
          }
        }
        TfsClientImplV2::Instance()->close(fd);
      }
      return ret;
    }

    int NsRequester::read_file_info_v2(const std::string& ns_addr, const std::string& filename, FileInfoV2& info)
    {
      TfsFileStat stat;
      memset(&info, 0, sizeof(info));
      memset(&stat, 0, sizeof(stat));
      int32_t ret = TfsClientImplV2::Instance()->stat_file(&stat, filename.c_str(), NULL, FORCE_STAT, ns_addr.c_str());
      if (TFS_SUCCESS == ret)
        FileStatToFileInfoV2(stat, info);
      else
        TBSYS_LOG(INFO, "stat file %s fail, ns addr:%s, ret:%d", filename.c_str(), ns_addr.c_str(), ret);
      return ret;
    }

    int NsRequester::read_file_stat(const std::string& ns_addr, const std::string& filename, common::TfsFileStat& info)
    {
      int32_t ret = TfsClientImplV2::Instance()->stat_file(&info, filename.c_str(), NULL, FORCE_STAT, ns_addr.c_str());
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "stat file:%s fail, ns:%s, ret:%d", filename.c_str(), ns_addr.c_str(), ret);
      }
      return ret;
    }

    int NsRequester::read_file_real_crc_v2(const std::string& ns_addr, const std::string& filename, common::FileInfoV2& info, const bool force)
    {
      info.crc_ = 0;
      int32_t fd = TfsClientImplV2::Instance()->open(filename.c_str(), NULL, ns_addr.c_str(), T_READ);
      int32_t ret = fd < 0 ? fd : TFS_SUCCESS;
      if (TFS_SUCCESS == ret && force)
      {
        ret = TfsClientImplV2::Instance()->set_option_flag(fd, READ_DATA_OPTION_FLAG_FORCE);
      }
      if (TFS_SUCCESS == ret)
      {
        int32_t total = 0;
        char data[MAX_READ_SIZE]={'\0'};
        TfsFileStat stat;
        while (true)
        {
          int32_t rlen = TfsClientImplV2::Instance()->readv2(fd, data, MAX_READ_SIZE, &stat);
          ret = rlen < 0 ? rlen : TFS_SUCCESS;
          if (TFS_SUCCESS == ret)
          {
            if (0 == total)
            {
              FileStatToFileInfoV2(stat, info);
              info.crc_ = 0;
            }
            total += rlen;
            info.crc_ = Func::crc(info.crc_, data, rlen);
            //Func::hex_dump(data, 10, true, TBSYS_LOG_LEVEL_INFO);
            //TBSYS_LOG(INFO, "FILENAME : %s, READ LENGTH: %d, crc: %u", filename.c_str(), rlen, crc);
            if(rlen < MAX_READ_SIZE)
              break;
          }
          else
          {
            TBSYS_LOG(ERROR, "read file fail, filename:%s, ns:%s, ret:%d", filename.c_str(), ns_addr.c_str(), ret);
            break;
          }
        }
        TfsClientImplV2::Instance()->close(fd);
      }
      return ret;
    }

    int NsRequester::write_file(const std::string& ns_addr, const std::string& filename, const char* data, const int32_t size, const int32_t status)
    {
      int32_t fd = -1;
      int32_t ret = (!ns_addr.empty() && !filename.empty()  && NULL != data && size > 0) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        fd = TfsClientImplV2::Instance()->open(filename.c_str(), NULL, ns_addr.c_str(), T_WRITE | T_NEWBLK);
        ret = fd < 0 ? fd : TFS_SUCCESS;
      }
      if (TFS_SUCCESS == ret)
      {
        ret = TfsClientImplV2::Instance()->set_option_flag(fd, TFS_FILE_NO_SYNC_LOG);
      }
      if (TFS_SUCCESS == ret)
      {
        int64_t wlen = TfsClientImplV2::Instance()->write(fd, data, size);
        ret = (wlen == size) ? TFS_SUCCESS : EXIT_WRITE_FILE_ERROR;
      }

      if (fd > 0)
      {
        int32_t result = TfsClientImplV2::Instance()->close(fd, NULL, 0, status);
        if (TFS_SUCCESS != result)
        {
          TBSYS_LOG(INFO, "close file %s failed, ret: %d, ns_addr: %s", filename.c_str(), ret, ns_addr.c_str());
        }
      }
      return ret;
    }

    int NsRequester::copy_file(const string& src_ns_addr, const string& dest_ns_addr, const string& file_name, const int32_t status)
    {
      int32_t ret = (!src_ns_addr.empty() && !dest_ns_addr.empty() && !file_name.empty()) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        int32_t source_fd = -1, dest_fd = -1;
        source_fd = TfsClientImplV2::Instance()->open(file_name.c_str(), NULL, src_ns_addr.c_str(), T_READ | T_FORCE);
        if (source_fd < 0)
        {
          ret = source_fd;
          TBSYS_LOG(INFO, "open source %s fail when copy file, ns_addr: %s ret:%d", file_name.c_str(), src_ns_addr.c_str(), ret);
        }
        if (TFS_SUCCESS == ret)
        {
          dest_fd = TfsClientImplV2::Instance()->open(file_name.c_str(), NULL, dest_ns_addr.c_str(), T_WRITE | T_NEWBLK);
          if (dest_fd < 0)
          {
            ret = dest_fd;
            TBSYS_LOG(INFO, "open dest %s fail when copy file, ns_addr: %s ret:%d", file_name.c_str(), dest_ns_addr.c_str(), ret);
          }
        }
        if (TFS_SUCCESS == ret)
        {
          ret = TfsClientImplV2::Instance()->set_option_flag(dest_fd, TFS_FILE_NO_SYNC_LOG);
        }
        if (TFS_SUCCESS == ret)
        {
          int32_t rlen = 0, wlen = 0;
          const int32_t MAX_READ_DATA_SIZE = 4 * 1024 * 1024;
          char data[MAX_READ_DATA_SIZE];
          for (;;)
          {
            rlen = TfsClientImplV2::Instance()->read(source_fd, data, MAX_READ_DATA_SIZE);
            if (rlen < 0)
            {
              ret = rlen;
              TBSYS_LOG(INFO, "read %s fail when copy file, ns_addr: %s ret:%d", file_name.c_str(), src_ns_addr.c_str(), ret);
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
              TBSYS_LOG(INFO, "write %s fail when copy file, ns_addr: %s ret:%d", file_name.c_str(), dest_ns_addr.c_str(), ret);
              break;
            }

            if (rlen < MAX_READ_DATA_SIZE)
            {
              break;
            }
          }
        }

        if (source_fd > 0)
        {
          TfsClientImplV2::Instance()->close(source_fd);
        }
        if (dest_fd > 0)
        {
          ret = TfsClientImplV2::Instance()->close(dest_fd, NULL, 0, status);
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(INFO, "close %s fail when copy file, ns_addr: %s ret:%d", file_name.c_str(), dest_ns_addr.c_str(), ret);
          }
        }
      }
      return ret;
    }

    int NsRequester::sync_file(const std::string& saddr, const std::string& daddr, const std::string& filename,
      const common::FileInfoV2& sfinfo, const common::FileInfoV2& dfinfo, const int64_t timestamp, const bool force)
    {
      //1. 首先处理MODIFY TIME， 只有符合MODIFY TIME才进行处理
      //2. 处理强制标记，设置了强制标记的不需要关注源和目标的任何信息直接同步
      //3. 处理CRC，SIZE不一致的情况(包含了目标不存在的情况)
      //4. 处理状态不一致的情况
      int32_t ret = (!saddr.empty() && !daddr.empty() && !filename.empty()) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = dfinfo.modify_time_ < timestamp ? TFS_SUCCESS : EXIT_SYNC_FILE_NOTHING;
        if (TFS_SUCCESS == ret)
        {
          if (force)//如果强制同步标记被设置，不需要关注源和目标的任何状态直接同步
          {
            ret = copy_file(saddr, daddr, filename, sfinfo.status_);
          }
          else
          {
            if (dfinfo.size_ != sfinfo.size_ || dfinfo.crc_ != sfinfo.crc_)
            {
              if (common::INVALID_FILE_ID != dfinfo.id_)
              {
                TBSYS_LOG(WARN, "%s size or crc conflict! fileid: %"PRI64_PREFIX"u <> %"PRI64_PREFIX"u, size: %d <> %d , crc %u <> %u , status: %d <> %d",
                  filename.c_str(), sfinfo.id_, dfinfo.id_, sfinfo.size_, dfinfo.size_, sfinfo.crc_, dfinfo.crc_, sfinfo.status_, dfinfo.status_);
              }
              ret = (0 == (sfinfo.status_ & FILE_STATUS_DELETE)) ? TFS_SUCCESS : EXIT_SYNC_FILE_NOTHING;
              if (TFS_SUCCESS == ret)
                ret = copy_file(saddr, daddr, filename, sfinfo.status_);
              else
                TBSYS_LOG(WARN, "ignore filename: %s althought it is not exist in %s, its status is 'DELETE' in %s , unlink is set 'false', ret: %d",
                    filename.c_str(), daddr.c_str(), saddr.c_str(), ret);
            }
            if (dfinfo.status_ != sfinfo.status_
              && common::INVALID_FILE_ID != dfinfo.id_)
            {
              TBSYS_LOG(WARN, "%s status conflict! size: %d <> %d , crc %u <> %u , status: %d <> %d , create_time: %s <> %s , modify_time: %s <> %s",
                  filename.c_str(), sfinfo.size_, dfinfo.size_, sfinfo.crc_, dfinfo.crc_, sfinfo.status_, dfinfo.status_,
                  Func::time_to_str(sfinfo.create_time_).c_str(), Func::time_to_str(dfinfo.create_time_).c_str(),
                  Func::time_to_str(sfinfo.modify_time_).c_str(), Func::time_to_str(dfinfo.modify_time_).c_str());
              int64_t file_size = 0;
              int32_t override_action = 0;
              SET_OVERRIDE_FLAG(override_action, sfinfo.status_);
              ret = TfsClientImplV2::Instance()->unlink(file_size, filename.c_str(), NULL,
                  static_cast<TfsUnlinkType>(override_action), daddr.c_str(), TFS_FILE_NO_SYNC_LOG);
              if (TFS_SUCCESS != ret)
              {
                TBSYS_LOG(WARN, "sync %s status %s, ret: %d, size: %d <> %d , crc %u <> %u , status: %d <> %d , create_time: %s <> %s , modify_time: %s <> %s",
                    filename.c_str(), TFS_SUCCESS == ret ? "successful" : "failed", ret, sfinfo.size_, dfinfo.size_, sfinfo.crc_, dfinfo.crc_, sfinfo.status_, dfinfo.status_,
                    Func::time_to_str(sfinfo.create_time_).c_str(), Func::time_to_str(dfinfo.create_time_).c_str(),
                    Func::time_to_str(sfinfo.modify_time_).c_str(), Func::time_to_str(dfinfo.modify_time_).c_str());
              }
            }
          }
        }
        else
        {
          TBSYS_LOG(WARN, "dest file %s has been modifyed, do nothing %s > %s, ret: %d",
              filename.c_str(), Func::time_to_str(dfinfo.modify_time_).c_str(), Func::time_to_str(timestamp).c_str(), ret);
        }
      }
      return ret;
    }

    int NsRequester::cmp_and_sync_file(const std::string& src_ns_addr, const std::string& dest_ns_addr, const std::string& file_name,
        const int64_t timestamp, const bool force, common::FileInfoV2& left, common::FileInfoV2& right)
    {
      memset(&left, 0, sizeof(left));
      memset(&right, 0, sizeof(right));
      int32_t ret = (!src_ns_addr.empty() && !dest_ns_addr.empty() && !file_name.empty()) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret =read_file_info_v2(src_ns_addr, file_name, left);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "read %s file info fail from %s, ret: %d", file_name.c_str(), src_ns_addr.c_str(), ret);
        }
      }
      if (TFS_SUCCESS == ret)
      {
        ret =read_file_info_v2(dest_ns_addr, file_name, right);
        if (EXIT_BLOCK_NOT_FOUND == ret || EXIT_NO_DATASERVER == ret || EXIT_META_NOT_FOUND_ERROR == ret)
        {
          ret = TFS_SUCCESS;
        }
      }
      if (TFS_SUCCESS == ret)
      {
        ret = sync_file(src_ns_addr, dest_ns_addr, file_name, left, right, timestamp, force);
      }
      return ret;
    }

    int NsRequester::remove_block(const uint64_t block, const std::string& addr, const int32_t flag)
    {
      ClientCmdMessage req;
      req.set_cmd(CLIENT_CMD_EXPBLK);
      req.set_value3(block);
      req.set_value4(flag);

      tbnet::Packet* rsp = NULL;
      NewClient* client = NewClientManager::get_instance().create_client();
      int32_t ret = send_msg_to_server(Func::get_host_ip(addr.c_str()), client, &req, rsp);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(INFO, "send remove block: %"PRI64_PREFIX"u command to %s failed,ret: %d", block, addr.c_str(), ret);
      }
      else
      {
        assert(NULL != rsp);
        if (STATUS_MESSAGE == rsp->getPCode())
        {
          StatusMessage* sm = dynamic_cast<StatusMessage*>(rsp);
          if (STATUS_MESSAGE_OK == sm->get_status())
          {
            TBSYS_LOG(INFO, "remove block: %"PRI64_PREFIX"u from %s successful", block, addr.c_str());
          }
          else
          {
            ret = sm->get_status();
            TBSYS_LOG(INFO, "remove block: %"PRI64_PREFIX"u from %s fail, ret: %d", block, addr.c_str(), ret);
          }
        }
        else
        {
          ret = EXIT_UNKNOWN_MSGTYPE;
          TBSYS_LOG(INFO, "remove block: %"PRI64_PREFIX"u from %s fail, ret: %d", block, addr.c_str(), ret);
        }
      }
      NewClientManager::get_instance().destroy_client(client);
      return ret;
    }

    int NsRequester::get_block_replicas_ex(const uint64_t ns_id,
            const uint64_t block_id, const int32_t flag, common::BlockMeta& meta)
    {
      int ret = INVALID_SERVER_ID != ns_id && INVALID_BLOCK_ID != block_id ?
        TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        GetBlockInfoMessageV2 gbi_message;
        gbi_message.set_block_id(block_id);
        gbi_message.set_mode(T_READ);
        gbi_message.set_flag(flag);

        tbnet::Packet* rsp = NULL;
        NewClient* client = NewClientManager::get_instance().create_client();
        ret = (NULL != client) ? TFS_SUCCESS : EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = send_msg_to_server(ns_id, client, &gbi_message, rsp);
        }

        if (TFS_SUCCESS == ret)
        {
          if (rsp->getPCode() == GET_BLOCK_INFO_RESP_MESSAGE_V2)
          {
            GetBlockInfoRespMessageV2* msg = dynamic_cast<GetBlockInfoRespMessageV2* >(rsp);
            meta = msg->get_block_meta();
          }
          else if (rsp->getPCode() == STATUS_MESSAGE)
          {
            ret = dynamic_cast<StatusMessage*>(rsp)->get_status();
          }
          else
          {
            ret = EXIT_UNKNOWN_MSGTYPE;
          }
        }
        NewClientManager::get_instance().destroy_client(client);
      }
      return ret;
    }
  }
}

